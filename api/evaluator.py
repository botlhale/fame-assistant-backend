"""
evaluator.py - Tier 1 Conversion Engine (Copilot-friendly contract + audit logging)

- Strict, predictable JSON response contract for orchestrators/Copilot.
- Tier-2 handoff signal when confidence is low or on error.
- Fabric SQL audit logging with Entra SP token auth.
"""

import os
import json
import textwrap
import struct
import logging
from uuid import uuid4
from datetime import datetime, timezone
from typing import Optional, Literal

import pyodbc
from azure.identity import ClientSecretCredential
from fastapi import APIRouter
from pydantic import BaseModel, Field

from fame2pygen.formulas_generator import (
    parse_fame_formula,
    render_polars_expr,
    sanitize_func_name,
)

router = APIRouter()
logger = logging.getLogger(__name__)

SQL_COPT_SS_ACCESS_TOKEN = 1256


# -----------------------------
# Request / Response Contracts
# -----------------------------
class EvaluateRequest(BaseModel):
    fame_code: str
    tier: Optional[int] = 1
    model_hint: Optional[str] = None
    created_by: Optional[str] = "evaluate_fame"


class EvaluateResult(BaseModel):
    python_code: Optional[str] = None
    explanations: Optional[list[str]] = None


class EvaluateError(BaseModel):
    code: Optional[str] = None
    message: Optional[str] = None


class EvaluateResponse(BaseModel):
    status: Literal["success", "low_confidence", "error"]
    run_id: str
    result: EvaluateResult
    next_action: Literal["none", "route_to_tier2"]
    reason_codes: list[str] = Field(default_factory=list)
    error: Optional[EvaluateError] = None


# -----------------------------
# Core logic helpers
# -----------------------------
def _check_confidence(fame_code: str) -> tuple[str, list[str]]:
    complex_keywords = ["dateof", "make", "contain", "ending", "beginning"]
    found = [kw for kw in complex_keywords if kw in fame_code.lower()]
    if found:
        return "low", ["contains_complex_keyword"]
    return "high", ["tier1_candidate"]


def _generate_vault_template(target: str, refs: list[str], polars_expr: str) -> str:
    load_blocks = "\n".join(
        [f'    "{sanitize_func_name(ref).upper()}": store["{ref.upper()}"],' for ref in refs]
    )

    target_sanitized = sanitize_func_name(target).upper()

    template = f"""
import polars as pl
from seriesvault import ParquetStore

# Initialize the vault
store = ParquetStore("path/to/vault")

# 1. Load dependencies from vault into a Polars DataFrame
df = pl.DataFrame({{
{load_blocks}
}})

# 2. Execute the FAME-equivalent Polars logic
df = df.with_columns([
    ({polars_expr}).alias("{target_sanitized}")
])

# 3. Save the result back to the vault
store["{target.upper()}"] = df.select(["DATE", "{target_sanitized}"])
    """
    return textwrap.dedent(template).strip()


# -----------------------------
# Fabric SQL audit logging
# -----------------------------
def _get_conn() -> pyodbc.Connection:
    server = os.environ["FABRIC_SQL_SERVER"]  # host only
    database = os.environ["FABRIC_SQL_DATABASE"]
    tenant_id = os.environ["FABRIC_TENANT_ID"]
    client_id = os.environ["FABRIC_CLIENT_ID"]
    client_secret = os.environ["FABRIC_CLIENT_SECRET"]

    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )

    token = credential.get_token("https://database.windows.net/.default").token
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})


def _write_audit(
    run_id: str,
    fame_code: str,
    python_code: Optional[str],
    tier: Optional[int],
    status: str,
    confidence_score: float,
    model_used: Optional[str],
    reason_codes: list[str],
    created_by: str,
) -> None:
    schema = os.getenv("FABRIC_SQL_SCHEMA", "dbo")
    table = os.getenv("FABRIC_SQL_TABLE", "conversion_audit")
    created_utc = datetime.now(timezone.utc)

    insert_sql = f"""
    INSERT INTO {schema}.{table}
    (run_id, fame_code, python_code, tier, status, confidence_score, model_used, reason_codes, created_by, created_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    fame_code_safe = (fame_code or "")[:8000]
    python_code_safe = (python_code or "")[:8000] if python_code else None
    reason_codes_safe = json.dumps(reason_codes)[:8000] if reason_codes else "[]"
    created_by_safe = (created_by or "evaluate_fame")[:100]

    with _get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            insert_sql,
            run_id,
            fame_code_safe,
            python_code_safe,
            tier,
            status,
            confidence_score,
            model_used,
            reason_codes_safe,
            created_by_safe,
            created_utc,
        )
        conn.commit()
        cur.close()


def _safe_audit_write(**kwargs) -> None:
    try:
        _write_audit(**kwargs)
    except Exception as e:
        logger.exception(
            "audit_write_failed run_id=%s status=%s error=%s",
            kwargs.get("run_id"),
            kwargs.get("status"),
            str(e),
        )


# -----------------------------
# API endpoint
# -----------------------------
@router.post("/evaluate_fame", response_model=EvaluateResponse)
def evaluate_fame(request: EvaluateRequest) -> EvaluateResponse:
    run_id = f"eval-{uuid4()}"
    logger.info("evaluate_fame_start run_id=%s", run_id)

    confidence, initial_reasons = _check_confidence(request.fame_code)

    # Low-confidence direct handoff path
    if confidence == "low":
        reason_codes = initial_reasons
        response = EvaluateResponse(
            status="low_confidence",
            run_id=run_id,
            result=EvaluateResult(
                python_code=None,
                explanations=["Tier 1 confidence is low; route to Tier 2."],
            ),
            next_action="route_to_tier2",
            reason_codes=reason_codes,
            error=None,
        )

        _safe_audit_write(
            run_id=run_id,
            fame_code=request.fame_code,
            python_code=None,
            tier=request.tier,
            status=response.status,
            confidence_score=0.25,
            model_used=request.model_hint,
            reason_codes=reason_codes,
            created_by=request.created_by or "evaluate_fame",
        )

        logger.info("evaluate_fame_low_confidence run_id=%s", run_id)
        return response

    # High-confidence conversion attempt
    try:
        parsed = parse_fame_formula(request.fame_code)
        if not parsed:
            reason_codes = ["parse_returned_none"]
            response = EvaluateResponse(
                status="low_confidence",
                run_id=run_id,
                result=EvaluateResult(
                    python_code=None,
                    explanations=["Parser could not confidently parse formula; route to Tier 2."],
                ),
                next_action="route_to_tier2",
                reason_codes=reason_codes,
                error=None,
            )

            _safe_audit_write(
                run_id=run_id,
                fame_code=request.fame_code,
                python_code=None,
                tier=request.tier,
                status=response.status,
                confidence_score=0.25,
                model_used=request.model_hint,
                reason_codes=reason_codes,
                created_by=request.created_by or "evaluate_fame",
            )
            return response

        polars_code = render_polars_expr(parsed["rhs"])
        final_code = _generate_vault_template(
            target=parsed["target"],
            refs=parsed["refs"],
            polars_expr=polars_code,
        )

        reason_codes = ["tier1_conversion"]
        response = EvaluateResponse(
            status="success",
            run_id=run_id,
            result=EvaluateResult(
                python_code=final_code,
                explanations=["Converted by Tier 1 deterministic parser."],
            ),
            next_action="none",
            reason_codes=reason_codes,
            error=None,
        )

        _safe_audit_write(
            run_id=run_id,
            fame_code=request.fame_code,
            python_code=final_code,
            tier=request.tier or 1,
            status=response.status,
            confidence_score=1.0,
            model_used=request.model_hint,
            reason_codes=reason_codes,
            created_by=request.created_by or "evaluate_fame",
        )

        logger.info("evaluate_fame_success run_id=%s", run_id)
        return response

    except Exception as e:
        logger.exception("evaluate_fame_exception run_id=%s error=%s", run_id, str(e))

        reason_codes = ["evaluate_exception"]
        response = EvaluateResponse(
            status="error",
            run_id=run_id,
            result=EvaluateResult(
                python_code=None,
                explanations=["Tier 1 evaluation failed unexpectedly."],
            ),
            next_action="route_to_tier2",
            reason_codes=reason_codes,
            error=EvaluateError(
                code="E_EVALUATE_001",
                message="Evaluation failed in Tier 1.",
            ),
        )

        _safe_audit_write(
            run_id=run_id,
            fame_code=request.fame_code,
            python_code=None,
            tier=request.tier,
            status=response.status,
            confidence_score=0.0,
            model_used=request.model_hint,
            reason_codes=reason_codes,
            created_by=request.created_by or "evaluate_fame",
        )

        return response
