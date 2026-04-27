"""
evaluator.py - Tier 1 Conversion Engine (with audit logging)

Validates FAME formulas and generates Polars-based Python code,
then writes audit logs to Fabric SQL endpoint.
"""

import os
import json
import textwrap
import struct
import logging
from uuid import uuid4
from datetime import datetime, timezone
from typing import Optional

import pyodbc
from azure.identity import ClientSecretCredential
from fastapi import APIRouter
from pydantic import BaseModel

from fame2pygen.formulas_generator import (
    parse_fame_formula,
    render_polars_expr,
    sanitize_func_name,
)

router = APIRouter()
logger = logging.getLogger(__name__)

SQL_COPT_SS_ACCESS_TOKEN = 1256


class EvaluateRequest(BaseModel):
    fame_code: str
    tier: Optional[int] = 1
    model_hint: Optional[str] = None
    created_by: Optional[str] = "evaluate_fame"


class EvaluateResponse(BaseModel):
    run_id: str
    tier: int | None = None
    confidence: str
    python_code: str | None = None


def _check_confidence(fame_code: str) -> str:
    complex_keywords = ["dateof", "make", "contain", "ending", "beginning"]
    if any(kw in fame_code.lower() for kw in complex_keywords):
        return "low"
    return "high"


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


def _get_conn() -> pyodbc.Connection:
    # Required env vars
    server = os.environ["FABRIC_SQL_SERVER"]       # host only, no tcp:, no ,1433
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
    confidence: str,
    model_used: Optional[str],
    reason_codes: Optional[list[str]],
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

    # Your table uses VARCHAR(8000) in several columns
    fame_code_safe = (fame_code or "")[:8000]
    python_code_safe = (python_code or "")[:8000] if python_code else None
    reason_codes_safe = json.dumps(reason_codes)[:8000] if reason_codes else None

    confidence_score = 1.0 if confidence == "high" else 0.25

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
            created_by[:100] if created_by else "evaluate_fame",
            created_utc,
        )
        conn.commit()
        cur.close()


@router.post("/evaluate_fame", response_model=EvaluateResponse)
def evaluate_fame(request: EvaluateRequest) -> EvaluateResponse:
    run_id = f"eval-{uuid4()}"
    logger.info("evaluate_fame_start run_id=%s", run_id)

    confidence = _check_confidence(request.fame_code)

    if confidence == "low":
        try:
            _write_audit(
                run_id=run_id,
                fame_code=request.fame_code,
                python_code=None,
                tier=request.tier,
                status="low_confidence",
                confidence="low",
                model_used=request.model_hint,
                reason_codes=["contains_complex_keyword"],
                created_by=request.created_by or "evaluate_fame",
            )
        except Exception as e:
            logger.exception(
                "audit_write_failed run_id=%s status=low_confidence error=%s",
                run_id,
                str(e),
            )

        logger.info("evaluate_fame_low_confidence run_id=%s", run_id)
        return EvaluateResponse(run_id=run_id, tier=request.tier, confidence="low")

    try:
        parsed = parse_fame_formula(request.fame_code)

        if not parsed:
            try:
                _write_audit(
                    run_id=run_id,
                    fame_code=request.fame_code,
                    python_code=None,
                    tier=request.tier,
                    status="parse_failed",
                    confidence="low",
                    model_used=request.model_hint,
                    reason_codes=["parse_returned_none"],
                    created_by=request.created_by or "evaluate_fame",
                )
            except Exception as e:
                logger.exception(
                    "audit_write_failed run_id=%s status=parse_failed error=%s",
                    run_id,
                    str(e),
                )
            return EvaluateResponse(run_id=run_id, tier=request.tier, confidence="low")

        polars_code = render_polars_expr(parsed["rhs"])
        final_code = _generate_vault_template(
            target=parsed["target"],
            refs=parsed["refs"],
            polars_expr=polars_code,
        )

        try:
            _write_audit(
                run_id=run_id,
                fame_code=request.fame_code,
                python_code=final_code,
                tier=request.tier or 1,
                status="success",
                confidence="high",
                model_used=request.model_hint,
                reason_codes=["tier1_conversion"],
                created_by=request.created_by or "evaluate_fame",
            )
        except Exception as e:
            logger.exception(
                "audit_write_failed run_id=%s status=success error=%s",
                run_id,
                str(e),
            )

        logger.info("evaluate_fame_success run_id=%s tier=%s", run_id, request.tier or 1)
        return EvaluateResponse(
            run_id=run_id,
            tier=request.tier or 1,
            confidence="high",
            python_code=final_code,
        )

    except Exception as e:
        logger.exception("evaluate_fame_exception run_id=%s error=%s", run_id, str(e))

        try:
            _write_audit(
                run_id=run_id,
                fame_code=request.fame_code,
                python_code=None,
                tier=request.tier,
                status="exception",
                confidence="low",
                model_used=request.model_hint,
                reason_codes=["evaluate_exception"],
                created_by=request.created_by or "evaluate_fame",
            )
        except Exception as audit_e:
            logger.exception(
                "audit_write_failed run_id=%s status=exception error=%s",
                run_id,
                str(audit_e),
            )

        return EvaluateResponse(run_id=run_id, tier=request.tier, confidence="low")
