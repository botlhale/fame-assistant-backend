"""
evaluator.py - Tier 1 Conversion Engine (with audit logging)

Logic for validating FAME formulas and generating Polars-based Python code
integrated with the seriesvault storage layer + Fabric SQL audit logging.
"""

import os
import re
import json
import textwrap
import struct
from uuid import uuid4
from datetime import datetime, timezone
from typing import Optional
import logging

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


class EvaluateResponse(BaseModel):
    run_id: str
    tier: int | None = None
    confidence: str
    python_code: str | None = None


def _check_confidence(fame_code: str) -> str:
    """Determines if a FAME script can be reliably converted by Tier 1 logic."""
    complex_keywords = ["dateof", "make", "contain", "ending", "beginning"]
    if any(kw in fame_code.lower() for kw in complex_keywords):
        return "low"
    return "high"


def _generate_vault_template(target: str, refs: list[str], polars_expr: str) -> str:
    """
    Wraps a Polars expression in a seriesvault ParquetStore block.
    Corrected to use dictionary-style access and sanitization.
    """
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
    server = os.environ["FABRIC_SQL_SERVER"]
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
    model_used: Optional[str] = None,
    reason_codes: Optional[list[str]] = None,
    created_by: str = "evaluate_fame",
) -> None:
    schema = os.getenv("FABRIC_SQL_SCHEMA", "dbo")
    table = os.getenv("FABRIC_SQL_TABLE", "conversion_audit")
    created_utc = datetime.now(timezone.utc)

    insert_sql = f"""
    INSERT INTO {schema}.{table}
    (run_id, fame_code, python_code, tier, status, confidence_score, model_used, reason_codes, created_by, created_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # keep within VARCHAR(8000) table limits you created
    fame_code_safe = (fame_code or "")[:8000]
    python_code_safe = (python_code or "")[:8000]
    reason_codes_safe = json.dumps(reason_codes)[:8000] if reason_codes else None

    # map confidence string to numeric score
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
            created_by,
            created_utc,
        )
        conn.commit()
        cur.close()


@router.post("/evaluate_fame", response_model=EvaluateResponse)
def evaluate_fame(request: EvaluateRequest) -> EvaluateResponse:
    """Attempts to convert a FAME formula into optimized Python/Polars code + logs audit row."""
    run_id = f"eval-{uuid4()}"
    confidence = _check_confidence(request.fame_code)

    if confidence == "low":
        # log low-confidence pass-through
        try:
            _write_audit(
                run_id=run_id,
                fame_code=request.fame_code,
                python_code=None,
                tier=None,
                status="low_confidence",
                confidence=confidence,
                reason_codes=["contains_complex_keyword"],
            )
        except Exception as e:
            logger.exception("audit_write_failed run_id=%s status=%s error=%s", run_id, "<status_here>", str(e))

        return EvaluateResponse(run_id=run_id, confidence="low")

    try:
        parsed = parse_fame_formula(request.fame_code)

        if not parsed:
            try:
                _write_audit(
                    run_id=run_id,
                    fame_code=request.fame_code,
                    python_code=None,
                    tier=None,
                    status="parse_failed",
                    confidence="low",
                    reason_codes=["parse_returned_none"],
                )
            except Exception as e:
                logger.exception("audit_write_failed run_id=%s status=%s error=%s", run_id, "<status_here>", str(e))
            return EvaluateResponse(run_id=run_id, confidence="low")

        polars_code = render_polars_expr(parsed["rhs"])

        final_code = _generate_vault_template(
            target=parsed["target"],
            refs=parsed["refs"],
            polars_expr=polars_code,
        )

        # log success
        try:
            _write_audit(
                run_id=run_id,
                fame_code=request.fame_code,
                python_code=final_code,
                tier=1,
                status="success",
                confidence="high",
                reason_codes=["tier1_conversion"],
            )
        except Exception as e:
            # do not fail evaluation if audit write fails
            logger.exception("audit_write_failed run_id=%s status=%s error=%s", run_id, "<status_here>", str(e))

        return EvaluateResponse(
            run_id=run_id,
            tier=1,
            confidence="high",
            python_code=final_code,
        )

    except Exception:
        # log exception outcome
        try:
            _write_audit(
                run_id=run_id,
                fame_code=request.fame_code,
                python_code=None,
                tier=None,
                status="exception",
                confidence="low",
                reason_codes=["evaluate_exception"],
            )
        except Exception as e:
            logger.exception("audit_write_failed run_id=%s status=%s error=%s", run_id, "<status_here>", str(e))

        return EvaluateResponse(run_id=run_id, confidence="low")
