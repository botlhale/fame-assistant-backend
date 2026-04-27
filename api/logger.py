import os
import json
import pyodbc
from datetime import datetime, timezone
from typing import Optional, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import struct
from azure.identity import ClientSecretCredential

SQL_COPT_SS_ACCESS_TOKEN = 1256

router = APIRouter()

class LogConversionRequest(BaseModel):
    fame_code: str
    python_code: str
    tier: int
    run_id: Optional[str] = None
    status: Optional[str] = "success"
    confidence_score: Optional[float] = None
    model_used: Optional[str] = None
    reason_codes: Optional[List[str]] = None
    created_by: Optional[str] = "api"

def _get_conn():
    server = os.environ["FABRIC_SQL_SERVER"]              # host only
    database = os.environ["FABRIC_SQL_DATABASE"]
    tenant_id = os.environ["FABRIC_TENANT_ID"]
    client_id = os.environ["FABRIC_CLIENT_ID"]
    client_secret = os.environ["FABRIC_CLIENT_SECRET"]

    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
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

@router.post("/log_conversion")
def log_conversion(req: LogConversionRequest):
    schema = os.getenv("FABRIC_SQL_SCHEMA", "dbo")
    table = os.getenv("FABRIC_SQL_TABLE", "conversion_audit")

    created_utc = datetime.now(timezone.utc)

    insert_sql = f"""
    INSERT INTO {schema}.{table}
    (run_id, fame_code, python_code, tier, status, confidence_score, model_used, reason_codes, created_by, created_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            insert_sql,
            req.run_id,
            req.fame_code,
            req.python_code,
            req.tier,
            req.status,
            req.confidence_score,
            req.model_used,
            json.dumps(req.reason_codes) if req.reason_codes else None,
            req.created_by,
            created_utc
        )
        conn.commit()
        cur.close()
        conn.close()

        return {
            "ok": True,
            "message": "Logged to Fabric Lakehouse SQL endpoint",
            "created_utc": created_utc.isoformat()
        }
    except KeyError as e:
        raise HTTPException(status_code=500, detail=f"Missing env var: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"fabric_sql_insert_failed: {e}")
