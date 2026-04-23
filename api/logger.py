"""
logger.py – /log_conversion endpoint

Auditing / SCD2 history tracking:
  Persists each conversion attempt to a Delta table using DayIDelta's
  SCD2Engine so that the full change history of every FAME formula is
  retained as a slowly changing dimension (SCD Type 2).
"""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Local-package import
# The DayIDelta package is expected to be installed from the sibling repo.
# ---------------------------------------------------------------------------
try:
    from dayidelta.core.scd2_engine import SCD2Engine  # type: ignore[import]
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "dayidelta is not installed. "
        "Run: pip install -e ../DayIDelta"
    ) from exc

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter()

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class LogConversionRequest(BaseModel):
    fame_code: str
    python_code: str
    tier: int


class LogConversionResponse(BaseModel):
    status: str
    message: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DELTA_TABLE_PATH = "delta_tables/conversion_history"

_engine: SCD2Engine | None = None


def _get_engine() -> SCD2Engine:
    """Return a (lazily initialized) SCD2Engine instance."""
    global _engine  # noqa: PLW0603
    if _engine is None:
        _engine = SCD2Engine(table_path=_DELTA_TABLE_PATH)
    return _engine


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------


@router.post("/log_conversion", response_model=LogConversionResponse)
def log_conversion(request: LogConversionRequest) -> LogConversionResponse:
    """
    Persist a conversion record to the Delta table as an SCD2 row.

    Fields written:
      • fame_code   – original FAME source
      • python_code – generated Python source
      • tier        – conversion tier that produced the output
    """
    engine = _get_engine()

    record = {
        "fame_code": request.fame_code,
        "python_code": request.python_code,
        "tier": request.tier,
    }

    engine.upsert(record)

    return LogConversionResponse(
        status="ok",
        message=f"Conversion record written to '{_DELTA_TABLE_PATH}'.",
    )
