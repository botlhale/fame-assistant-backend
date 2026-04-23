"""
main.py – FAME Assistant Backend

FastAPI application that exposes two endpoints:

  POST /evaluate_fame   – Tier 1 deterministic FAME → Python conversion
  POST /log_conversion  – SCD2 audit trail for conversion history
"""

from fastapi import FastAPI

from api.evaluator import router as evaluator_router
from api.logger import router as logger_router

app = FastAPI(
    title="FAME Assistant Backend",
    description=(
        "Compute engine for AI-assisted FAME-to-Python code conversion. "
        "Integrates Fame2PyGen, seriesvault (ParquetStore), and DayIDelta "
        "(SCD2Engine) for deterministic conversion and full audit history."
    ),
    version="0.1.0",
)

app.include_router(evaluator_router)
app.include_router(logger_router)


@app.get("/health")
def health() -> dict[str, str]:
    """Simple liveness probe."""
    return {"status": "ok"}
