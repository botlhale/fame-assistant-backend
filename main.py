"""
main.py - FAME Assistant Backend Entry Point

This module initializes the FastAPI application and orchestrates the routing
between the FAME-to-Python evaluator and the SCD Type 2 logging service.
"""

from fastapi import FastAPI
from api.evaluator import router as evaluator_router
from api.logger import router as logger_router

app = FastAPI(
    title="FAME Assistant Backend (Fabric)",
    description=(
        "Orchestration engine for legacy FAME migration. "
        "Provides deterministic conversion gates and Fabric-integrated auditing."
    ),
    version="0.2.0",
)

# Include sub-routers for specialized logic
app.include_router(evaluator_router, prefix="/api/v1", tags=["Evaluation"])
app.include_router(logger_router, prefix="/api/v1", tags=["Auditing"])

@app.get("/health")
def health_check() -> dict[str, str]:
    """
    Performs a simple liveness probe to verify the service is running.
    
    Returns:
        dict: A status indicator.
    """
    return {"status": "healthy"}
