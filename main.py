from fastapi import FastAPI

app = FastAPI(
    title="FAME Assistant Backend (Fabric)",
    description=(
        "Orchestration engine for legacy FAME migration. "
        "Provides deterministic conversion gates and Fabric-integrated auditing."
    ),
    version="0.2.0",
)

# Keep app alive even if optional modules fail
try:
    from api.evaluator import router as evaluator_router
    app.include_router(evaluator_router, prefix="/api/v1", tags=["Evaluation"])
except Exception as e:
    @app.get("/__router_error_evaluator")
    def _router_error_evaluator():
        return {"loaded": False, "error": str(e)}

try:
    from api.logger import router as logger_router
    app.include_router(logger_router, prefix="/api/v1", tags=["Auditing"])
except Exception as e:
    @app.get("/__router_error_logger")
    def _router_error_logger():
        return {"loaded": False, "error": str(e)}

@app.get("/")
def root():
    return {"status": "ok", "service": "fame-assistant-backend"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}
