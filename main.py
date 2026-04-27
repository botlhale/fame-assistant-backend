from fastapi import FastAPI

app = FastAPI(
    title="FAME Assistant Backend (Fabric)",
    description=(
        "Orchestration engine for legacy FAME migration. "
        "Provides deterministic conversion gates and Fabric-integrated auditing."
    ),
    version="0.2.0",
    openapi_version="3.0.3",
)

evaluator_import_error = None
logger_import_error = None

try:
    from api.evaluator import router as evaluator_router
    app.include_router(evaluator_router, prefix="/api/v1", tags=["Evaluation"])
except Exception as ex:
    evaluator_import_error = repr(ex)

try:
    from api.logger import router as logger_router
    app.include_router(logger_router, prefix="/api/v1", tags=["Auditing"])
except Exception as ex:
    logger_import_error = repr(ex)

@app.get("/__router_error_evaluator")
def _router_error_evaluator():
    return {"loaded": evaluator_import_error is None, "error": evaluator_import_error}

@app.get("/__router_error_logger")
def _router_error_logger():
    return {"loaded": logger_import_error is None, "error": logger_import_error}

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}
