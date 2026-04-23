from fastapi import APIRouter
from pydantic import BaseModel
from pyspark.sql import SparkSession
from dayidelta.core.scd2_engine import SCD2Engine
from dayidelta.core.models import TableSchema, SCD2Config
from dayidelta.platforms.unity_catalog import UnityCatalogAdapter

router = APIRouter()

class LogConversionRequest(BaseModel):
    fame_code: str
    python_code: str
    tier: int

_LOG_TABLE = "fame_assistant.audit.conversion_history"

@router.post("/log_conversion")
def log_conversion(request: LogConversionRequest):
    spark = SparkSession.builder.getOrCreate()
    
    # Setup SCD2 Engine for conversion tracking
    adapter = UnityCatalogAdapter()
    engine = SCD2Engine(adapter)
    
    schema = TableSchema(
        catalog="fame_assistant",
        schema="audit",
        table="conversion_history",
        key_columns=["fame_code"], # Track history based on the unique FAME script
        tracked_columns=["python_code", "tier"] # Version the Python output
    )
    
    config = SCD2Config(catalog="fame_assistant", schema="audit")
    
    # Create the log record as a Spark DataFrame
    log_df = spark.createDataFrame([request.dict()])
    
    # Maintain the audit trail using SCD Type 2 logic
    result = engine.process(spark, log_df, schema, config)
    
    return {"status": "success" if result.success else "error", "day_id": result.current_day_id}
