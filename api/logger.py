"""
logger.py - Microsoft Fabric SCD Type 2 Logging
"""

from fastapi import APIRouter
from pydantic import BaseModel
from pyspark.sql import SparkSession
from dayidelta.core.scd2_engine import SCD2Engine 
from dayidelta.core.models import TableSchema, SCD2Config 
from dayidelta.platforms.fabric import FabricAdapter 

router = APIRouter()

class LogRequest(BaseModel):
    fame_code: str
    python_code: str
    tier: int

@router.post("/log_conversion")
def log_conversion(request: LogRequest):
    """Logs the conversion attempt to Fabric using DayIDelta SCD2 logic."""
    spark = SparkSession.builder.getOrCreate()
    
    adapter = FabricAdapter()
    engine = SCD2Engine(adapter)
    
    # catalog should not be None to prevent property errors
    schema = TableSchema(
        catalog="fabric", 
        schema="migration_audit",
        table="conversion_history",
        key_columns=["fame_code"],
        tracked_columns=["python_code", "tier"]
    )
    
    config = SCD2Config(schema="migration_audit")
    
    # Prepare the log record for Spark processing
    log_data = [{
        "fame_code": request.fame_code,
        "python_code": request.python_code,
        "tier": request.tier
    }]
    source_df = spark.createDataFrame(log_data)
    
    # Execute the SCD2 process in Fabric
    result = engine.process(spark, source_df, schema, config)
    
    return {
        "status": "logged" if result.success else "failed",
        "version_id": result.current_day_id
    }
