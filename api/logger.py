"""
logger.py - Microsoft Fabric SCD Type 2 Logging

Orchestrates history tracking for every conversion attempt. Uses DayIDelta to 
manage a Delta table that versions the Python output for every FAME formula.
"""

from fastapi import APIRouter
from pydantic import BaseModel
from pyspark.sql import SparkSession
from dayidelta.core.scd2_engine import SCD2Engine #
from dayidelta.core.models import TableSchema, SCD2Config #
from dayidelta.platforms.fabric import FabricAdapter #

router = APIRouter()

class LogRequest(BaseModel):
    """Schema for logging a conversion attempt."""
    fame_code: str
    python_code: str
    tier: int

@router.post("/log_conversion")
def log_conversion(request: LogRequest):
    """
    Logs the result of a conversion into a Fabric Lakehouse table.
    
    This endpoint uses SCD Type 2 logic to ensure that if a FAME script is 
    re-processed (e.g., after an LLM improvement), both versions of the 
    Python code are preserved in the history.
    """
    spark = SparkSession.builder.getOrCreate()
    
    # Initialize the Fabric platform adapter
    adapter = FabricAdapter()
    engine = SCD2Engine(adapter) #
    
    # Define the Fabric table schema (2-level naming)
    schema = TableSchema(
        catalog=None, # Fabric uses 2-level naming: schema.table
        schema="migration_audit",
        table="conversion_history",
        key_columns=["fame_code"], # The unique FAME script is the natural key
        tracked_columns=["python_code", "tier"] # Version these changes
    )
    
    config = SCD2Config(schema="migration_audit")
    
    # Prepare the log record
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
