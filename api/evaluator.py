"""
evaluator.py - Tier 1 Conversion Engine

Logic for validating FAME formulas and generating Polars-based Python code
integrated with the seriesvault storage layer.
"""

import re
import textwrap
from fastapi import APIRouter
from pydantic import BaseModel
from fame2pygen.formulas_generator import parse_fame_formula, render_polars_expr #

router = APIRouter()

class EvaluateRequest(BaseModel):
    """Schema for the FAME conversion request."""
    fame_code: str

class EvaluateResponse(BaseModel):
    """Schema for the conversion result and confidence score."""
    tier: int | None = None
    confidence: str
    python_code: str | None = None

def _check_confidence(fame_code: str) -> str:
    """
    Determines if a FAME script can be reliably converted by Tier 1 logic.
    
    Checks for complex date functions or patterns that currently require
    generative fallback (Tier 2).
    
    Args:
        fame_code (str): The raw FAME source code.
        
    Returns:
        str: "high" for simple arithmetic/mapped functions, "low" otherwise.
    """
    # Flag complex FAME functions for manual/generative review
    complex_keywords = ["dateof", "make", "contain", "ending", "beginning"]
    if any(kw in fame_code.lower() for kw in complex_keywords):
        return "low"
    return "high"

def _generate_vault_template(target: str, refs: list[str], polars_expr: str) -> str:
    """
    Wraps a Polars expression in a seriesvault ParquetStore orchestration block.
    
    Args:
        target (str): The variable being assigned.
        refs (list): List of dependency variables identified by the parser.
        polars_expr (str): The rendered Polars code string.
        
    Returns:
        str: A complete Python script.
    """
    # Build dynamic series loading
    load_blocks = "\n".join([f'    "{ref.upper()}": store.get_series("{ref.upper()}"),' for ref in refs])
    
    template = f"""
import polars as pl
from seriesvault import ParquetStore

# Initialize the vault
store = ParquetStore()

# Load dependencies from vault into a Polars DataFrame
df = pl.DataFrame({{
{load_blocks}
}})

# Execute the FAME-equivalent Polars logic
df = df.with_columns([
    ({polars_expr}).alias("{target.upper()}")
])

# Save the result back to the vault
store["{target.upper()}"] = df.select(["DATE", "{target.upper()}"])
    """
    return textwrap.dedent(template).strip()

@router.post("/evaluate_fame", response_model=EvaluateResponse)
def evaluate_fame(request: EvaluateRequest) -> EvaluateResponse:
    """
    Attempts to convert a FAME formula into optimized Python/Polars code.
    
    If deterministic mapping is possible, returns the code and sets confidence 
    to 'high'. If the script is too complex, returns 'low' confidence to 
    trigger the Tier 2 generative fallback.
    """
    confidence = _check_confidence(request.fame_code)
    
    if confidence == "low":
        return EvaluateResponse(confidence="low")
    
    try:
        # Parse the formula components
        parsed = parse_fame_formula(request.fame_code)
        
        # Render the expression into Polars-compatible Python
        polars_code = render_polars_expr(parsed["rhs"])
        
        # Inject into the storage template
        final_code = _generate_vault_template(
            target=parsed["target"],
            refs=parsed["refs"],
            polars_expr=polars_code
        )
        
        return EvaluateResponse(
            tier=1,
            confidence="high",
            python_code=final_code
        )
    except Exception:
        return EvaluateResponse(confidence="low")
