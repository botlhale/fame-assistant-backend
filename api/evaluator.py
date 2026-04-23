"""
evaluator.py - Tier 1 Conversion Engine (Corrected)

Logic for validating FAME formulas and generating Polars-based Python code
integrated with the seriesvault storage layer.
"""

import re
import textwrap
from fastapi import APIRouter
from pydantic import BaseModel
# Added sanitize_func_name to imports
from fame2pygen.formulas_generator import parse_fame_formula, render_polars_expr, sanitize_func_name

router = APIRouter()

class EvaluateRequest(BaseModel):
    fame_code: str

class EvaluateResponse(BaseModel):
    tier: int | None = None
    confidence: str
    python_code: str | None = None

def _check_confidence(fame_code: str) -> str:
    """Determines if a FAME script can be reliably converted by Tier 1 logic."""
    complex_keywords = ["dateof", "make", "contain", "ending", "beginning"]
    if any(kw in fame_code.lower() for kw in complex_keywords):
        return "low"
    return "high"

def _generate_vault_template(target: str, refs: list[str], polars_expr: str) -> str:
    """
    Wraps a Polars expression in a seriesvault ParquetStore block.
    Corrected to use dictionary-style access and sanitization.
    """
    # Dynamically generate loading blocks with sanitized names
    load_blocks = "\n".join([
        f'    "{sanitize_func_name(ref).upper()}": store["{ref.upper()}"],' 
        for ref in refs
    ])
    
    target_sanitized = sanitize_func_name(target).upper()
    
    template = f"""
import polars as pl
from seriesvault import ParquetStore

# Initialize the vault
store = ParquetStore("path/to/vault")

# 1. Load dependencies from vault into a Polars DataFrame
df = pl.DataFrame({{
{load_blocks}
}})

# 2. Execute the FAME-equivalent Polars logic
df = df.with_columns([
    ({polars_expr}).alias("{target_sanitized}")
])

# 3. Save the result back to the vault
store["{target.upper()}"] = df.select(["DATE", "{target_sanitized}"])
    """
    return textwrap.dedent(template).strip()

@router.post("/evaluate_fame", response_model=EvaluateResponse)
def evaluate_fame(request: EvaluateRequest) -> EvaluateResponse:
    """Attempts to convert a FAME formula into optimized Python/Polars code."""
    confidence = _check_confidence(request.fame_code)
    
    if confidence == "low":
        return EvaluateResponse(confidence="low")
    
    try:
        parsed = parse_fame_formula(request.fame_code)
        
        # Added null check to prevent crash on invalid input
        if not parsed:
            return EvaluateResponse(confidence="low")
        
        polars_code = render_polars_expr(parsed["rhs"])
        
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
