"""
evaluator.py - Tier 1 Conversion Engine

Logic for validating FAME formulas and generating Polars-based Python code
integrated with the seriesvault storage layer.
"""

import re
import textwrap
from fastapi import APIRouter
from pydantic import BaseModel
# Ensure sanitize_func_name is imported for template generation
from fame2pygen.formulas_generator import parse_fame_formula, render_polars_expr, sanitize_func_name

router = APIRouter()

class EvaluateRequest(BaseModel):
    fame_code: str

class EvaluateResponse(BaseModel):
    tier: int | None = None
    confidence: str
    python_code: str | None = None

def _check_confidence(fame_code: str) -> str:
    """Flags complex FAME functions that require Tier 2 LLM fallback."""
    # List of keywords that trigger low confidence
    complex_keywords = ["dateof", "make", "contain", "ending", "beginning"]
    if any(kw in fame_code.lower() for kw in complex_keywords):
        return "low"
    return "high"

def _generate_vault_template(target: str, refs: list[str], polars_expr: str) -> str:
    """
    Wraps a Polars expression in a seriesvault ParquetStore block.
    Corrected to use dictionary access instead of missing .get_series().
    """
    # Create sanitized column names for the DataFrame keys
    # Access the store using the original FAME keys
    load_blocks = "\n".join([
        f'    "{sanitize_func_name(ref).upper()}": store["{ref.upper()}"],' 
        for ref in refs
    ])
    
    # Ensure the target name is also sanitized
    tgt_alias = sanitize_func_name(target).upper()
    
    template = f"""
import polars as pl
from seriesvault import ParquetStore

# Initialize the vault (assumes standard directory structure)
store = ParquetStore("path/to/vault")

# 1. Load dependencies from vault into a Polars DataFrame
# Note: scalars are fetched from RAM, series from disk
df = pl.DataFrame({{
{load_blocks}
}})

# 2. Execute the FAME-equivalent Polars logic
df = df.with_columns([
    ({polars_expr}).alias("{tgt_alias}")
])

# 3. Save the result back to the vault
# The DATE column is expected to be present in loaded series
store["{target.upper()}"] = df.select(["DATE", "{tgt_alias}"])
    """
    return textwrap.dedent(template).strip()

@router.post("/evaluate_fame", response_model=EvaluateResponse)
def evaluate_fame(request: EvaluateRequest) -> EvaluateResponse:
    """Attempts Tier 1 deterministic conversion."""
    confidence = _check_confidence(request.fame_code)
    
    if confidence == "low":
        return EvaluateResponse(confidence="low")
    
    try:
        parsed = parse_fame_formula(request.fame_code)
        # Check if parser returned a valid dictionary
        if not parsed:
            return EvaluateResponse(confidence="low")
            
        polars_code = render_polars_expr(parsed["rhs"])
        
        final_code = _generate_vault_template(
            target=parsed.get("target", "RESULT"),
            refs=parsed.get("refs", []),
            polars_expr=polars_code
        )
        
        return EvaluateResponse(
            tier=1,
            confidence="high",
            python_code=final_code
        )
    except Exception:
        # Catch unexpected parsing errors and signal fallback
        return EvaluateResponse(confidence="low")
