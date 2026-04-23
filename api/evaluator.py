import textwrap
import re
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from fame2pygen.formulas_generator import parse_fame_formula, render_polars_expr, sanitize_func_name

router = APIRouter()

class EvaluateRequest(BaseModel):
    fame_code: str

class EvaluateResponse(BaseModel):
    tier: int | None = None
    confidence: str
    python_code: str | None = None

def _is_high_confidence(fame_code: str) -> bool:
    """Checks for functions known to be complex/unsupported in Tier 1."""
    complex_patterns = [r"dateof\(", r"make\(", r"contain", r"ending"]
    return not any(re.search(p, fame_code.lower()) for p in complex_patterns)

def _generate_vault_template(target: str, refs: list, polars_expr: str) -> str:
    """Creates a runnable snippet using seriesvault and Polars."""
    # Ensure DATE is always loaded
    load_refs = list(set([r.upper() for r in refs] + ["DATE"]))
    
    loads = "\n".join([f'    "{r}": store["{r}"],' for r in load_refs if r != target.upper()])
    
    template = f"""
import polars as pl
from seriesvault import ParquetStore

store = ParquetStore("path/to/vault")

# 1. Load dependencies from vault into a Polars DataFrame
df = pl.DataFrame({{
{loads}
}})

# 2. Execute converted FAME logic
df = df.with_columns([
    ({polars_expr}).alias("{target.upper()}")
])

# 3. Persist result back to vault
store["{target.upper()}"] = df.select(["DATE", "{target.upper()}"])
    """
    return textwrap.dedent(template).strip()

@router.post("/evaluate_fame", response_model=EvaluateResponse)
def evaluate_fame(request: EvaluateRequest):
    # Parse the FAME formula
    parsed = parse_fame_formula(request.fame_code)
    
    if not parsed or not _is_high_confidence(request.fame_code):
        return EvaluateResponse(confidence="low")

    try:
        # Convert the RHS to a Polars expression
        polars_expr = render_polars_expr(parsed.get("rhs", ""))
        target = parsed.get("target", "RESULT")
        refs = parsed.get("refs", [])

        # Inject the seriesvault orchestration template
        python_code = _generate_vault_template(target, refs, polars_expr)

        return EvaluateResponse(
            tier=1,
            confidence="high",
            python_code=python_code
        )
    except Exception:
        return EvaluateResponse(confidence="low")
