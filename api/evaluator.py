"""
evaluator.py – /evaluate_fame endpoint

Tier 1 deterministic conversion:
  1. Calls fame2pygen.parse_fame_formula() to attempt an automatic conversion.
  2. If the parser succeeds without flagging complex / manual-review functions,
     injects a seriesvault.ParquetStore data-loading template around the output
     and returns {"tier": 1, "confidence": "high", "python_code": <code>}.
  3. If the parser cannot fully convert the formula it returns
     {"confidence": "low"} so the caller knows to escalate to a higher tier.
"""

from __future__ import annotations

import textwrap

from fastapi import APIRouter
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Local-package imports
# These packages are expected to be installed from the sibling repositories
# Fame2PyGen and seriesvault respectively.
# ---------------------------------------------------------------------------
try:
    from fame2pygen import parse_fame_formula  # type: ignore[import]
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "fame2pygen is not installed. "
        "Run: pip install -e ../Fame2PyGen"
    ) from exc

try:
    from seriesvault import ParquetStore  # type: ignore[import]
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "seriesvault is not installed. "
        "Run: pip install -e ../seriesvault"
    ) from exc

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter()

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class EvaluateRequest(BaseModel):
    fame_code: str


class EvaluateResponse(BaseModel):
    tier: int | None = None
    confidence: str
    python_code: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SERIESVAULT_TEMPLATE = textwrap.dedent(
    """\
    from seriesvault import ParquetStore

    store = ParquetStore()

    # ── scalar / metadata values are fetched from RAM / JSON ──────────────
    # scalar_value = store.get_scalar("{series_name}")

    # ── time-series data is fetched from Parquet ──────────────────────────
    # series = store.get_series("{series_name}")

    {converted_code}
    """
)


def _inject_seriesvault(converted_code: str) -> str:
    """Wrap *converted_code* with a seriesvault ParquetStore boilerplate."""
    return _SERIESVAULT_TEMPLATE.format(
        series_name="<series_name>",
        converted_code=converted_code,
    )


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------


@router.post("/evaluate_fame", response_model=EvaluateResponse)
def evaluate_fame(request: EvaluateRequest) -> EvaluateResponse:
    """
    Attempt a Tier 1 deterministic conversion of the supplied FAME formula.

    The parser (fame2pygen) returns a result object.  If it signals that all
    functions were mapped deterministically (no complex / manual-review
    flags), we wrap the output with a seriesvault data-loading template and
    respond with confidence *high*.  Otherwise we respond with confidence
    *low* so the caller can escalate.
    """
    result = parse_fame_formula(request.fame_code)

    # parse_fame_formula is expected to return an object with at least:
    #   • result.success  (bool)   – True when every token was mapped
    #   • result.code     (str)    – the generated Python source
    if not result.success:
        return EvaluateResponse(confidence="low")

    python_code = _inject_seriesvault(result.code)

    return EvaluateResponse(
        tier=1,
        confidence="high",
        python_code=python_code,
    )
