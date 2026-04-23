# LIBRARY_HANDBOOK

This handbook is the Tier-2 grounding artifact for the FAME-to-Python assistant.  
It summarizes how to use the project libraries together:

- `Fame2PyGen` → deterministic FAME formula parsing/transpilation
- `seriesvault` → local key-value data access for series/scalars
- `DayIDelta` → SCD Type 2 style audit logging/persistence
- `fame-assistant-backend` → orchestration API layer

> This is a first version intended for hackathon velocity. Keep this file concise and update with exact signatures from source as APIs evolve.

---

## Fame2PyGen

### Purpose
`Fame2PyGen` converts FAME-style expressions/scripts into executable Python (Polars-oriented) transformation logic.

### What to use it for
- Parse incoming FAME formulas.
- Extract target series and referenced series.
- Generate deterministic Python transformation snippets.
- Apply naming sanitization before generating Polars columns/functions.

### Key integration expectations
- Use parser output to decide deterministic confidence.
- If parsing fails or result is incomplete, return low confidence and trigger Tier-2 fallback.
- Always sanitize generated names for Polars-safe identifiers.

### Canonical conversion concerns
When converting FAME syntax:
- Preserve expression intent before optimization.
- Detect unsupported/ambiguous constructs early.
- Tag formulas containing complex keywords for lower confidence:
  - `dateof`
  - `make`
  - `contain`
  - `ending`
  - `beginning`

### Frequency mapping (canonical baseline)
Use a stable frequency mapping table when translating FAME semantics:

- Annual → `A`
- Semiannual → `S`
- Quarterly → `Q`
- Monthly → `M`
- Weekly → `W` (if supported by source model)
- Daily/Business Daily → `D` / business-calendar-aware variant

> If your runtime has a stricter enum, map to that enum and document the mapping in backend config.

---

## DayIDelta

### Purpose
`DayIDelta` provides data-delta/versioning patterns and table processing abstractions suitable for audit logging and SCD Type 2 style persistence.

### What to use it for in this project
- Log every conversion attempt and outcome.
- Persist records with history semantics (SCD Type 2).
- Manage merge/upsert behavior via engine processing.

### Required schema concept
Define a `TableSchema` for conversion audit records with consistent naming and 3-level qualification.

**Important:**
- Initialize `TableSchema` with `catalog="fabric"` (or your explicit Fabric catalog value) to ensure valid naming concatenation.
- Keep schema fields stable for repeatable merges.

### Suggested audit columns
- `conversion_id` (unique event id)
- `session_id`
- `user_id`
- `timestamp_utc`
- `input_formula`
- `parse_status`
- `confidence` (`high`/`low`)
- `confidence_reasons` (array/stringified list)
- `tier_used` (`tier1` or `tier2`)
- `tier1_output_code`
- `tier2_output_code`
- `target_series`
- `referenced_series`
- `status` (success/fail)
- `error_message`
- SCD fields (effective dates/current flag/version as required by DayIDelta model)

### Processing rule
Use `engine.process()` to apply SCD Type 2 merge logic into the Fabric Lakehouse target.

---

## seriesvault

### Purpose
`seriesvault` is a high-performance, disk-backed key-value store for local dataset sharing and retrieval.

### Storage behavior (project-relevant)
- Time series: stored in Parquet on disk.
- Scalars/metadata: JSON (and/or memory-backed structures depending on runtime configuration).

### Critical access pattern
Use dictionary-style access:

- ✅ `store["KEY"]`
- ❌ `store.get_series("KEY")`  (not valid for this integration contract)

### Integration notes
- Validate key existence before formula execution.
- Keep key naming conventions consistent with sanitized/transpiled references.
- Handle missing keys gracefully and lower confidence if required data is unavailable.

---

## Integration Contract for fame-assistant-backend

### API: `GET /health`
Purpose: Liveness/readiness probe for Azure hosting.

**Response example**
```json
{
  "status": "ok"
}
```

---

### API: `POST /evaluate_fame`
Purpose: Evaluate formula for deterministic transpilation and confidence decision.

**Request**
```json
{
  "formula": "X = Y + Z",
  "context": {
    "session_id": "abc123",
    "user_id": "user-1"
  }
}
```

**Processing contract**
1. Run FAME parse/transpile path (`Fame2PyGen`).
2. Extract:
   - `target`
   - `refs`
3. Detect complexity keywords:
   - `dateof`, `make`, `contain`, `ending`, `beginning`
4. Sanitize all generated names with `sanitize_func_name`.
5. If parser fails / incomplete output → force `confidence = "low"`.

**Response (high confidence example)**
```json
{
  "confidence": "high",
  "confidence_reasons": [],
  "target": "X",
  "refs": ["Y", "Z"],
  "tier1_code": "df.with_columns((pl.col('Y') + pl.col('Z')).alias('X'))",
  "fallback_required": false
}
```

**Response (low confidence example)**
```json
{
  "confidence": "low",
  "confidence_reasons": ["parser_failed_or_empty_result"],
  "target": null,
  "refs": [],
  "tier1_code": null,
  "fallback_required": true
}
```

---

### API: `POST /log_conversion`
Purpose: Persist conversion audit trail via DayIDelta into Fabric Lakehouse.

**Request**
```json
{
  "conversion_id": "evt-001",
  "session_id": "abc123",
  "user_id": "user-1",
  "timestamp_utc": "2026-04-23T12:00:00Z",
  "input_formula": "X = Y + Z",
  "parse_status": "success",
  "confidence": "high",
  "confidence_reasons": [],
  "tier_used": "tier1",
  "target_series": "X",
  "referenced_series": ["Y", "Z"],
  "status": "success",
  "error_message": null
}
```

**Processing contract**
1. Build/validate `TableSchema` with `catalog="fabric"` (or configured catalog).
2. Initialize adapter/engine for Fabric target.
3. Call `engine.process()` to apply SCD Type 2 merge behavior.
4. Return operation status and identifiers.

---

## Failure Handling & Confidence Rules

### Deterministic confidence gate
Set `confidence = "low"` if any of the following is true:
1. Formula contains complex keywords:
   - `dateof`, `make`, `contain`, `ending`, `beginning`
2. `parse_fame_formula` fails, throws, or returns empty/incomplete result.
3. Target series cannot be identified.
4. Referenced series extraction fails unexpectedly.
5. Required source keys are absent in `seriesvault`.

Otherwise, confidence can be `high`.

### Mandatory fallback trigger
If confidence is low:
- Return payload with `fallback_required = true`.
- Do not attempt risky deterministic code synthesis.
- Hand off to Copilot Studio Tier-2 RAG flow grounded by this handbook.

### Error-response principles
- Never return ambiguous status.
- Include machine-readable `confidence_reasons`.
- Log both success and failure via `/log_conversion`.

---

## Copy-paste Code Snippets

> These are reference snippets for orchestration. Adjust imports/signatures to match actual repo APIs.

### 1) Confidence keyword gate
```python
COMPLEX_KEYWORDS = {"dateof", "make", "contain", "ending", "beginning"}

def has_complex_keyword(formula: str) -> bool:
    normalized = (formula or "").lower()
    return any(k in normalized for k in COMPLEX_KEYWORDS)
```

### 2) Safe evaluate flow
```python
def evaluate_formula(formula: str):
    if not formula or not formula.strip():
        return {
            "confidence": "low",
            "confidence_reasons": ["empty_formula"],
            "fallback_required": True,
        }

    if has_complex_keyword(formula):
        return {
            "confidence": "low",
            "confidence_reasons": ["contains_complex_keyword"],
            "fallback_required": True,
        }

    parsed = parse_fame_formula(formula)  # Fame2PyGen integration point
    if not parsed:
        return {
            "confidence": "low",
            "confidence_reasons": ["parser_failed_or_empty_result"],
            "fallback_required": True,
        }

    target = parsed.get("target")
    refs = parsed.get("refs", [])
    if not target:
        return {
            "confidence": "low",
            "confidence_reasons": ["missing_target"],
            "fallback_required": True,
        }

    safe_target = sanitize_func_name(target)
    safe_refs = [sanitize_func_name(r) for r in refs]

    # Pseudocode transpilation output
    code = f"# generate polars code for {safe_target} from {safe_refs}"

    return {
        "confidence": "high",
        "confidence_reasons": [],
        "target": safe_target,
        "refs": safe_refs,
        "tier1_code": code,
        "fallback_required": False,
    }
```

### 3) seriesvault dictionary access
```python
def fetch_series(store, key: str):
    # Required contract: dictionary-style access
    return store[key]
```

### 4) DayIDelta process pattern (conceptual)
```python
def log_conversion(engine, record: dict):
    # engine configured with Fabric adapter + TableSchema(catalog="fabric", ...)
    result = engine.process([record])
    return result
```

### 5) Health endpoint
```python
from fastapi import FastAPI

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}
```

---

## Operational Notes for Copilot Studio Knowledge Upload

1. Upload this file in Copilot Studio → **Knowledge**.
2. Keep section headers stable (retrieval anchors).
3. When backend contracts change, update this handbook first, then re-upload.
4. Prefer short, explicit examples over long prose for better RAG grounding quality.