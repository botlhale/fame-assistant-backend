# FAME Assistant Backend

This repository serves as the orchestration engine for the **Internal Library Code Assistant**. It provides a FastAPI-based middleware that bridges legacy FAME scripts with modern Python/Polars implementations, specifically leveraging the `Fame2PyGen`, `seriesvault`, and `DayIDelta` libraries.

## 🚀 Overview

The backend facilitates a two-tiered conversion approach:
- **Tier 1 (Deterministic):** Uses `Fame2PyGen` to attempt a 1:1 transpilation of FAME logic into Polars expressions.
- **Tier 2 (Generative):** If Tier 1 confidence is low, the system signals the frontend (Copilot Studio) to trigger an LLM-based fallback.

All successful or attempted conversions are logged via `DayIDelta` into a Delta Lake as SCD Type 2 records for auditing and "code evolution" tracking.

## 🛠️ Repository Structure

- `main.py`: Application entry point and FastAPI initialization.
- `api/evaluator.py`: Logic for Tier 1 conversion and `seriesvault` template injection.
- `api/logger.py`: Integration with `DayIDelta` for conversion history tracking.
- `requirements.txt`: Project dependencies and local library references.

## 📋 Prerequisites

This project relies on three internal libraries. Ensure these are cloned as siblings to this directory:
- `Fame2PyGen`: The FAME-to-Python transpiler.
- `DayIDelta`: The SCD2 history management engine.
- `seriesvault`: The disk-backed Parquet storage library.

## ⚙️ Installation

1. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```
Install the backend dependencies and the local libraries in editable mode:

```bash
pip install -r requirements.txt
pip install -e ../Fame2PyGen
pip install -e ../DayIDelta
pip install -e ../seriesvault
```
Start the app

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
🖥️ API Endpoints
1. POST /evaluate_fame
Evaluates a FAME script for deterministic conversion.

Input: { "fame_code": "result = a + b" }

Output: Returns Python code using seriesvault and polars if confidence is high.

2. POST /log_conversion
Logs the details of a conversion to the Delta audit table.

Input: { "fame_code": "...", "python_code": "...", "tier": 1 }

🔗 Integration Context
Copilot Studio: Calls /evaluate_fame via a Power Automate flow.

Microsoft Teams: Serves as the user interface where researchers paste their legacy code.

Azure App Service: Recommended hosting platform for this backend.

⚠️ Disclaimer
This backend is a hackathon prototype. Always verify generated Python code against legacy FAME outputs before using in production models.
