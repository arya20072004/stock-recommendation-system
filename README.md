# Stock Recommendation System

## Overview
An ML-powered stock recommendation and analysis system that provides BUY/HOLD/SELL predictions for a universe of 51 Nifty 50 stocks. The system uses a strictly governed, canonical 57-feature pipeline, dynamic artifact hashing, and an independent 10-session settlement observation process to validate prediction performance against live market data.

## Current System Status
- **Current Production Universe**: 51 active tickers.
- **Canonical Feature Pipeline**: `v1` (57 features)
- **Active Model Bundles**: 51 dynamically verified bundles matching the canonical pipeline.
- **Settlement Process**: Actively observing cohorts (e.g., 2026-08-20 is at 3/10 valid sessions). Mature evaluation requires 10 valid future market sessions.
- **Latest Legitimate Commit**: `9123f6e9 fix: harden production candidate validation and add regression tests`

## Architecture
The system separates prediction generation from performance settlement. Predictions are generated using a rigorously governed feature pipeline, stored in MongoDB, and independently evaluated as future market data arrives.

The conceptual lifecycle flow is:
1. **Market data**
2. **Feature generation**
3. **Canonical feature pipeline**
4. **Active production model**
5. **Prediction**
6. **Prediction history**
7. **Future market observations**
8. **Settlement evaluation**
9. **10 valid future sessions**
10. **Mature evaluation**

## Production Pipeline
The daily production workflow currently uses:
```bash
python src/pipeline/daily.py
```
This pipeline performs:
1. Data acquisition and processing
2. Prediction generation
3. Prediction validation
4. API health check
5. Pipeline completion

**Note**: A successful pipeline execution (e.g. `daily.py` completing without errors) generates predictions but is *not* equivalent to a complete post-run verification. Persistence and settlement audits are separate verification layers.

## Feature Engineering and Canonical Pipeline
The system enforces a strict **canonical 57-feature schema** (`v1`). 
The canonical pipeline hash is dynamically resolved through:
```python
get_feature_pipeline_hash("v1")
```
The old 55-feature pipeline is explicitly rejected and is no longer accepted for active production use. 

## Model / Bundle Governance
Production model artifacts are not accepted merely because they exist. The production candidate-validation system enforces strict governance before a model is promoted to active status.

The preflight validation verifies:
- Model identity and artifact hashes
- Feature hashes
- Canonical feature pipeline identity (enforcing the expected schema)
- Cryptographic provenance
- Explicit rejection of obsolete pipeline bundles

Candidates must match the canonical, verified feature/model contract before being accepted. The system uses dynamic artifact verification rather than relying on hard-coded flags.

## Prediction Lifecycle
A daily production run generates predictions for the active 51-ticker universe. These predictions are then persisted and wait for future market data to be evaluated.

## Settlement and Maturity
Predictions are evaluated independently through an authoritative settlement implementation (`src/ml/settlement.py`).

**Maturity Horizon:** 10 valid future trading sessions.

A prediction's own `market_date` does NOT count as a settlement session. A valid settlement observation is a future observation satisfying the repository's settlement rules, specifically:
- `historical_data.date > prediction.market_date`
- `close > 0`

Settlement maturity is evaluated independently per ticker. The system is actively tracking live observation cohorts which advance as valid future market sessions are persisted. The cohorts are still being observed and are not yet mature.

## Production Preflight and Validation
The repository uses a production preflight to ensure the active universe matches the expected state. It validates:
- Active MongoDB records
- Active filesystem manifests
- Absence of unexpected hashes or old pipelines
- Manifest read errors and identity mismatches
- Pipeline version mismatches
- Stale model counts

## Testing
The targeted safety/acceptance suite for the corrective pipeline enforcement can be run via:
```powershell
$env:PYTHONPATH = (Get-Location).Path
pytest -q tests/test_canonical_schema.py tests/test_old_pipeline_rejection.py tests/test_phase14.py
```
This targeted safety suite currently passes (`15 passed, 1 warning` for pandas Copy-on-Write deprecation).

## Daily Operations
The standard daily operational workflow is:
1. Wait for completed market session/data availability.
2. Run `daily.py` after the operational data window.
3. Confirm pipeline execution success.
4. Verify prediction persistence.
5. Verify active model/bundle identity.
6. Verify settlement observation advancement.
7. Confirm maturity state.
8. Continue observation until cohorts reach 10 valid sessions.

## Current Production State
The most recent production run successfully generated and persisted predictions for the active 51-ticker universe.

Current verified settlement state for recent prediction cohorts (as they march towards 10 valid sessions):
- `2026-08-20` → 3 / 10
- `2026-08-21` → 2 / 10
- `2026-08-24` → 1 / 10
- `2026-08-25` → 0 / 10

Currently, 0 tickers are mature. The cohorts are live and will mature dynamically.

## Repository Structure
```text
stock-recommendations/
├── src/
│   ├── data/                  # Market data collection
│   ├── features/              # Canonical feature pipeline and routing
│   ├── ml/                    # Training, settlement, evaluation, and registry
│   ├── pipeline/              # daily.py production workflow
│   └── ...
├── tests/                     # Acceptance and regression tests
├── scripts/                   # Validation and diagnostics
├── saved_models/              # Trained ML models (do not commit)
├── saved_features/            # Feature lists for each stock (do not commit)
└── ...
```

## Development Guidelines
- **Artifacts**: Do NOT commit `saved_models/*`, `saved_features/*`, `saved_evaluations/*`, `scratch/*`, `*.joblib` generated artifacts, `*.parquet` datasets, audit JSON files, temporary reports, or test output files.
- **Validation**: Any changes to feature pipelines or model generation must pass the production candidate validation tests to ensure the canonical pipeline is not broken.

## Known Limitations / Operational Notes
- The settlement observation system requires forward-looking market data and deliberately delays mature evaluations by 10 valid sessions.
- Ensure that the `.env` file correctly defines `MONGO_URI` before executing validation scripts or `daily.py`.
