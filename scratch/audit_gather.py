import os
import json
import pymongo
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")
client = pymongo.MongoClient(mongo_uri)
db = client["stock_market_db"]

# 5. A. Identify exact production run
print("=== PIPELINE RUNS ===")
runs = list(db.pipeline_runs.find().sort("started_at", -1).limit(5))
for r in runs:
    print(f"Run ID: {r.get('run_id')}")
    print(f"Status: {r.get('status')}")
    print(f"Start: {r.get('started_at')} | End: {r.get('timestamp')}")
    print(f"Trading Session: {r.get('last_completed_session')}")
    print(f"Prediction Target: {r.get('prediction_target_date')}")
    print(f"Errors: {r.get('errors', [])}")
    print(f"Stages: {list(r.get('stages', {}).keys())}")
    for stage, details in r.get('stages', {}).items():
        print(f"  {stage}: {details.get('status')}")
    print("-" * 20)

# 6. Verify Production Lock / Run State
print("=== PIPELINE LOCKS ===")
locks = list(db.pipeline_locks.find())
for l in locks:
    print(f"Lock: {l}")

# 7. VIX Fallback Path
print("=== INDIA VIX (historical_data) ===")
vix = list(db.historical_data.find({"ticker": "^INDIAVIX"}).sort("date", -1).limit(5))
for v in vix:
    print(f"Date: {v.get('date')} Close: {v.get('close')} Volume: {v.get('volume')}")

# 8. Prediction Generation & 9. Ticker Coverage & 10. Persisted Records
print("=== PREDICTION HISTORY ===")
preds = list(db.prediction_history.find().sort("prediction_date", -1).limit(100))
# Let's filter to the successful run target date (or prediction date).
# Assuming trading session 2026-09-04 -> target 2026-09-07.
target_preds = [p for p in preds if p.get("prediction_date") == "2026-09-04"]
print(f"Found {len(target_preds)} predictions for 2026-09-04")

tickers = set()
duplicates = 0
invalid_hashes = 0
unexpected_hashes = 0
EXPECTED_HASH = "26cc670ca5d434821c0485bde3310599e2c077dbee5504c8b266d4a37333388c"

for p in target_preds:
    t = p.get("ticker")
    if t in tickers:
        duplicates += 1
    tickers.add(t)
    # Temporal check
    if "2026-09-05" in str(p) or "2026-09-06" in str(p):
        print(f"Lookahead leak in {t}: {p}")
        
    ph = p.get("feature_pipeline_hash")
    if ph != EXPECTED_HASH:
        invalid_hashes += 1
        unexpected_hashes += 1

print(f"Unique Tickers: {len(tickers)}")
print(f"Duplicates: {duplicates}")
print(f"Invalid/Unexpected Pipeline Hashes: {invalid_hashes}")

# 13. Model Registry Consistency
print("=== MODEL REGISTRY ===")
active_models = list(db.model_registry.find({"status": "ACTIVE"}))
print(f"Total ACTIVE models: {len(active_models)}")
registry_hash_mismatch = sum(1 for m in active_models if m.get("feature_pipeline_hash") != EXPECTED_HASH)
print(f"Registry Hash Mismatches: {registry_hash_mismatch}")

# 14. Manifest Consistency
print("=== MANIFEST CONSISTENCY ===")
import glob
manifests = glob.glob("saved_models/*_active.json")
print(f"Total manifests: {len(manifests)}")
manifest_mismatches = 0
for m_path in manifests:
    with open(m_path, "r") as f:
        m = json.load(f)
        if m.get("feature_pipeline_hash") != EXPECTED_HASH:
            manifest_mismatches += 1
print(f"Manifest Hash Mismatches: {manifest_mismatches}")
