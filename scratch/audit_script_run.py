import os
import sys
from datetime import datetime, timezone
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

sys.path.insert(0, r"c:\Users\aryab\Coding\stock_recommendations")
load_dotenv(r"c:\Users\aryab\Coding\stock_recommendations\.env")

mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri)
db = client["stock_market_db"]

from src.data.nifty50 import TICKERS
from src.data.session_calendar import next_session
from src.ml.model_utils import get_model_version

# 4. IDENTIFY THE EXACT PRODUCTION RUN
print("--- RUN IDENTIFICATION ---")
# Query run around Sep 10 20:00 IST
run = db.pipeline_runs.find_one({
    "started_at": {"$gte": datetime(2026, 9, 10, 14, 0, 0, tzinfo=timezone.utc)},
    "status": "SUCCESS"
})
if run:
    print(f"Run ID: {run['run_id']}")
    print(f"Start timestamp: {run['started_at']}")
    print(f"Completion timestamp: {run.get('completed_at')}")
    print(f"Status: {run['status']}")
    print(f"Prediction target date: {run.get('market_date')}")
else:
    print("Run not found!")

# 5. VERIFY SESSION AND TARGET DATE
print("--- SESSION / TARGET ---")
trading_session = datetime(2026, 9, 10).date()
print(f"Trading session: {trading_session}")
expected_next = next_session(trading_session)
print(f"Next valid trading session: {expected_next}")

preds = list(db.prediction_history.find({"market_date": str(expected_next), "prediction_horizon": 10}))
print(f"Predictions targeting expected date: {len(preds)}")
all_target_dates = set(p['market_date'] for p in preds)
print(f"All predictions target canonical next session: {'PASS' if all_target_dates == {str(expected_next)} else 'FAIL'}")

# 6. VERIFY EXACTLY 51 PREDICTIONS PERSISTED
print("--- PREDICTION COVERAGE ---")
print(f"Total persisted predictions: {len(preds)}")
pred_tickers = set(p['symbol'] for p in preds)
print(f"Unique tickers: {len(pred_tickers)}")
print(f"Missing tickers: {list(set(TICKERS) - pred_tickers)}")
print(f"Unexpected tickers: {list(pred_tickers - set(TICKERS))}")

import collections
counter = collections.Counter(p['symbol'] for p in preds)
dups = [k for k, v in counter.items() if v > 1]
print(f"Duplicate predictions: {len(dups)}")

# 9. VERIFY CURRENT PIPELINE HASH
print("--- PIPELINE PROVENANCE ---")
expected_hash = "f0fa3983b0114cc63635a51f26ef954bd4f64b151ed81673aadadc857717d862"
hashes = collections.Counter(p.get('feature_pipeline_hash') for p in preds)
print(f"Current canonical hash: {expected_hash}")
print(f"Prediction hash distribution: {dict(hashes)}")
print(f"Old hash count: {sum(v for k, v in hashes.items() if k != expected_hash and k)}")
print(f"Unexpected hash count: {sum(v for k, v in hashes.items() if k != expected_hash and k)}")
print(f"Missing hash count: {hashes.get(None, 0)}")

# 11. VERIFY TEMPORAL INTEGRITY
print("--- TEMPORAL INTEGRITY ---")
max_date = None
leakage = 0
for p in preds:
    fv = p.get('feature_vector', {})
    for k, v in fv.items():
        if isinstance(k, str) and ('date' in k or 'timestamp' in k):
            pass # dates in feature names?
        # we can check provenance or actual feature values
    
    # check max date from raw features if available
    prov = p.get('provenance', {})
    if 'max_feature_date' in prov:
        fd = prov['max_feature_date']
        if max_date is None or fd > max_date:
            max_date = fd
        if fd > str(trading_session):
            leakage += 1

print(f"Maximum feature/input date: {max_date}")
print(f"Future-data leakage: {leakage}")

# 12. VERIFY NASDAQ FEATURES
print("--- NASDAQ FEATURES ---")
nasdaq_5d_missing = 0
nasdaq_20d_missing = 0
for p in preds:
    fv = p.get('feature_vector', {})
    if 'nasdaq_ret_5d' not in fv or fv['nasdaq_ret_5d'] is None or pd.isna(fv['nasdaq_ret_5d']):
        nasdaq_5d_missing += 1
    if 'nasdaq_ret_20d' not in fv or fv['nasdaq_ret_20d'] is None or pd.isna(fv['nasdaq_ret_20d']):
        nasdaq_20d_missing += 1
print(f"nasdaq_ret_5d missing/null: {nasdaq_5d_missing}")
print(f"nasdaq_ret_20d missing/null: {nasdaq_20d_missing}")

# 13. VERIFY FEATURE SCHEMA
print("--- FEATURE SCHEMA ---")
if preds:
    dim = len(preds[0].get('feature_vector', {}))
    print(f"Feature dimensionality: {dim}")
    print(f"Schema compatibility: {'PASS' if dim == 57 else 'FAIL'}")

# 14. LIFECYCLE
print("--- LIFECYCLE ---")
states = collections.Counter(p.get('status') for p in preds)
print(f"Lifecycle state distribution: {dict(states)}")

# 18. PIPELINE STATE
print("--- PIPELINE STATE ---")
if run:
    for s in ["START", "ACQUIRE_LOCK", "PREDICTION_GENERATION", "PREDICTION_VALIDATION", "API_HEALTH"]:
        print(f"{s}: {run['stages'].get(s, {}).get('status', 'MISSING')}")

# 19. LOCK STATE
print("--- LOCK STATE ---")
lock = db.pipeline_locks.find_one({"lock_id": "daily_production_lock"})
orphaned = 0
if lock and lock.get('status') == 'RUNNING' and lock.get('run_id') == run['run_id'] and run['status'] == 'SUCCESS':
    orphaned = 1
print(f"Orphaned lock: {orphaned}")

# 20. REGISTRY
print("--- REGISTRY ---")
active_models = list(db.model_registry.find({"status": "ACTIVE"}))
print(f"ACTIVE v1: {len(active_models)}")
r_hashes = collections.Counter(m.get('feature_pipeline_hash') for m in active_models)
print(f"Current hash: {r_hashes.get(expected_hash, 0)}")

# 21. MANIFESTS
print("--- MANIFEST STATE ---")
manifest_dir = r"c:\Users\aryab\Coding\stock_recommendations\saved_models"
manifest_count = 0
invalid_manifests = 0
for t in TICKERS:
    path = os.path.join(manifest_dir, f"{t}_manifest.json")
    if os.path.exists(path):
        manifest_count += 1
        with open(path, 'r') as f:
            data = json.load(f)
            if data.get('feature_pipeline_hash') != expected_hash:
                invalid_manifests += 1
print(f"Manifests found: {manifest_count}")
print(f"Invalid manifests: {invalid_manifests}")

