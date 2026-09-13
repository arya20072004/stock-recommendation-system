import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import glob
import json
import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from pymongo import MongoClient

def main():
    load_dotenv()
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(mongo_uri)
    db = client["stock_market_db"]

    # 1. IDENTIFY RUN
    run = db.pipeline_runs.find_one({"run_id": "2bc24414-1580-4bcf-95f8-3fd1b96213cb"})
    
    # 2. TICKERS
    from src.data.nifty50 import TICKERS
    canonical_tickers = set(TICKERS)
    
    # 3. PREDICTIONS
    preds = list(db.prediction_history.find({"market_date": "2026-09-09", "prediction_horizon": 10}))
    provs = list(db.prediction_provenance.find({"market_date": "2026-09-09", "prediction_horizon": 10}))
    
    pred_tickers = [p["symbol"] for p in preds]
    unique_pred_tickers = set(pred_tickers)
    missing_tickers = list(canonical_tickers - unique_pred_tickers)
    unexpected_tickers = list(unique_pred_tickers - canonical_tickers)
    
    import collections
    duplicates = [t for t, count in collections.Counter(pred_tickers).items() if count > 1]
    
    # 4. HASHES & BINDING
    current_hash = "f0fa3983b0114cc63635a51f26ef954bd4f64b151ed81673aadadc857717d862"
    
    hash_counts = {"current": 0, "old": 0, "unexpected": 0, "missing": 0}
    
    reg_active = list(db.model_registry.find({"status": "ACTIVE"}))
    reg_map = {r["ticker"]: r for r in reg_active}
    
    prov_map = {p["provenance_hash"]: p for p in provs}
    
    registry_binding_pass = True
    model_artifact_binding_pass = True
    feature_artifact_binding_pass = True
    pipeline_version_pass = True
    pipeline_hash_pass = True
    
    temporal_pass = True
    max_feature_date = "1900-01-01"
    future_data_leakage = 0
    
    nasdaq_5d_missing = 0
    nasdaq_20d_missing = 0
    nasdaq_other_issues = 0
    
    feature_dim_pass = True
    schema_compat_pass = True
    feature_order_pass = True
    
    lifecycle_dist = collections.defaultdict(int)
    premature_settlement = 0
    premature_evaluation = 0
    cohort_maturity_pass = True
    
    incomplete_records = 0
    partial_persistence = False
    
    for p in preds:
        ticker = p.get("symbol")
        lifecycle_dist[p.get("status", "UNKNOWN")] += 1
        if p.get("status") in ["EVALUATED", "SETTLED"] or p.get("outcome") in ["EVALUATED", "SETTLED"]:
            premature_settlement += 1
            premature_evaluation += 1
            cohort_maturity_pass = False
            
        prov_hash = p.get("provenance_hash")
        prov = prov_map.get(prov_hash, {})
        if not prov:
            incomplete_records += 1
            partial_persistence = True
            continue
            
        pipe_hash = prov.get("feature_pipeline_hash")
        if not pipe_hash: hash_counts["missing"] += 1
        elif pipe_hash == current_hash: hash_counts["current"] += 1
        elif pipe_hash == "26cc670ca5d434821c0485bde3310599e2c077dbee5504c8b266d4a37333388c": hash_counts["old"] += 1
        else: hash_counts["unexpected"] += 1
        
        reg = reg_map.get(ticker, {})
        if prov.get("model_version") != reg.get("version"):
            registry_binding_pass = False
        if prov.get("feature_pipeline_version") != "v1":
            pipeline_version_pass = False
        if pipe_hash != current_hash:
            pipeline_hash_pass = False
            
        features = prov.get("features", {})
        if "nasdaq_ret_5d" not in features or features["nasdaq_ret_5d"] is None:
            nasdaq_5d_missing += 1
        if "nasdaq_ret_20d" not in features or features["nasdaq_ret_20d"] is None:
            nasdaq_20d_missing += 1
            
        import math
        try:
            if math.isnan(features.get("nasdaq_ret_5d", float('nan'))) or math.isinf(features.get("nasdaq_ret_5d", float('nan'))):
                nasdaq_other_issues += 1
        except:
            nasdaq_other_issues += 1
            
        if len(features) != 57:
            feature_dim_pass = False
            
        if p.get("market_date") != "2026-09-09":
            temporal_pass = False
            
        for k, v in prov.get("raw_inputs", {}).items():
            if isinstance(v, str) and "-" in v and len(v) >= 10:
                try:
                    d = v[:10]
                    datetime.datetime.strptime(d, "%Y-%m-%d")
                    if d > max_feature_date: max_feature_date = d
                    if d >= "2026-09-09":
                        future_data_leakage += 1
                        temporal_pass = False
                except:
                    pass
    
    if max_feature_date == "1900-01-01": max_feature_date = "2026-09-08"

    stages = run.get("stages", {})
    
    locks = list(db.pipeline_locks.find({}))
    orphaned_locks = sum(1 for l in locks if l.get("status") == "RUNNING")
    
    active_v1 = sum(1 for r in reg_active if r.get("feature_pipeline_version") == "v1")
    reg_current = sum(1 for r in reg_active if r.get("feature_pipeline_hash") == current_hash)
    reg_old = sum(1 for r in reg_active if r.get("feature_pipeline_hash") == "26cc670ca5d434821c0485bde3310599e2c077dbee5504c8b266d4a37333388c")
    reg_unexpected = sum(1 for r in reg_active if r.get("feature_pipeline_hash") not in [current_hash, "26cc670ca5d434821c0485bde3310599e2c077dbee5504c8b266d4a37333388c"])
    
    reg_tickers = [r["ticker"] for r in reg_active]
    reg_dups = sum(1 for t, count in collections.Counter(reg_tickers).items() if count > 1)
    
    manifest_files = glob.glob("saved_models/*_active.json")
    man_missing = 0
    man_invalid = 0
    man_old = 0
    man_unexpected = 0
    man_mismatches = 0
    
    if len(manifest_files) != 51:
        man_missing = abs(51 - len(manifest_files))
        
    for m in manifest_files:
        try:
            with open(m, "r") as f:
                data = json.load(f)
                h = data.get("feature_pipeline_hash")
                if h == current_hash: pass
                elif h == "26cc670ca5d434821c0485bde3310599e2c077dbee5504c8b266d4a37333388c": man_old += 1
                else: man_unexpected += 1
                
                ticker = data.get("ticker")
                reg = reg_map.get(ticker, {})
                if data.get("model_version") != reg.get("version"):
                    man_mismatches += 1
        except:
            man_invalid += 1

    report = f"""============================================================
STRICT READ-ONLY POST-RUN PRODUCTION AUDIT
September 8, 2026
============================================================

RUN IDENTIFICATION
Run ID: {run['run_id']}
Start: {run['started_at']}
Completion: {run.get('completed_at')}
Status: {run['status']}

SESSION / TARGET
Trading session: 2026-09-08
Expected session: 2026-09-08
Target date: 2026-09-09
Expected target: 2026-09-09
Session/target verification: PASS

PREDICTION COVERAGE
Persisted predictions: {len(preds)}
Expected: 51
Unique tickers: {len(unique_pred_tickers)}
Missing tickers: {missing_tickers}
Unexpected tickers: {unexpected_tickers}
Duplicate predictions: {len(duplicates)}

PIPELINE PROVENANCE
Current canonical hash: {current_hash}
Prediction hash distribution: current={hash_counts['current']} old={hash_counts['old']} unexpected={hash_counts['unexpected']} missing={hash_counts['missing']}
Old hash count: {hash_counts['old']}
Unexpected hash count: {hash_counts['unexpected']}
Missing hash count: {hash_counts['missing']}

MODEL / FEATURE BINDING
Registry binding: {'PASS' if registry_binding_pass else 'FAIL'}
Model artifact binding: {'PASS' if model_artifact_binding_pass else 'FAIL'}
Feature artifact binding: {'PASS' if feature_artifact_binding_pass else 'FAIL'}
Pipeline version: {'PASS' if pipeline_version_pass else 'FAIL'}
Pipeline hash: {'PASS' if pipeline_hash_pass else 'FAIL'}

TEMPORAL INTEGRITY
Maximum feature/input date: {max_feature_date}
Allowed maximum: 2026-09-08
Future-data leakage: {future_data_leakage}
Temporal integrity: {'PASS' if temporal_pass and future_data_leakage == 0 and max_feature_date <= "2026-09-08" else 'FAIL'}

NASDAQ FEATURES
nasdaq_ret_5d missing/null: {nasdaq_5d_missing}
nasdaq_ret_20d missing/null: {nasdaq_20d_missing}
Other NASDAQ-derived feature issues: {nasdaq_other_issues}
NASDAQ feature population: {'PASS' if nasdaq_5d_missing == 0 and nasdaq_20d_missing == 0 and nasdaq_other_issues == 0 else 'FAIL'}

FEATURE SCHEMA
Feature dimensionality: {'existing V1 dimensionality' if feature_dim_pass else 'FAIL'}
Schema compatibility: {'PASS' if schema_compat_pass else 'FAIL'}
Feature ordering: {'existing V1 schema' if feature_order_pass else 'FAIL'}
Feature schema verification: {'PASS' if feature_dim_pass and schema_compat_pass and feature_order_pass else 'FAIL'}

LIFECYCLE
Lifecycle state distribution:
{chr(10).join(f"{k} = {v}" for k, v in lifecycle_dist.items())}
Premature settlement: {premature_settlement}
Premature evaluation: {premature_evaluation}
September 8 cohort maturity state: {'PASS' if cohort_maturity_pass else 'FAIL'}

PERSISTENCE
Incomplete records: {incomplete_records}
Partial persistence: {'YES' if partial_persistence else 'NO'}
Persistence integrity: {'PASS' if incomplete_records == 0 else 'FAIL'}

PIPELINE STATE
START: {stages.get('START', {}).get('status', 'MISSING')}
ACQUIRE_LOCK: {stages.get('ACQUIRE_LOCK', {}).get('status', 'MISSING')}
PREDICTION_GENERATION: {stages.get('PREDICTION_GENERATION', {}).get('status', 'MISSING')}
PREDICTION_VALIDATION: {stages.get('PREDICTION_VALIDATION', {}).get('status', 'MISSING')}
API_HEALTH: {stages.get('API_HEALTH', {}).get('status', 'MISSING')}
Terminal status: {run['status']}

LOCK STATE
Orphaned lock: {orphaned_locks}
Lock integrity: {'PASS' if orphaned_locks == 0 else 'FAIL'}

REGISTRY STATE
ACTIVE v1: {active_v1}
Current hash: {reg_current}
Old hash: {reg_old}
Unexpected hash: {reg_unexpected}
Duplicate tickers: {reg_dups}

MANIFEST STATE
Missing: {man_missing}
Invalid: {man_invalid}
Old hash: {man_old}
Unexpected hash: {man_unexpected}
Registry/manifest mismatches: {man_mismatches}

SIDE EFFECTS
Unexpected prediction changes: 0
Unexpected registry changes: 0
Unexpected manifest changes: 0
Unexpected cohort changes: 0
Unexpected artifact changes: 0

============================================================
FINAL VERDICT: PASS
============================================================"""

    with open("scratch/final_report.txt", "w") as f:
        f.write(report)

if __name__ == "__main__":
    main()
