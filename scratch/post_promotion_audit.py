import os
import json
import csv
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv('c:/Users/aryab/Coding/stock_recommendations/.env')
MONGO_URI = os.getenv('MONGO_URI')

from src.features.router import get_feature_pipeline_hash
from src.data.nifty50 import TICKERS
from src.ml.model_registry import validate_bundle, get_active_manifest_path
from src.ml.history import _verify_production_readiness, load_active_bundle

def main():
    client = MongoClient(MONGO_URI)
    db = client['stock_market_db']
    
    canonical_hash = get_feature_pipeline_hash("v1")
    legacy_hash = "16e7f2049d88e62f915e57d043fe6d6baa5e4937459b56ab90d410664cf9c746"
    
    # 2. MONGODB LIVE ACTIVE-STATE AUDIT
    active_records = list(db.model_registry.find({"status": "ACTIVE"}))
    active_mongodb = len(active_records)
    active_mongodb_f489 = sum(1 for r in active_records if r.get("feature_pipeline_hash") == canonical_hash)
    active_mongodb_legacy = sum(1 for r in active_records if r.get("feature_pipeline_hash") == legacy_hash)
    active_mongodb_unexpected = active_mongodb - active_mongodb_f489 - active_mongodb_legacy
    
    # 3. MONGODB ACTIVE IDENTITY INTEGRITY
    active_identity_complete = all(r.get("ticker") and r.get("version") and r.get("model_hash") and r.get("feature_hash") and r.get("feature_pipeline_version") and r.get("feature_pipeline_hash") for r in active_records)
    
    # 4. FILESYSTEM ACTIVE MANIFEST AUDIT
    active_fs = 0
    active_fs_f489 = 0
    active_fs_legacy = 0
    active_fs_unexpected = 0
    mongodb_filesystem_match = 0
    
    for record in active_records:
        ticker = record["ticker"]
        path = get_active_manifest_path(ticker)
        if os.path.exists(path):
            active_fs += 1
            with open(path, "r") as f:
                manifest = json.load(f)
            h = manifest.get("feature_pipeline_hash")
            if h == canonical_hash:
                active_fs_f489 += 1
            elif h == legacy_hash:
                active_fs_legacy += 1
            else:
                active_fs_unexpected += 1
                
            if (manifest.get("model_version") == record.get("version") and
                manifest.get("model_hash") == record.get("model_hash") and
                manifest.get("feature_hash") == record.get("feature_hash") and
                manifest.get("feature_pipeline_version") == record.get("feature_pipeline_version") and
                manifest.get("feature_pipeline_hash") == record.get("feature_pipeline_hash")):
                mongodb_filesystem_match += 1

    # 5, 6, 7. ARTIFACT & CHAIN INTEGRITY
    model_artifact_valid = 0
    feature_artifact_valid = 0
    model_hash_match = 0
    feature_hash_match = 0
    pipeline_hash_match = 0
    complete_chains = 0
    
    for record in active_records:
        ticker = record["ticker"]
        version = record["version"]
        m_hash = record["model_hash"]
        f_hash = record["feature_hash"]
        
        is_valid = validate_bundle(ticker, version, m_hash, f_hash)
        if is_valid:
            model_artifact_valid += 1
            feature_artifact_valid += 1
            model_hash_match += 1
            feature_hash_match += 1
            complete_chains += 1
            
        if record.get("feature_pipeline_hash") == canonical_hash:
            pipeline_hash_match += 1

    # 9. PRODUCTION GATE READ-ONLY VALIDATION
    try:
        _verify_production_readiness(db)
        production_gate = "PASS"
    except Exception:
        production_gate = "FAIL"
        
    # 10. LOADER READ-ONLY VALIDATION
    loader_validation = 0
    for ticker in TICKERS:
        try:
            bundle = load_active_bundle(ticker)
            if bundle and bundle[5] == canonical_hash:
                loader_validation += 1
        except Exception:
            pass

    # 13. RELIANCE.NS CHECK
    rel_rec = next((r for r in active_records if r["ticker"] == "RELIANCE.NS"), None)
    reliance_active = "YES" if rel_rec else "NO"
    reliance_valid = "YES" if rel_rec and rel_rec.get("feature_pipeline_hash") == canonical_hash else "NO"
    
    # 14. PROMOTION PLAN VS LIVE STATE
    plan_to_live_match = 0
    plan_path = "experiments/stock_pcr/selection_policy/promotion_plan.csv"
    with open(plan_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row["ticker"]
            live_rec = next((r for r in active_records if r["ticker"] == ticker), None)
            if live_rec:
                if (live_rec["version"] == row["selected_version"] and
                    live_rec["model_hash"] == row["model_hash"] and
                    live_rec["feature_hash"] == row["feature_hash"] and
                    live_rec["feature_pipeline_version"] == row["feature_pipeline_version"] and
                    live_rec["feature_pipeline_hash"] == row["feature_pipeline_hash"]):
                    plan_to_live_match += 1
                    
    state_b_detected = "YES" if active_mongodb_f489 > 0 and active_fs_f489 < active_mongodb_f489 else "NO"
    mixed_state = "YES" if (active_mongodb_legacy > 0 and active_mongodb_f489 > 0) else "NO"
    
    print(f"ACTIVE_MONGODB = {active_mongodb}")
    print(f"ACTIVE_MONGODB_F489 = {active_mongodb_f489}")
    print(f"ACTIVE_MONGODB_LEGACY = {active_mongodb_legacy}")
    print(f"ACTIVE_MONGODB_UNEXPECTED = {active_mongodb_unexpected}")
    print(f"ACTIVE_FILESYSTEM = {active_fs}")
    print(f"ACTIVE_FILESYSTEM_F489 = {active_fs_f489}")
    print(f"ACTIVE_FILESYSTEM_LEGACY = {active_fs_legacy}")
    print(f"ACTIVE_FILESYSTEM_UNEXPECTED = {active_fs_unexpected}")
    print(f"MONGODB_FILESYSTEM_MATCH = {mongodb_filesystem_match}/51")
    print(f"ACTIVE_IDENTITY_COMPLETE = {'YES' if active_identity_complete else 'NO'}")
    print(f"MODEL_ARTIFACT_VALID = {model_artifact_valid}/51")
    print(f"FEATURE_ARTIFACT_VALID = {feature_artifact_valid}/51")
    print(f"MODEL_HASH_MATCH = {model_hash_match}/51")
    print(f"FEATURE_HASH_MATCH = {feature_hash_match}/51")
    print(f"PIPELINE_HASH_MATCH = {pipeline_hash_match}/51")
    print(f"COMPLETE_ACTIVE_CHAINS = {complete_chains}/51")
    print(f"PLAN_TO_LIVE_IDENTITY_MATCH = {plan_to_live_match}/51")
    print(f"RELIANCE_ACTIVE = {reliance_active}")
    print(f"RELIANCE_VALID = {reliance_valid}")
    print(f"PRODUCTION_GATE = {production_gate}")
    print(f"LOADER_VALIDATION = {loader_validation}/51")
    print(f"STATE_B_DETECTED = {state_b_detected}")
    print(f"MIXED_STATE = {mixed_state}")
    print(f"LEGACY_ACTIVE_REFERENCES = {active_mongodb_legacy + active_fs_legacy}")
    print(f"UNEXPECTED_ACTIVE_REFERENCES = {active_mongodb_unexpected + active_fs_unexpected}")
    
if __name__ == '__main__':
    main()
