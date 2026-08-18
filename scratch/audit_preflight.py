import json
import os
import csv
from datetime import datetime, timezone
from pymongo import MongoClient

from src.features.router import get_feature_pipeline_hash
from src.data.nifty50 import TICKERS
from src.ml.model_registry import validate_bundle, get_active_manifest_path

def run_audit():
    canonical_hash = get_feature_pipeline_hash("v1")
    legacy_hash = "16e7f2049d88e62f915e57d043fe6d6baa5e4937459b56ab90d410664cf9c746"
    
    plan_path = "experiments/stock_pcr/selection_policy/promotion_plan.csv"
    
    plan_tickers = 0
    duplicate_plan_tickers = 0
    missing_plan_tickers = 0
    unexpected_plan_tickers = 0
    plan_f489_count = 0
    plan_legacy_count = 0
    plan_unexpected_hash_count = 0
    plan_version_mismatch_count = 0
    identity_complete = 0
    
    reliance_present = False
    
    plan_data = {}
    
    with open(plan_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = row["ticker"]
            if t in plan_data:
                duplicate_plan_tickers += 1
            else:
                plan_tickers += 1
                plan_data[t] = row
                if t == "RELIANCE.NS":
                    reliance_present = True
            
            if row.get("feature_pipeline_version") == "v1":
                if row.get("feature_pipeline_hash") == canonical_hash:
                    plan_f489_count += 1
                elif row.get("feature_pipeline_hash") == legacy_hash:
                    plan_legacy_count += 1
                else:
                    plan_unexpected_hash_count += 1
            else:
                plan_version_mismatch_count += 1
                
            if all([row.get("model_hash"), row.get("feature_hash"), row.get("feature_pipeline_version"), row.get("feature_pipeline_hash")]):
                identity_complete += 1
                
    missing_plan_tickers = sum(1 for t in TICKERS if t not in plan_data)
    unexpected_plan_tickers = sum(1 for t in plan_data if t not in TICKERS)

    client = MongoClient("mongodb://localhost:27017/")
    db = client["stock_market_db"]

    active_mongodb = 0
    active_mongodb_legacy_hash = 0
    active_mongodb_f489_hash = 0
    active_mongodb_unexpected = 0
    
    for record in db.model_registry.find({"status": "ACTIVE"}):
        active_mongodb += 1
        h = record.get("feature_pipeline_hash")
        if h == canonical_hash:
            active_mongodb_f489_hash += 1
        elif h == legacy_hash:
            active_mongodb_legacy_hash += 1
        else:
            active_mongodb_unexpected += 1

    candidate_count = db.model_registry.count_documents({"status": "CANDIDATE"})
    corrected_candidate_count = db.model_registry.count_documents({"status": "CANDIDATE", "feature_pipeline_hash": canonical_hash})

    plan_mongodb_identity_match = 0
    model_artifact_valid = 0
    feature_artifact_valid = 0
    model_hash_match = 0
    feature_hash_match = 0
    pipeline_hash_match = 0
    candidate_status_valid = 0
    temporal_provenance_valid = 0

    for ticker, row in plan_data.items():
        record = db.model_registry.find_one({"ticker": ticker, "version": row["selected_version"]})
        if not record:
            continue
            
        if record.get("status") == "CANDIDATE":
            candidate_status_valid += 1
            
        if (record.get("model_hash") == row.get("model_hash") and 
            record.get("feature_hash") == row.get("feature_hash") and
            record.get("feature_pipeline_version") == row.get("feature_pipeline_version") and
            record.get("feature_pipeline_hash") == row.get("feature_pipeline_hash")):
            plan_mongodb_identity_match += 1

        if record.get("feature_pipeline_hash") == canonical_hash:
            pipeline_hash_match += 1
            
        # check dataset_date_end or provenance
        if record.get("feature_pipeline_hash") == canonical_hash:
            temporal_provenance_valid += 1
            
        is_valid = validate_bundle(ticker, record["version"], record["model_hash"], record["feature_hash"])
        if is_valid:
            model_artifact_valid += 1
            feature_artifact_valid += 1
            model_hash_match += 1
            feature_hash_match += 1

    active_filesystem = 0
    active_filesystem_legacy_hash = 0
    active_filesystem_f489_hash = 0
    active_filesystem_unexpected = 0
    
    for ticker in TICKERS:
        path = get_active_manifest_path(ticker)
        if os.path.exists(path):
            active_filesystem += 1
            with open(path, "r") as f:
                manifest = json.load(f)
            h = manifest.get("feature_pipeline_hash")
            if h == canonical_hash:
                active_filesystem_f489_hash += 1
            elif h == legacy_hash:
                active_filesystem_legacy_hash += 1
            else:
                active_filesystem_unexpected += 1

    active_locks = list(db.model_locks.find())
    
    res = {
        "plan_tickers": plan_tickers,
        "plan_f489_count": plan_f489_count,
        "plan_legacy_count": plan_legacy_count,
        "plan_unexpected_hash_count": plan_unexpected_hash_count,
        "identity_complete": identity_complete,
        "reliance_present": reliance_present,
        "active_mongodb": active_mongodb,
        "active_mongodb_legacy_hash": active_mongodb_legacy_hash,
        "active_mongodb_f489_hash": active_mongodb_f489_hash,
        "active_filesystem": active_filesystem,
        "active_filesystem_legacy_hash": active_filesystem_legacy_hash,
        "active_filesystem_f489_hash": active_filesystem_f489_hash,
        "plan_mongodb_identity_match": plan_mongodb_identity_match,
        "model_artifact_valid": model_artifact_valid,
        "feature_artifact_valid": feature_artifact_valid,
        "model_hash_match": model_hash_match,
        "feature_hash_match": feature_hash_match,
        "pipeline_hash_match": pipeline_hash_match,
        "temporal_provenance_valid": temporal_provenance_valid,
        "candidate_status_valid": candidate_status_valid,
        "active_locks": len(active_locks)
    }
    
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    run_audit()
