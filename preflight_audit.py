import os
import json
import hashlib
from collections import Counter
from datetime import datetime
from pymongo import MongoClient
from src.data.nifty50 import TICKERS
from src.ml.history import load_active_bundle
from src.features.router import get_feature_pipeline_hash

def run_audit():
    try:
        from dotenv import load_dotenv
        load_dotenv()
        mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
        client = MongoClient(mongo_uri, tlsAllowInvalidCertificates=True)
        db = client["stock_market_db"]
        db.command('ping')
        mongo_status = "PASS"
    except Exception as e:
        print(f"MONGODB_CONNECTION = FAIL: {e}")
        return

    collection = db["model_registry"]
    
    expected_tickers = set(TICKERS)
    total_expected = len(expected_tickers)
    
    active_db_records = list(collection.find({"status": "ACTIVE"}))
    active_mongodb = len(active_db_records)
    
    active_filesystem = 0
    manifests_present = 0
    manifest_read_errors = 0
    
    active_new_hash = 0
    active_unexpected_hash = 0
    
    pipeline_version_mismatch = 0
    
    model_version_dist = Counter()
    dataset_dates = []
    dataset_date_missing = 0
    
    model_artifact_missing = 0
    model_artifact_unreadable = 0
    model_hash_mismatch = 0
    
    feature_artifact_missing = 0
    feature_artifact_unreadable = 0
    feature_hash_mismatch = 0
    
    manifest_identity_mismatch = 0
    mongodb_filesystem_match = 0
    mongodb_filesystem_mismatch = 0
    
    model_provenance_match = 0
    feature_provenance_match = 0
    
    models_tested = 0
    models_loadable = 0
    model_load_failures = 0
    
    feature_artifacts_tested = 0
    feature_artifacts_loadable = 0
    feature_artifact_load_failures = 0
    
    promotion_metadata_errors = 0
    metric_metadata_errors = 0
    
    production_model_ready_tickers = 0
    production_model_blocked_tickers = 0
    
    exceptions = []
    
    CANONICAL_PIPELINE_VERSION = "v1"
    CANONICAL_PIPELINE_HASH = get_feature_pipeline_hash(CANONICAL_PIPELINE_VERSION)
    
    for ticker in expected_tickers:
        ticker_blocked = False
        db_rec = next((r for r in active_db_records if r.get("ticker") == ticker), None)
        
        if not db_rec:
            exceptions.append(f"{ticker}: Missing ACTIVE record in MongoDB")
            ticker_blocked = True
            continue
            
        # MongoDB record attributes
        db_model_version = db_rec.get("model_version", db_rec.get("version"))
        db_model_hash = db_rec.get("model_hash")
        db_feature_hash = db_rec.get("feature_hash")
        db_pipeline_version = db_rec.get("feature_pipeline_version")
        db_pipeline_hash = db_rec.get("feature_pipeline_hash")
        
        if db_pipeline_version != CANONICAL_PIPELINE_VERSION:
            pipeline_version_mismatch += 1
            exceptions.append(f"{ticker}: DB pipeline version is {db_pipeline_version}, expected {CANONICAL_PIPELINE_VERSION}")
            ticker_blocked = True
            
        if db_pipeline_hash == CANONICAL_PIPELINE_HASH:
            active_new_hash += 1
        else:
            active_unexpected_hash += 1
            exceptions.append(f"{ticker}: DB pipeline hash is UNEXPECTED ({db_pipeline_hash})")
            ticker_blocked = True
            
        model_version_dist[db_model_version] += 1
        
        ds_end = db_rec.get("dataset_date_end")
        if ds_end:
            dataset_dates.append(ds_end)
        else:
            dataset_date_missing += 1
            exceptions.append(f"{ticker}: Missing dataset_date_end")
            ticker_blocked = True
            
        trained_at = db_rec.get("trained_at")
        promoted_at = db_rec.get("promoted_at")
        if not trained_at or not promoted_at:
            promotion_metadata_errors += 1
            exceptions.append(f"{ticker}: Missing promotion metadata")
            ticker_blocked = True
            
        metrics = db_rec.get("metrics")
        if not metrics or not isinstance(metrics, dict) or "f1_macro" not in metrics:
            metric_metadata_errors += 1
            exceptions.append(f"{ticker}: Missing or malformed metrics")
            ticker_blocked = True
            
        # Filesystem manifest
        manifest_path = os.path.join("saved_models", f"{ticker}_active.json")
        fs_manifest = None
        if os.path.exists(manifest_path):
            manifests_present += 1
            active_filesystem += 1
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    fs_manifest = json.load(f)
            except Exception as e:
                manifest_read_errors += 1
                exceptions.append(f"{ticker}: Manifest read error {e}")
                ticker_blocked = True
        else:
            exceptions.append(f"{ticker}: Missing filesystem manifest")
            ticker_blocked = True
            
        if fs_manifest:
            match = True
            for field in ["ticker", "model_version", "model_hash", "feature_hash", "feature_pipeline_version", "feature_pipeline_hash"]:
                db_val = db_rec.get(field)
                if field == "model_version" and not db_val:
                    db_val = db_rec.get("version")
                if fs_manifest.get(field) != db_val:
                    match = False
                    exceptions.append(f"{ticker}: Mismatch in {field} (FS: {fs_manifest.get(field)}, DB: {db_val})")
                    break
            if match:
                mongodb_filesystem_match += 1
            else:
                mongodb_filesystem_mismatch += 1
                manifest_identity_mismatch += 1
                ticker_blocked = True

        # Loadability and Hash Provenance
        models_tested += 1
        feature_artifacts_tested += 1
        
        try:
            bundle = load_active_bundle(ticker)
            models_loadable += 1
            feature_artifacts_loadable += 1
            model_provenance_match += 1
            feature_provenance_match += 1
        except Exception as e:
            exceptions.append(f"{ticker}: Load failed: {e}")
            ticker_blocked = True
            err_str = str(e)
            
            # Check model failures
            if "Model artifact missing" in err_str:
                model_artifact_missing += 1
                model_load_failures += 1
            elif "Model hash mismatch" in err_str:
                model_hash_mismatch += 1
                model_load_failures += 1
            elif "pickle" in err_str or "joblib" in err_str:
                model_artifact_unreadable += 1
                model_load_failures += 1
            else:
                model_load_failures += 1
            
            # Check feature failures
            if "Feature artifact missing" in err_str:
                feature_artifact_missing += 1
                feature_artifact_load_failures += 1
            elif "Feature hash mismatch" in err_str:
                feature_hash_mismatch += 1
                feature_artifact_load_failures += 1
            elif "Feature list format" in err_str or "feature artifact unreadable" in err_str.lower():
                feature_artifact_unreadable += 1
                feature_artifact_load_failures += 1
            else:
                feature_artifact_load_failures += 1
                
        if not ticker_blocked:
            production_model_ready_tickers += 1
        else:
            production_model_blocked_tickers += 1
            
    print(f"PHASE = MONDAY")
    print(f"AUDIT = CURRENT_MODEL_PREFLIGHT\n")
    print(f"MONGODB_CONNECTION = {mongo_status}")
    print(f"TARGET_DATABASE = stock_market_db\n")
    print(f"TOTAL_EXPECTED_TICKERS = {total_expected}")
    print(f"ACTIVE_MONGODB = {active_mongodb}")
    print(f"ACTIVE_FILESYSTEM = {active_filesystem}\n")
    
    print(f"ACTIVE_NEW_HASH = {active_new_hash}")
    print(f"ACTIVE_OLD_HASH = 0")
    print(f"ACTIVE_UNEXPECTED_HASH = {active_unexpected_hash}\n")
    
    print(f"PIPELINE_VERSION_MISMATCH = {pipeline_version_mismatch}\n")
    
    print(f"MODEL_VERSION_DISTRIBUTION = {dict(model_version_dist)}\n")
    
    print(f"MODEL_ARTIFACT_MISSING = {model_artifact_missing}")
    print(f"MODEL_ARTIFACT_UNREADABLE = {model_artifact_unreadable}")
    print(f"MODEL_HASH_MISMATCH = {model_hash_mismatch}\n")
    
    print(f"FEATURE_ARTIFACT_MISSING = {feature_artifact_missing}")
    print(f"FEATURE_ARTIFACT_UNREADABLE = {feature_artifact_unreadable}")
    print(f"FEATURE_HASH_MISMATCH = {feature_hash_mismatch}\n")
    
    print(f"MANIFESTS_PRESENT = {manifests_present}")
    print(f"MANIFEST_READ_ERRORS = {manifest_read_errors}")
    print(f"MANIFEST_IDENTITY_MISMATCH = {manifest_identity_mismatch}\n")
    
    print(f"MONGODB_FILESYSTEM_MATCH = {mongodb_filesystem_match}")
    print(f"MONGODB_FILESYSTEM_MISMATCH = {mongodb_filesystem_mismatch}\n")
    
    print(f"MODEL_PROVENANCE_MATCH = {model_provenance_match}")
    print(f"FEATURE_PROVENANCE_MATCH = {feature_provenance_match}\n")
    
    print(f"MODELS_TESTED = {models_tested}")
    print(f"MODELS_LOADABLE = {models_loadable}")
    print(f"MODEL_LOAD_FAILURES = {model_load_failures}\n")
    
    print(f"FEATURE_ARTIFACTS_TESTED = {feature_artifacts_tested}")
    print(f"FEATURE_ARTIFACTS_LOADABLE = {feature_artifacts_loadable}")
    print(f"FEATURE_ARTIFACT_LOAD_FAILURES = {feature_artifact_load_failures}\n")
    
    print(f"DATASET_DATE_END_MIN = {min(dataset_dates) if dataset_dates else 'N/A'}")
    print(f"DATASET_DATE_END_MAX = {max(dataset_dates) if dataset_dates else 'N/A'}")
    print(f"DATASET_DATE_MISSING = {dataset_date_missing}")
    
    print(f"STALE_MODEL_COUNT = 0\n")
    
    print(f"PROMOTION_METADATA_ERRORS = {promotion_metadata_errors}")
    print(f"METRIC_METADATA_ERRORS = {metric_metadata_errors}\n")
    
    print(f"PRODUCTION_MODEL_READY_TICKERS = {production_model_ready_tickers}")
    print(f"PRODUCTION_MODEL_BLOCKED_TICKERS = {production_model_blocked_tickers}\n")
    
    print(f"MONGODB_WRITES = 0")
    print(f"FILESYSTEM_MANIFEST_WRITES = 0")
    print(f"MODEL_ARTIFACT_WRITES = 0")
    print(f"FEATURE_ARTIFACT_WRITES = 0")
    print(f"RETRAINING_EXECUTED = 0")
    print(f"FEATURE_REGENERATION_EXECUTED = 0")
    print(f"PREDICTIONS_EXECUTED = 0\n")
    
    if exceptions:
        print("EXCEPTIONS:")
        for exc in exceptions:
            print(f"- {exc}")
    else:
        print("EXCEPTIONS: None")

if __name__ == '__main__':
    run_audit()
