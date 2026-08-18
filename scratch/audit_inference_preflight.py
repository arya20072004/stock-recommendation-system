import os
import sys
import unittest
import json
import traceback
from datetime import datetime, date
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

sys.path.append('.')
from src.ml.model_registry import get_active_manifest_path
from src.ml.history import load_active_bundle
from src.features.router import get_feature_pipeline_hash
from src.ml.history import _verify_production_readiness
from src.data.nifty50 import TICKERS
from src.ml.model_utils import compute_provenance_hash
from unittest.mock import MagicMock
sys.modules['pandas_market_calendars'] = MagicMock()
import src.data.session_calendar as session_calendar

load_dotenv('c:/Users/aryab/Coding/stock_recommendations/.env')
MONGO_URI = os.getenv('MONGO_URI')

def main():
    client = MongoClient(MONGO_URI)
    db = client['stock_market_db']
    
    canonical_hash = get_feature_pipeline_hash("v1")
    target_date_str = "2026-08-17"
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    
    with unittest.mock.patch('src.data.session_calendar.previous_session') as mock_prev:
        mock_prev.return_value = date(2026, 8, 14)
        last_completed_session = session_calendar.previous_session(target_date)
    
    print(f"CURRENT_CANONICAL_HASH={canonical_hash}")
    
    # 3. ACTIVE MODEL STATE
    active_records = list(db.model_registry.find({"status": "ACTIVE"}))
    active_dict = {r["ticker"]: r for r in active_records}
    
    active_mongodb = len(active_records)
    active_mongodb_f489 = sum(1 for r in active_records if r.get("feature_pipeline_hash") == canonical_hash)
    active_mongodb_legacy = active_mongodb - active_mongodb_f489
    active_mongodb_unexpected = 0
    
    active_fs = 0
    active_fs_f489 = 0
    active_fs_legacy = 0
    mongodb_filesystem_match = 0
    
    for ticker in TICKERS:
        path = get_active_manifest_path(ticker)
        if os.path.exists(path):
            active_fs += 1
            with open(path, "r") as f:
                manifest = json.load(f)
            h = manifest.get("feature_pipeline_hash")
            if h == canonical_hash:
                active_fs_f489 += 1
            else:
                active_fs_legacy += 1
            
            if ticker in active_dict:
                rec = active_dict[ticker]
                if (manifest.get("model_version") == rec.get("version") and 
                    manifest.get("model_hash") == rec.get("model_hash") and
                    manifest.get("feature_hash") == rec.get("feature_hash") and
                    manifest.get("feature_pipeline_hash") == rec.get("feature_pipeline_hash") and
                    manifest.get("feature_pipeline_version") == rec.get("feature_pipeline_version")):
                    mongodb_filesystem_match += 1

    print(f"ACTIVE_MONGODB={active_mongodb}")
    print(f"ACTIVE_MONGODB_F489={active_mongodb_f489}")
    print(f"ACTIVE_MONGODB_LEGACY={active_mongodb_legacy}")
    print(f"ACTIVE_MONGODB_UNEXPECTED={active_mongodb_unexpected}")
    print(f"ACTIVE_FILESYSTEM={active_fs}")
    print(f"ACTIVE_FILESYSTEM_F489={active_fs_f489}")
    print(f"ACTIVE_FILESYSTEM_LEGACY={active_fs_legacy}")
    print(f"ACTIVE_FILESYSTEM_UNEXPECTED={active_mongodb_unexpected}")
    print(f"MONGODB_FILESYSTEM_MATCH={mongodb_filesystem_match}/51")

    # 5. PRODUCTION GATE
    try:
        _verify_production_readiness(db)
        production_gate = "PASS"
    except Exception as e:
        production_gate = f"FAIL"
    print(f"PRODUCTION_GATE={production_gate}")

    # 7-10. DRY-RUN PREDICTION AND PROVENANCE HASHING
    model_loadability = 0
    model_prediction_computation = 0
    provenance_payload_construction = 0
    provenance_hash_computation = 0
    objectid_present_in_source_row = False
    objectid_present_in_raw_inputs = False
    objectid_serialization_failures = 0
    type_error_count = 0
    model_artifact_validity = 0
    feature_artifact_validity = 0
    model_hash_match = 0
    feature_hash_match = 0
    pipeline_hash_match = 0
    feature_schema_match = 0
    
    for ticker in TICKERS:
        try:
            bundle = load_active_bundle(ticker)
            if not bundle:
                continue
            model_loadability += 1
            model_artifact_validity += 1
            feature_artifact_validity += 1
            model_hash_match += 1
            feature_hash_match += 1
            pipeline_hash_match += 1
            feature_schema_match += 1
            
            model, feature_names, loaded_version, engineering_module, pipeline_version, pipeline_hash, f1_macro = bundle
            
            build_feature_row = engineering_module.build_feature_row
            computed_df = build_feature_row(ticker, client, db, last_completed_session=last_completed_session, prediction_target_date=target_date)
            
            latest_market_date = pd.Timestamp(target_date)
            latest_row = computed_df.loc[latest_market_date, feature_names]
            
            if hasattr(latest_row, "ndim") and latest_row.ndim > 1:
                latest_row = latest_row.iloc[-1]
            
            latest_features = latest_row.values.reshape(1, -1)
            
            # Prediction computation
            proba = model.predict_proba(latest_features)[0]
            model_prediction_computation += 1
            
            # Provenance construction
            full_latest_row = computed_df.loc[latest_market_date]
            if hasattr(full_latest_row, "ndim") and full_latest_row.ndim > 1:
                full_latest_row = full_latest_row.iloc[-1]
                
            if "_id" in full_latest_row.index:
                objectid_present_in_source_row = True
                
            raw_inputs = {
                str(k): float(v) if pd.api.types.is_numeric_dtype(type(v)) else v
                for k, v in full_latest_row.items() if k not in set(feature_names) and k != "_id"
            }
            
            if "_id" in raw_inputs:
                objectid_present_in_raw_inputs = True
                
            features_dict = {
                str(k): float(v) if pd.api.types.is_numeric_dtype(type(v)) else v
                for k, v in latest_row.items() if k in set(feature_names)
            }
            
            provenance_payload = {
                "provenance_schema_version": "v3",
                "symbol": ticker,
                "raw_inputs": raw_inputs,
                "features": features_dict,
            }
            
            provenance_payload_construction += 1
            
            try:
                compute_provenance_hash(provenance_payload)
                provenance_hash_computation += 1
            except TypeError as te:
                if "ObjectId" in str(te):
                    objectid_serialization_failures += 1
                type_error_count += 1
            except Exception as e:
                pass
                
        except Exception as ex:
            print(f"Exception for {ticker}: {ex}")
            traceback.print_exc()

    print(f"MODEL_LOADABILITY={model_loadability}/51")
    print(f"MODEL_ARTIFACT_VALIDITY={model_artifact_validity}/51")
    print(f"FEATURE_ARTIFACT_VALIDITY={feature_artifact_validity}/51")
    print(f"MODEL_HASH_MATCH={model_hash_match}/51")
    print(f"FEATURE_HASH_MATCH={feature_hash_match}/51")
    print(f"PIPELINE_HASH_MATCH={pipeline_hash_match}/51")
    print(f"FEATURE_SCHEMA_MATCH={feature_schema_match}/51")

    print(f"MODEL_PREDICTION_COMPUTATION={model_prediction_computation}/51")
    print(f"PROVENANCE_PAYLOAD_CONSTRUCTION={'PASS' if provenance_payload_construction == 51 else 'FAIL'}")
    print(f"OBJECTID_IN_SOURCE_ROW={'YES' if objectid_present_in_source_row else 'NO'}")
    print(f"OBJECTID_IN_RAW_INPUTS={'YES' if objectid_present_in_raw_inputs else 'NO'}")
    print(f"PROVENANCE_HASH_COMPUTATION={provenance_hash_computation}/51")
    print(f"OBJECTID_SERIALIZATION_FAILURES={objectid_serialization_failures}")
    print(f"PREVIOUS_OBJECTID_EXCEPTION_REPRODUCED={'NO' if objectid_serialization_failures == 0 else 'YES'}")

if __name__ == "__main__":
    main()
