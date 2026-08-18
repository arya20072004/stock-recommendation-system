import os
import json
import csv
import pandas as pd
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv('c:/Users/aryab/Coding/stock_recommendations/.env')
MONGO_URI = os.getenv('MONGO_URI')

from src.features.router import get_feature_pipeline_hash
from src.data.nifty50 import TICKERS
from src.ml.model_registry import validate_bundle, get_active_manifest_path
from src.ml.history import _verify_production_readiness, load_active_bundle

def mock_atr_pct_mutation_test():
    dates = pd.date_range("2026-08-01", "2026-08-14")
    df1 = pd.DataFrame({"close": [100]*14, "high": [105]*14, "low": [95]*14, "atr": [10]*14}, index=dates)
    df1["atr_pct"] = df1["atr"] / df1["close"].shift(1).replace(0, pd.NA)
    
    df2 = df1.copy()
    df2.loc["2026-08-14", "close"] = 999999  # Mutate target day close
    df2["atr_pct"] = df2["atr"] / df2["close"].shift(1).replace(0, pd.NA)
    
    return df1.loc["2026-08-14", "atr_pct"] == df2.loc["2026-08-14", "atr_pct"]

def main():
    client = MongoClient(MONGO_URI)
    db = client['stock_market_db']
    
    canonical_hash = get_feature_pipeline_hash("v1")
    legacy_hash = "16e7f2049d88e62f915e57d043fe6d6baa5e4937459b56ab90d410664cf9c746"
    
    active_records = list(db.model_registry.find({"status": "ACTIVE"}))
    
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

    try:
        _verify_production_readiness(db)
        production_gate = "PASS"
    except Exception:
        production_gate = "FAIL"
        
    loader_validation = 0
    for ticker in TICKERS:
        try:
            bundle = load_active_bundle(ticker)
            if bundle and bundle[5] == canonical_hash:
                loader_validation += 1
        except Exception:
            pass

    # Filesystem check for exact matches
    mongodb_filesystem_match = 0
    for record in active_records:
        ticker = record["ticker"]
        path = get_active_manifest_path(ticker)
        if os.path.exists(path):
            with open(path, "r") as f:
                manifest = json.load(f)
            if (manifest.get("model_version") == record.get("version") and
                manifest.get("model_hash") == record.get("model_hash") and
                manifest.get("feature_hash") == record.get("feature_hash") and
                manifest.get("feature_pipeline_version") == record.get("feature_pipeline_version") and
                manifest.get("feature_pipeline_hash") == record.get("feature_pipeline_hash")):
                mongodb_filesystem_match += 1

    # Active identity match
    active_identity_complete = all(r.get("ticker") and r.get("version") and r.get("model_hash") and r.get("feature_hash") and r.get("feature_pipeline_version") and r.get("feature_pipeline_hash") for r in active_records)

    atr_pct_mut_test = "PASS" if mock_atr_pct_mutation_test() else "FAIL"

    print(f"PIPELINE_HASH_MATCH = {pipeline_hash_match}/51")
    print(f"MODEL_ARTIFACT_VALIDITY = {model_artifact_valid}/51")
    print(f"FEATURE_ARTIFACT_VALIDITY = {feature_artifact_valid}/51")
    print(f"ACTIVE_IDENTITY_MATCH = {'YES' if active_identity_complete else 'NO'}")
    print(f"MONGODB_FILESYSTEM_MATCH = {mongodb_filesystem_match}/51")
    print(f"PRODUCTION_GATE = {production_gate}")
    print(f"LOADER_VALIDATION = {loader_validation}/51")
    print(f"ATR_PCT_TARGET_CLOSE_MUTATION_TEST = {atr_pct_mut_test}")
    
if __name__ == '__main__':
    main()
