import json
import os
import sys
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv('c:/Users/aryab/Coding/stock_recommendations/.env')
MONGO_URI = os.getenv('MONGO_URI')

from src.features.router import get_feature_pipeline_hash
from src.data.nifty50 import TICKERS
from src.ml.model_registry import reconcile_all_manifests, validate_bundle, get_active_manifest_path
from src.ml.history import _verify_production_readiness, load_active_bundle

def main():
    client = MongoClient(MONGO_URI)
    db = client['stock_market_db']
    canonical_hash = get_feature_pipeline_hash('v1')
    
    # 1. Reconciliation
    reconciled = reconcile_all_manifests(db)
    print(f"RECONCILIATION_STATUS = {'PASS' if reconciled else 'FAIL'}")
    
    # 2. Production Gate
    try:
        _verify_production_readiness(db)
        print("PRODUCTION_GATE_STATUS = PASS")
    except Exception as e:
        print(f"PRODUCTION_GATE_STATUS = FAIL ({e})")
        
    # 3. Loader Validation
    loader_validation = 0
    active_mongodb_f489 = 0
    active_mongodb_legacy = 0
    
    for record in db.model_registry.find({"status": "ACTIVE"}):
        if record.get("feature_pipeline_hash") == canonical_hash:
            active_mongodb_f489 += 1
        else:
            active_mongodb_legacy += 1
            
    active_fs_f489 = 0
    active_fs_legacy = 0
    
    for ticker in TICKERS:
        try:
            bundle = load_active_bundle(ticker)
            if bundle:
                if bundle[5] == canonical_hash:
                    loader_validation += 1
        except Exception:
            pass
            
        path = get_active_manifest_path(ticker)
        if os.path.exists(path):
            with open(path, "r") as f:
                manifest = json.load(f)
            if manifest.get("feature_pipeline_hash") == canonical_hash:
                active_fs_f489 += 1
            else:
                active_fs_legacy += 1
                
    print(f"ACTIVE_MONGODB_F489_HASH = {active_mongodb_f489}")
    print(f"ACTIVE_MONGODB_LEGACY_HASH = {active_mongodb_legacy}")
    print(f"ACTIVE_FILESYSTEM_F489_HASH = {active_fs_f489}")
    print(f"ACTIVE_FILESYSTEM_LEGACY_HASH = {active_fs_legacy}")
    print(f"LOADER_VALIDATION = {loader_validation}/51")
    
if __name__ == '__main__':
    main()
