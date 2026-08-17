import argparse
import json
import os
import sys
from pymongo import MongoClient
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

OLD_HASH = "685cb3dbe63d7923126e44c597914c93a7bcebc83c6f6e42017dd1101f7d2c68"
NEW_HASH = "16e7f2049d88e62f915e57d043fe6d6baa5e4937459b56ab90d410664cf9c746"
PIPELINE_VERSION = "v1"

try:
    from src.ml.model_registry import sync_manifest
    from src.data.nifty50 import TICKERS
except ImportError as e:
    print(f"Migration dependency import failed: {e}")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Phase 2B Manifest Hash Migration")
    parser.add_argument("--dry-run", action="store_true", help="Perform a read-only validation")
    parser.add_argument("--apply", action="store_true", help="Execute the migration (OLD->NEW)")
    parser.add_argument("--rollback", action="store_true", help="Execute the rollback (NEW->OLD)")
    args = parser.parse_args()
    
    if not args.dry_run and not args.apply and not args.rollback:
        args.dry_run = True
        print("Defaulting to --dry-run")
        
    try:
        from dotenv import load_dotenv
        load_dotenv()
        mongo_uri = os.getenv("MONGO_URI")
        if not mongo_uri:
            print("MongoDB unavailable: MONGO_URI missing in environment")
            sys.exit(1)
            
        client = MongoClient(
            mongo_uri, 
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=10000
        )
        client.admin.command('ping')
        db = client["stock_market_db"]
        
        if "model_registry" not in db.list_collection_names():
            print("MongoDB unavailable: model_registry collection not found")
            sys.exit(1)
            
    except Exception as e:
        print(f"MongoDB unavailable: {e}")
        sys.exit(1)
        
    if len(TICKERS) != 51:
        print("MongoDB unavailable: TICKERS length is not 51")
        sys.exit(1)
        
    stats = {
        "TOTAL_ACTIVE_TICKERS": len(TICKERS),
        "ELIGIBLE": 0,
        "INELIGIBLE": 0,
        "ALREADY_MIGRATED": 0,
        "MIGRATED": 0,
        "RECOVERED": 0,
        "UNEXPECTED_STATES": 0,
        "MISSING_RECORDS": 0,
        "SYNC_FAILURES": 0,
        "SPLIT_BRAIN": 0,
        "ROLLBACK_FAILURES": 0,
        
        "MONGODB_OLD_HASH": 0,
        "MONGODB_NEW_HASH": 0,
        "MONGODB_UNEXPECTED_HASH": 0,
        "MONGODB_MISSING": 0,
        "MONGODB_DUPLICATE": 0,
        
        "FILESYSTEM_OLD_HASH": 0,
        "FILESYSTEM_NEW_HASH": 0,
        "FILESYSTEM_UNEXPECTED_HASH": 0,
        "FILESYSTEM_MISSING": 0,
        
        "MONGODB_FILESYSTEM_MATCH": 0,
        "MONGODB_FILESYSTEM_MISMATCH": 0,
        
        "MODEL_ARTIFACT_MISSING": 0,
        "FEATURE_ARTIFACT_MISSING": 0,
        
        "MUTATIONS_PERFORMED": 0,
        "MONGODB_WRITES": 0,
        "PRODUCTION_FILESYSTEM_WRITES": 0,
        "ARTIFACT_WRITES": 0,
        "AUDIT_SNAPSHOT_WRITES": 0
    }
    
    source_hash = OLD_HASH if args.apply or args.dry_run else NEW_HASH
    target_hash = NEW_HASH if args.apply or args.dry_run else OLD_HASH
    
    snapshot = []
    
    for ticker in TICKERS:
        m = f"saved_models/{ticker}_active.json"
        
        fs_exists = os.path.exists(m)
        if not fs_exists:
            print(f"{ticker}: Missing filesystem manifest.")
            stats["FILESYSTEM_MISSING"] += 1
            
        data = {}
        if fs_exists:
            try:
                with open(m, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except Exception as e:
                print(f"Failed to read {m}: {e}")
                
        fs_hash = data.get("feature_pipeline_hash")
        if fs_exists and data:
            if fs_hash == OLD_HASH:
                stats["FILESYSTEM_OLD_HASH"] += 1
            elif fs_hash == NEW_HASH:
                stats["FILESYSTEM_NEW_HASH"] += 1
            else:
                stats["FILESYSTEM_UNEXPECTED_HASH"] += 1
        
        records = list(db.model_registry.find({"ticker": ticker, "status": "ACTIVE"}))
        rec = {}
        if len(records) == 0:
            print(f"{ticker}: Missing MongoDB ACTIVE record.")
            stats["MONGODB_MISSING"] += 1
        elif len(records) > 1:
            print(f"{ticker}: Duplicate ACTIVE MongoDB records.")
            stats["MONGODB_DUPLICATE"] += 1
        else:
            rec = records[0]
            
        db_hash = rec.get("feature_pipeline_hash")
        if len(records) == 1:
            if db_hash == OLD_HASH:
                stats["MONGODB_OLD_HASH"] += 1
            elif db_hash == NEW_HASH:
                stats["MONGODB_NEW_HASH"] += 1
            else:
                stats["MONGODB_UNEXPECTED_HASH"] += 1
                
        # Field-by-field check
        match = True
        mismatches = []
        
        if data.get("ticker") != rec.get("ticker"):
            match = False
            mismatches.append(f"ticker: FS={data.get('ticker')} != DB={rec.get('ticker')}")
        if data.get("model_version") != rec.get("version"):
            match = False
            mismatches.append(f"version: FS={data.get('model_version')} != DB={rec.get('version')}")
        if data.get("model_hash") != rec.get("model_hash"):
            match = False
            mismatches.append(f"model_hash: FS={data.get('model_hash')} != DB={rec.get('model_hash')}")
        if data.get("feature_hash") != rec.get("feature_hash"):
            match = False
            mismatches.append(f"feature_hash: FS={data.get('feature_hash')} != DB={rec.get('feature_hash')}")
        if data.get("feature_pipeline_version") != rec.get("feature_pipeline_version"):
            match = False
            mismatches.append(f"feature_pipeline_version: FS={data.get('feature_pipeline_version')} != DB={rec.get('feature_pipeline_version')}")
        if fs_hash != db_hash:
            match = False
            mismatches.append(f"feature_pipeline_hash: FS={fs_hash} != DB={db_hash}")
            
        if not match or not fs_exists or len(records) != 1 or not data:
            if mismatches:
                print(f"{ticker} Mismatch: " + ", ".join(mismatches))
            stats["MONGODB_FILESYSTEM_MISMATCH"] += 1
            stats["INELIGIBLE"] += 1
            continue
            
        stats["MONGODB_FILESYSTEM_MATCH"] += 1
        
        mv = data.get("model_version")
        model_path = f"saved_models/model_{ticker}_{mv}.joblib"
        feat_path = f"saved_features/features_{ticker}_{mv}.json"
        
        if not os.path.exists(model_path):
            stats["MODEL_ARTIFACT_MISSING"] += 1
            stats["INELIGIBLE"] += 1
            continue
        if not os.path.exists(feat_path):
            stats["FEATURE_ARTIFACT_MISSING"] += 1
            stats["INELIGIBLE"] += 1
            continue
            
        if data.get("feature_pipeline_version") != PIPELINE_VERSION:
            stats["INELIGIBLE"] += 1
            continue
            
        if db_hash == OLD_HASH and fs_hash == OLD_HASH:
            stats["ELIGIBLE"] += 1
        elif db_hash == NEW_HASH and fs_hash == NEW_HASH:
            stats["ALREADY_MIGRATED"] += 1
        else:
            stats["INELIGIBLE"] += 1
            continue
            
        # Snapshot eligible or migrated
        snapshot.append({
            "ticker": ticker,
            "model_version": mv,
            "old_feature_pipeline_hash": db_hash,
            "model_hash": data.get("model_hash"),
            "feature_hash": data.get("feature_hash"),
            "feature_pipeline_version": PIPELINE_VERSION,
            "dataset_date_end": data.get("dataset_date_end"),
            "status": "ACTIVE"
        })
        
        if args.dry_run or db_hash == target_hash:
            continue
            
        # MIGRATION EXECUTION
        res = db.model_registry.update_one(
            {"ticker": ticker, "version": mv, "status": "ACTIVE", "feature_pipeline_hash": source_hash},
            {"$set": {"feature_pipeline_hash": target_hash}}
        )
        stats["MONGODB_WRITES"] += 1
        
        if res.matched_count == 0 or res.modified_count == 0:
            print(f"{ticker}: MongoDB update failed unexpectedly.")
            stats["UNEXPECTED_STATES"] += 1
            continue
            
        stats["MUTATIONS_PERFORMED"] += 1
        
        # SYNC
        sync_ok = False
        try:
            sync_ok = sync_manifest(db, ticker)
            stats["PRODUCTION_FILESYSTEM_WRITES"] += 1
        except Exception as e:
            print(f"{ticker}: sync_manifest error: {e}")
            sync_ok = False
            
        # READ BACK
        read_ok = False
        if sync_ok:
            try:
                with open(m, 'r', encoding='utf-8') as f:
                    new_data = json.load(f)
                read_ok = (new_data.get("feature_pipeline_hash") == target_hash and 
                           new_data.get("model_version") == mv and
                           new_data.get("model_hash") == rec.get("model_hash"))
            except Exception as e:
                print(f"{ticker}: readback error: {e}")
                read_ok = False
                
        verify_rec = db.model_registry.find_one({"ticker": ticker, "status": "ACTIVE"})
        if verify_rec and verify_rec.get("feature_pipeline_hash") == target_hash and read_ok:
            stats["MIGRATED"] += 1
        else:
            stats["SYNC_FAILURES"] += 1
            print(f"{ticker}: SYNC FAILURE. Initiating Split-Brain Recovery...")
            
            # RECOVERY
            rec_res = db.model_registry.update_one(
                {"ticker": ticker, "version": mv, "status": "ACTIVE", "feature_pipeline_hash": target_hash},
                {"$set": {"feature_pipeline_hash": source_hash}}
            )
            if rec_res.modified_count == 1:
                try:
                    sync_manifest(db, ticker)
                    with open(m, 'r', encoding='utf-8') as f:
                        rb_data = json.load(f)
                    
                    if rb_data.get("feature_pipeline_hash") == source_hash:
                        print(f"{ticker}: RECOVERED. Split-brain prevented.")
                        stats["RECOVERED"] += 1
                    else:
                        print(f"{ticker}: SPLIT BRAIN. Filesystem recovery failed.")
                        stats["SPLIT_BRAIN"] += 1
                except:
                    print(f"{ticker}: SPLIT BRAIN. Filesystem sync exception.")
                    stats["SPLIT_BRAIN"] += 1
            else:
                print(f"{ticker}: SPLIT BRAIN. MongoDB rollback failed.")
                stats["SPLIT_BRAIN"] += 1

    if args.dry_run and len(snapshot) > 0:
        os.makedirs("scratch", exist_ok=True)
        with open("scratch/migration_snapshot.json", "w") as f:
            json.dump(snapshot, f, indent=2)
        stats["AUDIT_SNAPSHOT_WRITES"] += 1

    print("\n--- MIGRATION SUMMARY ---")
    for k, v in stats.items():
        print(f"{k} = {v}")

if __name__ == "__main__":
    main()
