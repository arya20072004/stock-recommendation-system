import argparse
import sys
import os
import logging
import hashlib
import shutil
import json
from datetime import datetime, timezone
from pymongo import MongoClient

# Add src to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ml.model_registry import (
    promote_model,
    sync_manifest,
    setup_registry_indexes,
    MODELS_DIR,
    FEATURES_DIR,
    hash_file_sha256,
    update_manifest_atomically
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

def get_db():
    from dotenv import load_dotenv
    load_dotenv()
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    return client["stock_market_db"]

def do_promote(args):
    db = get_db()
    setup_registry_indexes(db)
    success = promote_model(db, args.ticker, args.version)
    if success:
        logger.info(f"Promotion successful for {args.ticker} {args.version}")
        sys.exit(0)
    else:
        logger.error("Promotion failed.")
        sys.exit(1)

def do_rollback(args):
    db = get_db()
    setup_registry_indexes(db)
    logger.info(f"Attempting rollback for {args.ticker} to version {args.version}")
    success = promote_model(db, args.ticker, args.version)
    if success:
        logger.info(f"Rollback successful for {args.ticker} {args.version}")
        sys.exit(0)
    else:
        logger.error("Rollback failed.")
        sys.exit(1)

def do_status(args):
    db = get_db()
    from src.ml.model_registry import read_active_manifest
    manifest = read_active_manifest(args.ticker)
    
    registry_active = db.model_registry.find_one({"ticker": args.ticker, "status": "ACTIVE"})
    
    print(f"\n--- Status for {args.ticker} ---")
    if manifest:
        print(f"Manifest Active Version: {manifest.get('model_version')}")
    else:
        print("Manifest: MISSING")
        
    if registry_active:
        print(f"Registry Active Version: {registry_active.get('version')}")
    else:
        print("Registry Active Version: NONE")
        
    if manifest and registry_active:
        if manifest.get('model_version') != registry_active.get('version'):
            print("WARNING: SPLIT-BRAIN DETECTED. Registry and manifest diverge.")
            print(f"To reconcile, run: python scripts/manage_models.py sync {args.ticker}")
    print("---------------------------------\n")

def do_sync(args):
    db = get_db()
    success = sync_manifest(db, args.ticker)
    if success:
        sys.exit(0)
    else:
        sys.exit(1)

def do_migrate(args):
    db = get_db()
    setup_registry_indexes(db)
    from src.data.nifty50 import TICKERS
    
    logger.info("Starting migration of existing 51 models to Phase 13 format...")
    for ticker in TICKERS:
        old_model = os.path.join(MODELS_DIR, f"model_{ticker}.joblib")
        old_features = os.path.join(FEATURES_DIR, f"features_{ticker}.json")
        old_metrics = os.path.join(MODELS_DIR, f"{ticker}_metrics.json")
        
        if not os.path.exists(old_model):
            continue
            
        model_hash = hash_file_sha256(old_model, truncate_to=12)
        feature_hash = hash_file_sha256(old_features, truncate_to=64)
        
        version = model_hash
        
        new_model = os.path.join(MODELS_DIR, f"model_{ticker}_{version}.joblib")
        new_features = os.path.join(FEATURES_DIR, f"features_{ticker}_{version}.json")
        new_metrics = os.path.join(MODELS_DIR, f"metrics_{ticker}_{version}.json")
        
        # Check if already migrated
        existing_record = db.model_registry.find_one({"ticker": ticker, "version": version})
        if existing_record:
            logger.info(f"Ticker {ticker} version {version} already registered. Skipping.")
            continue
            
        # Copy to new immutable paths
        if not os.path.exists(new_model):
            shutil.copy2(old_model, new_model)
        if not os.path.exists(new_features) and os.path.exists(old_features):
            shutil.copy2(old_features, new_features)
        if not os.path.exists(new_metrics) and os.path.exists(old_metrics):
            shutil.copy2(old_metrics, new_metrics)
            
        # Validate hashes of new files
        dest_model_hash = hash_file_sha256(new_model, truncate_to=12)
        dest_feature_hash = hash_file_sha256(new_features, truncate_to=64)
        
        if model_hash != dest_model_hash or feature_hash != dest_feature_hash:
            logger.error(f"Hash mismatch during migration for {ticker}. Aborting this ticker.")
            continue
            
        # Parse old metrics to get trained_at and dataset_fingerprint if available
        metrics_data = {}
        if os.path.exists(new_metrics):
            try:
                with open(new_metrics, "r", encoding="utf-8") as f:
                    metrics_data = json.load(f)
            except Exception as e:
                logger.error(f"Could not load metrics for {ticker}: {e}")
                
        trained_at = metrics_data.get("model_metadata", {}).get("trained_at", datetime.now(timezone.utc).isoformat())
        
        # Register as ACTIVE directly
        now = datetime.now(timezone.utc).isoformat()
        db.model_registry.insert_one({
            "ticker": ticker,
            "version": version,
            "status": "ACTIVE",
            "model_hash": model_hash,
            "feature_hash": feature_hash,
            "metrics": metrics_data,
            "trained_at": trained_at,
            "promoted_at": now
        })
        
        # Write active manifest
        manifest_data = {
            "ticker": ticker,
            "model_version": version,
            "model_hash": model_hash,
            "feature_hash": feature_hash,
            "promoted_at": now
        }
        update_manifest_atomically(ticker, manifest_data)
        
        logger.info(f"Successfully migrated {ticker} to {version}")

def main():
    parser = argparse.ArgumentParser(description="Model Lifecycle CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    promote_parser = subparsers.add_parser("promote")
    promote_parser.add_argument("ticker")
    promote_parser.add_argument("version")
    
    rollback_parser = subparsers.add_parser("rollback")
    rollback_parser.add_argument("ticker")
    rollback_parser.add_argument("version")
    
    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("ticker")
    
    sync_parser = subparsers.add_parser("sync")
    sync_parser.add_argument("ticker")
    
    subparsers.add_parser("migrate")
    
    args = parser.parse_args()
    
    if args.command == "promote":
        do_promote(args)
    elif args.command == "rollback":
        do_rollback(args)
    elif args.command == "status":
        do_status(args)
    elif args.command == "sync":
        do_sync(args)
    elif args.command == "migrate":
        do_migrate(args)

if __name__ == "__main__":
    main()
