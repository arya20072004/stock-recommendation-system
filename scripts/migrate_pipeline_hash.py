import sys
import os
import logging
import argparse
from datetime import datetime, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add parent directory to path to import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.features.router import get_feature_pipeline_hash
from src.ml.model_registry import read_active_manifest

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DB_NAME = "stock_market_db"

def abort(msg: str, exit_code: int = 1):
    logger.error(f"ABORT: {msg}")
    sys.exit(exit_code)

def run_migration(pipeline_version: str, expected_old_hash: str, dry_run: bool = False):
    logger.info(f"--- STARTING CANONICAL HASH MIGRATION FOR VERSION {pipeline_version} ---")
    logger.info(f"Dry run mode: {dry_run}")
    
    # Precondition A: New hash calculation
    try:
        new_hash = get_feature_pipeline_hash(pipeline_version)
        logger.info(f"Calculated new feature pipeline hash: {new_hash}")
    except Exception as e:
        abort(f"Failed to calculate new hash: {e}")

    if new_hash == expected_old_hash:
        abort("New hash is identical to expected old hash.")

    # Precondition B: Database Connection
    mongo_uri = os.environ.get("MONGO_URI")
    if not mongo_uri:
        abort("MONGO_URI environment variable is not configured. Do not fall back to local.")
        
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
    except Exception as e:
        abort(f"Failed to connect to MongoDB: {e}")

    # Precondition C: Database Identity
    logger.info(f"Targeting database: {DB_NAME}")
    db = client[DB_NAME]
    
    # Precondition D: Active Model Discovery
    try:
        # We explicitly target only ACTIVE status and the exact feature_pipeline_version.
        active_models = list(db.model_registry.find({"status": "ACTIVE", "feature_pipeline_version": pipeline_version}))
        
        # Additionally, check if there are ACTIVE models that LACK feature_pipeline_version
        missing_metadata_models = list(db.model_registry.find({"status": "ACTIVE", "feature_pipeline_version": {"$exists": False}}))
        if missing_metadata_models:
            abort(f"Found {len(missing_metadata_models)} ACTIVE models missing 'feature_pipeline_version' entirely. Cannot proceed.")
    except Exception as e:
        abort(f"Failed to query model_registry: {e}")
        
    active_count = len(active_models)
    logger.info(f"Discovered {active_count} ACTIVE models in registry with feature_pipeline_version {pipeline_version}.")
    
    if active_count == 0:
        abort(f"No ACTIVE models discovered for version {pipeline_version}. Unexpected state.")

    # Validation Pass
    invalid_models = []
    old_hash_count = 0
    new_hash_count = 0
    
    target_ids = []
    target_tickers = []
    
    for m in active_models:
        # Precondition F: Model Identity
        for field in ["_id", "ticker", "status", "feature_pipeline_hash", "version", "model_hash", "feature_pipeline_version"]:
            if field not in m or m[field] is None:
                invalid_models.append(f"Model {m.get('ticker', 'UNKNOWN')} missing required field '{field}'")
                continue
                
        # Precondition G: Manifest Consistency
        ticker = m["ticker"]
        manifest = read_active_manifest(ticker)
        if not manifest:
            invalid_models.append(f"Model {ticker} is ACTIVE in DB but missing filesystem manifest.")
            continue
            
        if manifest.get("model_version") != m["version"] or manifest.get("model_hash") != m["model_hash"]:
            invalid_models.append(f"Model {ticker} manifest is inconsistent with DB state.")
            continue
            
        # Hash Uniformity Tracking
        h = m.get("feature_pipeline_hash")
        if h == expected_old_hash:
            old_hash_count += 1
            target_ids.append(m["_id"])
            target_tickers.append(ticker)
        elif h == new_hash:
            new_hash_count += 1
        else:
            invalid_models.append(f"Model {ticker} has unexpected hash: {h}")

    if invalid_models:
        for err in invalid_models:
            logger.error(err)
        abort("Registry validation failed. Refusing to migrate.")

    # Precondition E: Old Hash Uniformity & Idempotency
    if new_hash_count == active_count and old_hash_count == 0:
        logger.info("All ACTIVE models already have the new hash. Migration appears already completed.")
        logger.info("No database modification performed.")
        sys.exit(0)
        
    if old_hash_count != active_count:
        abort(f"Mixed state detected! Expected {active_count} old hashes, found {old_hash_count} old and {new_hash_count} new hashes. Requires manual investigation.")

    logger.info(f"All {active_count} ACTIVE models validated. Target tickers: {target_tickers[:3]}... ({len(target_tickers)} total)")
    
    if dry_run:
        logger.info("DRY RUN: Preconditions satisfied. Generating report...")
        report = f"""
Database:
{DB_NAME}

Expected ACTIVE v1 count:
{active_count}

Expected old hash:
{expected_old_hash}

Expected new hash:
{new_hash}

Target count:
{old_hash_count}

Unexpected hashes:
{new_hash_count} (migrated) + {active_count - old_hash_count - new_hash_count} (invalid)

Missing metadata:
{len(invalid_models)}

Missing tickers:
0

Unexpected tickers:
0

Database mutations:
0

Filesystem mutations:
0
"""
        print(report)
        sys.exit(0)

    # Mutation
    logger.info("Initiating atomic mutation...")
    try:
        with client.start_session() as session:
            with session.start_transaction():
                result = db.model_registry.update_many(
                    {
                        "_id": {"$in": target_ids},
                        "status": "ACTIVE",
                        "feature_pipeline_version": pipeline_version,
                        "feature_pipeline_hash": expected_old_hash
                    },
                    {
                        "$set": {"feature_pipeline_hash": new_hash}
                    },
                    session=session
                )
                
                if result.modified_count != len(target_ids):
                    # Exception will roll back transaction automatically
                    raise RuntimeError(f"Modified count mismatch: expected {len(target_ids)}, modified {result.modified_count}")
                    
    except Exception as e:
        abort(f"Migration transaction failed: {e}")

    logger.info(f"Migration completed. Modified {active_count} records.")

    # Post-migration Verification
    post_active_models = list(db.model_registry.find({"status": "ACTIVE", "feature_pipeline_version": pipeline_version}))
    post_count = len(post_active_models)
    if post_count != active_count:
        abort(f"Post-migration active count mismatch. Expected {active_count}, found {post_count}.")

    for m in post_active_models:
        if m["_id"] not in target_ids:
            abort(f"Post-migration validation found unexpected ACTIVE model: {m.get('ticker')}")
        if m.get("feature_pipeline_hash") != new_hash:
            abort(f"Post-migration verification failed for ticker {m.get('ticker')}. Hash is {m.get('feature_pipeline_hash')} instead of new hash.")

    # Ensure zero target records have the old hash
    old_hash_remaining = db.model_registry.count_documents({
        "_id": {"$in": target_ids},
        "feature_pipeline_hash": expected_old_hash
    })
    if old_hash_remaining > 0:
        abort(f"Post-migration verification failed. {old_hash_remaining} target models still have the old hash.")

    logger.info("--- MIGRATION SUCCESSFULLY VERIFIED ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate feature pipeline hash for active models.")
    parser.add_argument("--pipeline-version", required=True, help="Exact feature pipeline version (e.g. v1)")
    parser.add_argument("--expected-old-hash", required=True, help="The expected old hash that will be migrated")
    parser.add_argument("--dry-run", action="store_true", help="Perform validation without database modification")
    args = parser.parse_args()
    
    run_migration(
        pipeline_version=args.pipeline_version,
        expected_old_hash=args.expected_old_hash,
        dry_run=args.dry_run
    )
