import os
import json
import logging
from pymongo import MongoClient
from dotenv import load_dotenv

# Ensure we can import src modules
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.features.router import get_feature_pipeline_hash
from src.ml.model_registry import sync_manifest

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def run_migration():
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(mongo_uri)
    db = client["stock_market_db"]
    try:
        v1_hash = get_feature_pipeline_hash("v1")
    except RuntimeError as e:
        logger.error(f"Cannot perform migration. v1 pipeline hash could not be calculated: {e}")
        return
    models = list(db.model_registry.find({"feature_pipeline_version": {"$exists": False}}))
    logger.info(f"Found {len(models)} legacy models needing migration.")
    for model in models:
        # Extract historically knowable metadata if possible (row count, date range are unavailable)
        update_doc = {
            "feature_pipeline_version": "v1",
            "feature_pipeline_hash": v1_hash,
            "dataset_hash": "LEGACY_UNAVAILABLE",
            "provenance_status": "LEGACY_UNAVAILABLE"
        }
        db.model_registry.update_one(
            {"_id": model["_id"]},
            {"$set": update_doc}
        )
        logger.info(f"Migrated model {model['ticker']} version {model['version']}")
    logger.info("MongoDB migration complete.")
    # Sync all ACTIVE models to update their manifests on disk
    active_models = list(db.model_registry.find({"status": "ACTIVE"}))
    for active_model in active_models:
        sync_manifest(db, active_model["ticker"])
        logger.info(f"Synced manifest for {active_model['ticker']}")
    logger.info("Migration successful.")

if __name__ == "__main__":
    run_migration()
