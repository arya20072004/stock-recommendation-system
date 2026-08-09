import argparse
import logging
import os
import sys

from pymongo import MongoClient
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

def migrate_legacy_predictions(client, apply=False):
    db = client["stock_market_db"]
    collection = db["prediction_history"]

    query = {"provenance_hash": {"$exists": False}}
    
    count = collection.count_documents(query)
    logger.info("Found %d legacy predictions missing Phase 15 provenance.", count)
    
    if not apply:
        logger.info("DRY-RUN mode. Pass --apply to perform the migration.")
        return

    if count > 0:
        result = collection.update_many(
            query,
            {
                "$set": {
                    "provenance_hash": "LEGACY_UNAVAILABLE",
                    "provenance_status": "LEGACY_UNAVAILABLE"
                }
            }
        )
        logger.info("Migrated %d records to LEGACY_UNAVAILABLE.", result.modified_count)
    else:
        logger.info("No records to migrate.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate legacy predictions for Phase 15.")
    parser.add_argument("--apply", action="store_true", help="Apply mutations to MongoDB")
    args = parser.parse_args()
    
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(mongo_uri)
    
    try:
        migrate_legacy_predictions(client, apply=args.apply)
    except Exception as e:
        logger.exception("Migration failed: %s", e)
        sys.exit(1)
    finally:
        client.close()
