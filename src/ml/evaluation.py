"""
src/ml/evaluation.py
Compatibility entry point for prediction evaluation and settlement.
Delegates to the canonical settlement engine in src.ml.settlement.
"""
import argparse
import logging
import os
from pymongo import MongoClient
from dotenv import load_dotenv

from src.ml.settlement import evaluate_predictions

logger = logging.getLogger(__name__)


def evaluate_pending_predictions(client, apply=True):
    """
    Evaluates PENDING predictions by delegating to the canonical settlement engine.

    Delegates to src.ml.settlement.evaluate_predictions, enforcing:
      - 10 valid NSE trading sessions maturity
      - Dynamic target_return_threshold evaluation
      - Cryptographic provenance and settlement sealing
      - Safe quarantining of LEGACY_UNSETTLEABLE records
      - Idempotent execution
    """
    logger.info("Delegating prediction evaluation to canonical settlement engine (src.ml.settlement)...")
    return evaluate_predictions(client, apply=apply)


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate pending predictions (compatibility wrapper for src.ml.settlement)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode without modifying database records",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        default=True,
        help="Apply mutations to MongoDB (default: True)",
    )
    args = parser.parse_args()

    apply = not args.dry_run

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(mongo_uri)

    try:
        stats = evaluate_pending_predictions(client, apply=apply)
        logger.info(f"Evaluation finished with canonical settlement summary: {stats}")
        return stats
    finally:
        client.close()


if __name__ == "__main__":
    main()
