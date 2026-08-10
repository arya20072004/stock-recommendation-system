import argparse
import logging
import os
from datetime import datetime, timezone
import pandas as pd

from pymongo import MongoClient
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

def evaluate_predictions(client, apply=False):
    db = client["stock_market_db"]

    pending_predictions = list(db.prediction_history.find({"status": "PENDING"}).sort("market_date", 1))

    stats = {
        "READY_TO_SETTLE": 0,
        "NOT_MATURE": 0,
        "LEGACY_UNSETTLEABLE": 0,
        "MISSING_MARKET_DATA": 0,
        "INVALID_PREDICTION_RECORD": 0,
        "ALREADY_EVALUATED": 0,
        "ERRORS": 0
    }

    # Pre-compute global dates to avoid distinct queries per prediction if possible
    # Just query all distinct dates in historical_data once
    all_dates = sorted(db.historical_data.distinct("date"))

    evaluated_records = 0

    for record in pending_predictions:
        try:
            ticker = record.get("symbol")
            market_date_str = record.get("market_date")
            horizon = record.get("prediction_horizon", 10)

            if not ticker or not market_date_str:
                logger.error(f"Invalid record missing ticker or market_date: {record.get('_id')}")
                stats["INVALID_PREDICTION_RECORD"] += 1
                continue

            market_date = pd.to_datetime(market_date_str).to_pydatetime()

            # Phase D: Legacy Record Policy
            target_return_threshold = record.get("target_return_threshold")
            if target_return_threshold is None:
                stats["LEGACY_UNSETTLEABLE"] += 1
                continue

            # Phase H: Validate price_at_prediction
            price_at_prediction = record.get("price_at_prediction")
            if price_at_prediction is None or price_at_prediction <= 0:
                logger.error(f"Invalid price_at_prediction for {ticker} on {market_date_str}")
                stats["INVALID_PREDICTION_RECORD"] += 1
                continue

            # Phase F: Maturity Rule
            # Exactly 10 VALID trading sessions after market_date for this ticker
            future_sessions = list(db.historical_data.find({
                "ticker": ticker,
                "date": {"$gt": market_date},
                "close": {"$gt": 0, "$type": "double"}
            }).sort("date", 1).limit(horizon))

            ticker_future_count = len(future_sessions)

            if ticker_future_count < horizon:
                # Phase G: Fix Maturity Classification
                # Check global trading sessions
                global_future_sessions = sum(1 for d in all_dates if d > market_date)

                if global_future_sessions >= horizon:
                    stats["MISSING_MARKET_DATA"] += 1
                else:
                    stats["NOT_MATURE"] += 1
                continue

            # The prediction is mature!

            # Phase 18: Cryptographic Business-Decision Sealing & History/Provenance Integrity Binding
            provenance_hash = record.get("provenance_hash")
            if not provenance_hash:
                logger.error(f"Missing provenance_hash for {ticker} on {market_date_str}")
                stats["INVALID_PREDICTION_RECORD"] += 1
                result = db.prediction_history.update_one({"_id": record["_id"]}, {"$set": {"status": "INVALID_PROVENANCE"}})
                continue

            provenance_doc = db.prediction_provenance.find_one({"provenance_hash": provenance_hash})
            if not provenance_doc:
                logger.error(f"Missing provenance document for hash {provenance_hash}")
                stats["INVALID_PREDICTION_RECORD"] += 1
                result = db.prediction_history.update_one({"_id": record["_id"]}, {"$set": {"status": "INVALID_PROVENANCE"}})
                continue

            # Verify Identity Match
            if (provenance_doc.get("symbol") != ticker or
                provenance_doc.get("market_date") != market_date_str or
                provenance_doc.get("prediction_horizon") != horizon or
                provenance_doc.get("model_version") != record.get("model_version")):
                logger.error(f"History/Provenance identity mismatch for {ticker} on {market_date_str}")
                stats["INVALID_PREDICTION_RECORD"] += 1
                result = db.prediction_history.update_one({"_id": record["_id"]}, {"$set": {"status": "INVALID_PROVENANCE"}})
                continue

            # Verify Hash Integrity
            from src.ml.model_utils import compute_provenance_hash, reconstruct_canonical_payload
            canonical_payload = reconstruct_canonical_payload(provenance_doc)
            recomputed_hash = compute_provenance_hash(canonical_payload)
            if recomputed_hash != provenance_hash:
                logger.error(f"Provenance hash mismatch. Recomputed {recomputed_hash} != {provenance_hash}")
                stats["INVALID_PREDICTION_RECORD"] += 1
                result = db.prediction_history.update_one({"_id": record["_id"]}, {"$set": {"status": "INVALID_PROVENANCE"}})
                continue

            # Verify Semantic Binding (V3)
            if provenance_doc.get("provenance_schema_version") == "v3":
                if (provenance_doc.get("recommendation") != record.get("recommendation") or
                    provenance_doc.get("confidence_tier") != record.get("confidence_tier") or
                    abs(provenance_doc.get("target_return_threshold") - target_return_threshold) > 1e-9):
                    logger.error(f"History/Provenance semantic mismatch for {ticker} on {market_date_str}")
                    stats["INVALID_PREDICTION_RECORD"] += 1
                    result = db.prediction_history.update_one({"_id": record["_id"]}, {"$set": {"status": "INVALID_PROVENANCE"}})
                    continue

            stats["READY_TO_SETTLE"] += 1

            settlement_session = future_sessions[horizon - 1]
            settlement_close = float(settlement_session["close"])
            settlement_market_date = settlement_session["date"].strftime("%Y-%m-%d")

            actual_return = (settlement_close / price_at_prediction) - 1

            # Phase I: Actual Class
            if actual_return > target_return_threshold:
                actual_class = "BUY"
            elif actual_return < -target_return_threshold:
                actual_class = "SELL"
            else:
                actual_class = "HOLD"

            raw_prediction = record.get("raw_prediction")
            recommendation = record.get("recommendation")

            # Phase J & K: Correctness metrics
            raw_prediction_correct = (raw_prediction == actual_class)
            recommendation_correct = (recommendation == actual_class)

            # Settlement payload
            update_payload = {
                "actual_price": settlement_close,
                "actual_return": actual_return,
                "actual_class": actual_class,
                "raw_prediction_correct": raw_prediction_correct,
                "recommendation_correct": recommendation_correct,
                "prediction_correct": recommendation_correct, # Alias
                "outcome": "CORRECT" if recommendation_correct else "INCORRECT",
                "status": "EVALUATED",
                "evaluation_timestamp": datetime.now(timezone.utc),
                "settlement_market_date": settlement_market_date
            }

            if not apply:
                logger.info(
                    f"[DRY-RUN READY_TO_SETTLE] {ticker} | "
                    f"Market Date: {market_date_str} | Horizon: {horizon} | "
                    f"Price @ Pred: {price_at_prediction:.2f} | "
                    f"Target Thr: {target_return_threshold:.4f} | "
                    f"Settlement Date: {settlement_market_date} | "
                    f"Settlement Close: {settlement_close:.2f} | "
                    f"Actual Return: {actual_return:.4f} | "
                    f"Actual Class: {actual_class} | "
                    f"Raw Pred: {raw_prediction} | Rec: {recommendation} | "
                    f"Raw Correct: {raw_prediction_correct} | "
                    f"Rec Correct: {recommendation_correct}"
                )
            else:
                # Phase N: Atomic Database Transition
                result = db.prediction_history.update_one(
                    {
                        "_id": record["_id"],
                        "status": "PENDING"
                    },
                    {
                        "$set": update_payload
                    }
                )

                if result.matched_count == 1:
                    evaluated_records += 1
                else:
                    logger.warning(f"Failed to update record {record['_id']}, possibly already settled.")
                    stats["ALREADY_EVALUATED"] += 1

        except Exception as exc:
            logger.error(f"Error processing record {record.get('_id')} ({record.get('symbol')}): {exc}")
            stats["ERRORS"] += 1

    logger.info("=== SETTLEMENT SUMMARY ===")
    logger.info(f"Total PENDING scanned: {len(pending_predictions)}")
    logger.info(f"READY_TO_SETTLE: {stats['READY_TO_SETTLE']}")
    logger.info(f"NOT_MATURE: {stats['NOT_MATURE']}")
    logger.info(f"LEGACY_UNSETTLEABLE: {stats['LEGACY_UNSETTLEABLE']}")
    logger.info(f"MISSING_MARKET_DATA: {stats['MISSING_MARKET_DATA']}")
    logger.info(f"INVALID_PREDICTION_RECORD: {stats['INVALID_PREDICTION_RECORD']}")
    logger.info(f"ALREADY_EVALUATED: {stats['ALREADY_EVALUATED']}")
    logger.info(f"ERRORS: {stats['ERRORS']}")

    if apply:
        logger.info(f"Records successfully EVALUATED in MongoDB: {evaluated_records}")

    return stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prediction Outcome Settlement")
    parser.add_argument("--apply", action="store_true", help="Apply mutations to MongoDB")
    args = parser.parse_args()

    if not args.apply:
        logger.info("Running in DRY-RUN mode. No database records will be modified.")
    else:
        logger.warning("Running in APPLY mode. Modifying database records.")

    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(mongo_uri)

    try:
        evaluate_predictions(client, apply=args.apply)
    finally:
        client.close()
