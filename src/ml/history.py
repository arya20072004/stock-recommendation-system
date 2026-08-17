"""
history.py

Generate and persist immutable daily prediction snapshots.

This module:

1. Loads the latest trained model for each ticker.
2. Loads the exact feature list used by that model.
3. Rebuilds the feature frame from MongoDB.
4. Verifies that the latest market row is inference-ready.
5. Generates the model prediction.
6. Applies threshold calibration and confidence gating.
7. Persists an immutable prediction snapshot to MongoDB.
8. Fails the batch if one or more tickers could not generate a valid
   prediction.

IMPORTANT:
Prediction history must NEVER silently fall back to an older feature row.
If the latest market row is incomplete, the ticker is treated as an error.
"""

import hashlib
import json
import logging
import os
import warnings

from datetime import datetime, timezone, date

import joblib
import pandas as pd
import pymongo

from pymongo import MongoClient
from xgboost import XGBClassifier

from src.data.nifty50 import TICKERS
from src.features.router import (
    resolve_feature_pipeline,
    get_feature_pipeline_hash,
)
from src.ml.confidence import (
    compute_confidence_tier,
    get_display_signal,
    get_confidence_boundaries,
)


# ======================================================================
# Configuration
# ======================================================================

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

MODELS_DIR = "saved_models"
FEATURES_DIR = "saved_features"

DEFAULT_PREDICTION_HORIZON = 10


class PredictionDataNotReadyError(Exception):
    """
    Raised when a ticker cannot safely receive a prediction because required
    inference data is unavailable or not inference-ready (e.g. missing PCR).
    This represents a recognized local prediction failure, not a systemic defect.
    """
    pass


# ======================================================================
# Model loading
# ======================================================================

def load_active_bundle(ticker: str):
    """
    Load the explicitly versioned model and feature contract for a ticker based on the active manifest.
    Fails closed if the manifest is missing, malformed, or if hashes do not match.
    """
    manifest_path = os.path.join(MODELS_DIR, f"{ticker}_active.json")
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(f"Active manifest missing for {ticker}. Failing closed.")

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    version = manifest.get("model_version")
    expected_model_hash = manifest.get("model_hash")
    expected_feature_hash = manifest.get("feature_hash")
    expected_pipeline_version = manifest.get("feature_pipeline_version")
    expected_pipeline_hash = manifest.get("feature_pipeline_hash")

    if not version or not expected_model_hash or not expected_feature_hash or not expected_pipeline_version or not expected_pipeline_hash:
        raise ValueError(f"Malformed active manifest for {ticker}. Failing closed.")

    # Check 1: Route the pipeline
    try:
        engineering_module = resolve_feature_pipeline(expected_pipeline_version)
    except RuntimeError as e:
        raise RuntimeError(f"Feature pipeline version '{expected_pipeline_version}' is unavailable.") from e

    # Check 2: Verify pipeline hash
    actual_pipeline_hash = get_feature_pipeline_hash(expected_pipeline_version)
    if expected_pipeline_hash and actual_pipeline_hash != expected_pipeline_hash:
        raise ValueError(f"Feature pipeline hash mismatch for {ticker}. Expected {expected_pipeline_hash}, got {actual_pipeline_hash}. Failing closed.")

    # Verify Model
    model_path = os.path.join(MODELS_DIR, f"model_{ticker}_{version}.joblib")
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model artifact missing for {ticker} version {version}")

    sha256_model = hashlib.sha256()
    with open(model_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_model.update(chunk)
    actual_model_hash = sha256_model.hexdigest()[:12]

    if actual_model_hash != expected_model_hash:
        raise ValueError(f"Model hash mismatch for {ticker}. Expected {expected_model_hash}, got {actual_model_hash}. Failing closed.")

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message=".*pickle.*")
        model = joblib.load(model_path)

    # Verify Features
    features_path = os.path.join(FEATURES_DIR, f"features_{ticker}_{version}.json")
    if not os.path.exists(features_path):
        raise FileNotFoundError(f"Feature artifact missing for {ticker} version {version}")

    sha256_features = hashlib.sha256()
    with open(features_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_features.update(chunk)
    actual_feature_hash = sha256_features.hexdigest()[:64]

    if actual_feature_hash != expected_feature_hash:
        raise ValueError(f"Feature hash mismatch for {ticker}. Expected {expected_feature_hash}, got {actual_feature_hash}. Failing closed.")

    with open(features_path, "r", encoding="utf-8") as f:
        feature_names = json.load(f)

    if not isinstance(feature_names, list) or not feature_names:
        raise ValueError(f"Invalid feature list format in artifact for {ticker}")

    f1_macro = manifest.get("f1_macro", 0.0)

    return model, feature_names, version, engineering_module, expected_pipeline_version, expected_pipeline_hash, f1_macro


# ======================================================================
# Latest-row validation
# ======================================================================

def get_latest_valid_feature_row(
    ticker: str,
    computed_df,
    feature_names: list[str],
    prediction_target_date: date,
):
    """
    Return the feature vector for the latest market date.

    CRITICAL RULE:

    We DO NOT use:

        computed_df[feature_names].dropna().iloc[-1]

    because doing that can silently fall back months into the past.

    The latest market row itself must contain every feature required by
    the trained model.

    If it does not, inference for this ticker fails explicitly.
    """

    if computed_df.empty:

        raise PredictionDataNotReadyError(
            f"Feature engineering returned an empty DataFrame "
            f"for {ticker}"
        )

    # --------------------------------------------------------------
    # Verify model features exist
    # --------------------------------------------------------------

    missing_columns = [
        feature
        for feature in feature_names
        if feature not in computed_df.columns
    ]

    if missing_columns:

        raise ValueError(
            f"{ticker}: trained model expects feature columns "
            f"that are missing from build_feature_row(): "
            f"{missing_columns}"
        )

    latest_market_date = pd.Timestamp(prediction_target_date)

    if latest_market_date not in computed_df.index:

        raise PredictionDataNotReadyError(
            f"{ticker}: prediction target date {prediction_target_date} not in computed_df."
        )

    latest_row = computed_df.loc[
        latest_market_date,
        feature_names,
    ]

    # Defensive handling in case duplicate index values return a
    # DataFrame rather than a Series.
    if hasattr(latest_row, "ndim") and latest_row.ndim > 1:

        logger.warning(
            "%s: duplicate rows found for latest market date %s. "
            "Using final row.",
            ticker,
            latest_market_date,
        )

        latest_row = latest_row.iloc[-1]

    # --------------------------------------------------------------
    # Check latest row for NaNs
    # --------------------------------------------------------------

    missing_features = (
        latest_row[
            latest_row.isna()
        ]
        .index
        .tolist()
    )

    if missing_features:

        # Find the latest historical row where every model feature
        # happened to be available. This is diagnostic ONLY.
        #
        # We deliberately do NOT use this row for prediction.
        valid_history = (
            computed_df[feature_names]
            .dropna()
        )

        if valid_history.empty:
            last_fully_valid_date = None
        else:
            last_fully_valid_date = valid_history.index.max()

        raise PredictionDataNotReadyError(
            f"{ticker}: latest market row is not inference-ready. "
            f"latest_market_date={latest_market_date}, "
            f"last_fully_valid_date={last_fully_valid_date}, "
            f"missing_feature_count={len(missing_features)}, "
            f"missing_features={missing_features}"
        )

    return latest_market_date, latest_row


def _verify_production_readiness(db):
    """
    Validates complete ACTIVE model registry and filesystem manifest state.
    """
    import os
    import json
    
    current_version = "v1"
    current_hash = get_feature_pipeline_hash(current_version)
    
    expected_tickers = set(TICKERS)
    
    active_records = list(db.model_registry.find({"status": "ACTIVE"}))
    if len(active_records) != len(expected_tickers):
        raise RuntimeError(f"PRODUCTION INFERENCE BLOCKED: Expected {len(expected_tickers)} ACTIVE records, found {len(active_records)}")
        
    db_tickers = set(r.get("ticker") for r in active_records)
    if db_tickers != expected_tickers:
        raise RuntimeError("PRODUCTION INFERENCE BLOCKED: ACTIVE records do not match TICKERS universe")
        
    for rec in active_records:
        ticker = rec.get("ticker")
        
        if rec.get("feature_pipeline_version") != current_version:
            raise RuntimeError(f"PRODUCTION INFERENCE BLOCKED: {ticker} has wrong pipeline version in MongoDB")
        if rec.get("feature_pipeline_hash") != current_hash:
            raise RuntimeError(f"PRODUCTION INFERENCE BLOCKED: {ticker} has wrong pipeline hash in MongoDB")
        if not rec.get("model_hash") or not rec.get("feature_hash"):
            raise RuntimeError(f"PRODUCTION INFERENCE BLOCKED: {ticker} missing artifact references in MongoDB")
            
        manifest_path = os.path.join(MODELS_DIR, f"{ticker}_active.json")
        if not os.path.exists(manifest_path):
            raise RuntimeError(f"PRODUCTION INFERENCE BLOCKED: {ticker} missing filesystem manifest")
            
        try:
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
        except Exception:
            raise RuntimeError(f"PRODUCTION INFERENCE BLOCKED: {ticker} filesystem manifest unreadable")
            
        if manifest.get("feature_pipeline_version") != current_version:
            raise RuntimeError(f"PRODUCTION INFERENCE BLOCKED: {ticker} wrong pipeline version in filesystem")
        if manifest.get("feature_pipeline_hash") != current_hash:
            raise RuntimeError(f"PRODUCTION INFERENCE BLOCKED: {ticker} wrong pipeline hash in filesystem")
            
        if manifest.get("ticker") != ticker:
            raise RuntimeError(f"PRODUCTION INFERENCE BLOCKED: {ticker} mismatch in ticker field")
        if manifest.get("model_version") != rec.get("version"):
            raise RuntimeError(f"PRODUCTION INFERENCE BLOCKED: {ticker} mismatch in model_version field")
        if manifest.get("model_hash") != rec.get("model_hash"):
            raise RuntimeError(f"PRODUCTION INFERENCE BLOCKED: {ticker} mismatch in model_hash field")
        if manifest.get("feature_hash") != rec.get("feature_hash"):
            raise RuntimeError(f"PRODUCTION INFERENCE BLOCKED: {ticker} mismatch in feature_hash field")
            
        mv = manifest.get("model_version")
        model_path = os.path.join(MODELS_DIR, f"model_{ticker}_{mv}.joblib")
        feat_path = os.path.join(FEATURES_DIR, f"features_{ticker}_{mv}.json")
        
        if not os.path.exists(model_path):
            raise RuntimeError(f"PRODUCTION INFERENCE BLOCKED: {ticker} missing model artifact")
        if not os.path.exists(feat_path):
            raise RuntimeError(f"PRODUCTION INFERENCE BLOCKED: {ticker} missing feature artifact")

# ======================================================================
# Prediction generation
# ======================================================================

def generate_and_persist_predictions(client, last_completed_session: date, prediction_target_date: date):
    """
    Generate and persist one immutable daily prediction snapshot for
    every configured ticker.
    # ==================================================================
    # Production Readiness Gate
    # ==================================================================
    _verify_production_readiness(client["stock_market_db"])

    Idempotency key:

        symbol + market_date + prediction_horizon

    Existing records are not overwritten.

    If any ticker fails, the function raises RuntimeError after processing
    the complete ticker list. This ensures:

    - all errors are visible in one run
    - successful tickers may still persist
    - the calling pipeline receives a non-zero exit status
    """

    db = client["stock_market_db"]

    # ==================================================================
    # MongoDB indexes
    # ==================================================================

    db.prediction_history.create_index(
        [
            ("symbol", pymongo.ASCENDING),
            ("market_date", pymongo.DESCENDING),
            ("prediction_horizon", pymongo.ASCENDING),
        ],
        unique=True,
    )

    db.prediction_history.create_index(
        "status"
    )

    db.prediction_history.create_index(
        "prediction_timestamp"
    )

    db.prediction_provenance.create_index(
        [
            ("symbol", pymongo.ASCENDING),
            ("market_date", pymongo.DESCENDING),
            ("prediction_horizon", pymongo.ASCENDING),
        ],
        unique=True,
    )

    # ==================================================================
    # Counters
    # ==================================================================

    generated_count = 0
    skipped_count = 0
    error_count = 0
    existing_count = 0

    stale_tickers = []
    failed_tickers = []
    errors = []

    logger.info(
        "Starting historical prediction generation..."
    )

    # ==================================================================
    # Generate ticker snapshots
    # ==================================================================

    for ticker in TICKERS:

        try:

            logger.info(
                "Generating prediction snapshot for %s...",
                ticker,
            )

            # ----------------------------------------------------------
            # Load model + training feature contract
            # ----------------------------------------------------------

            try:
                bundle = load_active_bundle(ticker)
                if not bundle:
                    logger.warning(f"No active bundle found for {ticker}")
                    result["errors"].append(f"{ticker}: No active bundle")
                    continue
                model, feature_names, loaded_version, engineering_module, pipeline_version, pipeline_hash, f1_macro = bundle
            except Exception as e:
                logger.error(f"Error loading bundle for {ticker}: {e}")
                raise

            build_feature_row = engineering_module.build_feature_row
            TICKER_CLASS_THRESHOLDS = engineering_module.TICKER_CLASS_THRESHOLDS
            apply_threshold_calibration = engineering_module.apply_threshold_calibration
            get_target_return_threshold = engineering_module.get_target_return_threshold

            # ----------------------------------------------------------
            # Rebuild features
            # ----------------------------------------------------------

            computed_df = build_feature_row(
                ticker,
                client,
                db,
                last_completed_session=last_completed_session,
                prediction_target_date=prediction_target_date,
            )

            # ----------------------------------------------------------
            # Validate latest market row
            # ----------------------------------------------------------

            (
                market_date,
                latest_row,
            ) = get_latest_valid_feature_row(
                ticker,
                computed_df,
                feature_names,
                prediction_target_date,
            )

            market_date_obj = market_date.date() if hasattr(market_date, "date") else market_date

            if market_date_obj != prediction_target_date:
                raise PredictionDataNotReadyError(
                    f"Market date mismatch for {ticker}: "
                    f"{market_date_obj} != {prediction_target_date}"
                )

            market_date_str = (
                market_date.strftime(
                    "%Y-%m-%d"
                )
            )

            # ----------------------------------------------------------
            # Price at prediction
            # ----------------------------------------------------------

            if "close" not in computed_df.columns:

                raise ValueError(
                    f"{ticker}: build_feature_row() output "
                    f"does not contain 'close'."
                )

            price_value = computed_df.loc[
                pd.Timestamp(last_completed_session),
                "close",
            ]

            # Defensive handling for duplicate dates.
            if hasattr(price_value, "iloc"):

                price_value = (
                    price_value.iloc[-1]
                )

            price_at_prediction = float(
                price_value
            )

            # ----------------------------------------------------------
            # Return threshold for future outcome settlement
            # ----------------------------------------------------------

            if "atr_pct" not in computed_df.columns:

                raise ValueError(
                    f"{ticker}: build_feature_row() output "
                    f"does not contain 'atr_pct'."
                )

            atr_pct_val = computed_df.loc[
                market_date,
                "atr_pct",
            ]

            if hasattr(atr_pct_val, "iloc"):

                atr_pct_val = (
                    atr_pct_val.iloc[-1]
                )

            atr_pct = float(atr_pct_val)

            target_return_threshold = float(
                get_target_return_threshold(
                    ticker,
                    atr_pct,
                )
            )

            if not (target_return_threshold > 0):

                raise ValueError(
                    f"{ticker}: target_return_threshold "
                    f"must be > 0. Got: {target_return_threshold}"
                )

            # ----------------------------------------------------------
            # Prepare inference vector
            # ----------------------------------------------------------

            latest_features = (
                latest_row
                .values
                .reshape(1, -1)
            )

            # ----------------------------------------------------------
            # Model inference
            # ----------------------------------------------------------

            proba = model.predict_proba(
                latest_features
            )[0]

            model_probabilities = [float(p) for p in proba]

            # ----------------------------------------------------------
            # Threshold calibration
            # ----------------------------------------------------------

            thresholds = (
                TICKER_CLASS_THRESHOLDS.get(
                    ticker
                )
            )

            decision_thresholds = {str(k): float(v) for k, v in thresholds.items()} if thresholds else {}

            predicted_class_idx = (
                apply_threshold_calibration(
                    proba,
                    thresholds,
                )
            )

            predicted_class = [
                "SELL",
                "HOLD",
                "BUY",
            ][predicted_class_idx]

            # ----------------------------------------------------------
            # Confidence
            # ----------------------------------------------------------

            max_proba = float(
                proba.max()
            )

            sorted_p = sorted(
                proba,
                reverse=True,
            )

            if len(sorted_p) >= 2:

                top2_margin = float(
                    sorted_p[0]
                    - sorted_p[1]
                )

            else:

                top2_margin = 0.0

            confidence = compute_confidence_tier(
                ticker=ticker,
                max_proba=max_proba,
                top2_margin=top2_margin,
                f1_macro=f1_macro,
            )

            confidence_metrics = {
                "f1_macro": f1_macro,
                "max_proba": max_proba,
                "top2_margin": top2_margin
            }

            # ----------------------------------------------------------
            # User-facing recommendation
            # ----------------------------------------------------------

            display_signal = get_display_signal(
                predicted_class,
                confidence,
            )

            # ----------------------------------------------------------
            # Feature importance snapshot
            # ----------------------------------------------------------

            if not hasattr(
                model,
                "feature_importances_",
            ):

                raise ValueError(
                    f"{ticker}: loaded model does not expose "
                    f"feature_importances_."
                )

            importances = (
                model.feature_importances_
            )

            if len(importances) != len(
                feature_names
            ):

                raise ValueError(
                    f"{ticker}: model feature importance count "
                    f"({len(importances)}) does not match saved "
                    f"feature count ({len(feature_names)})."
                )

            feature_importance_pairs = sorted(
                zip(
                    feature_names,
                    importances,
                ),
                key=lambda item: item[1],
                reverse=True,
            )[:15]

            feature_snapshot = {
                name: float(
                    latest_row[name]
                )
                for name, _ in feature_importance_pairs
            }

            # ----------------------------------------------------------
            # Prediction horizon
            # ----------------------------------------------------------
            #
            # The previous history.py imported:
            #
            #     TICKER_HORIZON_OVERRIDE
            #
            # from engineering.py, but that constant is not part of the
            # current engineering module.
            #
            # Until horizon configuration has a canonical persisted
            # source, use the system's current default of 10 sessions.
            # ----------------------------------------------------------

            horizon = (
                DEFAULT_PREDICTION_HORIZON
            )

            # ----------------------------------------------------------
            # Model version
            # ----------------------------------------------------------

            model_version = loaded_version

            # ----------------------------------------------------------
            # MongoDB record
            # ----------------------------------------------------------

            record = {

                "symbol": ticker,

                "market_date": (
                    market_date_str
                ),

                "prediction_timestamp": (
                    datetime.now(
                        timezone.utc
                    )
                ),

                "recommendation": (
                    display_signal
                ),

                "raw_prediction": (
                    predicted_class
                ),

                "confidence": round(
                    max_proba * 100,
                    1,
                ),

                "confidence_tier": (
                    confidence["tier"]
                ),

                "price_at_prediction": (
                    price_at_prediction
                ),

                "prediction_horizon": (
                    horizon
                ),

                "threshold_pct": (
                    thresholds[1]
                    if thresholds
                    else None
                ),

                "target_return_threshold": (
                    target_return_threshold
                ),

                "actual_price": None,

                "actual_return": None,

                "outcome": "PENDING",

                "prediction_correct": None,

                "model_version": (
                    model_version
                ),

                "feature_snapshot": (
                    feature_snapshot
                ),

                "status": "PENDING",

                "provenance_hash": None, # Will be updated below
                "provenance_status": "COMPLETE",
            }

            # ----------------------------------------------------------
            # Provenance Payload
            # ----------------------------------------------------------

            full_latest_row = computed_df.loc[market_date]
            if hasattr(full_latest_row, "ndim") and full_latest_row.ndim > 1:
                full_latest_row = full_latest_row.iloc[-1]

            raw_inputs = {
                str(k): float(v) if pd.api.types.is_numeric_dtype(type(v)) else v
                for k, v in full_latest_row.items() if k not in set(feature_names)
            }

            features_dict = {
                str(k): float(v) if pd.api.types.is_numeric_dtype(type(v)) else v
                for k, v in latest_row.items() if k in set(feature_names)
            }

            provenance_payload = {
                "provenance_schema_version": "v3",
                "symbol": ticker,
                "market_date": market_date_str,
                "prediction_horizon": horizon,
                "model_version": model_version,
                "feature_pipeline_version": pipeline_version,
                "feature_pipeline_hash": pipeline_hash,
                "feature_columns": feature_names,
                "raw_inputs": raw_inputs,
                "features": features_dict,
                "model_probabilities": model_probabilities,
                "decision_thresholds": decision_thresholds,
                "confidence_metrics": confidence_metrics,
                "recommendation": display_signal,
                "confidence_tier": confidence["tier"],
                "target_return_threshold": target_return_threshold,
                "class_mapping": {"0": "SELL", "1": "HOLD", "2": "BUY"},
                "confidence_tier_boundaries": get_confidence_boundaries(),
                "decision_context": {
                    "actionable": confidence["actionable"],
                    "f1_macro_used": confidence["f1_macro"]
                }
            }

            from src.ml.model_utils import compute_provenance_hash
            provenance_hash = compute_provenance_hash(provenance_payload)

            record["provenance_hash"] = provenance_hash
            provenance_payload["provenance_hash"] = provenance_hash
            provenance_payload["created_at"] = datetime.now(timezone.utc)

            # ----------------------------------------------------------
            # Idempotent persistence with Transaction
            # ----------------------------------------------------------
            # Phase 15 Atomic write
            try:
                with client.start_session() as session:
                    with session.start_transaction():
                        # Write prediction_history
                        history_result = db.prediction_history.update_one(
                            {
                                "symbol": ticker,
                                "market_date": market_date_str,
                                "prediction_horizon": horizon,
                            },
                            {
                                "$setOnInsert": record
                            },
                            upsert=True,
                            session=session
                        )

                        # Write prediction_provenance
                        # Only insert if history was inserted or if we are verifying idempotency
                        # Actually, if history exists but provenance doesn't, that's an inconsistent state
                        # We use update_one with $setOnInsert for idempotency as well.

                        # But wait: "identical payload -> idempotent success. conflicting payload -> raise integrity error"
                        existing_prov = db.prediction_provenance.find_one({
                            "symbol": ticker,
                            "market_date": market_date_str,
                            "prediction_horizon": horizon,
                        }, session=session)

                        if existing_prov:
                            if existing_prov.get("provenance_hash") != provenance_hash:
                                raise pymongo.errors.OperationFailure(
                                    f"Integrity Error: Provenance collision for {ticker} on {market_date_str}. "
                                    f"Existing hash {existing_prov.get('provenance_hash')} != New hash {provenance_hash}"
                                )
                        else:
                            db.prediction_provenance.insert_one(provenance_payload, session=session)

                        result = history_result

            except pymongo.errors.OperationFailure as e:
                # If transaction support is missing, PyMongo raises OperationFailure on start_transaction() or commit
                if "TransactionSupport" in str(e) or "transaction" in str(e).lower() or "replica set" in str(e).lower():
                     raise RuntimeError(f"MongoDB transaction support unavailable. Failing closed. Details: {e}")
                raise e

            if result.upserted_id:

                generated_count += 1

                logger.info(
                    "%s: prediction persisted "
                    "(market_date=%s, signal=%s, confidence=%.1f%%)",
                    ticker,
                    market_date_str,
                    display_signal,
                    max_proba * 100,
                )

            else:

                existing_count += 1

                logger.info(
                    "%s: prediction already exists "
                    "for market_date=%s, horizon=%s — idempotent.",
                    ticker,
                    market_date_str,
                    horizon,
                )

        except (PredictionDataNotReadyError, FileNotFoundError) as exc:

            error_count += 1
            failed_tickers.append(ticker)

            error_message = (
                f"{ticker}: {exc.__class__.__name__} - {exc}"
            )

            errors.append(
                error_message
            )

            logger.warning(
                "Recognized localized prediction failure for %s: %s",
                ticker,
                exc,
            )

    # ==================================================================
    # Final summary
    # ==================================================================

    logger.info(
        "Prediction history generation finished: "
        "%d new, %d skipped (stale), %d existing (idempotent), %d errors.",
        generated_count,
        skipped_count,
        existing_count,
        error_count,
    )

    if error_count > 0:

        logger.warning(
            "Prediction history generation encountered "
            "%d localized ticker errors. See logs.",
            error_count,
        )

    logger.info(
        "Prediction history generation completed."
    )

    return {
        "generated": generated_count,
        "skipped": skipped_count,
        "stale": stale_tickers,
        "existing": existing_count,
        "failed": failed_tickers,
        "errors": errors,
    }


# ======================================================================
# CLI
# ======================================================================

if __name__ == "__main__":

    import argparse
    from dotenv import load_dotenv

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, required=True, help="Target market date YYYY-MM-DD")
    args = parser.parse_args()
    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    load_dotenv()

    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://localhost:27017/",
    )

    client = MongoClient(
        mongo_uri
    )

    try:

        result = (
            generate_and_persist_predictions(
                client,
                target_date
            )
        )

        logger.info(
            "Final result: "
            "%d generated, "
            "%d skipped, "
            "%d stale, "
            "%d errors.",
            result["generated"],
            result["skipped"],
            len(result.get("stale", [])),
            len(result.get("errors", [])),
        )

    finally:

        client.close()
