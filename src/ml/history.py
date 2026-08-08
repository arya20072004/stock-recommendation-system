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

from datetime import datetime, timezone

import joblib
import pymongo

from pymongo import MongoClient
from xgboost import XGBClassifier

from src.data.nifty50 import TICKERS
from src.features.engineering import (
    build_feature_row,
    TICKER_CLASS_THRESHOLDS,
    apply_threshold_calibration,
    get_target_return_threshold,
)
from src.ml.confidence import (
    compute_confidence_tier,
    get_display_signal,
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

    if not version or not expected_model_hash or not expected_feature_hash:
        raise ValueError(f"Malformed active manifest for {ticker}. Failing closed.")

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

    return model, feature_names, version


# ======================================================================
# Latest-row validation
# ======================================================================

def get_latest_valid_feature_row(
    ticker: str,
    computed_df,
    feature_names: list[str],
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

        raise ValueError(
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

    # --------------------------------------------------------------
    # Determine latest market date
    # --------------------------------------------------------------

    latest_market_date = computed_df.index.max()

    if latest_market_date is None:

        raise ValueError(
            f"{ticker}: could not determine latest market date."
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

        raise ValueError(
            f"{ticker}: latest market row is not inference-ready. "
            f"latest_market_date={latest_market_date}, "
            f"last_fully_valid_date={last_fully_valid_date}, "
            f"missing_feature_count={len(missing_features)}, "
            f"missing_features={missing_features}"
        )

    return latest_market_date, latest_row


# ======================================================================
# Prediction generation
# ======================================================================

def generate_and_persist_predictions(client):
    """
    Generate and persist one immutable daily prediction snapshot for
    every configured ticker.

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

    # ==================================================================
    # Counters
    # ==================================================================

    generated_count = 0
    skipped_count = 0
    error_count = 0

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

            model, feature_names, loaded_version = load_active_bundle(
                ticker
            )

            # ----------------------------------------------------------
            # Rebuild features
            # ----------------------------------------------------------

            computed_df = build_feature_row(
                ticker,
                client,
                db,
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
                market_date,
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

            # ----------------------------------------------------------
            # Threshold calibration
            # ----------------------------------------------------------

            thresholds = (
                TICKER_CLASS_THRESHOLDS.get(
                    ticker
                )
            )

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
            )

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
            }

            # ----------------------------------------------------------
            # Idempotent persistence
            # ----------------------------------------------------------

            result = (
                db.prediction_history
                .update_one(
                    {
                        "symbol": ticker,
                        "market_date": (
                            market_date_str
                        ),
                        "prediction_horizon": (
                            horizon
                        ),
                    },
                    {
                        "$setOnInsert": record
                    },
                    upsert=True,
                )
            )

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

                skipped_count += 1

                logger.info(
                    "%s: prediction already exists "
                    "for market_date=%s, horizon=%s — skipped.",
                    ticker,
                    market_date_str,
                    horizon,
                )

        except Exception as exc:

            error_count += 1

            error_message = (
                f"{ticker}: {exc}"
            )

            errors.append(
                error_message
            )

            logger.error(
                "Failed to generate prediction for %s: %s",
                ticker,
                exc,
            )

    # ==================================================================
    # Final summary
    # ==================================================================

    logger.info(
        "Prediction history generation finished: "
        "%d new, %d skipped, %d errors.",
        generated_count,
        skipped_count,
        error_count,
    )

    # ==================================================================
    # Fail batch when ticker errors occurred
    # ==================================================================

    if error_count > 0:

        logger.error(
            "Prediction history generation encountered "
            "%d ticker errors:",
            error_count,
        )

        for error in errors:

            logger.error(
                "  - %s",
                error,
            )

        raise RuntimeError(
            "Prediction history generation failed for "
            f"{error_count} ticker(s). "
            "See errors above."
        )

    logger.info(
        "Prediction history generation completed successfully."
    )

    return {
        "generated": generated_count,
        "skipped": skipped_count,
        "errors": error_count,
    }


# ======================================================================
# CLI
# ======================================================================

if __name__ == "__main__":

    from dotenv import load_dotenv

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
                client
            )
        )

        logger.info(
            "Final result: "
            "%d generated, "
            "%d skipped, "
            "%d errors.",
            result["generated"],
            result["skipped"],
            result["errors"],
        )

    finally:

        client.close()
