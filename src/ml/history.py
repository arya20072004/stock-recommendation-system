import logging
import hashlib
import os
import json
import warnings
from datetime import datetime
from pymongo import MongoClient
import pymongo
import joblib
from xgboost import XGBClassifier

from src.data.nifty50 import TICKERS
from src.features.engineering import build_feature_row, TICKER_CLASS_THRESHOLDS, apply_threshold_calibration
from src.ml.confidence import compute_confidence_tier, get_display_signal

logger = logging.getLogger(__name__)

MODELS_DIR = "saved_models"
FEATURES_DIR = "saved_features"

def get_model_version(ticker):
    """
    Computes a deterministic model version by hashing the .ubj file.
    Returns the first 12 hex characters of the SHA-256 hash.
    """
    ubj_path = os.path.join(MODELS_DIR, f"model_{ticker}.ubj")
    if not os.path.exists(ubj_path):
        # Fallback to joblib if ubj is missing, though ubj is preferred
        joblib_path = os.path.join(MODELS_DIR, f"model_{ticker}.joblib")
        if not os.path.exists(joblib_path):
            return "unknown"
        ubj_path = joblib_path

    sha256 = hashlib.sha256()
    try:
        with open(ubj_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()[:12]
    except Exception as e:
        logger.error(f"Error hashing model for {ticker}: {e}")
        return "error"

def generate_and_persist_predictions(client):
    """
    Runs the inference pipeline for all tickers and persists the results
    to the prediction_history collection idempotently.
    Intended to be run at the end of the daily batch pipeline.
    """
    db = client['stock_market_db']
    
    # Ensure a unique index exists to make inserts idempotent
    # We use symbol + market_date + horizon
    db.prediction_history.create_index(
        [("symbol", pymongo.ASCENDING), ("market_date", pymongo.DESCENDING), ("prediction_horizon", pymongo.ASCENDING)],
        unique=True
    )
    db.prediction_history.create_index("status")
    db.prediction_history.create_index("prediction_timestamp")
    
    generated_count = 0
    skipped_count = 0
    error_count = 0
    
    logger.info("Starting historical prediction generation...")
    
    for ticker in TICKERS:
        try:
            ubj_path = os.path.join(MODELS_DIR, f"model_{ticker}.ubj")
            joblib_path = os.path.join(MODELS_DIR, f"model_{ticker}.joblib")
            features_path = os.path.join(FEATURES_DIR, f"features_{ticker}.json")
            
            if not os.path.exists(features_path):
                continue
                
            model = None
            if os.path.exists(ubj_path):
                model = XGBClassifier()
                model.load_model(ubj_path)
            elif os.path.exists(joblib_path):
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', category=UserWarning, message='.*pickle.*')
                    model = joblib.load(joblib_path)
            else:
                continue
                
            with open(features_path, 'r') as f:
                feature_names = json.load(f)
                
            computed_df = build_feature_row(ticker, client, db)
            if computed_df.empty:
                logger.warning(f"Feature engineering returned empty DataFrame for {ticker}")
                error_count += 1
                continue
                
            valid_df = computed_df[feature_names].dropna()
            if valid_df.empty:
                logger.warning(f"Not enough valid data for {ticker} after feature engineering")
                error_count += 1
                continue
                
            latest_row = valid_df.iloc[-1]
            latest_features = latest_row.values.reshape(1, -1)
            
            # The market date is the index of the last row
            market_date = valid_df.index[-1]
            market_date_str = market_date.strftime("%Y-%m-%d")
            
            # Extract price at prediction (close price)
            price_at_prediction = float(computed_df.loc[market_date, 'close'])
            
            # Inference
            proba = model.predict_proba(latest_features)[0]
            thresholds = TICKER_CLASS_THRESHOLDS.get(ticker)
            predicted_class_idx = apply_threshold_calibration(proba, thresholds)
            predicted_class = ["SELL", "HOLD", "BUY"][predicted_class_idx]
            
            max_proba = float(proba.max())
            sorted_p = sorted(proba, reverse=True)
            top2_margin = float(sorted_p[0] - sorted_p[1])
            
            confidence = compute_confidence_tier(
                ticker=ticker,
                max_proba=max_proba,
                top2_margin=top2_margin,
            )
            display_signal = get_display_signal(predicted_class, confidence)
            
            # Calculate top features for snapshot
            importances = model.feature_importances_
            feature_importance_pairs = sorted(
                zip(feature_names, importances),
                key=lambda x: x[1],
                reverse=True
            )[:15] # Store top 15 features for history snapshot
            
            feature_snapshot = {
                name: float(latest_row[name]) for name, _ in feature_importance_pairs
            }
            
            # Use 10 as default horizon (aligning with TICKER_HORIZON_OVERRIDE default)
            # You could import TICKER_HORIZON_OVERRIDE if it varies per ticker
            from src.features.engineering import TICKER_HORIZON_OVERRIDE
            horizon = TICKER_HORIZON_OVERRIDE.get(ticker, 10)
            
            model_version = get_model_version(ticker)
            
            record = {
                "symbol": ticker,
                "market_date": market_date_str,
                "prediction_timestamp": datetime.utcnow(),
                "recommendation": display_signal,
                "raw_prediction": predicted_class,
                "confidence": round(max_proba * 100, 1),
                "confidence_tier": confidence["tier"],
                "price_at_prediction": price_at_prediction,
                "prediction_horizon": horizon,
                "threshold_pct": thresholds[1] if thresholds else None, # Buy threshold
                "actual_price": None,
                "actual_return": None,
                "outcome": "PENDING",
                "prediction_correct": None,
                "model_version": model_version,
                "feature_snapshot": feature_snapshot,
                "status": "PENDING"
            }
            
            # Upsert using symbol, market_date, and horizon
            result = db.prediction_history.update_one(
                {
                    "symbol": ticker,
                    "market_date": market_date_str,
                    "prediction_horizon": horizon
                },
                {"$setOnInsert": record},
                upsert=True
            )
            
            if result.upserted_id:
                generated_count += 1
            else:
                skipped_count += 1
                
        except Exception as e:
            logger.error(f"Failed to generate prediction for {ticker}: {e}")
            error_count += 1
            
    logger.info(f"Historical predictions generated: {generated_count} new, {skipped_count} skipped (already exist), {error_count} errors.")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    generate_and_persist_predictions(client)
