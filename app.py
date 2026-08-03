import logging
# pyrefly: ignore [missing-import]
from flask import Flask, jsonify, render_template, g
from flask_caching import Cache
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import pandas as pd
import numpy as np
import yfinance as yf
import joblib
import json
import os
import warnings
from datetime import datetime
from dotenv import load_dotenv
from xgboost import XGBClassifier
from src.data.nifty50 import TICKERS
from src.ml.confidence import compute_confidence_tier, get_display_signal
from src.features.engineering import build_feature_row, TICKER_CLASS_THRESHOLDS, apply_threshold_calibration

logger = logging.getLogger(__name__)

# --- SETUP ---
load_dotenv()
app = Flask(__name__)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# --- CACHING SETUP ---
cache = Cache(app, config={'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 900}) # 15-minute cache

# --- DATABASE & MODEL LOADING ---
client = MongoClient(MONGO_URI)
db = client['stock_market_db']
models = {}
feature_lists = {}

print("--- Loading ML Models on Startup ---")
MODELS_DIR = "saved_models"
FEATURES_DIR = "saved_features"

for ticker in TICKERS:
    try:
        ubj_path = os.path.join(MODELS_DIR, f"model_{ticker}.ubj")
        joblib_path = os.path.join(MODELS_DIR, f"model_{ticker}.joblib")
        features_path = os.path.join(FEATURES_DIR, f"features_{ticker}.json")
        
        if os.path.exists(features_path):
            # Try to load from .ubj first (native format)
            if os.path.exists(ubj_path):
                print(f"Loading model and features for {ticker} (native format)...")
                model = XGBClassifier()
                model.load_model(ubj_path)
                models[ticker] = model
            # Fall back to .joblib if .ubj doesn't exist
            elif os.path.exists(joblib_path):
                print(f"Loading model and features for {ticker} (joblib format)...")
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', category=UserWarning, message='.*pickle.*')
                    models[ticker] = joblib.load(joblib_path)
            else:
                print(f"No model file found for {ticker}")
                continue
            
            with open(features_path, 'r') as f:
                feature_lists[ticker] = json.load(f)
    except Exception as e:
        print(f"Could not load model for {ticker}: {e}")
print("--- Model Loading Complete ---")


# --- PREDICTION FUNCTION ---
def get_latest_prediction(ticker):
    """
    Builds the full feature DataFrame via the shared feature_engineering
    module (identical pipeline to ml_trainer.py's create_dataset minus
    target labels), then generates a prediction using the loaded model.

    Returns a dict with recommendation, confidence, and timestamp.
    """
    # Build the full feature DataFrame using the shared pipeline.
    # build_feature_row fetches HISTORY_YEARS of data (or overridden
    # amount for tickers in TICKER_HISTORY_OVERRIDE) — enough to
    # satisfy all rolling windows (200-day Nifty SMA, 30-day
    # correlation, 20-day OBV/VWAP, 5-day sector momentum).
    computed_df = build_feature_row(ticker, client, db)

    if computed_df.empty:
        raise ValueError(f"Feature engineering returned empty DataFrame for {ticker}")

    model = models[ticker]
    feature_names = feature_lists[ticker]

    # --- Safety check: detect feature columns missing from computed DataFrame ---
    missing_cols = set(feature_names) - set(computed_df.columns)
    if missing_cols:
        logger.error(
            "%s: %d feature columns required by model are missing from "
            "computed DataFrame: %s. Available columns: %s",
            ticker, len(missing_cols), sorted(missing_cols),
            sorted(computed_df.columns.tolist()),
        )
        raise ValueError(
            f"{ticker}: model expects features {sorted(missing_cols)} "
            f"which are missing from the computed feature DataFrame. "
            f"This indicates a training/inference pipeline mismatch."
        )

    # Drop rows where any required feature is NaN, then take the last row
    valid_df = computed_df[feature_names].dropna()
    if valid_df.empty:
        raise ValueError(
            f"Not enough data for {ticker} after feature engineering "
            f"— all rows have NaN in required feature columns."
        )

    latest_features = valid_df.iloc[-1].values.reshape(1, -1)

    proba = model.predict_proba(latest_features)[0]
    thresholds = TICKER_CLASS_THRESHOLDS.get(ticker)
    predicted_class_idx = apply_threshold_calibration(proba, thresholds)
    predicted_class = ["SELL", "HOLD", "BUY"][predicted_class_idx]
    raw_argmax_class = ["SELL", "HOLD", "BUY"][int(proba.argmax())]
    calibration_changed_prediction = (predicted_class != raw_argmax_class)

    max_proba   = float(proba.max())
    sorted_p    = sorted(proba, reverse=True)
    top2_margin = float(sorted_p[0] - sorted_p[1])

    confidence = compute_confidence_tier(
        ticker=ticker,
        max_proba=max_proba,
        top2_margin=top2_margin,
    )
    display_signal = get_display_signal(predicted_class, confidence)

    return {
        "recommendation":  display_signal,
        "raw_prediction":  predicted_class,
        "raw_argmax_prediction": raw_argmax_class,
        "threshold_calibration_applied": thresholds is not None,
        "calibration_changed_prediction": calibration_changed_prediction,
        "confidence_tier": confidence["tier"],
        "confidence":      round(max_proba * 100, 1),
        "probabilities": {
            "SELL": round(float(proba[0]), 4),
            "HOLD": round(float(proba[1]), 4),
            "BUY":  round(float(proba[2]), 4),
        },
        "confidence_detail": confidence,
        "predicted_at": datetime.now().strftime("%d %b %Y, %I:%M %p"),
    }


# --- API Endpoints ---

@app.route('/')
def index():
    """Serves the main HTML application."""
    return render_template('index.html')

@app.route('/api/stocks')
def get_stock_list():
    """Returns the list of available stocks."""
    return jsonify(sorted(list(models.keys())))

@app.route('/api/stocks/<ticker>')
@cache.cached(timeout=900)
def get_stock_data(ticker):
    """
    Returns historical chart data and a fresh ML prediction with confidence.
    """
    if ticker not in models:
        return jsonify({'error': f'Model for {ticker} is not loaded or available.'}), 404

    try:
        prediction_data = get_latest_prediction(ticker)
        model = models[ticker]
        features_list = feature_lists[ticker]
        importances = model.feature_importances_
        feature_importance_pairs = sorted(
            zip(features_list, importances),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        top_features = [
            {'feature': name, 'importance': round(float(score), 4)}
            for name, score in feature_importance_pairs
        ]
        
        chart_df = pd.DataFrame(list(db.historical_data.find({'ticker': ticker}).sort('date', 1)))
        
        # Drop rows with missing crucial price data so frontend doesn't calculate with nulls
        chart_df = chart_df.dropna(subset=['close', 'open'])
        
        chart_df['prev_close'] = chart_df['close'].shift(1).fillna(chart_df['open'])
        
        chart_data_list = [
            {
                'time': row['date'].strftime('%Y-%m-%d'),
                'open': row['open'], 
                'high': row.get('high') if pd.notna(row.get('high')) else row['open'],
                'low': row.get('low') if pd.notna(row.get('low')) else row['open'],
                'close': row['close'],
                'prev_close': row['prev_close'],
                'volume': row.get('volume', 0) if pd.notna(row.get('volume', 0)) else 0
            } for _, row in chart_df.iterrows()
        ]
        
        return jsonify({
            'chartData': chart_data_list,
            'recommendation': prediction_data['recommendation'],
            'confidence': prediction_data['confidence'],
            'predicted_at': prediction_data['predicted_at'],
            'top_features': top_features,
            'threshold_calibration_applied': prediction_data.get('threshold_calibration_applied', False),
            'calibration_changed_prediction': prediction_data.get('calibration_changed_prediction', False),
            'raw_argmax_prediction': prediction_data.get('raw_argmax_prediction', '')
        })

    except ServerSelectionTimeoutError as e:
        print(f"MongoDB connection error for {ticker}: {e}")
        return jsonify({'error': 'Database unavailable. Please ensure MongoDB is running.', 'status': 503}), 503
    except FileNotFoundError as e:
        print(f"File Not Found error for {ticker}: {e}")
        return jsonify({'error': f'Data for {ticker} not found in the database.'}), 404
    except (KeyError, ValueError, IndexError) as e:
        print(f"Data processing error for {ticker}: {type(e).__name__} - {e}")
        return jsonify({'error': f'Could not process live data for {ticker}. The data might be insufficient.'}), 500
    except Exception as e:
        print(f"An unexpected error occurred for {ticker}: {type(e).__name__} - {e}")
        return jsonify({'error': 'An unexpected server error occurred.'}), 500

@app.route('/portfolio')
def portfolio_page():
    """Serves the portfolio overview page."""
    return render_template('portfolio.html')

@app.route('/api/portfolio')
@cache.cached(timeout=900)
def get_portfolio():
    """Returns ML signals for all loaded stocks, sorted by conviction."""
    portfolio_data = []

    for ticker in models:
        try:
            prediction = get_latest_prediction(ticker)

            # Get latest valid data points for price stats
            latest_docs = list(db.historical_data.find({'ticker': ticker}).sort('date', -1).limit(5))
            latest_docs = [d for d in latest_docs if pd.notna(d.get('close')) and pd.notna(d.get('open'))]
            
            if len(latest_docs) >= 1:
                last_close = latest_docs[0].get('close', 0)
                if len(latest_docs) >= 2:
                    prev_close = latest_docs[1].get('close', latest_docs[0].get('open', last_close))
                else:
                    prev_close = latest_docs[0].get('open', last_close)
                day_change_pct = round(((last_close - prev_close) / prev_close) * 100, 2) if prev_close else 0
            else:
                last_close = 0
                day_change_pct = 0

            portfolio_data.append({
                'ticker': ticker,
                'recommendation': prediction['recommendation'],
                'confidence': prediction['confidence'],
                'last_close': round(last_close, 2),
                'day_change_pct': day_change_pct,
                'threshold_calibration_applied': prediction.get('threshold_calibration_applied', False),
                'calibration_changed_prediction': prediction.get('calibration_changed_prediction', False)
            })
        except Exception as e:
            print(f"Portfolio: skipping {ticker} — {e}")
            continue

    order = {'BUY': 0, 'HOLD': 1, 'UNCERTAIN': 2, 'SELL': 3}
    portfolio_data.sort(key=lambda x: (order.get(x['recommendation'], 4), -x['confidence']))

    return jsonify({'portfolio': portfolio_data})

if __name__ == '__main__':
    app.run(debug=True)

