import logging
# pyrefly: ignore [missing-import]
from flask import Flask, jsonify, render_template, g, request
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
from dateutil.relativedelta import relativedelta
from pymongo.errors import PyMongoError
from dotenv import load_dotenv
from xgboost import XGBClassifier
from src.data.nifty50 import TICKERS, NIFTY50_TICKER_MAP
from src.ml.confidence import compute_confidence_tier, get_display_signal
from src.features.engineering import build_feature_row, TICKER_CLASS_THRESHOLDS, apply_threshold_calibration
from src.data.sector_index_builder import NIFTY500_SECTOR_MAP

logger = logging.getLogger(__name__)

# --- SETUP ---
load_dotenv()
app = Flask(__name__)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# --- CACHING SETUP ---
cache = Cache(app, config={'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 900}) # 15-minute cache

# --- INVERT SECTOR MAP ---
TICKER_TO_SECTOR = {}
for sector, sector_tickers in NIFTY500_SECTOR_MAP.items():
    for t in sector_tickers:
        TICKER_TO_SECTOR[t] = sector


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

def get_latest_predictions_snapshot(db, active_tickers):
    """
    Finds the latest authoritative market_date and fetches predictions 
    for all active tickers. Handles mixed-date snapshots if some tickers
    are missing from the latest date.
    
    Returns: (predictions_list, meta_dict)
    """
    latest_doc = db.prediction_history.find_one({}, sort=[("market_date", -1)])
    if not latest_doc:
        return [], {
            "market_date": None,
            "expected_tickers": len(active_tickers),
            "returned_tickers": 0,
            "missing_tickers": active_tickers,
            "complete": False,
            "mixed_date": False
        }
        
    latest_market_date = latest_doc['market_date']
    
    # Check if we have predictions for all active tickers for this date
    predictions = list(db.prediction_history.find({
        "market_date": latest_market_date,
        "symbol": {"$in": active_tickers}
    }))
    
    returned_symbols = {p['symbol'] for p in predictions}
    missing_symbols = set(active_tickers) - returned_symbols
    
    mixed_date = False
    
    # If missing tickers, fetch their latest valid prediction
    if missing_symbols:
        mixed_date = True
        for ticker in missing_symbols:
            latest_ticker_doc = db.prediction_history.find_one(
                {"symbol": ticker},
                sort=[("market_date", -1)]
            )
            if latest_ticker_doc:
                predictions.append(latest_ticker_doc)
                returned_symbols.add(ticker)
                
        # Re-evaluate missing
        missing_symbols = set(active_tickers) - returned_symbols
        
    meta = {
        "market_date": latest_market_date,
        "expected_tickers": len(active_tickers),
        "returned_tickers": len(returned_symbols),
        "missing_tickers": list(missing_symbols),
        "complete": len(missing_symbols) == 0,
        "mixed_date": mixed_date
    }
    
    return predictions, meta

@app.route('/api/recommendations')
@cache.cached(timeout=300)
def get_recommendations():
    """Returns authoritative recommendation snapshots from prediction_history."""
    predictions, meta = get_latest_predictions_snapshot(db, TICKERS)
    
    if not predictions:
        return jsonify({'error': 'No predictions available.'}), 404
        
    data = []
    
    for p in predictions:
        ticker = p['symbol']
        
        # Get price data from historical_data
        # We need last_close and previous close for day_change_pct
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
            
        pred_ts = p.get('prediction_timestamp')
        if pred_ts and hasattr(pred_ts, 'isoformat'):
            pred_ts = pred_ts.isoformat()
            
        data.append({
            "ticker": ticker,
            "market_date": p['market_date'],
            "prediction_timestamp": pred_ts,
            "recommendation": p['recommendation'],
            "raw_prediction": p.get('raw_prediction', p['recommendation']),
            "confidence": p['confidence'],
            "confidence_tier": p.get('confidence_tier', 'UNKNOWN'),
            "model_version": p.get('model_version', 'unknown'),
            "last_close": round(last_close, 2),
            "day_change_pct": day_change_pct
        })
        
    # Sort data for consistent display (e.g., conviction order)
    order = {'BUY': 0, 'HOLD': 1, 'UNCERTAIN': 2, 'SELL': 3}
    data.sort(key=lambda x: (order.get(x['recommendation'], 4), -x['confidence']))
    
    return jsonify({
        "data": data,
        "meta": meta,
        "total": len(data)
    })

@app.route('/api/stocks/summary')
@cache.cached(timeout=300)
def get_stocks_summary():
    """Shared authoritative endpoint for Stocks and Screener."""
    predictions, meta = get_latest_predictions_snapshot(db, TICKERS)
    
    if not predictions:
        return jsonify({'error': 'No predictions available.'}), 404
        
    # Batch price lookup
    pipeline = [
        {"$match": {"ticker": {"$in": TICKERS}}},
        {"$sort": {"date": -1}},
        {"$group": {
            "_id": "$ticker",
            "docs": {"$push": {"close": "$close", "open": "$open", "volume": "$volume"}}
        }},
        {"$project": {"docs": {"$slice": ["$docs", 2]}}}
    ]
    price_results = list(db.historical_data.aggregate(pipeline))
    price_map = {}
    for r in price_results:
        ticker = r["_id"]
        docs = r["docs"]
        valid_docs = [d for d in docs if pd.notna(d.get('close')) and pd.notna(d.get('open'))]
        
        if len(valid_docs) >= 1:
            last_close = valid_docs[0].get('close')
            volume = valid_docs[0].get('volume')
            
            if len(valid_docs) >= 2:
                prev_close = valid_docs[1].get('close')
            else:
                prev_close = valid_docs[0].get('open')
            
            if pd.isna(prev_close) or prev_close == 0:
                prev_close = None
                day_change = None
                day_change_pct = None
            else:
                day_change = round(last_close - prev_close, 2)
                day_change_pct = round(((last_close - prev_close) / prev_close) * 100, 2)
                
            price_map[ticker] = {
                "last_close": round(last_close, 2) if pd.notna(last_close) else None,
                "previous_close": round(prev_close, 2) if pd.notna(prev_close) else None,
                "day_change": day_change,
                "day_change_pct": day_change_pct,
                "volume": volume if pd.notna(volume) else None
            }
            
    data = []
    
    for p in predictions:
        ticker = p['symbol']
        price_info = price_map.get(ticker, {})
        
        data.append({
            "ticker": ticker,
            "company_name": NIFTY50_TICKER_MAP.get(ticker, ticker),
            "sector": TICKER_TO_SECTOR.get(ticker, None),
            "market_date": p['market_date'],
            
            "last_close": price_info.get("last_close"),
            "previous_close": price_info.get("previous_close"),
            "day_change": price_info.get("day_change"),
            "day_change_pct": price_info.get("day_change_pct"),
            "volume": price_info.get("volume"),
            
            "recommendation": p['recommendation'],
            "raw_prediction": p.get('raw_prediction', p['recommendation']),
            "confidence": p['confidence'],
            "confidence_tier": p.get('confidence_tier', 'UNKNOWN'),
            "model_version": p.get('model_version', 'unknown')
        })
        
    return jsonify({
        "data": data,
        "meta": meta,
        "total": len(data)
    })

@app.route('/api/stocks/<ticker>/details')
@cache.cached(timeout=300, query_string=True)
def get_stock_details_persisted(ticker):
    """
    Returns authoritative persisted data for the Stock Details view.
    No live inference is performed.
    """
    if ticker not in TICKERS:
        return jsonify({'error': 'Ticker not supported'}), 404
        
    range_param = request.args.get('range', '1Y')
    valid_ranges = {'1M', '3M', '6M', '1Y', '5Y'}
    if range_param not in valid_ranges:
        return jsonify({'error': 'Invalid range'}), 400
        
    try:
        # Get latest market date to anchor the chart range
        latest_hist_doc = db.historical_data.find_one({"ticker": ticker}, sort=[("date", -1)])
        if not latest_hist_doc:
            return jsonify({'error': 'Historical data not found'}), 404
            
        latest_market_date_obj = latest_hist_doc['date']
        
        if range_param == '1M':
            start_date = latest_market_date_obj - relativedelta(months=1)
        elif range_param == '3M':
            start_date = latest_market_date_obj - relativedelta(months=3)
        elif range_param == '6M':
            start_date = latest_market_date_obj - relativedelta(months=6)
        elif range_param == '1Y':
            start_date = latest_market_date_obj - relativedelta(years=1)
        elif range_param == '5Y':
            start_date = latest_market_date_obj - relativedelta(years=5)
            
        chart_cursor = db.historical_data.find({
            "ticker": ticker,
            "date": {"$gte": start_date, "$lte": latest_market_date_obj}
        }).sort("date", 1)
        
        chart_data_list = []
        for row in chart_cursor:
            chart_data_list.append({
                'time': row['date'].strftime('%Y-%m-%d'),
                'open': row.get('open'),
                'high': row.get('high'),
                'low': row.get('low'),
                'close': row.get('close'),
                'volume': row.get('volume')
            })
            
        latest_docs = list(db.historical_data.find({"ticker": ticker}).sort("date", -1).limit(2))
        latest_docs = [d for d in latest_docs if pd.notna(d.get('close')) and pd.notna(d.get('open'))]
        
        market_stats = {
            "market_date": latest_market_date_obj.strftime('%Y-%m-%d'),
            "open": None, "high": None, "low": None, "last_close": None, 
            "previous_close": None, "day_change": None, "day_change_pct": None, "volume": None
        }
        
        if len(latest_docs) >= 1:
            d0 = latest_docs[0]
            market_stats["open"] = d0.get('open')
            market_stats["high"] = d0.get('high')
            market_stats["low"] = d0.get('low')
            market_stats["last_close"] = d0.get('close')
            market_stats["volume"] = d0.get('volume')
            
            if len(latest_docs) >= 2:
                prev_close = latest_docs[1].get('close')
            else:
                prev_close = d0.get('open')
                
            market_stats["previous_close"] = prev_close
            
            if prev_close:
                market_stats["day_change"] = round(d0.get('close') - prev_close, 2)
                market_stats["day_change_pct"] = round(((d0.get('close') - prev_close) / prev_close) * 100, 2)

        pred_doc = db.prediction_history.find_one({"symbol": ticker}, sort=[("market_date", -1)])
        prediction_obj = None
        if pred_doc:
            prediction_obj = {
                "market_date": pred_doc.get("market_date"),
                "recommendation": pred_doc.get("recommendation"),
                "raw_prediction": pred_doc.get("raw_prediction"),
                "confidence": pred_doc.get("confidence"),
                "confidence_tier": pred_doc.get("confidence_tier"),
                "prediction_timestamp": pred_doc.get("prediction_timestamp").isoformat() if hasattr(pred_doc.get("prediction_timestamp"), 'isoformat') else pred_doc.get("prediction_timestamp"),
                "prediction_horizon": pred_doc.get("prediction_horizon", 10),
                "model_version": pred_doc.get("model_version")
            }

        response = {
            "ticker": ticker,
            "company": {
                "name": NIFTY50_TICKER_MAP.get(ticker, ticker),
                "sector": TICKER_TO_SECTOR.get(ticker, None)
            },
            "market": market_stats,
            "prediction": prediction_obj,
            "chartData": chart_data_list,
            "meta": {
                "range": range_param,
                "chart_start": start_date.strftime('%Y-%m-%d'),
                "chart_end": latest_market_date_obj.strftime('%Y-%m-%d'),
                "chart_points": len(chart_data_list)
            }
        }
        return jsonify(response)

    except PyMongoError as e:
        logger.error(f"Database error for {ticker}: {e}")
        return jsonify({'error': 'Database failure'}), 503
    except Exception as e:
        logger.error(f"Unexpected error for {ticker}: {e}")
        return jsonify({'error': 'Unexpected internal error'}), 500

from bson import ObjectId
import math
import datetime

def normalize_news_article(doc):
    tickers = doc.get("tickers", [])
    if not tickers and doc.get("ticker"):
        tickers = [doc.get("ticker")]
    # Some older docs don't have ticker populated if they were malformed, filter Nones
    tickers = [t for t in tickers if t]
    
    return {
        "id": str(doc.get("_id", "")),
        "headline": doc.get("title", ""),
        "summary": doc.get("description", doc.get("content", "")),
        "source": doc.get("source", ""),
        "url": doc.get("url"),
        "published_at": doc.get("published_at").isoformat() + "Z" if hasattr(doc.get("published_at"), "isoformat") else None,
        "sentiment": doc.get("label", "neutral").upper(),
        "ticker": tickers[0] if tickers else None,
        "tickers": tickers
    }

@app.route('/api/news')
def get_news():
    try:
        ticker = request.args.get('ticker')
        sentiment = request.args.get('sentiment')
        try:
            page = int(request.args.get('page', 1))
            limit = int(request.args.get('limit', 25))
        except ValueError:
            return jsonify({'error': 'Invalid page or limit'}), 400
            
        if page < 1 or limit < 1 or limit > 100:
            return jsonify({'error': 'Page must be >= 1, limit must be between 1 and 100'}), 400
            
        if ticker and ticker not in TICKERS:
            return jsonify({'error': 'Unsupported ticker'}), 400
            
        if sentiment and sentiment not in ['POSITIVE', 'NEUTRAL', 'NEGATIVE']:
            return jsonify({'error': 'Unsupported sentiment'}), 400
            
        offset = (page - 1) * limit
        
        
        # Freshness Check
        newest_doc = db.news_articles.find_one({}, sort=[("published_at", -1)])
        newest_article_at = newest_doc.get("published_at") if newest_doc else None
        is_stale = False
        newest_article_at_str = None
        if newest_article_at:
            newest_article_at_str = newest_article_at.isoformat() + "Z" if hasattr(newest_article_at, "isoformat") else None
            is_stale = (datetime.datetime.utcnow() - newest_article_at).days >= 7

        query = {}
        if ticker:
            # Query matches either the new `tickers` array or the old `ticker` string field
            query["$or"] = [{"tickers": ticker}, {"ticker": ticker}]
        if sentiment:
            query["label"] = sentiment.lower()
            
        # Compute sentiment counts
        sentiment_pipeline = [
            {"$match": query},
            {"$group": {"_id": "$label", "count": {"$sum": 1}}}
        ]
        sentiment_results = list(db.news_articles.aggregate(sentiment_pipeline))
        sentiment_counts = {"POSITIVE": 0, "NEUTRAL": 0, "NEGATIVE": 0, "UNSCORED": 0}
        for r in sentiment_results:
            label = r.get("_id")
            if label and label.upper() in sentiment_counts:
                sentiment_counts[label.upper()] = r["count"]
            else:
                sentiment_counts["UNSCORED"] += r["count"]
        total = sum(sentiment_counts.values())

        cursor = db.news_articles.find(query).sort("published_at", -1).skip(offset).limit(limit)
        data = [normalize_news_article(doc) for doc in cursor]
            
        return jsonify({
            "data": data,
            "meta": {
                "page": page,
                "limit": limit,
                "total": total,
                "total_pages": math.ceil(total / limit) if total > 0 else 0,
                "sentiment_counts": sentiment_counts,
                "newest_article_at": newest_article_at_str,
                "stale": is_stale
            }
        })
    except PyMongoError as e:
        logger.error(f"Database error in /api/news: {e}")
        return jsonify({'error': 'Database unavailable'}), 503
    except Exception as e:
        logger.error(f"Unexpected error in /api/news: {e}")
        return jsonify({'error': 'Internal error'}), 500

@app.route('/api/news/<news_id>')
def get_news_detail(news_id):
    try:
        doc = db.news_articles.find_one({"_id": ObjectId(news_id)})
        if not doc:
            return jsonify({"error": "Article not found"}), 404
            
        # Fetch associated tickers (logical deduplication)
        title = doc.get("title")
        source = doc.get("source")
        if title and source:
            matching_docs = db.news_articles.find({"title": title, "source": source}, {"ticker": 1})
            tickers = list(set(d.get("ticker") for d in matching_docs if d.get("ticker")))
            doc["tickers"] = tickers
            
        return jsonify(normalize_news_article(doc, deduplicated=True))
    except Exception as e:
        return jsonify({"error": "Invalid news ID or request"}), 400
@app.route('/api/predictions/history')
def get_prediction_history():
    from flask import request
    symbol = request.args.get('symbol')
    recommendation = request.args.get('recommendation')
    outcome = request.args.get('outcome')
    model_version = request.args.get('model_version')
    limit = int(request.args.get('limit', 50))
    offset = int(request.args.get('offset', 0))
    
    query = {}
    if symbol:
        import re
        query['symbol'] = {'$regex': re.escape(symbol), '$options': 'i'}
    if recommendation: query['recommendation'] = recommendation
    if outcome: query['outcome'] = outcome
    if model_version: query['model_version'] = model_version
        
    total_count = db.prediction_history.count_documents(query)
    
    cursor = db.prediction_history.find(query).sort('market_date', -1).skip(offset).limit(limit)
    
    results = []
    for doc in cursor:
        doc['_id'] = str(doc['_id'])
        doc['prediction_timestamp'] = doc['prediction_timestamp'].isoformat() if doc.get('prediction_timestamp') else None
        if 'evaluation_timestamp' in doc and doc['evaluation_timestamp']:
            doc['evaluation_timestamp'] = doc['evaluation_timestamp'].isoformat()
        # Clean up feature_snapshot for listing
        if 'feature_snapshot' in doc:
            del doc['feature_snapshot']
        results.append(doc)
        
    return jsonify({
        'total': total_count,
        'limit': limit,
        'offset': offset,
        'data': results
    })

@app.route('/api/predictions/history/<prediction_id>')
def get_prediction_detail(prediction_id):
    try:
        doc = db.prediction_history.find_one({'_id': ObjectId(prediction_id)})
        if not doc:
            return jsonify({'error': 'Prediction not found'}), 404
            
        doc['_id'] = str(doc['_id'])
        doc['prediction_timestamp'] = doc['prediction_timestamp'].isoformat() if doc.get('prediction_timestamp') else None
        if 'evaluation_timestamp' in doc and doc['evaluation_timestamp']:
            doc['evaluation_timestamp'] = doc['evaluation_timestamp'].isoformat()
            
        return jsonify(doc)
    except Exception as e:
        return jsonify({'error': str(e)}), 400

from src.ml.monitoring import get_system_health, get_ticker_performance, fetch_evaluated_predictions, analyze_performance

@app.route('/api/predictions/performance')
@cache.cached(timeout=900, query_string=True)
def get_prediction_performance():
    ticker = request.args.get('ticker')
    model_version = request.args.get('model_version')
    
    query = {}
    if ticker: query["symbol"] = ticker
    if model_version: query["model_version"] = model_version
    
    try:
        preds = fetch_evaluated_predictions(db, query)
        perf = analyze_performance(preds)
        
        # Add basic count stats for backward compatibility
        total_predictions = db.prediction_history.count_documents(query)
        evaluated_predictions = len(preds)
        pending_query = {"status": "PENDING"}
        if ticker: pending_query["symbol"] = ticker
        pending_predictions = db.prediction_history.count_documents(pending_query)
        
        perf["total_predictions"] = total_predictions
        perf["evaluated_predictions"] = evaluated_predictions
        perf["pending_predictions"] = pending_predictions
        
        return jsonify(perf)
    except Exception as e:
        logger.error(f"Error in /api/predictions/performance: {e}")
        return jsonify({'error': 'Monitoring aggregation degraded', 'state': 'DEGRADED'}), 503

@app.route('/api/models/health')
@cache.cached(timeout=900)
def get_models_health():
    try:
        health_data = get_system_health(db)
        return jsonify(health_data)
    except Exception as e:
        logger.error(f"Error in /api/models/health: {e}")
        return jsonify({'error': 'Monitoring aggregation degraded', 'state': 'DEGRADED'}), 503

@app.route('/api/models/<ticker>/performance')
@cache.cached(timeout=900, query_string=True)
def get_ticker_model_performance(ticker):
    model_version = request.args.get('model_version')
    try:
        perf = get_ticker_performance(db, ticker, model_version)
        return jsonify(perf)
    except Exception as e:
        logger.error(f"Error in /api/models/<ticker>/performance: {e}")
        return jsonify({'error': 'Monitoring aggregation degraded', 'state': 'DEGRADED'}), 503

@app.route('/api/models')
def get_models():
    """Return a list of available models and their high-level metadata."""
    MODELS_DIR = "saved_models"
    models_data = []
    
    if os.path.exists(MODELS_DIR):
        for filename in os.listdir(MODELS_DIR):
            if filename.endswith("_metrics.json"):
                ticker = filename.replace("_metrics.json", "")
                filepath = os.path.join(MODELS_DIR, filename)
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    
                    model_metadata = data.get("model_metadata", {})
                    models_data.append({
                        "ticker": ticker,
                        "model_version": model_metadata.get("model_version"),
                        "trained_at": model_metadata.get("trained_at"),
                        "f1_macro": data.get("f1_macro"),
                        "very_low_confidence": data.get("very_low_confidence", False)
                    })
                except Exception as e:
                    print(f"Error reading {filename}: {e}")
                    
    return jsonify({
        "data": models_data,
        "total": len(models_data)
    })

@app.route('/api/models/<ticker>/intelligence')
def get_model_intelligence(ticker):
    """Return complete Model Intelligence payload for a specific ticker."""
    MODELS_DIR = "saved_models"
    metrics_path = os.path.join(MODELS_DIR, f"{ticker}_metrics.json")
    
    if not os.path.exists(metrics_path):
        return jsonify({"error": "Metrics artifact not found"}), 404
        
    try:
        with open(metrics_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        # Structure the payload as requested
        payload = {
            "ticker": ticker,
            "model_metadata": data.get("model_metadata"),  # will be null if legacy
            "metrics": {
                "f1_macro": data.get("f1_macro"),
                "per_class": data.get("per_class_metrics", {}),
                "train_size": data.get("train_size"),
                "test_size": data.get("test_size"),
                "total_rows_after_features": data.get("total_rows_after_features"),
                "mean_max_probability": data.get("confidence_stats", {}).get("mean_max_proba"),
                "mean_top2_margin": data.get("confidence_stats", {}).get("mean_top2_margin"),
                "very_low_confidence": data.get("very_low_confidence", False)
            },
            "distributions": {
                "training_labels": data.get("label_distribution", {}),
                "test_predictions": data.get("test_prediction_distribution") # will be null if legacy
            },
            "feature_importance": data.get("feature_importance", []), # will be [] if legacy
            "training": {
                "data_start": data.get("data_fingerprint", {}).get("feature_date_min"),
                "data_end": data.get("data_fingerprint", {}).get("feature_date_max"),
                "data_fingerprint": str(data.get("data_fingerprint", {}).get("row_hash", "")),
                "optuna": data.get("optuna"),
                "smote_floors": data.get("smote_floors_used"),
                "threshold_calibration": data.get("threshold_calibration")
            }
        }
        
        # Normalize numeric keys in training_labels to strings ("0" -> "SELL", etc.)
        class_map = {"0": "SELL", "1": "HOLD", "2": "BUY"}
        if payload["distributions"]["training_labels"]:
            normalized_labels = {}
            for k, v in payload["distributions"]["training_labels"].items():
                normalized_labels[class_map.get(k, k)] = v
            payload["distributions"]["training_labels"] = normalized_labels
            
        return jsonify(payload)
        
    except Exception as e:
        return jsonify({"error": f"Error reading metrics for {ticker}: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True)
