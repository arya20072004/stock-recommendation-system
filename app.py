from flask import Flask, jsonify, render_template, g
from flask_caching import Cache
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import pandas as pd
import pandas_ta as ta
import yfinance as yf
import joblib
import json
import os
import warnings
from datetime import datetime
from dotenv import load_dotenv
from xgboost import XGBClassifier
from nifty50 import TICKERS

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
MODELS_DIR = "models"
FEATURES_DIR = "features"

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
    Efficiently fetches recent data to calculate the latest features and generate a prediction.
    Returns a dict with recommendation, confidence, and timestamp.
    """
    prices_df = pd.DataFrame(list(db.historical_data.find({'ticker': ticker}).sort('date', -1).limit(90)))
    if prices_df.empty:
        raise FileNotFoundError("No historical data found for this ticker.")
    
    prices_df.set_index('date', inplace=True)
    prices_df.sort_index(inplace=True)
    
    nifty_df = yf.download('^NSEI', start=prices_df.index.min(), end=prices_df.index.max(), progress=False, auto_adjust=True)
    if isinstance(nifty_df.columns, pd.MultiIndex):
        nifty_df.columns = nifty_df.columns.get_level_values(0)
    
    prices_df['return'] = prices_df['close'].pct_change()
    nifty_df['nifty_return'] = nifty_df['Close'].pct_change()
    df = prices_df.join(nifty_df['nifty_return'], how='left')
    
    df['outperformance'] = df['return'] - df['nifty_return']
    
    news_df = pd.DataFrame(list(db.news_articles.find({
        'ticker': ticker, 
        'published_at': {'$gte': prices_df.index.min().to_pydatetime()}
    })))
    if not news_df.empty:
        news_df['date'] = pd.to_datetime(news_df['published_at'].dt.date)
        if 'sentiment' in news_df.columns:
            sentiment_df = news_df.groupby('date')['sentiment'].apply(lambda x: x.str['score'].mean()).to_frame()
            df = df.join(sentiment_df, how='left')
        else:
            df['sentiment'] = 0.0
    else:
        df['sentiment'] = 0.0
        
    df.fillna(0, inplace=True)
    
    # Technical indicators with proper lagging to prevent data leakage
    df.ta.rsi(length=14, append=True)
    df.ta.macd(append=True)
    df.ta.bbands(append=True)
    df.ta.atr(append=True)
    
    # Shift technical indicator columns to use only past data
    for col in df.columns:
        if 'RSI' in col or 'MACD' in col or 'BBL' in col or 'BBM' in col or 'BBU' in col or 'ATR' in col:
            df[col] = df[col].shift(1)
    
    df['sentiment_7d_avg'] = df['sentiment'].shift(1).rolling(window=7).mean()
    df['price_change_1d'] = df['close'].shift(1).pct_change(1)
    df['price_change_5d'] = df['close'].shift(1).pct_change(5)
    df['market_correlation'] = df['return'].shift(1).rolling(window=30).corr(df['nifty_return'])
    df.dropna(inplace=True)

    if df.empty:
        raise ValueError("Not enough data to make a prediction after feature engineering.")

    model = models[ticker]
    feature_names = feature_lists[ticker]
    
    latest_features = df[feature_names].iloc[-1].values.reshape(1, -1)
    
    # Use predict_proba for confidence score
    proba = model.predict_proba(latest_features)[0]
    confidence = float(max(proba))
    predicted_class_idx = proba.argmax()
    
    # Map prediction to class name
    recommendation_map = {0: 'SELL', 1: 'HOLD', 2: 'BUY'}
    recommendation = recommendation_map.get(predicted_class_idx, 'HOLD')
    
    # If confidence is low, mark as UNCERTAIN
    if confidence < 0.60:
        recommendation = 'UNCERTAIN'
    
    return {
        'recommendation': recommendation,
        'confidence': round(confidence * 100, 1),
        'predicted_at': datetime.now().strftime('%d %b %Y, %I:%M %p')
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
        
        chart_data_list = [
            {
                'time': row['date'].strftime('%Y-%m-%d'),
                'open': row['open'], 'high': row['high'],
                'low': row['low'], 'close': row['close'],
                'volume': row.get('volume', 0)
            } for _, row in chart_df.iterrows()
        ]
        
        return jsonify({
            'chartData': chart_data_list,
            'recommendation': prediction_data['recommendation'],
            'confidence': prediction_data['confidence'],
            'predicted_at': prediction_data['predicted_at'],
            'top_features': top_features
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

            # Get latest two data points for price stats
            latest_docs = list(db.historical_data.find({'ticker': ticker}).sort('date', -1).limit(2))
            if len(latest_docs) >= 1:
                last_close = latest_docs[0].get('close', 0)
                last_open = latest_docs[0].get('open', last_close)
                day_change_pct = round(((last_close - last_open) / last_open) * 100, 2) if last_open else 0
            else:
                last_close = 0
                day_change_pct = 0

            portfolio_data.append({
                'ticker': ticker,
                'recommendation': prediction['recommendation'],
                'confidence': prediction['confidence'],
                'last_close': round(last_close, 2),
                'day_change_pct': day_change_pct
            })
        except Exception as e:
            print(f"Portfolio: skipping {ticker} — {e}")
            continue

    order = {'BUY': 0, 'HOLD': 1, 'UNCERTAIN': 2, 'SELL': 3}
    portfolio_data.sort(key=lambda x: (order.get(x['recommendation'], 4), -x['confidence']))

    return jsonify({'portfolio': portfolio_data})

if __name__ == '__main__':
    app.run(debug=True)

