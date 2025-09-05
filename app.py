from flask import Flask, jsonify, render_template, g
from flask_caching import Cache
from pymongo import MongoClient
import pandas as pd
import pandas_ta as ta
import yfinance as yf
import joblib
import json
import os
from dotenv import load_dotenv
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
        model_path = os.path.join(MODELS_DIR, f"model_{ticker}.joblib")
        features_path = os.path.join(FEATURES_DIR, f"features_{ticker}.json")
        
        if os.path.exists(model_path) and os.path.exists(features_path):
            print(f"Loading model and features for {ticker}...")
            models[ticker] = joblib.load(model_path)
            with open(features_path, 'r') as f:
                feature_lists[ticker] = json.load(f)
    except Exception as e:
        print(f"Could not load model for {ticker}: {e}")
print("--- Model Loading Complete ---")


# --- PREDICTION FUNCTION ---
def get_latest_prediction(ticker):
    """
    Efficiently fetches recent data to calculate the latest features and generate a prediction.
    """
    prices_df = pd.DataFrame(list(db.historical_data.find({'ticker': ticker}).sort('date', -1).limit(90)))
    if prices_df.empty:
        raise FileNotFoundError("No historical data found for this ticker.")
    
    prices_df.set_index('date', inplace=True)
    prices_df.sort_index(inplace=True)
    
    nifty_df = yf.download('^NSEI', start=prices_df.index.min(), end=prices_df.index.max(), progress=False, auto_adjust=True)
    
    prices_df['return'] = prices_df['close'].pct_change()
    nifty_df['nifty_return'] = nifty_df['Close'].pct_change()
    df = prices_df.join(nifty_df['nifty_return'], how='left')
    
    # --- CORRECTED LINE ---
    # The 'outperformance' feature was missing, which caused a mismatch with the trained model.
    # This line makes the live data preparation identical to the training data preparation.
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
    df.ta.rsi(length=14, append=True)
    df.ta.macd(append=True)
    df.ta.bbands(append=True)
    df.ta.atr(append=True)
    df['sentiment_7d_avg'] = df['sentiment'].rolling(window=7).mean()
    df['price_change_1d'] = df['close'].pct_change(1)
    df['price_change_5d'] = df['close'].pct_change(5)
    df['market_correlation'] = df['return'].rolling(window=30).corr(df['nifty_return'])
    df.dropna(inplace=True)

    if df.empty:
        raise ValueError("Not enough data to make a prediction after feature engineering.")

    model = models[ticker]
    feature_names = feature_lists[ticker]
    
    latest_features = df[feature_names].iloc[-1].values.reshape(1, -1)
    prediction_code = model.predict(latest_features)[0]
    
    recommendation_map = {0: 'SELL', 1: 'HOLD', 2: 'BUY'}
    return recommendation_map.get(prediction_code, 'HOLD')

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
    Returns historical chart data and a fresh ML prediction.
    """
    if ticker not in models:
        return jsonify({'error': f'Model for {ticker} is not loaded or available.'}), 404

    try:
        recommendation = get_latest_prediction(ticker)
        
        chart_df = pd.DataFrame(list(db.historical_data.find({'ticker': ticker}).sort('date', 1)))
        
        chart_data_list = [
            {
                'date': row['date'].strftime('%Y-%m-%d'),
                'open': row['open'], 'high': row['high'],
                'low': row['low'], 'close': row['close']
            } for _, row in chart_df.iterrows()
        ]
        
        return jsonify({
            'chartData': chart_data_list,
            'recommendation': recommendation
        })

    except FileNotFoundError as e:
        print(f"File Not Found error for {ticker}: {e}")
        return jsonify({'error': f'Data for {ticker} not found in the database.'}), 404
    except (KeyError, ValueError, IndexError) as e:
        print(f"Data processing error for {ticker}: {type(e).__name__} - {e}")
        return jsonify({'error': f'Could not process live data for {ticker}. The data might be insufficient.'}), 500
    except Exception as e:
        print(f"An unexpected error occurred for {ticker}: {type(e).__name__} - {e}")
        return jsonify({'error': 'An unexpected server error occurred.'}), 500

if __name__ == '__main__':
    app.run(debug=True)

