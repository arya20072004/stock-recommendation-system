# app.py

from flask import Flask, render_template
from pymongo import MongoClient
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta

# --- SETUP ---
app = Flask(__name__)
client = MongoClient('mongodb://localhost:27017/')
db = client['stock_market_db']
TICKERS = ['RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS']

# --- RECOMMENDATION LOGIC ---
def get_recommendations():
    recommendations = []
    
    for ticker in TICKERS:
        # 1. Get Technical Indicator (RSI)
        hist_data = list(db.historical_data.find({'ticker': ticker}))
        if not hist_data:
            continue
            
        df = pd.DataFrame(hist_data)
        df.set_index('date', inplace=True)
        df.sort_index(inplace=True)
        df.ta.rsi(length=14, append=True)
        latest_rsi = df['RSI_14'].iloc[-1]
        
        # 2. Get News Sentiment Score
        seven_days_ago = datetime.now() - timedelta(days=7)
        news_articles = list(db.news_articles.find({
            'ticker': ticker,
            'published_at': {'$gte': seven_days_ago}
        }))
        
        avg_sentiment = 0
        if news_articles:
            total_sentiment = sum(article['sentiment']['score'] for article in news_articles)
            avg_sentiment = total_sentiment / len(news_articles)

        # 3. Apply Rules
        recommendation = 'HOLD'
        if avg_sentiment > 0.15 and latest_rsi < 70: # Positive sentiment and not overbought
            recommendation = 'BUY'
        elif avg_sentiment < -0.15 and latest_rsi > 70: # Negative sentiment and overbought
            recommendation = 'SELL'
            
        recommendations.append({
            'ticker': ticker,
            'rsi': latest_rsi,
            'sentiment': avg_sentiment,
            'recommendation': recommendation
        })
        
    return recommendations

# --- FLASK ROUTE ---
@app.route('/')
def dashboard():
    stocks_data = get_recommendations()
    return render_template('index.html', stocks=stocks_data)

if __name__ == '__main__':
    app.run(debug=True)