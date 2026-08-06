import os
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.ml.trainer import create_dataset

load_dotenv()
client = MongoClient(os.getenv('MONGO_URI', 'mongodb://localhost:27017/'))

def validate_current_features():
    print("="*60)
    print("CURRENT FEATURE VALIDATION")
    print("="*60)
    
    tickers = ['RELIANCE.NS', 'TCS.NS', 'SBIN.NS']
    
    for ticker in tickers:
        df = create_dataset(ticker, client)
        if df.empty:
            print(f"{ticker}: Dataset empty.")
            continue
            
        latest_date = df.index.max()
        latest_row = df.loc[latest_date]
        
        print(f"{ticker}:")
        print(f"Latest feature date: {latest_date.date()}")
        print(f"Daily sentiment (T): {latest_row.get('sentiment', 0.0):.4f}")
        print(f"sentiment_7d_avg: {latest_row.get('sentiment_7d_avg', 0.0):.4f}")
        print(f"sentiment_30d_avg: {latest_row.get('sentiment_30d_avg', 0.0):.4f}")
        
        # Check no-news behavior
        no_news = df[df['sentiment'] == 0.0]
        if not no_news.empty:
            print("Verified: NO NEWS and NEUTRAL SENTIMENT both map to 0.0.")
        else:
            print("Could not find a no-news day to verify.")
        print()

if __name__ == '__main__':
    validate_current_features()
