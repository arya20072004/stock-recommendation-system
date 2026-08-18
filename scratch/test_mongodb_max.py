import os
import sys
import pandas as pd
from pymongo import MongoClient

sys.path.append('C:/Users/aryab/Coding/stock_recommendations')
from src.data.nifty50 import TICKERS
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
client = MongoClient(mongo_uri, tlsAllowInvalidCertificates=True)
db = client['stock_market_db']

print("--- HISTORICAL DATA ---")
hist_dates = list(db.historical_data.aggregate([
    {"$group": {"_id": "$ticker", "max_date": {"$max": "$date"}}}
]))
if not hist_dates:
    print("No historical data found")
else:
    max_dates = [doc['max_date'] for doc in hist_dates if isinstance(doc['max_date'], pd.Timestamp) or type(doc['max_date']).__name__ == 'datetime']
    if max_dates:
        print(f"HISTORICAL_MIN_MAX: {min(max_dates)}")
        print(f"HISTORICAL_MAX_MAX: {max(max_dates)}")
        
        # Check specific dates for all tickers
        import datetime
        d_14 = datetime.datetime(2026, 8, 14)
        d_17 = datetime.datetime(2026, 8, 17)
        c_14 = db.historical_data.count_documents({"date": d_14})
        c_17 = db.historical_data.count_documents({"date": d_17})
        print(f"AUG_14_COUNT: {c_14}")
        print(f"AUG_17_COUNT: {c_17}")
    else:
        print("Max dates not parseable")

print("--- PCR DATA ---")
pcr_max = db.pcr_data.find_one(sort=[("date", -1)])
if pcr_max:
    print(f"PCR_MAX_DATE: {pcr_max.get('date')}")
else:
    print("No PCR data")

print("--- NEWS DATA ---")
news_max = db.news_articles.find_one(sort=[("published_at", -1)])
if news_max:
    print(f"NEWS_MAX_DATE: {news_max.get('published_at')}")
else:
    print("No News data")

from datetime import datetime, timedelta
now = datetime.now()
days_7 = now - timedelta(days=7)
recent_news_tickers = len(db.news_articles.distinct("tickers", {"published_at": {"$gte": days_7}}))
print(f"NEWS_7D_TICKERS: {recent_news_tickers}")

