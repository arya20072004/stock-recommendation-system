from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import yfinance as yf

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
db = client["stock_market_db"]

docs = list(db.historical_data.find(
    {"date": datetime(2026, 8, 3)},
    {"ticker": 1, "date": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "_id": 0}
))
df = pd.DataFrame(docs)
pd.set_option("display.max_rows", None)
print(df)
print("\n--- NaN counts per column ---")
print(df[["open", "high", "low", "close", "volume"]].isna().sum())

for ticker in ["RELIANCE.NS", "TCS.NS", "SBIN.NS", "HDFCBANK.NS"]:
    data = yf.download(ticker, start="2026-08-01", end="2026-08-05", interval="1d", progress=False)
    print(f"\n=== {ticker} ===")
    print(data)

    # 1. Confirm TMPV.NS status
print("TMPV.NS rows:", db.historical_data.count_documents({"ticker": "TMPV.NS"}))

# 2. Full NaN sweep across all 51 (using your existing test_ohlv-style query)
docs = list(db.historical_data.find(
    {"date": {"$gte": datetime(2026,8,1)}},
    {"ticker":1,"date":1,"open":1,"high":1,"low":1,"close":1,"volume":1,"_id":0}
))
df = pd.DataFrame(docs)
print(df[df[["open","high","low","close","volume"]].isna().any(axis=1)])