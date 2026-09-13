import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv

def get_db():
    load_dotenv()
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(uri)
    return client["stock_market_db"]

def main():
    db = get_db()
    
    preds = list(db["prediction_history"].find({"market_date": "2026-09-02"}).limit(1))
    for p in preds:
        f = p.get("feature_snapshot", {})
        print("Features keys:", list(f.keys()))
        print("outperformance:", f.get("outperformance"))
        print("market_regime:", f.get("market_regime"))
        print("nifty_return:", f.get("nifty_return", f.get("nifty_ret_1d")))

    # Let's check historical data for NSEI
    nsei = list(db["historical_data"].find({"ticker": "^NSEI"}).sort("date", -1).limit(5))
    for n in nsei:
        print("NSEI:", n.get("date"))

if __name__ == "__main__":
    main()
