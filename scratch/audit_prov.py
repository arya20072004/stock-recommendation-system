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
    
    provs = list(db["prediction_provenance"].find({"market_date": "2026-09-02"}).limit(1))
    for p in provs:
        f = p.get("features", {})
        print("Provenance features keys count:", len(f.keys()))
        print("outperformance:", f.get("outperformance"))
        print("market_regime:", f.get("market_regime"))
        print("nifty_return:", f.get("nifty_ret_1d")) # often nifty_return is nifty_ret_1d

if __name__ == "__main__":
    main()
