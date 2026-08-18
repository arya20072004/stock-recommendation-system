import os
import sys
from pymongo import MongoClient

def get_db():
    from dotenv import load_dotenv
    load_dotenv("c:/Users/aryab/Coding/stock_recommendations/.env")
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    return client["stock_market_db"]

db = get_db()
actives = list(db.model_registry.find({"ticker": "RELIANCE.NS", "status": "ACTIVE"}))

print(f"RELIANCE.NS ACTIVE count: {len(actives)}")
for a in actives:
    print(f"Version: {a.get('version')}, Pipeline: {a.get('feature_pipeline_hash')}")
