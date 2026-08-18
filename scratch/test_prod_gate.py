import os
import sys
import json
from pymongo import MongoClient

sys.path.append("c:/Users/aryab/Coding/stock_recommendations")

def get_db():
    from dotenv import load_dotenv
    load_dotenv("c:/Users/aryab/Coding/stock_recommendations/.env")
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    return client["stock_market_db"]

db = get_db()

try:
    from src.ml.history import _verify_production_readiness
    _verify_production_readiness(db)
    print("PRODUCTION GATE PASS")
except Exception as e:
    print(f"PRODUCTION GATE FAIL: {e}")
