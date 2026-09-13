import os
import sys
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

sys.path.insert(0, r"c:\Users\aryab\Coding\stock_recommendations")
load_dotenv(r"c:\Users\aryab\Coding\stock_recommendations\.env")

mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri)
db = client["stock_market_db"]

pred = db.prediction_history.find_one({"market_date": "2026-09-11"})
prov = db.prediction_provenance.find_one({"market_date": "2026-09-10", "symbol": "RELIANCE"})
if not prov:
    prov = db.prediction_provenance.find_one({"symbol": "RELIANCE"})
print("History keys:", pred.keys() if pred else None)
print("Prov keys:", prov.keys() if prov else None)
if prov:
    print("market_date in prov:", prov.get('market_date'))
    print("features in prov keys:", prov.get('features', {}).keys())
    print("raw inputs keys:", prov.get('raw_inputs', {}).keys())
    
print("Older cohorts status:", db.prediction_history.distinct("status", {"market_date": {"$lt": "2026-09-11"}}))
print("Older cohorts outcome:", db.prediction_history.distinct("outcome", {"market_date": {"$lt": "2026-09-11"}}))

import datetime
print("History objects for old cohort sample:", db.prediction_history.find_one({"market_date": "2026-09-09"}))
