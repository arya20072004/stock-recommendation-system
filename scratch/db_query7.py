import sys
import os
import pprint
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

try:
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    db = client["stock_market_db"]

    print("--- 2026-09-03 COHORT ---")
    preds = list(db.prediction_history.find({"market_date": "2026-09-03"}))
    print(f"Total records: {len(preds)}")
    if preds:
        tickers = [p.get("symbol") for p in preds]
        print(f"Unique tickers: {len(set(tickers))}")
        print(f"Duplicates: {len(tickers) - len(set(tickers))}")
        
        sample = preds[0]
        print(f"Sample Ticker: {sample.get('symbol')}")
        print(f"Model Version: {sample.get('model_version')}")
        print(f"Provenance Hash: {sample.get('provenance_hash')}")
        print(f"Provenance Status: {sample.get('provenance_status')}")
        
        # Check provenance document
        prov = db.prediction_provenance.find_one({"provenance_hash": sample.get("provenance_hash")})
        if prov:
            print(f"Pipeline Hash in Provenance: {prov.get('feature_pipeline_hash')}")

    print("\n--- ACTIVE MODEL GOVERNANCE ---")
    registry = list(db.model_registry.find({"status": "ACTIVE"}))
    print(f"Active registry records: {len(registry)}")
    if registry:
        print(f"Sample registry Hash: {registry[0].get('feature_pipeline_hash')}")
        print(f"Sample registry Model Hash: {registry[0].get('model_hash')}")
        
except Exception as e:
    print(f"Error connecting to DB: {e}")
    sys.exit(1)
