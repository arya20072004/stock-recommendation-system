import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

def get_db():
    load_dotenv()
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(uri)
    return client["stock_market_db"]

def main():
    db = get_db()
    preds = list(db["prediction_history"].find().sort("_id", -1).limit(5))
    print(f"Total predictions fetched: {len(preds)}")
    for p in preds:
        print({k:v for k,v in p.items() if k not in ["features"]})
        
    provs = list(db["prediction_provenance"].find().sort("_id", -1).limit(5))
    print(f"\nTotal provenance fetched: {len(provs)}")
    for p in provs:
        print({k:v for k,v in p.items() if k not in ["raw_inputs", "features", "feature_columns"]})

if __name__ == "__main__":
    main()
