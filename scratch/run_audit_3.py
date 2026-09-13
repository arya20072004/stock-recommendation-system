import os
import json
import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

def default_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    return str(obj)

def main():
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(mongo_uri)
    db = client["stock_market_db"]

    prov = db.prediction_provenance.find_one({"market_date": "2026-09-09", "prediction_horizon": 10})
    
    with open("scratch/sample_prov.json", "w") as f:
        json.dump(prov, f, indent=2, default=default_serializer)

    registry = list(db.model_registry.find({"status": "ACTIVE"}))
    with open("scratch/sample_reg.json", "w") as f:
        json.dump(registry, f, indent=2, default=default_serializer)

if __name__ == "__main__":
    main()
