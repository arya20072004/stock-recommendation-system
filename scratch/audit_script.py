import os
from pymongo import MongoClient
import json
from bson import json_util
from dotenv import load_dotenv

load_dotenv()

def get_db():
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(uri)
    return client["stock_market_db"]

if __name__ == "__main__":
    db = get_db()
    collections = db.list_collection_names()
    print(f"Collections: {collections}")
    
    print("\n--- Pipeline Runs ---")
    if "pipeline_runs" in collections:
        runs = list(db.pipeline_runs.find().sort("started_at", -1).limit(5))
        for r in runs:
            print(json.dumps(r, default=json_util.default, indent=2))
            
    print("\n--- Pipeline Locks ---")
    if "pipeline_locks" in collections:
        locks = list(db.pipeline_locks.find())
        for l in locks:
            print(json.dumps(l, default=json_util.default, indent=2))

