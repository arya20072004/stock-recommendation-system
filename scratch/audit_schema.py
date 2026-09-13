import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv

def get_db():
    load_dotenv()
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(uri)
    return client["stock_market_db"]

def default_serializer(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    return str(obj)

def main():
    db = get_db()
    
    samples = {}
    collections = db.list_collection_names()
    for col in collections:
        doc = db[col].find_one()
        if doc:
            samples[col] = doc
            
    # Also fetch the specific pipeline run again
    run = db["pipeline_runs"].find_one({"status": "SUCCESS"}, sort=[("started_at", -1)])
    if run:
        samples["latest_run"] = run
        
    # Get max date from historical data
    hist = list(db["historical_data"].find().sort("date", -1).limit(1))
    if hist:
        samples["max_historical_date"] = hist[0].get("date")

    with open("scratch/schema_samples.json", "w") as f:
        json.dump(samples, f, indent=2, default=default_serializer)

if __name__ == "__main__":
    main()
