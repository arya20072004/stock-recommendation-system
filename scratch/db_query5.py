import sys
import os
import pprint
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

try:
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    db = client["stock_market_db"]

    # Historical data info
    dates = db.historical_data.distinct("date", {"date": {"$gte": datetime(2026, 8, 1)}})
    dates.sort()
    print("Dates >= 2026-08-01:")
    print([d.strftime("%Y-%m-%d") for d in dates])

    print("\n--- RUN METADATA ---")
    runs = list(db.pipeline_runs.find().sort("started_at", -1).limit(5))
    for r in runs:
        print(f"Run ID: {r.get('run_id')} | Date: {r.get('market_date')} | Status: {r.get('status')} | Started: {r.get('started_at')}")

except Exception as e:
    print(f"Error connecting to DB: {e}")
    sys.exit(1)
