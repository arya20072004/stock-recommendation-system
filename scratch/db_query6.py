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

    market_date = datetime(2026, 8, 20)
    future_sessions = list(db.historical_data.find({
        "ticker": "ADANIENT.NS",
        "date": {"$gt": market_date},
        "close": {"$gt": 0, "$type": "double"}
    }).sort("date", 1).limit(10))
    
    print(f"Number of future sessions for 2026-08-20: {len(future_sessions)}")
    print([s["date"].strftime("%Y-%m-%d") for s in future_sessions])

    # Try for 2026-08-19
    market_date_19 = datetime(2026, 8, 19)
    future_sessions_19 = list(db.historical_data.find({
        "ticker": "ADANIENT.NS",
        "date": {"$gt": market_date_19},
        "close": {"$gt": 0, "$type": "double"}
    }).sort("date", 1).limit(10))
    print(f"\nNumber of future sessions for 2026-08-19: {len(future_sessions_19)}")
    print([s["date"].strftime("%Y-%m-%d") for s in future_sessions_19])

    print("\n--- 2026-08-19 PREDICTION STATUS ---")
    status_19 = db.prediction_history.find_one({"market_date": "2026-08-19"})
    if status_19:
        print(f"Status for 2026-08-19: {status_19.get('status')} | Outcome: {status_19.get('outcome')} | Eval Time: {status_19.get('evaluation_timestamp')}")

    # Also list the stages of the latest run
    run = db.pipeline_runs.find_one({"run_id": "58b40556-3b80-450f-8099-b74d4884f5c9"})
    if run:
        print("\n--- SETTLEMENT STAGE METRICS OF RUN 2026-09-02 ---")
        pprint.pprint(run.get("stages", {}).get("SETTLEMENT", {}))

except Exception as e:
    print(f"Error connecting to DB: {e}")
    sys.exit(1)
