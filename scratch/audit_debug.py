import os
import pymongo
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")
client = pymongo.MongoClient(mongo_uri)
db = client["stock_market_db"]

# 1. Pipeline Run
run = db.pipeline_runs.find_one({"run_id": "53633ae2-1e7a-4a47-a7dd-222a86f91918"})
if run:
    print("RUN KEYS:", run.keys())
    print("Session:", run.get("last_completed_session"), type(run.get("last_completed_session")))
    print("Target:", run.get("prediction_target_date"), type(run.get("prediction_target_date")))

# 2. VIX
vix = list(db.historical_data.find({"ticker": {"$in": ["^INDIAVIX", "INDIAVIX"]}}).sort("date", -1).limit(5))
print("VIX count:", len(vix))
for v in vix:
    print(f"Ticker: {v.get('ticker')} Date: {v.get('date')} Close: {v.get('close')}")

# 3. Predictions
preds = list(db.prediction_history.find().sort("created_at", -1).limit(5))
print("Latest Predictions count:", len(preds))
if preds:
    p = preds[0]
    print("PREDICTION KEYS:", p.keys())
    print("prediction_date:", p.get("prediction_date"), type(p.get("prediction_date")))
    print("target_date:", p.get("target_date"), type(p.get("target_date")))
    print("created_at:", p.get("created_at"), type(p.get("created_at")))
    print(p)

