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

db = get_db()

report = {}

preds = list(db.predictions.find().sort("prediction_timestamp", -1).limit(5))
report["latest_preds"] = preds

aug20_preds = list(db.predictions.find({"market_date": "2026-08-20"}).limit(2))
if not aug20_preds:
    # try target_date or created_at ?
    aug20_preds = list(db.predictions.find({"target_date": "2026-08-20"}).limit(2))
    
report["aug20_sample"] = aug20_preds

with open("scratch/audit_report_4.json", "w") as f:
    json.dump(report, f, default=json_util.default, indent=2)

print("Report generated")
