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

runs = list(db.pipeline_runs.find().sort("started_at", -1).limit(10))
report["recent_runs"] = [{ 
    "run_id": r["run_id"], 
    "market_date": r.get("market_date"), 
    "status": r.get("status"), 
    "started_at": r.get("started_at"),
    "duration": r.get("duration_ms"),
    "errors": r.get("errors")
} for r in runs]

sample_pred = db.predictions.find_one()
if sample_pred:
    report["sample_pred"] = sample_pred

with open("scratch/audit_report_3.json", "w") as f:
    json.dump(report, f, default=json_util.default, indent=2)

print("Report generated at scratch/audit_report_3.json")
