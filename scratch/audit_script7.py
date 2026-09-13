import os
from pymongo import MongoClient
import json
from bson import json_util
from dotenv import load_dotenv

load_dotenv()
db = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))["stock_market_db"]

report = {}
sep3 = db.prediction_history.find_one({"market_date": "2026-09-03"})
active_model = db.model_registry.find_one({"status": "ACTIVE"})

report["sep3_sample"] = sep3
report["active_model_sample"] = active_model

with open("scratch/audit_report_7.json", "w") as f:
    json.dump(report, f, default=json_util.default, indent=2)
