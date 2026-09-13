import os
from pymongo import MongoClient
import json
from bson import json_util
from dotenv import load_dotenv

load_dotenv()
db = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))["stock_market_db"]

report = {}

hd_sample = db.historical_data.find_one()
report["hd_sample"] = hd_sample

if hd_sample:
    sym_field = "ticker" if "ticker" in hd_sample else "symbol"
    sym_val = hd_sample.get(sym_field)
    
    obs = list(db.historical_data.find({
        sym_field: sym_val,
        "date": {"$gt": "2026-08-20", "$lte": "2026-09-03"}
    }).sort("date", 1))
    report["obs_dates"] = [o.get("date") for o in obs]

with open("scratch/audit_report_10.json", "w") as f:
    json.dump(report, f, default=json_util.default, indent=2)
