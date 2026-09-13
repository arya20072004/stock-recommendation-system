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

runs = list(db.pipeline_runs.find().sort("started_at", -1).limit(6))
report["runs"] = [{ 
    "run_id": r["run_id"], 
    "market_date": r.get("market_date"), 
    "status": r.get("status"), 
    "started_at": r.get("started_at"),
    "duration": r.get("duration_ms"),
    "errors": r.get("errors")
} for r in runs]

lock = db.pipeline_locks.find_one({"lock_id": "daily_production_lock"})
report["lock"] = lock

# Sept 3 target vs market date? Let's check a sample
sample = db.prediction_history.find_one()
report["sample_pred"] = sample

# Aug 20 predictions
aug20 = list(db.prediction_history.find({"target_date": "2026-08-20"}))
if not aug20:
    aug20 = list(db.prediction_history.find({"market_date": "2026-08-20"}))
report["aug20_count"] = len(aug20)
report["aug20_statuses"] = list(set(p.get("status") for p in aug20))
report["aug20_settlement_states"] = list(set(p.get("settlement_state", "none") for p in aug20))
report["aug20_samples"] = aug20[:1]

# Sep 3/4 predictions
sep3_md = list(db.prediction_history.find({"market_date": "2026-09-03"}))
sep3_td = list(db.prediction_history.find({"target_date": "2026-09-03"}))
sep4_td = list(db.prediction_history.find({"target_date": "2026-09-04"}))
sep4_md = list(db.prediction_history.find({"market_date": "2026-09-04"}))
report["sep3_counts"] = {
    "md_sep3": len(sep3_md),
    "td_sep3": len(sep3_td),
    "md_sep4": len(sep4_md),
    "td_sep4": len(sep4_td),
}

# The latest predictions that are 51
latest = list(db.prediction_history.find().sort("prediction_timestamp", -1).limit(55))
report["latest_counts_by_md"] = {}
report["latest_counts_by_td"] = {}
for p in latest:
    md = p.get("market_date", "missing")
    td = p.get("target_date", "missing")
    report["latest_counts_by_md"][md] = report["latest_counts_by_md"].get(md, 0) + 1
    report["latest_counts_by_td"][td] = report["latest_counts_by_td"].get(td, 0) + 1
    
if latest:
    report["latest_hashes"] = list(set(p.get("provenance", {}).get("pipeline_hash") for p in latest[:51]))
    report["latest_runs"] = list(set(p.get("provenance", {}).get("run_id") for p in latest[:51]))

# Observations for Aug 20 cohort
if aug20:
    sym = aug20[0]["symbol"]
    # Check if there is a 'market_data' collection or similar
    # In earlier scripts, db.list_collection_names() should tell us.
    pass

with open("scratch/audit_report_5.json", "w") as f:
    json.dump(report, f, default=json_util.default, indent=2)

print("done")
