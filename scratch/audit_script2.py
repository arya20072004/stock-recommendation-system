import os
from pymongo import MongoClient
import json
from bson import json_util
from dotenv import load_dotenv
import subprocess

load_dotenv()

def get_db():
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(uri)
    return client["stock_market_db"]

db = get_db()

report = {}

# 1. Pipeline Runs
report["runs"] = {}
for run_id in ["4c83bf0b-5f8e-4aab-ae82-d36fff99e2af", "91d09ca4-4ef1-46fb-8a79-8e1aa238863c"]:
    run = db.pipeline_runs.find_one({"run_id": run_id})
    report["runs"][run_id] = run

# 2. Pipeline Lock
lock = db.pipeline_locks.find_one({"lock_id": "daily_production_lock"})
report["lock"] = lock

# 3. Sep 3 Predictions
sep3_preds = list(db.predictions.find({"market_date": "2026-09-03"}))
report["sep3_preds_count"] = len(sep3_preds)
report["sep3_preds_symbols"] = list(set(p["symbol"] for p in sep3_preds)) if sep3_preds else []
if sep3_preds:
    report["sep3_sample"] = sep3_preds[0]
report["sep3_hashes"] = list(set(p.get("provenance", {}).get("pipeline_hash", "missing") for p in sep3_preds)) if sep3_preds else []
report["sep3_runs"] = list(set(p.get("provenance", {}).get("run_id", "missing") for p in sep3_preds)) if sep3_preds else []

# 4. Aug 20 Cohort
aug20_preds = list(db.predictions.find({"market_date": "2026-08-20"}))
report["aug20_preds_count"] = len(aug20_preds)
if aug20_preds:
    report["aug20_sample"] = aug20_preds[0]
report["aug20_statuses"] = list(set(p.get("status", "missing") for p in aug20_preds)) if aug20_preds else []

# Get observations for Aug 20
if aug20_preds:
    sample_symbol = aug20_preds[0]["symbol"]
    obs = list(db.market_data.find({
        "symbol": sample_symbol,
        "date": {"$gt": "2026-08-20", "$lte": "2026-09-03"}
    }).sort("date", 1))
    report["aug20_observations"] = [o["date"] for o in obs]

# 5. Later Cohorts (Aug 21 to Sep 2)
report["later_cohorts"] = {}
for date in ["2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03"]:
    preds = list(db.predictions.find({"market_date": date}))
    statuses = list(set(p.get("status", "missing") for p in preds)) if preds else []
    obs = list(db.market_data.find({
        "symbol": preds[0]["symbol"] if preds else "NIFTY",
        "date": {"$gt": date, "$lte": "2026-09-03"}
    }).sort("date", 1))
    report["later_cohorts"][date] = {
        "count": len(preds),
        "statuses": statuses,
        "obs_count": len(obs)
    }

# 6. Active Models / Registry
active_models = list(db.model_registry.find({"is_active": True}))
report["active_models_count"] = len(active_models)
report["active_models_hashes"] = list(set(m.get("pipeline_hash", "missing") for m in active_models)) if active_models else []

# 7. Git Status
try:
    git_status = subprocess.check_output(["git", "status", "--short"]).decode()
    git_branch = subprocess.check_output(["git", "branch", "--show-current"]).decode().strip()
    git_head = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    git_untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard"]).decode()
    report["git"] = {
        "status": git_status,
        "branch": git_branch,
        "head": git_head,
        "untracked": git_untracked
    }
except Exception as e:
    report["git"] = str(e)

with open("scratch/audit_report.json", "w") as f:
    json.dump(report, f, default=json_util.default, indent=2)

print("Report generated at scratch/audit_report.json")
