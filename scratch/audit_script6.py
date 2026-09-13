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

cols = db.list_collection_names()
report["collections"] = cols

# Interrupted Run Reconciliation
run_old = db.pipeline_runs.find_one({"run_id": "91d09ca4-4ef1-46fb-8a79-8e1aa238863c"})
report["run_interrupted"] = run_old

# Successful Run
run_new = db.pipeline_runs.find_one({"run_id": "4c83bf0b-5f8e-4aab-ae82-d36fff99e2af"})
report["run_successful"] = run_new

# Lock
lock = db.pipeline_locks.find_one({"lock_id": "daily_production_lock"})
report["lock"] = lock

# Sep 3 Prediction Cohort
sep3 = list(db.prediction_history.find({"market_date": "2026-09-03"}))
report["sep3_preds_count"] = len(sep3)
report["sep3_unique_symbols"] = list(set(p.get("symbol") for p in sep3))
report["sep3_statuses"] = list(set(p.get("status") for p in sep3))
if sep3:
    report["sep3_provenance_statuses"] = list(set(p.get("provenance_status") for p in sep3))
    report["sep3_pipeline_hashes"] = list(set(p.get("provenance", {}).get("pipeline_hash") for p in sep3))
    report["sep3_run_ids"] = list(set(p.get("provenance", {}).get("run_id") for p in sep3))
    report["sep3_model_versions"] = list(set(p.get("model_version") for p in sep3))

# Aug 20 Maturity Checkpoint
aug20 = list(db.prediction_history.find({"market_date": "2026-08-20"}))
report["aug20_count"] = len(aug20)
report["aug20_statuses"] = list(set(p.get("status") for p in aug20))
correct = len([p for p in aug20 if p.get("prediction_correct") == True])
incorrect = len([p for p in aug20 if p.get("prediction_correct") == False])
report["aug20_accuracy"] = {
    "correct": correct,
    "incorrect": incorrect,
    "total_evaluated": correct + incorrect
}
if aug20:
    report["aug20_sample"] = aug20[0]

# Later Cohort Safety
cohorts = ["2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02"]
report["later_cohorts"] = {}
for d in cohorts:
    preds = list(db.prediction_history.find({"market_date": d}))
    statuses = list(set(p.get("status") for p in preds))
    report["later_cohorts"][d] = {
        "count": len(preds),
        "statuses": statuses
    }

# Active Models
active_models = list(db.model_registry.find({"status": "ACTIVE"}))
report["active_models_count"] = len(active_models)
report["active_models_hashes"] = list(set(m.get("pipeline_hash") for m in active_models))

with open("scratch/audit_report_6.json", "w") as f:
    json.dump(report, f, default=json_util.default, indent=2)

print("done")
