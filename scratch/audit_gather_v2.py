import os
import pymongo
from dotenv import load_dotenv
from bson import json_util
import json

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")
client = pymongo.MongoClient(mongo_uri)
db = client["stock_market_db"]

out = {}

# PIPELINE RUNS
runs = list(db.pipeline_runs.find().sort("started_at", -1).limit(5))
out["runs"] = runs

# PIPELINE LOCKS
locks = list(db.pipeline_locks.find())
out["locks"] = locks

# VIX
vix = list(db.historical_data.find({"symbol": "^INDIAVIX"}).sort("date", -1).limit(5))
out["vix"] = vix
if not vix:
    # try ticker instead of symbol just in case, or list distinct symbols matching VIX
    vix_symbols = db.historical_data.distinct("symbol", {"symbol": {"$regex": "VIX", "$options": "i"}})
    out["vix_symbols"] = vix_symbols

# PREDICTIONS for the run date
# The run was on 2026-09-05. Let's find predictions where prediction_timestamp >= 2026-09-05
import datetime
dt_start = datetime.datetime(2026, 9, 5)
dt_end = datetime.datetime(2026, 9, 6)
preds = list(db.prediction_history.find({"prediction_timestamp": {"$gte": dt_start, "$lt": dt_end}}))
out["predictions"] = preds

# PROVENANCE
provs = list(db.prediction_provenance.find({"prediction_timestamp": {"$gte": dt_start, "$lt": dt_end}}))
out["provenance"] = provs

# REGISTRY
reg = list(db.model_registry.find({"status": "ACTIVE"}))
out["registry"] = reg

with open("scratch/audit_dump_v2.json", "w") as f:
    f.write(json_util.dumps(out, indent=2))
print(f"Dumped v2 with {len(preds)} predictions, {len(provs)} provenances.")
