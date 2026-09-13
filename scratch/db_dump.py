import os
import pymongo
import json
from bson import json_util
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

client = pymongo.MongoClient(mongo_uri)
db = client["stock_market_db"]

out = {}

# 1. Pipeline Runs
runs = list(db.pipeline_runs.find({"start_time": {"$gte": "2026-09-04"}}).sort("start_time", -1))
out["recent_runs"] = runs

# 2. Pipeline Locks
locks = list(db.pipeline_locks.find())
out["locks"] = locks

# 3. India VIX Fallback (Logs or DB)
# Let's get predictions generated on Sep 5
predictions = list(db.predictions.find({"prediction_date": "2026-09-04"}).sort("_id", -1))
if len(predictions) == 0:
    predictions = list(db.predictions.find().sort("_id", -1).limit(60))

out["predictions"] = predictions

# 4. Model Registry Active
registry = list(db.model_registry.find({"status": "ACTIVE"}))
out["registry"] = registry

# 5. Market data for India VIX
vix_data = list(db.market_data.find({"ticker": "^INDIAVIX"}).sort("date", -1).limit(5))
out["india_vix"] = vix_data

with open("scratch/db_dump.json", "w") as f:
    f.write(json_util.dumps(out, indent=2))
print(f"DB Dump complete. Got {len(out['recent_runs'])} runs, {len(out['predictions'])} predictions, {len(out['registry'])} registry models.")
