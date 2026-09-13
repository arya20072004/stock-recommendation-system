import os
import pymongo
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")
client = pymongo.MongoClient(mongo_uri)
db = client["stock_market_db"]

print("Collections:", db.list_collection_names())

# Check pipeline_runs without filter
runs = list(db.pipeline_runs.find().sort("start_time", -1).limit(5))
print("Recent runs count:", len(runs))
if len(runs) > 0:
    print("Recent run dates:")
    for r in runs:
        print(f"ID: {r.get('run_id')} Start: {r.get('start_time')} Status: {r.get('status')}")

# Check predictions without filter
preds = list(db.predictions.find().sort("_id", -1).limit(5))
print("Recent predictions count:", len(preds))
if len(preds) > 0:
    print("Sample prediction:")
    print(preds[0])

