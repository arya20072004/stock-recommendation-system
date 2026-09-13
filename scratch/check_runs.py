import sys
from pymongo import MongoClient
import pprint

client = MongoClient("mongodb+srv://stockuser:Stockml2024@cluster0.qlhakda.mongodb.net/?appName=Cluster0")
db = client["stock_market_db"]

runs = list(db.pipeline_runs.find().sort("started_at", -1).limit(5))
for r in runs:
    print(r["started_at"], r["status"], r.get("market_date"), r["run_id"])

# And check prediction_history market_date
preds = list(db.prediction_history.find().sort("prediction_timestamp", -1).limit(1))
print("\nPrediction target date (market_date):", preds[0].get("market_date"))
