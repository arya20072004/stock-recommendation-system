import sys
from pymongo import MongoClient

client = MongoClient("mongodb+srv://stockuser:Stockml2024@cluster0.qlhakda.mongodb.net/?appName=Cluster0")
db = client["stock_market_db"]

run = db.pipeline_runs.find_one({"status": "SUCCESS"}, sort=[("started_at", -1)])
print("Latest Pipeline Run:", run)

pred_prov = db.prediction_provenance.find_one(sort=[("created_at", -1)])
print("\nLatest Prediction Provenance:", pred_prov)

pred_hist = db.prediction_history.find_one(sort=[("prediction_timestamp", -1)])
print("\nLatest Prediction History:", pred_hist)
