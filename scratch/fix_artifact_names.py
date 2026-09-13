import sys
from pymongo import MongoClient

client = MongoClient("mongodb+srv://stockuser:Stockml2024@cluster0.qlhakda.mongodb.net/?appName=Cluster0")
db = client["stock_market_db"]

r = db.model_registry.find_one({"status": "ACTIVE", "ticker": "WIPRO.NS"})
print("WIPRO.NS ACTIVE model_registry:")
for k, v in r.items():
    print(f"  {k}: {v}")
