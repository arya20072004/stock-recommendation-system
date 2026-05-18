# Run this one-off check before anything else
from pymongo import MongoClient
import os

client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
db = client["stock_market_db"]

# Check what sector names actually exist in the collection
print(db.sector_indices.distinct("sector"))

# Check a specific translation
count = db.sector_indices.count_documents({"sector": "OilGasAndConsumableFuels"})
print(f"OilGasAndConsumableFuels docs: {count}")

count2 = db.sector_indices.count_documents({"sector": "OilGas"})
print(f"OilGas docs: {count2}")