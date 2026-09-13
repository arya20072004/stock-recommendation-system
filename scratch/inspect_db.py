import os
import sys
from pymongo import MongoClient

client = MongoClient("mongodb+srv://stockuser:Stockml2024@cluster0.qlhakda.mongodb.net/?appName=Cluster0")
db = client["stock_market_db"]

print("Collections:")
for col in db.list_collection_names():
    print(f" - {col}")
    doc = db[col].find_one()
    print(f"   Example doc keys: {list(doc.keys()) if doc else 'empty'}")
