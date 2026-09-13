import os
import sys
from pymongo import MongoClient
from dotenv import load_dotenv

sys.path.insert(0, r"c:\Users\aryab\Coding\stock_recommendations")
load_dotenv(r"c:\Users\aryab\Coding\stock_recommendations\.env")

mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri)
db = client["stock_market_db"]

print("History dates:", db.prediction_history.distinct("market_date"))
print("Prov dates:", db.prediction_provenance.distinct("market_date"))

p10 = db.prediction_provenance.count_documents({"market_date": "2026-09-10"})
p11 = db.prediction_provenance.count_documents({"market_date": "2026-09-11"})
print("Prov 10:", p10, "Prov 11:", p11)
