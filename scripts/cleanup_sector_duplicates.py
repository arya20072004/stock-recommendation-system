# cleanup_sector_duplicates.py — run ONCE then delete this file
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI"))
db = client["stock_market_db"]

# Short names to delete (long-form canonical names will be kept)
SHORT_NAMES_TO_DELETE = [
    "Auto", "Banking", "CementInfra", "FMCG", "IT",
    "Metals", "OilGas", "Pharma", "Power", "PowerUtility",
    "Realty", "Telecom", "Conglomerate", "Diversified",
    "Construction", "Chemicals", "Services", "Textiles",
    "ConsumerDiscretionary", "ConsumerDurables", "ConsumerServices",
    "CapitalGoods",
]

for name in SHORT_NAMES_TO_DELETE:
    result = db.sector_indices.delete_many({"sector": name})
    print(f"Deleted {result.deleted_count} docs for sector='{name}'")

print("\nRemaining sectors:", db.sector_indices.distinct("sector"))