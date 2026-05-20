import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pymongo import MongoClient

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client["stock_market_db"]

cutoff = datetime.utcnow() - timedelta(days=365 * 5 + 10)
count = db.sector_indices.count_documents({
    "sector": "OilGasAndConsumableFuels",
    "date": {"$gte": cutoff}
})
print(count)  # Should be ~1255