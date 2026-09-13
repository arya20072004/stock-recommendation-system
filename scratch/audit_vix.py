import pymongo
from dotenv import load_dotenv
import os

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")
db = pymongo.MongoClient(mongo_uri)["stock_market_db"]

indices = list(db.sector_indices.find({"ticker": {"$regex": "VIX", "$options": "i"}}).limit(5))
print("Found in sector_indices:", len(indices))
if indices:
    print(indices[0])

# Wait, if not in sector_indices, let's dump all unique tickers in historical_data and sector_indices
si_tickers = db.sector_indices.distinct("ticker")
print("Sector tickers:", si_tickers)

hd_tickers = db.historical_data.distinct("ticker")
print("Historical data tickers count:", len(hd_tickers))

