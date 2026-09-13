import pymongo
from dotenv import load_dotenv
import os

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")
db = pymongo.MongoClient(mongo_uri)["stock_market_db"]

hd_tickers = db.historical_data.distinct("ticker")
print(hd_tickers)
