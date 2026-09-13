import pymongo

client = pymongo.MongoClient()
db = client["stock_market_db"]
print(db.list_collection_names())
