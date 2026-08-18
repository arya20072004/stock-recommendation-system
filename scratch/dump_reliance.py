import json
from bson import json_util
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['stock_market_db']
records = list(db.model_registry.find({'ticker': 'RELIANCE.NS'}))

with open('c:/Users/aryab/Coding/stock_recommendations/scratch/reliance_records.json', 'w') as f:
    f.write(json_util.dumps(records, indent=2))
