import pymongo
import json
from bson import ObjectId

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return str(o)

import os
from dotenv import load_dotenv

def main():
    load_dotenv()
    mongo_uri = os.environ.get("MONGO_URI")
    client = pymongo.MongoClient(mongo_uri)
    db = client["stock_market_db"]
    
    print(db.list_collection_names())
    
    runs = list(db["pipeline_runs"].find().sort("_id", -1).limit(5))
    with open("c:/Users/aryab/Coding/stock_recommendations/scratch/run_data.json", "w") as f:
        json.dump(runs, f, cls=JSONEncoder, indent=2)
        
    logs = list(db["pipeline_logs"].find({"timestamp": {"$gte": "2026-09-07"}}).sort("_id", -1).limit(500))
    if not logs:
        logs = list(db["pipeline_logs"].find().sort("_id", -1).limit(200))
    with open("c:/Users/aryab/Coding/stock_recommendations/scratch/recent_logs.json", "w") as f:
        json.dump(logs, f, cls=JSONEncoder, indent=2)

if __name__ == "__main__":
    main()
