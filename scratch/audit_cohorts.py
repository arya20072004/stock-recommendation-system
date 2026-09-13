import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv

def get_db():
    load_dotenv()
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(uri)
    return client["stock_market_db"]

def main():
    db = get_db()
    
    cohorts = {}
    for p in db["prediction_history"].find():
        date = str(p.get("market_date"))
        status = p.get("status")
        if date not in cohorts:
            cohorts[date] = {"total": 0, "status": {}}
        cohorts[date]["total"] += 1
        cohorts[date]["status"][status] = cohorts[date]["status"].get(status, 0) + 1
        
    for k in sorted(cohorts.keys()):
        print(f"Date: {k}, Total: {cohorts[k]['total']}, Status: {cohorts[k]['status']}")

if __name__ == "__main__":
    main()
