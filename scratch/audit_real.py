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
    
    # Let's count predictions for 2026-09-02
    preds = list(db["prediction_history"].find({"market_date": "2026-09-02"}))
    print("Predictions for 2026-09-02:", len(preds))
    
    missing_f = 0
    for p in preds:
        f = p.get("feature_snapshot", {})
        if "outperformance" not in f or "market_regime" not in f:
            missing_f += 1
    print("Predictions missing outperformance/market_regime:", missing_f)

    # Let's check provenance for 2026-09-02
    provs = list(db["prediction_provenance"].find({"market_date": "2026-09-02"}))
    print("Provenance for 2026-09-02:", len(provs))
    
    # What's the latest pipeline_run?
    run = db["pipeline_runs"].find_one({"status": "SUCCESS"}, sort=[("started_at", -1)])
    if run:
        print("Run ID:", run.get("run_id"))
        print("Market Date:", run.get("market_date"))
        
    # Is there a LOOKAHEAD violation?
    # Max date in historical_data for ADANIENT.NS
    hist = list(db["historical_data"].find({"ticker": "ADANIENT.NS"}).sort("date", -1).limit(5))
    for h in hist:
        print("Hist Date:", h.get("date"))

if __name__ == "__main__":
    main()
