import os
from pymongo import MongoClient
import json
from bson import json_util
from dotenv import load_dotenv

load_dotenv()
db = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))["stock_market_db"]

report = {}

# NIFTY Observations from Aug 20 to Sep 3
obs = list(db.historical_data.find({
    "symbol": "^NSEI",
    "date": {"$gt": "2026-08-20", "$lte": "2026-09-03"}
}).sort("date", 1))
report["nifty_observations"] = [o["date"] for o in obs]

# Sep 3 Prediction Provenance
sep3_provs = list(db.prediction_provenance.find({"market_date": "2026-09-03"}))
report["sep3_prov_count"] = len(sep3_provs)
if sep3_provs:
    report["sep3_pipeline_hashes"] = list(set(p.get("pipeline_hash") for p in sep3_provs))
    report["sep3_run_ids"] = list(set(p.get("run_id") for p in sep3_provs))
    
# Let's also check Aug 20 settlement price basis
# Find one historical data entry for ADANIENT for the settlement date (which should be Sep 3 or Sep 2 depending on horizon)
aug20_sample = db.prediction_history.find_one({"market_date": "2026-08-20", "symbol": "ADANIENT.NS"})
if aug20_sample:
    # 2901.0 is the actual_price. Let's see what date it corresponds to.
    sd = aug20_sample.get("settlement_market_date")
    report["aug20_adani_settlement_date"] = sd
    adani_hd = db.historical_data.find_one({"symbol": "ADANIENT.NS", "date": sd})
    if adani_hd:
        report["adani_historical"] = adani_hd
        report["adani_price_match_close"] = (adani_hd.get("close") == aug20_sample.get("actual_price"))

with open("scratch/audit_report_8.json", "w") as f:
    json.dump(report, f, default=json_util.default, indent=2)
