import os
from pymongo import MongoClient
import json
from bson import json_util
from dotenv import load_dotenv

load_dotenv()
db = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))["stock_market_db"]

report = {}

p = db.prediction_provenance.find_one({"market_date": "2026-09-03"})
report["sep3_prov_sample"] = p

adani = db.historical_data.find_one({"symbol": "ADANIENT.NS", "date": "2026-09-03"})
report["adani_sep3"] = adani

# How is date stored in historical data?
adani_any = db.historical_data.find_one({"symbol": "ADANIENT.NS"})
report["adani_any"] = adani_any

# NIFTY name?
nifty_any = db.historical_data.find_one({"symbol": "^NSEI"})
if not nifty_any:
    nifty_any = db.historical_data.find_one({"symbol": "NIFTY_50"})
report["nifty_any"] = nifty_any

with open("scratch/audit_report_9.json", "w") as f:
    json.dump(report, f, default=json_util.default, indent=2)
