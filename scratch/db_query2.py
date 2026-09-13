import os
import sys
from datetime import datetime, timezone
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

sys.path.insert(0, r"c:\Users\aryab\Coding\stock_recommendations")
load_dotenv(r"c:\Users\aryab\Coding\stock_recommendations\.env")

mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri)
db = client["stock_market_db"]

from src.data.nifty50 import TICKERS
from src.data.session_calendar import next_session
from src.ml.model_utils import get_model_version

# IDENTIFY RUN
run = db.pipeline_runs.find_one({
    "started_at": {"$gte": datetime(2026, 9, 10, 14, 0, 0, tzinfo=timezone.utc)},
    "status": "SUCCESS"
})
run_id = run["run_id"]

trading_session = datetime(2026, 9, 10).date()
expected_next = next_session(trading_session)

# predictions are stored with market_date = input date = trading_session
preds = list(db.prediction_history.find({"market_date": str(trading_session), "prediction_horizon": 10}))
provs = list(db.prediction_provenance.find({"market_date": str(trading_session), "prediction_horizon": 10}))

print("PREDS:", len(preds))
print("PROVS:", len(provs))

max_date = None
leakage = 0
nasdaq_5d_missing = 0
nasdaq_20d_missing = 0
dims = []
hashes = []
old_hashes = 0
expected_hash = "f0fa3983b0114cc63635a51f26ef954bd4f64b151ed81673aadadc857717d862"

for p in provs:
    fv = p.get('features', {})
    dims.append(len(fv))
    
    if 'nasdaq_ret_5d' not in fv or fv['nasdaq_ret_5d'] is None or pd.isna(fv['nasdaq_ret_5d']):
        nasdaq_5d_missing += 1
    if 'nasdaq_ret_20d' not in fv or fv['nasdaq_ret_20d'] is None or pd.isna(fv['nasdaq_ret_20d']):
        nasdaq_20d_missing += 1
        
    phash = p.get('feature_pipeline_hash')
    hashes.append(phash)
    if phash != expected_hash:
        old_hashes += 1
        
    # Check max date
    raw = p.get('raw_inputs', {})
    # raw dates? typically we can't easily find it unless there is a 'date' column.
    # We will assume market_date is the max date for temporal integrity, or check if 'date' in raw.
    if 'date' in raw:
        d = raw['date']
        if pd.Timestamp(d) > pd.Timestamp(str(trading_session)):
            leakage += 1
        if max_date is None or pd.Timestamp(d) > pd.Timestamp(max_date):
            max_date = d

print("DIMS:", list(set(dims)))
print("NASDAQ missing:", nasdaq_5d_missing, nasdaq_20d_missing)
print("Max date:", max_date)
print("Leakage:", leakage)
print("Old hashes:", old_hashes)
