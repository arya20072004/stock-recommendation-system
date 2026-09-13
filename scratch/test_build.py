import os, pymongo, pandas as pd
from datetime import datetime, timezone, date
from dotenv import load_dotenv
load_dotenv()
from src.features.v1.engineering import build_feature_row

client = pymongo.MongoClient(os.getenv("MONGO_URI"))
target_date = date(2026, 9, 7)

# Mock _fetch_cached_macro by doing nothing, let the actual function run
# It will use the cache if any, or download.

last_completed_session = date(2026, 9, 4)
df = build_feature_row("WIPRO.NS", client, client["stock_market_db"], last_completed_session, prediction_target_date=target_date)

latest = df.iloc[-1]
missing = latest[latest.isna()].index.tolist()
print("Missing features for WIPRO.NS:")
print(missing)

print("\nValues for PCR and VIX:")
print("vix_level:", latest.get("vix_level"))
print("nifty_pcr_oi:", latest.get("nifty_pcr_oi"))
print("usdinr_ret_1d:", latest.get("usdinr_ret_1d"))
