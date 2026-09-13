import json
import sys
import os
sys.path.append(os.getcwd())
from src.data.nifty50 import TICKERS

with open("scratch/audit_dump_v2.json", "r") as f:
    data = json.load(f)

preds = data["predictions"]
predicted_tickers = set([p["symbol"] for p in preds])
expected_tickers = set(TICKERS)

missing = expected_tickers - predicted_tickers
unexpected = predicted_tickers - expected_tickers

print("Missing:", missing)
print("Unexpected:", unexpected)
print("Duplicates:", len(preds) - len(predicted_tickers))
