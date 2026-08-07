"""
train_single_ticker.py
Run this to train (or re-train) a single ticker using the existing
ml_trainer.py pipeline — useful for resuming after an interrupted batch run.

Usage:
    python train_single_ticker.py
"""

import logging
from pymongo import MongoClient

from src.ml.trainer import create_dataset, train_model, MONGO_URI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

TICKERS = ["INFY.NS"]

def run_single(ticker):
    client = MongoClient(MONGO_URI)
    try:
        logger.info("Processing %s", ticker)
        dataset = create_dataset(ticker, client)
        if dataset.empty:
            logger.warning("%s: dataset creation failed or returned empty; aborting", ticker)
            return
        train_model(dataset, ticker)
    finally:
        client.close()
        logger.info("Single-ticker training run complete for %s", ticker)

if __name__ == "__main__":
    for ticker in TICKERS:
        run_single(ticker)

# # 1. Is TMPV.NS in the ticker list?
# from src.data.nifty50 import TICKERS
# print("TMPV.NS" in TICKERS)
# print([t for t in TICKERS if "TATA" in t.upper() or "TMPV" in t.upper()])

# # 2. Does historical_data actually have rows under "TMPV.NS"?
# from pymongo import MongoClient
# import os
# client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
# db = client["stock_market_db"]

# print("TMPV.NS rows:", db.historical_data.count_documents({"ticker": "TMPV.NS"}))
# print("TATAMOTORS.NS rows:", db.historical_data.count_documents({"ticker": "TATAMOTORS.NS"}))

# # check date range if any rows exist
# doc = db.historical_data.find_one({"ticker": "TMPV.NS"}, sort=[("date", -1)])
# print("latest TMPV.NS date:", doc["date"] if doc else None)

# from pymongo import MongoClient
# from datetime import datetime
# import os
# from src.data.pcr_builder import build_pcr_history

# client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
# try:
#     build_pcr_history(
#         client,
#         start_date=datetime(2020, 1, 1),  # match your original 5yr backfill start
#         end_date=datetime.now(),
#     )
# finally:
#     client.close()
# from pymongo import MongoClient
# import os
# from dotenv import load_dotenv

# load_dotenv()
# client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
# db = client["stock_market_db"]

# count = db.historical_data.count_documents({"ticker": "TMPV.NS"})
# print("TMPV.NS rows:", count)

# if count > 0:
#     latest = db.historical_data.find_one({"ticker": "TMPV.NS"}, sort=[("date", -1)])
#     earliest = db.historical_data.find_one({"ticker": "TMPV.NS"}, sort=[("date", 1)])
#     print("earliest date:", earliest["date"])
#     print("latest date:", latest["date"])

# import pandas as pd
# docs = list(db.historical_data.find({"ticker": "TMPV.NS"}, {"date": 1, "close": 1, "_id": 0}).sort("date", 1))
# df = pd.DataFrame(docs)
# df["date"] = pd.to_datetime(df["date"])
# df["daily_ret"] = df["close"].pct_change()

# # look at Oct 2025 window specifically — demerger timing
# window = df[(df["date"] >= "2025-09-15") & (df["date"] <= "2025-11-15")]
# print(window[["date", "close", "daily_ret"]].to_string())

# # also flag any single-day move >15% anywhere in the series
# print(df[df["daily_ret"].abs() > 0.15][["date", "close", "daily_ret"]])