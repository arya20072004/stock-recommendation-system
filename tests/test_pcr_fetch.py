# from datetime import datetime
# from pcr_builder import _fetch_bhavcopy, _normalize, _compute_daily_pcr

# TEST_DATES = [
#     # Pre-cutover (legacy format)
#     datetime(2021, 3, 15),   # 2021
#     datetime(2022, 6, 10),   # 2022
#     datetime(2023, 9, 21),   # 2023 — F&O expiry Thursday, should have rich OI
#     datetime(2024, 1, 15),   # early 2024
#     datetime(2024, 6, 28),   # last Friday before cutover
#     # Post-cutover (UDiFF format)
#     datetime(2024, 7, 9),    # first day after cutover
#     datetime(2024, 12, 26),  # UDiFF, 2024
#     datetime(2025, 3, 27),   # UDiFF, 2025 expiry week
#     datetime(2025, 9, 15),   # UDiFF, mid-2025
#     datetime(2026, 6, 25),   # UDiFF, most recent
# ]

# for dt in TEST_DATES:
#     print(f"\n=== {dt.date()} ===")
#     raw = _fetch_bhavcopy(dt)
#     if raw is None:
#         print("  FETCH FAILED — check URL / response manually")
#         continue
#     print(f"  raw shape: {raw.shape}, columns: {list(raw.columns)[:8]}...")

#     try:
#         norm = _normalize(raw, dt)
#         print(f"  normalized OK — SYMBOL sample: {norm['SYMBOL'].unique()[:5]}")
#     except Exception as ex:
#         print(f"  NORMALIZE FAILED: {ex}")
#         continue

#     recs = _compute_daily_pcr(dt)
#     if not recs:
#         print("  NO PCR RECORDS — NIFTY/BANKNIFTY rows not found after filter")
#         continue
#     for r in recs:
#         print(f"  {r['underlying']}: call_oi={r['call_oi']:.0f} put_oi={r['put_oi']:.0f} pcr_oi={r['pcr_oi']:.3f}")
# from pymongo import MongoClient
# from pcr_builder import build_pcr_history, MONGO_URI

# client = MongoClient(MONGO_URI)
# try:
#     build_pcr_history(client)
# finally:
#     client.close()
# from pymongo import MongoClient
# import pandas as pd
# from src.data.pcr_builder import MONGO_URI

# client = MongoClient(MONGO_URI)
# db = client["stock_market_db"]

# for underlying in ["NIFTY", "BANKNIFTY"]:
#     docs = list(db.pcr_data.find({"underlying": underlying}, {"date": 1, "pcr_oi": 1, "_id": 0}).sort("date", 1))
#     df = pd.DataFrame(docs)
#     df["date"] = pd.to_datetime(df["date"])

#     print(f"\n=== {underlying} ===")
#     print(f"rows: {len(df)}, date range: {df['date'].min().date()} to {df['date'].max().date()}")
#     print(f"pcr_oi stats: min={df['pcr_oi'].min():.3f} max={df['pcr_oi'].max():.3f} "
#           f"mean={df['pcr_oi'].mean():.3f} std={df['pcr_oi'].std():.3f}")

#     # Gap check — flag any stretch >5 calendar days between consecutive records
#     gaps = df["date"].diff().dt.days
#     big_gaps = df.loc[gaps > 5, "date"]
#     print(f"gaps >5 days: {len(big_gaps)}")
#     if len(big_gaps) > 0:
#         print(big_gaps.dt.date.tolist()[:10])

#     # Flatline check — any 10-day rolling window with zero variance
#     flat = (df["pcr_oi"].rolling(10).std() == 0).sum()
#     print(f"flatlined 10-day windows: {flat}")

# client.close()

# from pymongo import MongoClient
# import os
# from dotenv import load_dotenv

# load_dotenv()
# client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
# db = client["stock_market_db"]

# # Control tickers known to update fine, plus the two suspects
# check_tickers = ["ONGC.NS", "HDFCBANK.NS", "ETERNAL.NS", "JIOFIN.NS"]

# for t in check_tickers:
#     latest = db.historical_data.find({"ticker": t}).sort("date", -1).limit(1)
#     doc = next(latest, None)
#     print(f"{t}: max date = {doc['date'] if doc else 'NO DATA'}")

# client.close()

# from pymongo import MongoClient
# import os
# from dotenv import load_dotenv

# load_dotenv()
# client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
# db = client["stock_market_db"]

# check_tickers = ["ONGC.NS", "HDFCBANK.NS", "ETERNAL.NS", "JIOFIN.NS"]

# for t in check_tickers:
#     docs = list(
#         db.historical_data.find({"ticker": t}, {"date": 1, "_id": 0})
#         .sort("date", -1)
#         .limit(10)
#     )
#     dates = [d["date"].strftime("%Y-%m-%d") for d in docs]
#     print(f"{t}: last 10 dates (most recent first) = {dates}")

# client.close()
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
db = client["stock_market_db"]

for underlying in ["NIFTY", "BANKNIFTY"]:
    latest = db.pcr_data.find({"underlying": underlying}).sort("date", -1).limit(1)
    doc = next(latest, None)
    print(f"{underlying} PCR max date = {doc['date'] if doc else 'NO DATA'}")

client.close()