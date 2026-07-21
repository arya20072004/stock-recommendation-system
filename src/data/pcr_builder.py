"""
Backfills daily NIFTY index options Put-Call Ratio (OI-based) from NSE
F&O Bhavcopy archives, bridging the legacy format (pre 8-Jul-2024) and
the UDiFF format (post 8-Jul-2024). Stores in MongoDB pcr_data collection.

Run once for full 5yr backfill, then daily/weekly via APScheduler for
new dates only (check max date in Mongo first — same pattern as
sector_index_builder.py).
"""

import io
import logging
import os
import time
import zipfile
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
HISTORY_YEARS = 5
CUTOVER_DATE = datetime(2024, 7, 8)  # legacy -> UDiFF switch

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/all-reports-derivatives",
})

# Underlyings to track PCR for — index-level only, per_ticker PCR too thin/noisy
UNDERLYINGS = ["NIFTY", "BANKNIFTY"]


def _legacy_url(dt: datetime) -> str:
    mon = dt.strftime("%b").upper()
    return (
        f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/"
        f"{dt.year}/{mon}/fo{dt.strftime('%d')}{mon}{dt.year}bhav.csv.zip"
    )


def _udiff_url(dt: datetime) -> str:
    return (
        f"https://nsearchives.nseindia.com/content/fo/"
        f"BhavCopy_NSE_FO_0_0_0_{dt.strftime('%Y%m%d')}_F_0000.csv.zip"
    )


def _fetch_bhavcopy(dt: datetime) -> pd.DataFrame | None:
    url = _udiff_url(dt) if dt >= CUTOVER_DATE else _legacy_url(dt)
    try:
        resp = SESSION.get(url, timeout=15)
        if resp.status_code != 200 or len(resp.content) < 200:
            return None
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_name = zf.namelist()[0]
            with zf.open(csv_name) as f:
                df = pd.read_csv(f)
        return df
    except Exception as ex:
        logger.debug("%s: fetch failed — %s", dt.date(), ex)
        return None


def _normalize(df: pd.DataFrame, dt: datetime) -> pd.DataFrame:
    """Map legacy or UDiFF columns to a common schema."""
    df.columns = [c.strip() for c in df.columns]

    if dt >= CUTOVER_DATE:
        # UDiFF schema
        keep = df.rename(columns={
            "TckrSymb": "SYMBOL",
            "FinInstrmTp": "INSTRUMENT",
            "OptnTp": "OPTION_TYP",
            "OpnIntrst": "OPEN_INT",
        })
    else:
        keep = df

    keep["SYMBOL"] = keep["SYMBOL"].astype(str).str.strip().str.upper()
    keep["OPTION_TYP"] = keep["OPTION_TYP"].astype(str).str.strip().str.upper()
    return keep


def _compute_daily_pcr(dt: datetime) -> list[dict]:
    raw = _fetch_bhavcopy(dt)
    if raw is None or raw.empty:
        return []

    df = _normalize(raw, dt)
    records = []
    for underlying in UNDERLYINGS:
        opt_rows = df[
            (df["SYMBOL"] == underlying)
            & (df["OPTION_TYP"].isin(["CE", "PE"]))
        ]
        if opt_rows.empty:
            continue

        call_oi = pd.to_numeric(opt_rows.loc[opt_rows["OPTION_TYP"] == "CE", "OPEN_INT"], errors="coerce").sum()
        put_oi  = pd.to_numeric(opt_rows.loc[opt_rows["OPTION_TYP"] == "PE", "OPEN_INT"], errors="coerce").sum()

        if call_oi <= 0:
            continue

        records.append({
            "underlying": underlying,
            "date": dt,
            "call_oi": float(call_oi),
            "put_oi": float(put_oi),
            "pcr_oi": float(put_oi / call_oi),
            "updated_at": datetime.now(timezone.utc),
        })
    return records


def build_pcr_history(client: MongoClient, start_date: datetime = None, end_date: datetime = None):
    db = client["stock_market_db"]
    end_date = end_date or datetime.now()
    start_date = start_date or (end_date - timedelta(days=365 * HISTORY_YEARS + 30))

    all_dates = pd.bdate_range(start_date, end_date)  # trading-day proxy; NSE holidays will just 404/empty-skip
    logger.info("Backfilling PCR for %d candidate trading days", len(all_dates))

    ops = []
    fetched, skipped = 0, 0
    for i, ts in enumerate(all_dates):
        dt = ts.to_pydatetime()
        recs = _compute_daily_pcr(dt)
        if not recs:
            skipped += 1
        for rec in recs:
            fetched += 1
            ops.append(UpdateOne(
                {"underlying": rec["underlying"], "date": rec["date"]},
                {"$set": rec},
                upsert=True,
            ))

        # Flush every 250 to keep memory bounded, be polite to NSE
        if len(ops) >= 250:
            db.pcr_data.bulk_write(ops, ordered=False)
            ops = []
        if i % 50 == 0:
            logger.info("Progress: %d/%d days (fetched=%d, skipped=%d)", i, len(all_dates), fetched, skipped)
        time.sleep(0.3)  # throttle — avoid tripping NSE rate limiting

    if ops:
        db.pcr_data.bulk_write(ops, ordered=False)

    db.pcr_data.create_index([("underlying", 1), ("date", 1)], unique=True)
    logger.info("PCR backfill complete: %d records fetched, %d days skipped/holiday", fetched, skipped)


if __name__ == "__main__":
    client = MongoClient(MONGO_URI)
    try:
        build_pcr_history(client)
    finally:
        client.close()