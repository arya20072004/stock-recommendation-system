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
from src.data.nifty50 import TICKERS

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

TICKER_TO_FO_SYMBOL_OVERRIDES = {
    "M&M.NS":        "M&M",
    "BAJAJ-AUTO.NS": "BAJAJ-AUTO",
    "TMPV.NS":       "TMPV",   # UNVERIFIED — confirm against live Bhavcopy;
                                      # F&O symbol may not have migrated post-demerger
    "ETERNAL.NS":    "ETERNAL",      # UNVERIFIED — confirm rename propagated to F&O
}


def _fo_symbol_for_ticker(ticker: str) -> str:
    if ticker in TICKER_TO_FO_SYMBOL_OVERRIDES:
        return TICKER_TO_FO_SYMBOL_OVERRIDES[ticker]
    return ticker.replace(".NS", "")


def _compute_daily_stock_pcr(dt: datetime, df: pd.DataFrame, tickers: list[str]) -> list[dict]:
    """
    Computes per-stock OI-based PCR from an already-fetched/normalized
    Bhavcopy dataframe (reuses the same day's fetch as index-level PCR
    to avoid a second network round-trip).
    """
    records = []
    for ticker in tickers:
        fo_symbol = _fo_symbol_for_ticker(ticker)

        opt_rows = df[
            (df["SYMBOL"] == fo_symbol)
            & (df["OPTION_TYP"].isin(["CE", "PE"]))
        ]
        if opt_rows.empty:
            continue  # not F&O-enabled, or symbol mismatch — zero-filled downstream

        call_oi = pd.to_numeric(opt_rows.loc[opt_rows["OPTION_TYP"] == "CE", "OPEN_INT"], errors="coerce").sum()
        put_oi  = pd.to_numeric(opt_rows.loc[opt_rows["OPTION_TYP"] == "PE", "OPEN_INT"], errors="coerce").sum()

        if call_oi <= 0:
            continue

        records.append({
            "underlying": fo_symbol,
            "ticker": ticker,          # stored for traceability back to .NS ticker
            "date": dt,
            "call_oi": float(call_oi),
            "put_oi": float(put_oi),
            "pcr_oi": float(put_oi / call_oi),
            "updated_at": datetime.now(timezone.utc),
        })
    return records

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
            "XpryDt": "EXPIRY_DT",
            "ClsPric": "CLOSE",
            "TckrSymb": "SYMBOL",
            "FinInstrmTp": "INSTRUMENT",
            "OptnTp": "OPTION_TYP",
            "OpnIntrst": "OPEN_INT",
        })
    else:
        keep = df

    keep["SYMBOL"] = keep["SYMBOL"].astype(str).str.strip().str.upper()
    keep["OPTION_TYP"] = keep["OPTION_TYP"].astype(str).str.strip().str.upper()
    if "INSTRUMENT" not in keep.columns and "FinInstrmTp" in df.columns:
        keep["INSTRUMENT"] = df["FinInstrmTp"]
    keep["INSTRUMENT"] = keep["INSTRUMENT"].astype(str).str.strip().str.upper()
    return keep


def _compute_daily_pcr(dt: datetime, stock_tickers: list[str] | None = None) -> list[dict]:
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

        record = {
            "underlying": underlying,
            "date": dt,
            "call_oi": float(call_oi),
            "put_oi": float(put_oi),
            "pcr_oi": float(put_oi / call_oi),
            "updated_at": datetime.now(timezone.utc),
        }

        if underlying == "NIFTY":
            fut_rows = df[
                (df["SYMBOL"] == "NIFTY")
                & (df["INSTRUMENT"].isin(["FUTIDX", "IDF"]))
            ]
            if not fut_rows.empty and "EXPIRY_DT" in fut_rows.columns:
                fut_rows = fut_rows.copy()
                fut_rows["EXPIRY_DT"] = pd.to_datetime(fut_rows["EXPIRY_DT"], errors="coerce")
                live_fut_rows = fut_rows[fut_rows["EXPIRY_DT"] > pd.Timestamp(dt)]
                if not live_fut_rows.empty:
                    near_month = live_fut_rows.sort_values("EXPIRY_DT").iloc[0]
                    fut_close = pd.to_numeric(near_month.get("CLOSE"), errors="coerce")
                    if pd.notna(fut_close) and fut_close > 0:
                        record["nifty_fut_close"] = float(fut_close)
                        record["nifty_fut_expiry"] = near_month["EXPIRY_DT"].to_pydatetime()

        records.append(record)

    if stock_tickers:
        records.extend(_compute_daily_stock_pcr(dt, df, stock_tickers))

    return records


def build_pcr_history(
    client: MongoClient,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    full_backfill: bool = False
) -> dict:
    """
    Collects daily NIFTY and stock PCR records.
    In incremental mode (default), checks MongoDB pcr_data for the latest recorded date,
    resolves missing canonical NSE trading sessions up to end_date, and fetches only new dates.
    If full_backfill=True or the database is uninitialized, performs a historical bootstrap.
    """
    db = client["stock_market_db"]
    import src.data.session_calendar as session_calendar

    # Normalize end_date to date object
    if end_date is None:
        target_end_date = datetime.now().date()
    elif isinstance(end_date, datetime):
        target_end_date = end_date.date()
    else:
        target_end_date = end_date

    # Normalize start_date if provided
    target_start_date = start_date.date() if isinstance(start_date, datetime) else start_date

    if full_backfill:
        if target_start_date is None:
            target_start_date = target_end_date - timedelta(days=365 * HISTORY_YEARS + 30)
        logger.info("Executing full historical PCR backfill from %s to %s", target_start_date, target_end_date)
        candidate_dates = [ts.date() for ts in pd.bdate_range(target_start_date, target_end_date)]
    else:
        if target_start_date is None:
            max_doc = db.pcr_data.find_one({"underlying": "NIFTY"}, sort=[("date", -1)])
            if max_doc and "date" in max_doc:
                max_dt = max_doc["date"]
                max_date = max_dt.date() if isinstance(max_dt, datetime) else max_dt
                target_start_date = session_calendar.next_session(max_date)
            else:
                target_start_date = target_end_date - timedelta(days=365 * HISTORY_YEARS + 30)
                logger.info("PCR database uninitialized. Bootstrapping history from %s to %s", target_start_date, target_end_date)

        # Collect only canonical trading sessions in [target_start_date, target_end_date]
        candidate_dates = []
        if target_start_date <= target_end_date:
            curr = target_start_date
            while curr <= target_end_date:
                if session_calendar.is_session(curr):
                    candidate_dates.append(curr)
                curr += timedelta(days=1)

        if not candidate_dates:
            logger.info("PCR data is already up to date through %s (0 missing sessions)", target_end_date)
            return {"fetched": 0, "skipped": 0, "candidate_days": 0}

        logger.info("Collecting incremental PCR for %d candidate trading days (%s to %s)",
                    len(candidate_dates), candidate_dates[0], candidate_dates[-1])

    ops = []
    fetched, skipped = 0, 0
    for i, day in enumerate(candidate_dates):
        dt = datetime.combine(day, datetime.min.time()) if not isinstance(day, datetime) else day
        recs = _compute_daily_pcr(dt, stock_tickers=TICKERS)
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
        if (i + 1) % 50 == 0:
            logger.info("Progress: %d/%d days (fetched=%d, skipped=%d)", i + 1, len(candidate_dates), fetched, skipped)
        time.sleep(0.3)  # throttle — avoid tripping NSE rate limiting

    if ops:
        db.pcr_data.bulk_write(ops, ordered=False)

    db.pcr_data.create_index([("underlying", 1), ("date", 1)], unique=True)
    logger.info("PCR collection complete: %d records fetched, %d days skipped/holiday", fetched, skipped)
    logger.warning(
        "Stock-level PCR uses TICKER_TO_FO_SYMBOL_OVERRIDES for symbol mapping — "
        "spot-check a sample of tickers against a real Bhavcopy SYMBOL column "
        "before trusting results, especially TMPV.NS and ETERNAL.NS (unverified overrides)."
    )
    return {"fetched": fetched, "skipped": skipped, "candidate_days": len(candidate_dates)}


def backfill_full_pcr_history(client: MongoClient, start_date: datetime | None = None, end_date: datetime | None = None):
    """Explicit convenience interface for performing a full 5-year historical PCR backfill."""
    return build_pcr_history(client, start_date=start_date, end_date=end_date, full_backfill=True)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="NSE F&O PCR Data Collector")
    parser.add_argument("--full", action="store_true", help="Perform full 5-year historical backfill")
    parser.add_argument("--start-date", type=str, default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    start = datetime.strptime(args.start_date, "%Y-%m-%d").date() if args.start_date else None
    end = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else None

    client = MongoClient(MONGO_URI)
    try:
        build_pcr_history(client, start_date=start, end_date=end, full_backfill=args.full)
    finally:
        client.close()