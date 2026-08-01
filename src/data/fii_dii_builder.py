"""
fii_dii_builder.py
Fetches daily FII/FPI and DII cash-market trading activity from NSE
and stores it in MongoDB (fii_dii_data collection).

IMPORTANT CAVEAT — read before relying on backfill:
NSE's fiidiiTradeReact endpoint only serves recent days (observed to
return roughly the last 1-2 trading days in a single call), NOT a
multi-year historical archive like the Bhavcopy ZIP files pcr_builder.py
uses. There is no known NSE endpoint that returns 5 years of FII/DII
history in one shot. This means:
  1. A one-shot 5yr backfill (like collector.py / pcr_builder.py) is
     NOT possible from this endpoint alone.
  2. This script should be run DAILY (via APScheduler, same cadence
     as pcr_builder.py's incremental mode) to accumulate history
     going forward from whenever it's first deployed.
  3. For genuine historical depth, the honest options are: (a) source
     a vendor/paid historical FII-DII CSV once and bulk-import it
     separately, or (b) accept that this feature group will only have
     as much history as accumulates from today forward, which means
     it CANNOT be used in create_dataset() for older training rows
     until enough calendar time has passed. This directly affects
     whether/when this feature is testable via the normal 3-run
     confirmation track — flagging this before any training changes.
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/reports/fii-dii",
})

FII_DII_API_URL = "https://www.nseindia.com/api/fiidiiTradeReact"
WARMUP_URL = "https://www.nseindia.com/reports/fii-dii"

# Category strings as returned by the NSE endpoint — kept as constants
# in case NSE changes casing/spacing, so a single fix point exists.
FII_CATEGORY_LABELS = {"FII/FPI", "FII", "FPI"}
DII_CATEGORY_LABELS = {"DII"}


def _warm_up_session():
    """
    NSE's API blocks requests without valid session cookies obtained
    from a prior visit to the main site — same anti-bot pattern
    pcr_builder.py's Bhavcopy fetches don't need (those are static
    file downloads), but this JSON API does need it.
    """
    try:
        SESSION.get(WARMUP_URL, timeout=10)
        time.sleep(0.5)
    except Exception as ex:
        logger.warning("fii_dii: warm-up request failed — %s", ex)


def _parse_value(raw):
    """NSE returns numeric strings like '1234.56' or with commas — normalize."""
    if raw is None:
        return None
    try:
        return float(str(raw).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _fetch_fii_dii_raw():
    """
    Hits the live NSE endpoint. Returns the raw JSON list on success,
    or None on failure/block. Retries once after a fresh warm-up if
    the first attempt is blocked (common NSE behavior — session
    cookies expire faster than the Bhavcopy static files).
    """
    for attempt in range(2):
        try:
            resp = SESSION.get(FII_DII_API_URL, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and data:
                    return data
            logger.debug(
                "fii_dii: attempt %d got status=%s — retrying after warm-up",
                attempt + 1, resp.status_code,
            )
        except Exception as ex:
            logger.debug("fii_dii: attempt %d fetch failed — %s", attempt + 1, ex)

        _warm_up_session()

    return None


def _normalize_records(raw_data):
    """
    Maps NSE's raw JSON rows to a common schema. Expected raw shape
    (per NSE convention, subject to drift — hence the flexible
    category matching):
        {"category": "FII/FPI", "date": "31-Jul-2026",
         "buyValue": "12345.67", "sellValue": "11000.00",
         "netValue": "1345.67"}
    """
    records = {}
    for row in raw_data:
        category = str(row.get("category", "")).strip().upper()
        date_str = row.get("date")
        if not date_str:
            continue

        try:
            dt = pd.to_datetime(date_str, format="%d-%b-%Y").to_pydatetime()
        except (ValueError, TypeError):
            try:
                dt = pd.to_datetime(date_str).to_pydatetime()
            except Exception:
                logger.warning("fii_dii: unparseable date '%s' — skipping row", date_str)
                continue

        buy_value  = _parse_value(row.get("buyValue"))
        sell_value = _parse_value(row.get("sellValue"))
        net_value  = _parse_value(row.get("netValue"))
        if net_value is None and buy_value is not None and sell_value is not None:
            net_value = buy_value - sell_value

        if net_value is None:
            continue

        if category in FII_CATEGORY_LABELS or "FII" in category or "FPI" in category:
            investor_type = "FII"
        elif category in DII_CATEGORY_LABELS or "DII" in category:
            investor_type = "DII"
        else:
            logger.debug("fii_dii: unrecognized category '%s' — skipping", category)
            continue

        key = (investor_type, dt)
        records[key] = {
            "investor_type": investor_type,
            "date": dt,
            "buy_value_cr":  buy_value,
            "sell_value_cr": sell_value,
            "net_value_cr":  net_value,
            "updated_at": datetime.now(timezone.utc),
        }

    return list(records.values())


def fetch_and_store_latest(client: MongoClient) -> int:
    """
    Fetches whatever recent data NSE currently serves and upserts it.
    Safe to call daily (e.g. via APScheduler) — re-fetching an
    already-stored date is a no-op update, not a duplicate.

    Returns the number of records upserted.
    """
    _warm_up_session()
    raw = _fetch_fii_dii_raw()
    if not raw:
        logger.warning("fii_dii: no data returned from NSE — endpoint may be blocked or down")
        return 0

    records = _normalize_records(raw)
    if not records:
        logger.warning("fii_dii: fetched data but normalization produced 0 usable records")
        return 0

    db = client["stock_market_db"]
    ops = [
        UpdateOne(
            {"investor_type": rec["investor_type"], "date": rec["date"]},
            {"$set": rec},
            upsert=True,
        )
        for rec in records
    ]
    result = db.fii_dii_data.bulk_write(ops, ordered=False)
    upserted = result.upserted_count + result.modified_count
    logger.info(
        "fii_dii: upserted %d records (dates covered: %s)",
        upserted,
        sorted({rec["date"].strftime("%Y-%m-%d") for rec in records}),
    )
    return upserted


def build_fii_dii_index(client: MongoClient) -> None:
    db = client["stock_market_db"]
    db.fii_dii_data.create_index([("investor_type", 1), ("date", 1)], unique=True)
    logger.info("fii_dii_data compound index ensured.")


if __name__ == "__main__":
    client = MongoClient(MONGO_URI)
    try:
        build_fii_dii_index(client)
        n = fetch_and_store_latest(client)
        logger.info("fii_dii_builder run complete — %d records upserted", n)
    finally:
        client.close()