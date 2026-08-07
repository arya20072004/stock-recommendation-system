import pandas as pd
import yfinance as yf
from pymongo import MongoClient
from pymongo import UpdateOne
import os
import math
from dotenv import load_dotenv
from src.data.nifty50 import TICKERS
from src.data.equity_bhavcopy import fetch_equity_ohlcv_for_date
from src.data.pcr_builder import TICKER_TO_FO_SYMBOL_OVERRIDES

def validate_ohlcv_record(record):
    for f in ['open', 'high', 'low', 'close']:
        val = record.get(f)
        if val is None or not isinstance(val, (int, float)) or not math.isfinite(val) or val <= 0:
            return False, f"{f} is invalid"
    
    vol = record.get('volume')
    if vol is None or not isinstance(vol, (int, float)) or not math.isfinite(vol) or vol < 0:
        return False, "volume is invalid"
    
    high, low, open_p, close_p = record['high'], record['low'], record['open'], record['close']
    if high < low: return False, "high < low"
    if high < open_p: return False, "high < open"
    if high < close_p: return False, "high < close"
    if low > open_p: return False, "low > open"
    if low > close_p: return False, "low > close"
    
    return True, ""

def _get_trading_day_calendar(db, start_date, end_date):
    """
    Ground-truth trading-day calendar sourced from pcr_data (populated
    directly from NSE Bhavcopy, not yfinance) — NIFTY index-level PCR
    rows exist only for genuine trading days, so this is a reliable
    reference to detect gaps in yfinance's per-ticker OHLCV data.
    """
    docs = db.pcr_data.find(
        {"underlying": "NIFTY", "date": {"$gte": start_date, "$lte": end_date}},
        {"date": 1, "_id": 0}
    )
    return {pd.Timestamp(d["date"]).normalize() for d in docs}


def _backfill_gap_dates(db, collection, ticker, gap_dates):
    """
    For trading days yfinance failed to return (e.g. the 2026-08-03 CAS
    transition, where Yahoo's backend has not yet adapted to the new
    auction-based closing price for F&O-eligible stocks), fall back to
    NSE's own equity Bhavcopy — the authoritative source, unaffected by
    yfinance's downstream parsing.
    """
    filled = 0
    for gap_date in sorted(gap_dates):
        dt = gap_date.to_pydatetime()
        records = fetch_equity_ohlcv_for_date(
            dt,
            fo_symbol_overrides=TICKER_TO_FO_SYMBOL_OVERRIDES,
            tickers=[ticker],
        )
        if not records:
            print(f"  Bhavcopy fallback: no data found for {ticker} on {gap_date.date()} — leaving gap")
            continue
        for record in records:
            is_valid, reason = validate_ohlcv_record(record)
            if not is_valid:
                print(f"  Bhavcopy fallback: validation failed for {ticker} on {gap_date.date()} - {reason}. DATA_UNAVAILABLE / INVALID_PROVIDER_DATA")
                continue
            collection.update_one(
                {'ticker': record['ticker'], 'date': record['date']},
                {'$set': record},
                upsert=True,
            )
            filled += 1
        if filled > 0:
            print(f"  Bhavcopy fallback: filled {ticker} for {gap_date.date()}")
    return filled

def run():
    """
    Connects to MongoDB, fetches 5-year historical data for all Nifty 50 stocks,
    and stores it, using environment variables for configuration.
    Falls back to NSE equity Bhavcopy for any trading-day gaps yfinance
    misses (e.g. post-CAS closing price transition issues).
    """
    # --- SETUP ---
    load_dotenv()
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    db = client['stock_market_db']
    collection = db['historical_data']
    collection.create_index([('ticker', 1), ('date', 1)], unique=True)

    print("Starting data collection for all Nifty 50 stocks...")

    results = {
        "attempted": 0,
        "successful": 0,
        "failed": 0,
        "failed_tickers": [],
        "invalid_rows": 0,
        "target_market_date": None
    }

    global_max_date = None

    for ticker in TICKERS:
        results["attempted"] += 1
        try:
            # Fetch data for the last 5 years
            data = yf.download(ticker, period="5y", interval="1d", progress=False)
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            if data.empty:
                print(f"No data found for {ticker}, it may be delisted.")
                results["failed"] += 1
                results["failed_tickers"].append(ticker)
                continue

            records_to_insert = []
            yf_valid_dates = set()
            invalid_count = 0
            for date, row in data.iterrows():
                record = {
                    'ticker': ticker,
                    'date': date,
                    'open': float(row['Open']),
                    'high': float(row['High']),
                    'low': float(row['Low']),
                    'close': float(row['Close']),
                    'volume': int(row['Volume'])
                }
                is_valid, reason = validate_ohlcv_record(record)
                if is_valid:
                    records_to_insert.append(record)
                    yf_valid_dates.add(pd.Timestamp(date).normalize())
                else:
                    invalid_count += 1
                    results["invalid_rows"] += 1
                    print(f"Validation failed for {ticker} on {date.date()}: {reason}")

            if records_to_insert:
                # Remove old data to prevent duplicates
                operations = [
                    UpdateOne(
                        {'ticker': ticker, 'date': record['date']},
                        {'$set': record},
                        upsert=True
                    )
                    for record in records_to_insert
                ]
                collection.bulk_write(operations, ordered=False)
                print(f"Upserted {len(operations)} records for {ticker}.")

            # --- Gap detection: compare yfinance's returned dates against
            # the Bhavcopy-derived trading-day calendar, and backfill any
            # missing trading days directly from NSE equity Bhavcopy. ---
            yf_dates = yf_valid_dates
            range_start = data.index.min()
            range_end = data.index.max()
            trading_calendar = _get_trading_day_calendar(db, range_start, range_end)
            gap_dates = trading_calendar - yf_dates
            if gap_dates:
                print(f"  {ticker}: detected {len(gap_dates)} trading-day gap(s) vs Bhavcopy calendar — attempting fallback")
                filled = _backfill_gap_dates(db, collection, ticker, gap_dates)
                if filled < len(gap_dates):
                    results["failed"] += 1
                    results["failed_tickers"].append(ticker)
                    continue
                    
            if range_end is not None:
                if global_max_date is None or range_end > global_max_date:
                    global_max_date = range_end
                    
            results["successful"] += 1

        except Exception as e:
            print(f"An error occurred for {ticker}: {e}")
            results["failed"] += 1
            results["failed_tickers"].append(ticker)

    if global_max_date is not None:
        results["target_market_date"] = global_max_date.strftime("%Y-%m-%d")

    print("Data collection finished.")
    client.close()
    
    return results

if __name__ == "__main__":
    import sys
    result = run()
    if result["failed"] > 0:
        print(f"Collector failed for {result['failed']} tickers: {result['failed_tickers']}")
        sys.exit(1)
    sys.exit(0)
