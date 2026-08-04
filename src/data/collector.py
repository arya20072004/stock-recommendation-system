import pandas as pd
import yfinance as yf
from pymongo import MongoClient
from pymongo import UpdateOne
import os
from dotenv import load_dotenv
from src.data.nifty50 import TICKERS
from src.data.equity_bhavcopy import fetch_equity_ohlcv_for_date
from src.data.pcr_builder import TICKER_TO_FO_SYMBOL_OVERRIDES

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
            collection.update_one(
                {'ticker': record['ticker'], 'date': record['date']},
                {'$set': record},
                upsert=True,
            )
            filled += 1
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

    for ticker in TICKERS:
        try:
            # Fetch data for the last 5 years
            data = yf.download(ticker, period="5y", interval="1d", progress=False)
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            if data.empty:
                print(f"No data found for {ticker}, it may be delisted.")
                continue

            records_to_insert = []
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
                records_to_insert.append(record)

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
            yf_dates = {pd.Timestamp(d).normalize() for d in data.index}
            range_start = data.index.min()
            range_end = data.index.max()
            trading_calendar = _get_trading_day_calendar(db, range_start, range_end)
            gap_dates = trading_calendar - yf_dates
            if gap_dates:
                print(f"  {ticker}: detected {len(gap_dates)} trading-day gap(s) vs Bhavcopy calendar — attempting fallback")
                _backfill_gap_dates(db, collection, ticker, gap_dates)

        except Exception as e:
            print(f"An error occurred for {ticker}: {e}")

    print("Data collection finished.")
    client.close()

if __name__ == "__main__":
    run()
