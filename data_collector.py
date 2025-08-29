# data_collector.py

import yfinance as yf # Import the library
import pandas as pd
from pymongo import MongoClient

# --- 1. SETUP ---
TICKERS = ['RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS']
client = MongoClient('mongodb://localhost:27017/')
db = client['stock_market_db']
collection = db['historical_data']

print("Starting data collection for Indian stocks...")

for ticker in TICKERS:
    try:
        # Fetch data using yfinance for the last year
        data = yf.download(ticker, period="1y", interval="1d")

        if data.empty:
            print(f"No data found for {ticker}, it may be delisted.")
            continue

        # --- 3. DATA FORMATTING & STORAGE ---
        records_to_insert = []
        for date, row in data.iterrows():
            # Corrected record with explicit type conversion
            record = {
                'ticker': ticker,
                'date': date,
                'open': float(row['Open']),      # Convert to float
                'high': float(row['High']),      # Convert to float
                'low': float(row['Low']),        # Convert to float
                'close': float(row['Close']),    # Convert to float
                'volume': int(row['Volume'])     # Convert to int
            }
            records_to_insert.append(record)

        if records_to_insert:
            # To avoid duplicates, you can remove old data first
            collection.delete_many({'ticker': ticker})
            collection.insert_many(records_to_insert)
            print(f"Successfully inserted {len(records_to_insert)} records for {ticker}.")

    except Exception as e:
        print(f"An error occurred for {ticker}: {e}")

print("Data collection finished.")
client.close()