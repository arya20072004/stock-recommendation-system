# data_collector.py

import yfinance as yf
import pandas as pd
from pymongo import MongoClient

def run():
    """
    Connects to MongoDB, fetches the latest 1-year historical data for a 
    predefined list of Indian stock tickers, and stores it in the database.
    """
    # --- SETUP ---
    TICKERS = ['RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS']
    client = MongoClient('mongodb://localhost:27017/')
    db = client['stock_market_db']
    collection = db['historical_data']

    print("Starting data collection for Indian stocks...")

    for ticker in TICKERS:
        try:
            # Fetch data using yfinance for the last year
            # The FutureWarning is a notice and can be ignored
            data = yf.download(ticker, period="1y", interval="1d")

            if data.empty:
                print(f"No data found for {ticker}, it may be delisted.")
                continue

            # --- DATA FORMATTING & STORAGE ---
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
                # To avoid duplicates, remove old data first
                collection.delete_many({'ticker': ticker})
                collection.insert_many(records_to_insert)
                print(f"Successfully inserted {len(records_to_insert)} records for {ticker}.")

        except Exception as e:
            print(f"An error occurred for {ticker}: {e}")

    print("Data collection finished.")
    client.close()

# This block ensures that run() is called only when the script is executed directly
if __name__ == "__main__":
    run()