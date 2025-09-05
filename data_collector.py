import yfinance as yf
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from nifty50 import TICKERS

def run():
    """
    Connects to MongoDB, fetches 1-year historical data for all Nifty 50 stocks,
    and stores it, using environment variables for configuration.
    """
    # --- SETUP ---
    load_dotenv()
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    db = client['stock_market_db']
    collection = db['historical_data']

    print("Starting data collection for all Nifty 50 stocks...")

    for ticker in TICKERS:
        try:
            # Fetch data for the last year
            data = yf.download(ticker, period="1y", interval="1d", progress=False)

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
                collection.delete_many({'ticker': ticker})
                collection.insert_many(records_to_insert)
                print(f"Successfully inserted {len(records_to_insert)} records for {ticker}.")

        except Exception as e:
            print(f"An error occurred for {ticker}: {e}")

    print("Data collection finished.")
    client.close()

if __name__ == "__main__":
    run()
