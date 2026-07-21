import pandas as pd
import yfinance as yf
from pymongo import MongoClient
from pymongo import UpdateOne
import os
from dotenv import load_dotenv
from src.data.nifty50 import TICKERS

def run():
    """
    Connects to MongoDB, fetches 5-year historical data for all Nifty 50 stocks,
    and stores it, using environment variables for configuration.
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
                # collection.insert_many(records_to_insert)
                # print(f"Successfully inserted {len(records_to_insert)} records for {ticker}.")

        except Exception as e:
            print(f"An error occurred for {ticker}: {e}")

    print("Data collection finished.")
    client.close()

if __name__ == "__main__":
    run()
