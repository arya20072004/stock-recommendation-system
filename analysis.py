# analysis.py

import pandas as pd
import pandas_ta as ta
from pymongo import MongoClient

# --- 1. LOAD DATA FROM MONGODB ---

# Connect to your local MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['stock_market_db']
collection = db['historical_data']

# Fetch data for a specific ticker and load into a pandas DataFrame
# The find() method returns a cursor, which we convert to a list
aapl_data = list(collection.find({'ticker': 'AAPL'}))

if not aapl_data:
    print("No data found for AAPL. Please run data_collector.py first.")
else:
    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(aapl_data)

    # Set the 'date' column as the index for time series analysis
    df.set_index('date', inplace=True)
    # Sort by date to ensure correct order for TA calculations
    df.sort_index(inplace=True)

    print("Original data for AAPL loaded from DB:")
    print(df.tail()) # Show the last 5 days of data

    # --- 2. PERFORM TECHNICAL ANALYSIS ---

    # Calculate the 14-day RSI using the pandas-ta library
    df.ta.rsi(length=14, append=True)

    print("\nData after calculating 14-day RSI:")
    # The 'append=True' argument adds a new column 'RSI_14' to our DataFrame
    print(df.tail())

client.close()