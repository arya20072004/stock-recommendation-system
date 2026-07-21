from pymongo import MongoClient

# --- Connection Setup for LOCAL MongoDB ---
connection_string = "mongodb://localhost:27017/"
client = MongoClient(connection_string)

# Select your database
db = client['stock_market_db']

print("Successfully connected to LOCAL MongoDB!")

# --- Test Reading Data ---
# Select the 'stocks' collection
stocks_collection = db['stocks']

# Find the document we inserted earlier
microsoft_stock = stocks_collection.find_one({"_id": "MSFT"})

if microsoft_stock:
    print("Successfully fetched data:")
    print(microsoft_stock)
else:
    print("Could not find the test stock.")

# Close the connection
client.close()