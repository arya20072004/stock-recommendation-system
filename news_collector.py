# news_collector.py

import requests
from pymongo import MongoClient
from datetime import datetime, timedelta
import config # Your config file with API keys

# --- 1. SETUP ---

# List of stock tickers you want to track
TICKER_MAP = {
    'RELIANCE.NS': 'Reliance Industries',
    'TCS.NS': 'Tata Consultancy Services',
    'HDFCBANK.NS': 'HDFC Bank',
    'INFY.NS': 'Infosys'
}

# Get your NewsAPI key from the config file
API_KEY = config.NEWS_API_KEY

# Connect to your local MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['stock_market_db']
collection = db['news_articles']

# Calculate the date 7 days ago for the API query
date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

# --- 2. DATA FETCHING & STORAGE ---

for ticker, company_name in TICKER_MAP.items():
    print(f"Fetching news for {company_name}...")
    url = (
        f'https://newsapi.org/v2/everything?'
        f'q="{company_name}"' # Use quotes for exact phrase
        f'&from={date_from}'
        f'&language=en' # Specify English language
        f'&sortBy=relevancy'
        f'&apiKey={API_KEY}'
    )

    try:
        response = requests.get(url)
        response.raise_for_status() # Check for request errors

        data = response.json()
        articles = data.get('articles', [])

        if not articles:
            print(f"No articles found for {ticker}.")
            continue

        records_to_insert = []
        for article in articles:
            record = {
                'ticker': ticker,
                'source': article['source']['name'],
                'title': article['title'],
                'description': article['description'],
                'url': article['url'],
                'published_at': datetime.fromisoformat(article['publishedAt'].replace('Z', '+00:00'))
            }
            records_to_insert.append(record)

        # Insert all articles for the current ticker into MongoDB
        if records_to_insert:
            collection.insert_many(records_to_insert)
            print(f"Successfully inserted {len(records_to_insert)} news articles for {ticker}.")

    except requests.exceptions.RequestException as e:
        print(f"HTTP Request failed for {ticker}: {e}")
    except Exception as e:
        print(f"An error occurred for {ticker}: {e}")

print("News collection finished.")
client.close()