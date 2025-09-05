import requests
from pymongo import MongoClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import time # Import the time module

# --- SETUP ---
load_dotenv()
API_KEY = os.getenv("NEWS_API_KEY")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# Import the full ticker-to-name map
from nifty50 import NIFTY50_TICKER_MAP

def run():
    """
    Connects to MongoDB, fetches news for all Nifty 50 companies,
    and stores articles in the database, with delays to prevent rate-limiting.
    """
    if not API_KEY:
        print("ERROR: NEWS_API_KEY not found in .env file. Skipping news collection.")
        return

    client = MongoClient(MONGO_URI)
    db = client['stock_market_db']
    collection = db['news_articles']
    date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    print("--- Starting News Collection ---")

    # --- DATA FETCHING & STORAGE ---
    for ticker, company_name in NIFTY50_TICKER_MAP.items():
        print(f"Fetching news for {company_name}...")
        url = (
            f'https://newsapi.org/v2/everything?'
            f'q="{company_name}"'
            f'&from={date_from}'
            f'&language=en'
            f'&sortBy=relevancy'
            f'&apiKey={API_KEY}'
        )

        try:
            response = requests.get(url)
            response.raise_for_status() # This will raise an HTTPError for bad responses (4xx or 5xx)

            data = response.json()
            articles = data.get('articles', [])

            if not articles:
                print(f"No articles found for {ticker}.")
                continue

            records_to_insert = []
            for article in articles:
                description = article.get('description') or ''
                record = {
                    'ticker': ticker,
                    'source': article['source']['name'],
                    'title': article['title'],
                    'description': description,
                    'url': article['url'],
                    'published_at': datetime.fromisoformat(article['publishedAt'].replace('Z', '+00:00'))
                }
                records_to_insert.append(record)
            
            if records_to_insert:
                collection.delete_many({'ticker': ticker})
                collection.insert_many(records_to_insert)
                print(f"Successfully inserted {len(records_to_insert)} news articles for {ticker}.")

        except requests.exceptions.HTTPError as e:
            # Specifically catch HTTP errors to see the status code
            if e.response.status_code == 429:
                print(f"Rate limit exceeded for {ticker}. Please wait and try again later, or consider upgrading your NewsAPI plan.")
            else:
                print(f"HTTP Error for {ticker}: {e}")
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {ticker}: {e}")
        except Exception as e:
            print(f"An error occurred for {ticker}: {e}")
        
        # --- ADDED DELAY ---
        # Wait for 1 second before the next request to avoid hitting API rate limits.
        time.sleep(1)

    print("--- News Collection Finished ---")
    client.close()

if __name__ == "__main__":
    run()

