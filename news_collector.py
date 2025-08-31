# news_collector.py

import requests
from pymongo import MongoClient
from datetime import datetime, timedelta
import config # Your config file with API keys

def run():
    """
    Connects to MongoDB, fetches recent news articles for a predefined list
    of Indian companies from NewsAPI, and stores them in the database.
    """
    # --- SETUP ---
    TICKER_MAP = {
        'RELIANCE.NS': 'Reliance Industries',
        'TCS.NS': 'Tata Consultancy Services',
        'HDFCBANK.NS': 'HDFC Bank',
        'INFY.NS': 'Infosys'
    }
    API_KEY = config.NEWS_API_KEY
    client = MongoClient('mongodb://localhost:27017/')
    db = client['stock_market_db']
    collection = db['news_articles']
    date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    print("Starting news collection...")

    # --- DATA FETCHING & STORAGE ---
    for ticker, company_name in TICKER_MAP.items():
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
            response.raise_for_status()

            data = response.json()
            articles = data.get('articles', [])

            if not articles:
                print(f"No articles found for {ticker}.")
                continue

            records_to_insert = []
            for article in articles:
                # Ensure description is not None before storing
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
            
            # To avoid duplicates, you can remove old data first
            if records_to_insert:
                collection.delete_many({'ticker': ticker})
                collection.insert_many(records_to_insert)
                print(f"Successfully inserted {len(records_to_insert)} news articles for {ticker}.")

        except requests.exceptions.RequestException as e:
            print(f"HTTP Request failed for {ticker}: {e}")
        except Exception as e:
            print(f"An error occurred for {ticker}: {e}")

    print("News collection finished.")
    client.close()

# This block allows the script to be run directly
if __name__ == "__main__":
    run()