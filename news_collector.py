import feedparser
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import os
import time

from nifty50 import NIFTY50_TICKER_MAP

# --- RSS SETUP ---
load_dotenv()
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')

RSS_FEEDS = {
    'Economic Times Markets': 'https://economictimes.indiatimes.com/markets/rss.cms',
    'Moneycontrol Markets': 'https://www.moneycontrol.com/rss/marketreports.xml',
    'Business Standard Markets': 'https://www.business-standard.com/rss/markets-106.rss'
}

GENERIC_PHRASES = [
    'taking stock', 'sensex', 'nifty50',
    'market rally', 'market falls', 'week ahead',
    'mid-day mood', 'top gainers', 'top losers',
    'market wrap', 'closing bell'
]

SEARCH_TERMS = {
    'RELIANCE.NS': ['Reliance', 'RIL'],
    'HDFCBANK.NS': ['HDFC Bank', 'HDFCBank'],
    'INFY.NS': ['Infosys', 'Infy'],
    'TCS.NS': ['TCS', 'Tata Consultancy'],
    'ICICIBANK.NS': ['ICICI Bank', 'ICICI'],
    'HINDUNILVR.NS': ['Hindustan Unilever', 'HUL'],
    'SBIN.NS': ['SBI', 'State Bank'],
    'BHARTIARTL.NS': ['Bharti Airtel', 'Airtel'],
    'KOTAKBANK.NS': ['Kotak Bank', 'Kotak Mahindra'],
    'WIPRO.NS': ['Wipro'],
    'AXISBANK.NS': ['Axis Bank'],
    'MARUTI.NS': ['Maruti', 'Suzuki'],
    'BAJFINANCE.NS': ['Bajaj Finance'],
    'BAJAJFINSV.NS': ['Bajaj Finserv'],
    'TITAN.NS': ['Titan'],
    'ADANIENT.NS': ['Adani Enterprises', 'Adani'],
    'ADANIPORTS.NS': ['Adani Ports'],
    'TATASTEEL.NS': ['Tata Steel'],
    'TATACONSUM.NS': ['Tata Consumer'],
    'NTPC.NS': ['NTPC'],
    'ONGC.NS': ['ONGC', 'Oil and Natural Gas'],
    'POWERGRID.NS': ['Power Grid', 'PGCIL'],
    'COALINDIA.NS': ['Coal India'],
    'BPCL.NS': ['BPCL', 'Bharat Petroleum'],
    'SUNPHARMA.NS': ['Sun Pharma', 'Sun Pharmaceutical'],
    'CIPLA.NS': ['Cipla'],
    'DRREDDY.NS': ['Dr Reddy', 'DRL'],
    'DIVISLAB.NS': ["Divi's Lab", 'Divis'],
    'APOLLOHOSP.NS': ['Apollo Hospital'],
    'HDFCLIFE.NS': ['HDFC Life'],
    'SBILIFE.NS': ['SBI Life'],
    'TECHM.NS': ['Tech Mahindra'],
    'HCLTECH.NS': ['HCL Tech', 'HCL Technologies'],
    'LTIM.NS': ['LTIMindtree', 'LTI'],
    'LT.NS': ['Larsen', 'L&T'],
    'ULTRACEMCO.NS': ['UltraTech Cement', 'UltraTech'],
    'GRASIM.NS': ['Grasim'],
    'ASIANPAINT.NS': ['Asian Paints'],
    'NESTLEIND.NS': ['Nestle'],
    'BRITANNIA.NS': ['Britannia'],
    'HEROMOTOCO.NS': ['Hero Moto', 'Hero MotoCorp'],
    'BAJAJ-AUTO.NS': ['Bajaj Auto'],
    'EICHERMOT.NS': ['Eicher', 'Royal Enfield'],
    'JSWSTEEL.NS': ['JSW Steel'],
    'HINDALCO.NS': ['Hindalco'],
    'M&M.NS': ['Mahindra', 'M&M'],
    'TRENT.NS': ['Trent', 'Westside'],
    'INDUSINDBK.NS': ['IndusInd Bank', 'IndusInd'],
    'ITC.NS': ['ITC'],
    'UPL.NS': ['UPL']
}

for ticker, company_name in NIFTY50_TICKER_MAP.items():
    terms = SEARCH_TERMS.setdefault(ticker, [])
    terms.append(ticker.replace('.NS', ''))
    if company_name not in terms:
        terms.append(company_name)


def normalize_text(text):
    return (text or '').lower()


def matches_ticker(text, ticker):
    normalized = normalize_text(text)
    terms = SEARCH_TERMS.get(ticker, [])
    return any(term.lower() in normalized for term in terms)


def parse_published(entry):
    published_time = None
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        published_time = datetime.fromtimestamp(time.mktime(entry.published_parsed))
    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
        published_time = datetime.fromtimestamp(time.mktime(entry.updated_parsed))
    return published_time or datetime.utcnow()


def run():
    client = MongoClient(MONGO_URI)
    db = client['stock_market_db']
    collection = db['news_articles']

    print('--- Starting RSS News Collection ---')

    for source_name, feed_url in RSS_FEEDS.items():
        print(f'Parsing feed: {source_name}')
        feed = feedparser.parse(feed_url)
        entries = getattr(feed, 'entries', [])

        for entry in entries:
            title = entry.get('title', '')
            summary = entry.get('summary', '') or entry.get('description', '') or ''
            combined_text = f"{title}\n\n{summary}"
            normalized_title = normalize_text(title)
            published_at = parse_published(entry)
            is_generic_market_update = any(
                phrase in normalized_title for phrase in GENERIC_PHRASES
            )

            for ticker, company_name in NIFTY50_TICKER_MAP.items():
                if matches_ticker(combined_text, ticker) and not is_generic_market_update:
                    record = {
                        'ticker': ticker,
                        'title': title,
                        'content': summary,
                        'published_at': published_at,
                        'source': source_name
                    }

                    if collection.find_one({'ticker': ticker, 'title': title, 'source': source_name}):
                        continue

                    collection.insert_one(record)
                    print(f"Inserted article for {ticker}: {title}")

        time.sleep(1)

    print('--- RSS News Collection Finished ---')
    client.close()


if __name__ == '__main__':
    run()

