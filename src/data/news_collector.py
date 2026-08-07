import hashlib
import time
import urllib.parse
import calendar
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
import feedparser
import yfinance as yf
from dotenv import load_dotenv
import os
import re

from src.data.nifty50 import NIFTY50_TICKER_MAP, TICKERS

# --- RSS SETUP ---
load_dotenv()
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')

def get_utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

GENERIC_PHRASES = [
    'taking stock', 'sensex', 'nifty50',
    'market rally', 'market falls', 'week ahead',
    'mid-day mood', 'top gainers', 'top losers',
    'market wrap', 'closing bell'
]

SEARCH_TERMS = {
    'ADANIENT.NS': ['Adani Enterprises', 'Adani'],
    'ADANIPORTS.NS': ['Adani Ports'],
    'APOLLOHOSP.NS': ['Apollo Hospital', 'Apollo Hospitals'],
    'ASIANPAINT.NS': ['Asian Paints'],
    'AXISBANK.NS': ['Axis Bank'],
    'BAJAJ-AUTO.NS': ['Bajaj Auto'],
    'BAJFINANCE.NS': ['Bajaj Finance'],
    'BAJAJFINSV.NS': ['Bajaj Finserv'],
    'BEL.NS': ['Bharat Electronics', 'BEL'],
    'BHARTIARTL.NS': ['Bharti Airtel', 'Airtel'],
    'BRITANNIA.NS': ['Britannia'],
    'CIPLA.NS': ['Cipla'],
    'COALINDIA.NS': ['Coal India'],
    'DRREDDY.NS': ['Dr Reddy', 'DRL', "Dr Reddy's"],
    'EICHERMOT.NS': ['Eicher', 'Royal Enfield'],
    'ETERNAL.NS': ['Zomato'],
    'GRASIM.NS': ['Grasim'],
    'HCLTECH.NS': ['HCL Tech', 'HCL Technologies'],
    'HDFCBANK.NS': ['HDFC Bank', 'HDFCBank'],
    'HDFCLIFE.NS': ['HDFC Life'],
    'HEROMOTOCO.NS': ['Hero Moto', 'Hero MotoCorp'],
    'HINDALCO.NS': ['Hindalco'],
    'HINDUNILVR.NS': ['Hindustan Unilever', 'HUL'],
    'ICICIBANK.NS': ['ICICI Bank', 'ICICI'],
    'INDIGO.NS': ['InterGlobe Aviation', 'IndiGo'],
    'INFY.NS': ['Infosys', 'Infy'],
    'ITC.NS': ['ITC'],
    'JIOFIN.NS': ['Jio Financial Services', 'Jio Financial', 'JIOFIN'],
    'JSWSTEEL.NS': ['JSW Steel'],
    'KOTAKBANK.NS': ['Kotak Bank', 'Kotak Mahindra'],
    'LT.NS': ['Larsen', 'L&T', 'Larsen & Toubro'],
    'M&M.NS': ['Mahindra', 'M&M', 'Mahindra & Mahindra'],
    'MARUTI.NS': ['Maruti', 'Suzuki', 'Maruti Suzuki'],
    'MAXHEALTH.NS': ['Max Healthcare', 'Max Health'],
    'NESTLEIND.NS': ['Nestle', 'Nestle India'],
    'NTPC.NS': ['NTPC'],
    'ONGC.NS': ['ONGC', 'Oil and Natural Gas'],
    'POWERGRID.NS': ['Power Grid', 'PGCIL'],
    'RELIANCE.NS': ['Reliance', 'RIL'],
    'SBILIFE.NS': ['SBI Life'],
    'SBIN.NS': ['SBI', 'State Bank', 'State Bank of India'],
    'SHRIRAMFIN.NS': ['Shriram Finance', 'Shriram Transport'],
    'SUNPHARMA.NS': ['Sun Pharma', 'Sun Pharmaceutical'],
    'TATACONSUM.NS': ['Tata Consumer', 'Tata Consumer Products'],
    'TMPV.NS': ['Tata Motors', 'TaMo'],
    'TATASTEEL.NS': ['Tata Steel'],
    'TCS.NS': ['TCS', 'Tata Consultancy', 'Tata Consultancy Services'],
    'TECHM.NS': ['Tech Mahindra', 'TechM'],
    'TITAN.NS': ['Titan', 'Titan Company'],
    'TRENT.NS': ['Trent', 'Westside'],
    'ULTRACEMCO.NS': ['UltraTech Cement', 'UltraTech'],
    'WIPRO.NS': ['Wipro']
}

for ticker, company_name in NIFTY50_TICKER_MAP.items():
    terms = SEARCH_TERMS.setdefault(ticker, [])
    terms.append(ticker.replace('.NS', ''))
    if company_name not in terms:
        terms.append(company_name)


import html

def normalize_text(text):
    return (text or '').lower()

def clean_html_description(raw_html):
    if not raw_html:
        return None
    # Decode HTML entities
    text = html.unescape(raw_html)
    # Strip HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text if text else None

def is_description_meaningful(desc, title, source):
    if not desc:
        return False
    
    desc_lower = desc.lower()
    title_lower = title.lower() if title else ""
    source_lower = source.lower() if source else ""
    
    # URL only or mostly URL
    if 'http://' in desc_lower or 'https://' in desc_lower:
        # If it's literally just a URL or a Google news redirect path
        if 'news.google.com/rss/articles' in desc_lower:
            return False
        # If the entire description is just a single URL
        if len(desc.split()) == 1 and desc.startswith('http'):
            return False
            
    # Too short boilerplate
    if len(desc) < 15:
        return False
        
    # Identical to title
    if desc_lower == title_lower:
        return False
        
    # Merely "title - source"
    if desc_lower == f"{title_lower} - {source_lower}".strip():
        return False
        
    # Only publisher text
    if desc_lower == source_lower:
        return False
        
    return True


def matches_ticker(text, ticker):
    normalized = normalize_text(text)
    terms = SEARCH_TERMS.get(ticker, [])
    for term in terms:
        if len(term) <= 4:
            # Word boundary check for short terms to avoid false positives 
            # (e.g. "BEL" in "below", "ITC" in "pitch")
            if re.search(r'\b' + re.escape(term.lower()) + r'\b', normalized):
                return True
        else:
            if term.lower() in normalized:
                return True
    return False

def generate_article_id(url, title, source, published_at):
    if url:
        canonical_url = url.split('?')[0] # Basic parameter stripping
        return hashlib.sha256(canonical_url.encode('utf-8')).hexdigest(), canonical_url
    else:
        # Fallback identity
        fallback_str = f"{normalize_text(source)}_{normalize_text(title)}_{published_at.strftime('%Y-%m-%d')}"
        return hashlib.sha256(fallback_str.encode('utf-8')).hexdigest(), None

def fetch_google_news(ticker):
    company_name = NIFTY50_TICKER_MAP.get(ticker, ticker.replace('.NS', ''))
    query = urllib.parse.quote(f'"{company_name}" NSE stock')
    url = f"https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en"
    
    articles = []
    malformed_count = 0
    try:
        feed = feedparser.parse(url)
        if feed.bozo and getattr(feed, 'bozo_exception', None):
            # A bozo exception doesn't always mean the whole feed is invalid, but we log it.
            pass
        
        entries = getattr(feed, 'entries', [])
        for entry in entries:
            try:
                title = entry.get('title', '')
                link = entry.get('link', '')
                raw_summary = entry.get('summary', '') or entry.get('description', '') or ''
                
                source_dict = entry.get('source') or {}
                source = source_dict.get('title', 'Google News') if isinstance(source_dict, dict) else 'Google News'
                
                clean_summary = clean_html_description(raw_summary)
                if not is_description_meaningful(clean_summary, title, source=source):
                    clean_summary = None
                
                published_time = None
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    # Treat published_parsed as UTC, convert safely
                    published_time = datetime.fromtimestamp(calendar.timegm(entry.published_parsed), timezone.utc).replace(tzinfo=None)
                else:
                    published_time = get_utc_now()
                    
                articles.append({
                    'title': title,
                    'description': clean_summary,
                    'url': link,
                    'source': source,
                    'published_at': published_time,
                    'fetched_source': 'Google News'
                })
            except Exception:
                malformed_count += 1
    except Exception as e:
        print(f"  Google News exception for {ticker}: {e}")
        return None, malformed_count
        
    return articles, malformed_count

def fetch_yahoo_finance(ticker):
    articles = []
    malformed_count = 0
    try:
        tkr = yf.Ticker(ticker)
        news = tkr.news
        for item in news:
            try:
                content = item.get('content') or {}
                if not content: continue
                    
                title = content.get('title', '')
                
                # Defensive nested dictionary parsing
                click_through = content.get('clickThroughUrl') or {}
                link = click_through.get('url', '') if isinstance(click_through, dict) else ''
                
                raw_summary = content.get('summary', '') or content.get('description', '') or ''
                clean_summary = clean_html_description(raw_summary)
                
                provider = content.get('provider') or {}
                source = provider.get('displayName', 'Yahoo Finance') if isinstance(provider, dict) else 'Yahoo Finance'
                
                if not is_description_meaningful(clean_summary, title, source):
                    clean_summary = None
                
                pubDate = content.get('pubDate')
                published_time = get_utc_now()
                if pubDate:
                    try:
                        published_time = datetime.fromisoformat(pubDate.replace('Z', '+00:00'))
                        published_time = published_time.replace(tzinfo=None)
                    except:
                        pass
                
                articles.append({
                    'title': title,
                    'description': clean_summary,
                    'url': link,
                    'source': source,
                    'published_at': published_time,
                    'fetched_source': 'Yahoo Finance'
                })
            except Exception:
                malformed_count += 1
    except Exception as e:
        print(f"  Yahoo Finance exception for {ticker}: {e}")
        return None, malformed_count
        
    return articles, malformed_count

def run():
    client = MongoClient(MONGO_URI)
    db = client['stock_market_db']
    collection = db['news_articles']
    
    # Create required indexes for the new schema
    collection.create_index('article_id', unique=True, sparse=True) # sparse in case of legacy docs
    collection.create_index('tickers')
    collection.create_index('published_at')

    print("=" * 50)
    print("NEWS COLLECTION SUMMARY")
    print("=" * 50)
    
    stats = {
        'articles_fetched': 0,
        'articles_normalized': 0,
        'articles_rejected': 0,
        'articles_matching_active': 0,
        'generic_unmatched': 0,
        'new_articles_inserted': 0,
        'existing_articles_skipped': 0,
        'newest_fetched': None,
        'oldest_fetched': None
    }
    
    per_ticker_stats = {}
    sources_status = {
        'Google News': {'attempted': 0, 'succeeded': 0, 'failed': 0, 'malformed': 0},
        'Yahoo Finance': {'attempted': 0, 'succeeded': 0, 'failed': 0, 'malformed': 0}
    }
    
    fallback_metrics = {
        'tickers_triggered': 0,
        'articles_recovered': 0,
        'tickers_failed': []
    }
    
    ticker_coverage = {t: [] for t in TICKERS}
    
    def classify_freshness(dt):
        now = get_utc_now()
        age = now - dt
        if age <= timedelta(days=1): return '<= 24h'
        if age <= timedelta(days=3): return '<= 3d'
        if age <= timedelta(days=7): return '<= 7d'
        if age <= timedelta(days=30): return '<= 30d'
        if age <= timedelta(days=90): return '<= 90d'
        return '> 90d'

    freshness_fetched = {'<= 24h': 0, '<= 3d': 0, '<= 7d': 0, '<= 30d': 0, '<= 90d': 0, '> 90d': 0}
    freshness_inserted = {'<= 24h': 0, '<= 3d': 0, '<= 7d': 0, '<= 30d': 0, '<= 90d': 0, '> 90d': 0}

    for ticker in TICKERS:
        per_ticker_stats[ticker] = {
            'retrieved_articles': 0,
            'association_validated': 0,
            'association_rejected': 0,
        }
        
        sources_status['Google News']['attempted'] += 1
        google_res, google_malformed = fetch_google_news(ticker)
        
        sources_status['Google News']['malformed'] += google_malformed
        
        if google_res is None:
            sources_status['Google News']['failed'] += 1
            articles = []
        else:
            sources_status['Google News']['succeeded'] += 1
            articles = google_res
            
        recent_articles = [a for a in articles if a['published_at'] > get_utc_now() - timedelta(days=7)]
        
        if not recent_articles:
            fallback_metrics['tickers_triggered'] += 1
            sources_status['Yahoo Finance']['attempted'] += 1
            yahoo_res, yahoo_malformed = fetch_yahoo_finance(ticker)
            
            sources_status['Yahoo Finance']['malformed'] += yahoo_malformed
            
            if yahoo_res is None:
                sources_status['Yahoo Finance']['failed'] += 1
                fallback_metrics['tickers_failed'].append(ticker)
            else:
                sources_status['Yahoo Finance']['succeeded'] += 1
                articles.extend(yahoo_res)
                fallback_metrics['articles_recovered'] += len(yahoo_res)
            
        stats['articles_fetched'] += len(articles)
        per_ticker_stats[ticker]['retrieved_articles'] += len(articles)
        
        valid_records = []
        for article in articles:
            stats['articles_normalized'] += 1
            
            if stats['newest_fetched'] is None or article['published_at'] > stats['newest_fetched']:
                stats['newest_fetched'] = article['published_at']
            if stats['oldest_fetched'] is None or article['published_at'] < stats['oldest_fetched']:
                stats['oldest_fetched'] = article['published_at']
            
            combined_text = f"{article['title']}\n\n{article['description']}"
            normalized_title = normalize_text(article['title'])
            
            is_generic = any(phrase in normalized_title for phrase in GENERIC_PHRASES)
            if is_generic:
                stats['generic_unmatched'] += 1
                per_ticker_stats[ticker]['association_rejected'] += 1
                stats['articles_rejected'] += 1
                continue
                
            if matches_ticker(combined_text, ticker):
                article_id, canonical_url = generate_article_id(article['url'], article['title'], article['source'], article['published_at'])
                
                record = {
                    'article_id': article_id,
                    'title': article['title'],
                    'description': article['description'],
                    'url': article['url'],
                    'canonical_url': canonical_url,
                    'source': article['source'],
                    'published_at': article['published_at'],
                    'fetched_source': article['fetched_source'],
                    'collected_at': get_utc_now()
                }
                
                valid_records.append(record)
                stats['articles_matching_active'] += 1
                per_ticker_stats[ticker]['association_validated'] += 1
            else:
                stats['generic_unmatched'] += 1
                per_ticker_stats[ticker]['association_rejected'] += 1
                stats['articles_rejected'] += 1

        for record in valid_records:
            freshness_fetched[classify_freshness(record['published_at'])] += 1
            ticker_coverage[ticker].append(record['published_at'])
            
            try:
                result = collection.update_one(
                    {'article_id': record['article_id']},
                    {
                        '$setOnInsert': {
                            'title': record['title'],
                            'description': record['description'],
                            'url': record['url'],
                            'canonical_url': record['canonical_url'],
                            'source': record['source'],
                            'published_at': record['published_at'],
                            'collected_at': record['collected_at'],
                            'fetched_source': record['fetched_source']
                        },
                        '$addToSet': {
                            'tickers': ticker
                        }
                    },
                    upsert=True
                )
                if result.upserted_id:
                    stats['new_articles_inserted'] += 1
                    freshness_inserted[classify_freshness(record['published_at'])] += 1
                else:
                    stats['existing_articles_skipped'] += 1
            except PyMongoError as e:
                print(f"  DB Error inserting article for {ticker}: {e}")

        time.sleep(1.0)
        
    coverage_24h = [t for t, dates in ticker_coverage.items() if any(get_utc_now() - d <= timedelta(days=1) for d in dates)]
    coverage_7d = [t for t, dates in ticker_coverage.items() if any(get_utc_now() - d <= timedelta(days=7) for d in dates)]
    coverage_30d = [t for t, dates in ticker_coverage.items() if any(get_utc_now() - d <= timedelta(days=30) for d in dates)]
    
    zero_7d = [t for t in TICKERS if t not in coverage_7d]
    zero_30d = [t for t in TICKERS if t not in coverage_30d]
        
    print("=" * 50)
    print("DIAGNOSTICS & HARDENING METRICS")
    print("=" * 50)
    
    # Let's print out the required metrics
    print("PROVIDER HEALTH:")
    for provider, p_stats in sources_status.items():
        print(f"  {provider}:")
        print(f"    Requests Attempted: {p_stats['attempted']}")
        print(f"    Requests Succeeded: {p_stats['succeeded']}")
        print(f"    Requests Failed:    {p_stats['failed']}")
        print(f"    Malformed Articles: {p_stats['malformed']}")
        
    print("\nFALLBACK DIAGNOSTICS:")
    print(f"  Tickers triggering fallback: {fallback_metrics['tickers_triggered']}")
    print(f"  Articles recovered: {fallback_metrics['articles_recovered']}")
    if fallback_metrics['tickers_failed']:
        print(f"  Failed Yahoo tickers: {', '.join(fallback_metrics['tickers_failed'])}")
        
    print(f"\nArticles fetched: {stats['articles_fetched']}")
    print(f"Articles normalized: {stats['articles_normalized']}")
    print(f"Articles rejected: {stats['articles_rejected']}")
    print(f"Articles matching active tickers: {stats['articles_matching_active']}")
    print(f"Generic/unmatched articles: {stats['generic_unmatched']}")
    print(f"New articles inserted: {stats['new_articles_inserted']}")
    print(f"Existing articles skipped: {stats['existing_articles_skipped']}\n")
    print(f"Newest fetched article: {stats['newest_fetched']}")
    print(f"Oldest fetched article: {stats['oldest_fetched']}\n")
    
    print("FRESHNESS DISTRIBUTION (ALL FETCHED/MATCHED):")
    for k, v in freshness_fetched.items():
        print(f"  {k}: {v}")
        
    print("\nFRESHNESS DISTRIBUTION (NEWLY INSERTED):")
    for k, v in freshness_inserted.items():
        print(f"  {k}: {v}")
        
    print(f"\nACTIVE TICKER COVERAGE (Universe size: {len(TICKERS)}):")
    print(f"  24h coverage: {len(coverage_24h)} tickers with >= 1 article ({len(TICKERS) - len(coverage_24h)} with zero)")
    print(f"  7d coverage:  {len(coverage_7d)} tickers with >= 1 article ({len(zero_7d)} with zero)")
    print(f"  30d coverage: {len(coverage_30d)} tickers with >= 1 article ({len(zero_30d)} with zero)")
    
    if zero_7d:
        print(f"\n  Zero-coverage (7d): {', '.join(zero_7d)}")
    if zero_30d:
        print(f"  Zero-coverage (30d): {', '.join(zero_30d)}")
        
    print("\nCANONICAL DEDUPLICATION:")
    total_docs = collection.count_documents({'article_id': {'$exists': True}})
    # PyMongo doesn't have a direct distinct count that scales perfectly if big, but distinct is fine here
    unique_ids = len(collection.distinct('article_id'))
    print(f"  Total canonical documents: {total_docs}")
    print(f"  COUNT(DISTINCT article_id): {unique_ids}")
    print(f"  Duplicate article_id count: {total_docs - unique_ids}")
    
    client.close()

if __name__ == '__main__':
    run()
