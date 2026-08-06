import pymongo, os
from dotenv import load_dotenv
load_dotenv()
client = pymongo.MongoClient(os.getenv('MONGO_URI'))
db = client['stock_market_db']
collection = db['news_articles']

print(f'Total count: {collection.count_documents({})}')
print(f"Canonical count: {collection.count_documents({'article_id': {'$exists': True}})}")
print(f"Legacy count: {collection.count_documents({'article_id': {'$exists': False}})}")

canonical = list(collection.find({'article_id': {'$exists': True}}).limit(2000))
html_count = sum(1 for d in canonical if '<html' in str(d.get('description', '')).lower())
a_count = sum(1 for d in canonical if '<a ' in str(d.get('description', '')).lower())
google_url_count = sum(1 for d in canonical if 'news.google.com/rss/articles' in str(d.get('description', '')).lower())

print(f'<html: {html_count}')
print(f'<a: {a_count}')
print(f'google_url: {google_url_count}')
