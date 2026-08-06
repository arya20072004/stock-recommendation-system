import pymongo
import os
import argparse
from dotenv import load_dotenv

# Import the exact normalizer from production
from src.data.news_collector import clean_html_description, is_description_meaningful

load_dotenv()
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')

def run(dry_run=True):
    client = pymongo.MongoClient(MONGO_URI)
    db = client['stock_market_db']
    collection = db['news_articles']
    
    # Target only CANONICAL documents
    cursor = collection.find({'article_id': {'$exists': True}})
    
    stats = {
        'scanned': 0,
        'already_clean': 0,
        'html_cleaned': 0,
        'url_garbage_removed': 0,
        'changed': 0,
        'cleared': 0,
        'errors': 0
    }
    
    print(f"Starting Canonical Description Cleanup (Dry Run: {dry_run})")
    print("=" * 60)
    
    for doc in cursor:
        try:
            stats['scanned'] += 1
            
            raw_desc = doc.get('description')
            title = doc.get('title', '')
            source = doc.get('source', '')
            
            if not raw_desc:
                stats['already_clean'] += 1
                continue
                
            clean_desc = clean_html_description(raw_desc)
            
            if not is_description_meaningful(clean_desc, title, source):
                clean_desc = None
                
            if clean_desc == raw_desc:
                stats['already_clean'] += 1
                continue
                
            # It changed. Determine classification for reporting
            if clean_desc is None:
                stats['cleared'] += 1
                if 'http' in raw_desc.lower() and len(raw_desc.split()) < 3:
                    stats['url_garbage_removed'] += 1
            else:
                stats['changed'] += 1
                if '<' in raw_desc or '&' in raw_desc:
                    stats['html_cleaned'] += 1
                    
            if not dry_run:
                collection.update_one(
                    {'_id': doc['_id']},
                    {'$set': {'description': clean_desc}}
                )
                
        except Exception as e:
            print(f"Error processing doc {doc.get('_id')}: {e}")
            stats['errors'] += 1
            
    print("=" * 60)
    print(f"Documents scanned: {stats['scanned']}")
    print(f"Already clean: {stats['already_clean']}")
    print(f"Descriptions changed: {stats['changed']}")
    print(f"Descriptions cleared: {stats['cleared']}")
    print(f"HTML cleaned (subset of changed): {stats['html_cleaned']}")
    print(f"URL garbage removed (subset of cleared): {stats['url_garbage_removed']}")
    print(f"Errors: {stats['errors']}")
    print("=" * 60)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Clean canonical news descriptions")
    parser.add_argument('--write', action='store_true', help="Execute writes to MongoDB")
    args = parser.parse_args()
    
    run(dry_run=not args.write)
