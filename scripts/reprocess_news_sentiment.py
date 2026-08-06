import os
import argparse
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
from transformers import pipeline

load_dotenv()
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')

def build_sentiment_text(title, clean_description):
    """
    Preferred semantic input: title + '. ' + meaningful_clean_description
    If no meaningful description exists: title
    """
    title = (title or "").strip()
    desc = (clean_description or "").strip()
    
    if desc:
        return f"{title}. {desc[:500]}".strip()
    return title

def run(apply=False):
    client = MongoClient(MONGO_URI)
    db = client['stock_market_db']
    collection = db['news_articles']
    
    print("=" * 60)
    print(f"SENTIMENT REMEDIATION (Dry Run: {not apply})")
    print("=" * 60)
    
    try:
        # Load pipeline once
        finbert = pipeline(
            'sentiment-analysis',
            model='ProsusAI/finbert',
            tokenizer='ProsusAI/finbert'
        )
    except Exception as e:
        print(f"Failed to load FinBERT model: {e}")
        return
        
    # Get total canonical documents
    query = {"article_id": {"$exists": True}}
    total_canonical = collection.count_documents(query)
    
    cursor = collection.find(query)
    
    stats = {
        'targeted': total_canonical,
        'successfully_reprocessed': 0,
        'updated': 0,
        'unchanged': 0,
        'failed': 0,
        'missing_descriptions': 0,
        'title_only_inference': 0,
        'legacy_touched': 0
    }
    
    # Process in batches for performance if we were doing bulk updates, but single-document inference
    # is slow enough that updating one by one is fine for 1800 docs.
    for i, doc in enumerate(cursor):
        if 'article_id' not in doc:
            # Safety check, should be impossible with query
            stats['legacy_touched'] += 1
            continue
            
        try:
            title = doc.get('title', '')
            desc = doc.get('description')
            
            if not desc:
                stats['missing_descriptions'] += 1
                stats['title_only_inference'] += 1
                
            input_text = build_sentiment_text(title, desc)
            
            if not input_text:
                stats['failed'] += 1
                continue
                
            result = finbert(input_text)[0]
            new_label = result['label'].lower()
            new_score = float(result['score'])
            
            if new_label == 'positive':
                new_compound = new_score
            elif new_label == 'negative':
                new_compound = -new_score
            else:
                new_compound = 0.0
                
            old_label = doc.get('label')
            old_score = doc.get('score')
            old_compound = doc.get('compound')
            
            # Check if an update is needed
            # We consider it unchanged if label matches and compound is within a tiny float tolerance
            is_changed = True
            if old_label == new_label and old_compound is not None:
                if abs(old_compound - new_compound) < 1e-4:
                    is_changed = False
                    
            if is_changed:
                stats['updated'] += 1
            else:
                stats['unchanged'] += 1
                
            stats['successfully_reprocessed'] += 1
            
            if apply and is_changed:
                collection.update_one(
                    {'_id': doc['_id']},
                    {'$set': {
                        'label': new_label,
                        'score': new_score,
                        'compound': new_compound,
                        'sentiment_model': 'ProsusAI/finbert',
                        'sentiment_processed_at': datetime.utcnow(),
                        'sentiment_provenance': 'v2_normalized'
                    }}
                )
                
            if (i + 1) % 100 == 0:
                print(f"Processed {i + 1}/{total_canonical} documents...")
                
        except Exception as e:
            print(f"Error inferring sentiment for doc {doc.get('_id')}: {e}")
            stats['failed'] += 1
            
    print("=" * 60)
    print("REMEDIATION SUMMARY")
    print("=" * 60)
    print(f"Canonical documents targeted: {stats['targeted']}")
    print(f"Successfully reprocessed: {stats['successfully_reprocessed']}")
    print(f"Updated: {stats['updated']}")
    print(f"Unchanged: {stats['unchanged']}")
    print(f"Failed: {stats['failed']}")
    print(f"Missing descriptions: {stats['missing_descriptions']}")
    print(f"Title-only inference count: {stats['title_only_inference']}")
    print(f"Legacy documents touched: {stats['legacy_touched']}")
    print("=" * 60)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Reprocess news sentiment")
    parser.add_argument('--apply', action='store_true', help="Execute writes to MongoDB")
    args = parser.parse_args()
    
    run(apply=args.apply)
