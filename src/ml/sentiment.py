import os
import time
from pymongo import MongoClient
from dotenv import load_dotenv
from transformers import pipeline
from datetime import datetime

# --- SETUP ---
load_dotenv()
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')


def run():
    client = MongoClient(MONGO_URI)
    db = client['stock_market_db']
    collection = db['news_articles']

    finbert = pipeline(
        'sentiment-analysis',
        model='ProsusAI/finbert',
        tokenizer='ProsusAI/finbert'
    )

    articles_to_analyze = collection.find({
        '$or': [
            {'sentiment_processed_at': {'$exists': False}},
            {'compound': {'$exists': False}},
            {'label': {'$exists': False}}
        ]
    })
    articles_list = list(articles_to_analyze)

    if not articles_list:
        print('No new articles to analyze.')
        client.close()
        return

    print("=" * 50)
    print("SENTIMENT ANALYSIS SUMMARY")
    print("=" * 50)
    print(f"Articles awaiting sentiment: {len(articles_list)}")
    
    analyzed_count = 0
    failed_count = 0

    for index, article in enumerate(articles_list, start=1):
        try:
            title = article.get('title', '')
            content = article.get('description', '') or article.get('content', '')
            text_to_analyze = f"{title}. {content[:500]}".strip()

            if not text_to_analyze:
                failed_count += 1
                continue

            result = finbert(text_to_analyze)[0]
            label = result['label'].lower()
            score = float(result['score'])

            if label == 'positive':
                compound = score
            elif label == 'negative':
                compound = -score
            else:
                compound = 0.0

            collection.update_one(
                {'_id': article['_id']},
                {'$set': {
                    'label': label,
                    'score': score,
                    'compound': compound,
                    'sentiment_model': 'ProsusAI/finbert',
                    'sentiment_processed_at': datetime.utcnow()
                }}
            )
            analyzed_count += 1

            if index % 20 == 0:
                print(f"Processed {index}/{len(articles_list)} articles...")

        except Exception as e:
            print(f'Could not analyze article {article.get("_id")}: {e}')
            failed_count += 1

    remaining = collection.count_documents({
        '$or': [
            {'sentiment_processed_at': {'$exists': False}},
            {'compound': {'$exists': False}},
            {'label': {'$exists': False}}
        ]
    })
    
    print(f"Processed successfully: {analyzed_count}")
    print(f"Failed to process: {failed_count}")
    print(f"Remaining unprocessed: {remaining}")
    print("=" * 50)
    
    client.close()


if __name__ == '__main__':
    run()
