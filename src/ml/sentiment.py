import os
import time
from pymongo import MongoClient
from dotenv import load_dotenv
from transformers import pipeline

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
            {'sentiment': {'$exists': False}},
            {'compound': {'$exists': False}},
            {'label': {'$exists': False}}
        ]
    })
    articles_list = list(articles_to_analyze)

    if not articles_list:
        print('No new articles to analyze.')
        client.close()
        return

    print(f'Starting sentiment analysis for {len(articles_list)} new articles...')
    analyzed_count = 0

    for index, article in enumerate(articles_list, start=1):
        try:
            title = article.get('title', '')
            content = article.get('content', '')
            text_to_analyze = f"{title}. {content[:500]}".strip()

            if not text_to_analyze:
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
                    'compound': compound
                }}
            )
            analyzed_count += 1

            if index % 10 == 0:
                print(f'Processed {index} articles...')

        except Exception as e:
            print(f'Could not analyze article {article.get("_id")}: {e}')

    print(f'Sentiment analysis complete. Analyzed and updated {analyzed_count} articles.')
    client.close()


if __name__ == '__main__':
    run()
