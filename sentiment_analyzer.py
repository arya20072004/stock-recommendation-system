import nltk
from pymongo import MongoClient
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import os
from dotenv import load_dotenv

def run():
    """
    Connects to MongoDB, analyzes sentiment of news articles that haven't
    been processed yet, and updates the database.
    """
    # --- SETUP AND DATABASE CONNECTION ---
    load_dotenv()
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    db = client['stock_market_db']
    collection = db['news_articles']

    # --- NLTK VADER SETUP ---
    try:
        nltk.data.find('sentiment/vader_lexicon.zip')
    except LookupError:
        print("Downloading the VADER lexicon (one-time setup)...")
        nltk.download('vader_lexicon')

    # --- SENTIMENT ANALYSIS ---
    sia = SentimentIntensityAnalyzer()
    
    # Find articles where the 'sentiment' field does not exist
    articles_to_analyze = collection.find({'sentiment': {'$exists': False}})
    
    # Convert cursor to list to get a count and avoid cursor exhaustion
    articles_list = list(articles_to_analyze)
    if not articles_list:
        print("No new articles to analyze.")
        client.close()
        return
        
    print(f"Starting sentiment analysis for {len(articles_list)} new articles...")
    analyzed_count = 0

    for article in articles_list:
        try:
            title = article.get('title') or ''
            description = article.get('description') or ''
            text_to_analyze = title + ". " + description

            if not text_to_analyze.strip():
                continue

            sentiment_scores = sia.polarity_scores(text_to_analyze)
            compound_score = sentiment_scores['compound']

            if compound_score >= 0.05:
                sentiment_label = 'positive'
            elif compound_score <= -0.05:
                sentiment_label = 'negative'
            else:
                sentiment_label = 'neutral'

            # --- UPDATE DATABASE ---
            collection.update_one(
                {'_id': article['_id']},
                {'$set': {
                    'sentiment': {
                        'score': compound_score,
                        'label': sentiment_label
                    }
                }}
            )
            analyzed_count += 1

        except Exception as e:
            print(f"Could not analyze article {article['_id']}: {e}")

    print(f"Sentiment analysis complete. Analyzed and updated {analyzed_count} articles.")
    client.close()

if __name__ == "__main__":
    run()
