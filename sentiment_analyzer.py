# sentiment_analyzer.py

import nltk
from pymongo import MongoClient
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# --- 1. NLTK VADER SETUP ---

# This downloads the 'vader_lexicon' which is the model VADER uses.
# It only needs to be downloaded once.
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    print("Downloading the VADER lexicon (one-time setup)...")
    nltk.download('vader_lexicon')

# --- 2. DATABASE CONNECTION ---

client = MongoClient('mongodb://localhost:27017/')
db = client['stock_market_db']
collection = db['news_articles']

# --- 3. SENTIMENT ANALYSIS ---

# Initialize the VADER sentiment analyzer
sia = SentimentIntensityAnalyzer()

# Find articles that have not been analyzed yet
# The '$exists: False' query finds documents where the 'sentiment' field does not exist
articles_to_analyze = collection.find({'sentiment': {'$exists': False}})

print("Starting sentiment analysis...")
analyzed_count = 0

for article in articles_to_analyze:
    try:
        # Analyze the title and description (if available)
        title = article.get('title') or ''
        description = article.get('description') or ''
        text_to_analyze = title + ". " + description

        if not text_to_analyze.strip():
            continue # Skip if there's no text

        # Get the polarity scores from VADER
        sentiment_scores = sia.polarity_scores(text_to_analyze)

        # The 'compound' score is a single, normalized score from -1 (negative) to +1 (positive)
        compound_score = sentiment_scores['compound']

        # Determine a simple label based on the compound score
        if compound_score >= 0.05:
            sentiment_label = 'positive'
        elif compound_score <= -0.05:
            sentiment_label = 'negative'
        else:
            sentiment_label = 'neutral'

        # --- 4. UPDATE DATABASE ---

        # Update the document in MongoDB with the new sentiment data
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