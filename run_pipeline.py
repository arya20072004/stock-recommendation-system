# run_pipeline.py

from data_collector import run as collect_market_data
from news_collector import run as collect_news
from sentiment_analyzer import run as analyze_sentiment

def main():
    print("Starting the automated data pipeline...")
    collect_market_data()
    collect_news()
    analyze_sentiment()
    print("Pipeline finished successfully.")

if __name__ == "__main__":
    main()