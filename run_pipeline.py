import data_collector
import news_collector
import sentiment_analyzer
import ml_trainer
from nifty50 import TICKERS
import os

# --- CONFIGURATION ---
# To avoid API rate limits, you can process a smaller number of stocks at a time.
# Set this to a number (e.g., 10) to process the first N stocks.
# Set this to None to process all 50 stocks.
STOCKS_TO_PROCESS = 10 

def main():
    """
    Executes the entire data collection, analysis, and model training pipeline.
    """
    print("==========================================================")
    print("          STARTING THE ML STOCK ANALYSIS PIPELINE         ")
    print("==========================================================")
    
    # Step 1: Collect historical stock data
    print("\n--- Step 1: Kicking off Data Collector ---")
    data_collector.run()
    
    # Step 2: Collect news articles
    print("\n--- Step 2: Kicking off News Collector ---")
    news_collector.run()
    
    # Step 3: Run sentiment analysis on the collected news
    print("\n--- Step 3: Kicking off Sentiment Analyzer ---")
    sentiment_analyzer.run()

    # Determine which tickers to train
    tickers_to_train = TICKERS
    if STOCKS_TO_PROCESS is not None:
        tickers_to_train = TICKERS[:STOCKS_TO_PROCESS]
        print(f"\n--- NOTE: Will process the first {STOCKS_TO_PROCESS} stocks as configured. ---")

    # Step 4: Train the ML model for each stock
    print("\n--- Step 4: Kicking off ML Model Trainer ---")
    # --- CORRECTED LINE ---
    # We now pass the 'tickers_to_train' list to the run function as required.
    ml_trainer.run(tickers_to_train)
    
    print("\n==========================================================")
    print("            PIPELINE EXECUTION COMPLETE!                  ")
    print("==========================================================")
    print("You can now start the Flask server with the 'flask run' command.")

if __name__ == '__main__':
    main()

