import os
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.features.engineering import _prepare_sentiment_data

load_dotenv()
client = MongoClient(os.getenv('MONGO_URI', 'mongodb://localhost:27017/'))
db = client['stock_market_db']

tickers_to_test = ['RELIANCE.NS', 'TCS.NS', 'SBIN.NS', 'HDFCBANK.NS', 'TITAN.NS']

def get_sentiment_impact():
    print("="*60)
    print("HISTORICAL FEATURE IMPACT AUDIT")
    print("="*60)
    
    # We will fetch all canonical news articles and their old & new sentiment
    # Assuming 'compound' is the new (current DB) compound
    # But wait, we haven't executed the remediation script yet!
    # Let's execute the remediation script inside a DRY RUN, but how do we get the new scores?
    # We can just fetch the old scores from MongoDB and compute the new scores using FinBERT in memory.
    
    from transformers import pipeline
    print("Loading FinBERT...")
    finbert = pipeline('sentiment-analysis', model='ProsusAI/finbert', tokenizer='ProsusAI/finbert')
    print("Loaded FinBERT.")
    
    def build_sentiment_text(title, desc):
        title = (title or "").strip()
        desc = (desc or "").strip()
        if desc: return f"{title}. {desc[:500]}".strip()
        return title

    for ticker in tickers_to_test:
        docs = list(db.news_articles.find({
            "$or": [{"tickers": ticker}, {"ticker": ticker}]
        }, {"published_at": 1, "compound": 1, "label": 1, "score": 1, "title": 1, "description": 1, "article_id": 1}))
        
        # Build Old DataFrame
        # Convert legacy to match canonical
        old_docs = []
        new_docs = []
        
        for doc in docs:
            old_doc = {'published_at': doc['published_at'], 'compound': doc.get('compound', 0.0)}
            old_docs.append(old_doc)
            
            if 'article_id' in doc and 'label' in doc:
                input_text = build_sentiment_text(doc.get('title'), doc.get('description'))
                if input_text:
                    res = finbert(input_text)[0]
                    l = res['label'].lower()
                    s = float(res['score'])
                    new_c = s if l == 'positive' else (-s if l == 'negative' else 0.0)
                    new_docs.append({'published_at': doc['published_at'], 'compound': new_c})
                else:
                    new_docs.append({'published_at': doc['published_at'], 'compound': doc.get('compound', 0.0)})
            else:
                new_docs.append({'published_at': doc['published_at'], 'compound': doc.get('compound', 0.0)})
                
        old_sent_df = _prepare_sentiment_data(old_docs)
        new_sent_df = _prepare_sentiment_data(new_docs)
        
        if old_sent_df.empty or new_sent_df.empty:
            print(f"Skipping {ticker} - empty sentiment.")
            continue
            
        # We need a date index to do rolling
        start_date = min(old_sent_df.index.min(), new_sent_df.index.min())
        end_date = max(old_sent_df.index.max(), new_sent_df.index.max())
        idx = pd.date_range(start_date, end_date, freq='D')
        
        old_sent_df = old_sent_df.reindex(idx).fillna(0.0)
        new_sent_df = new_sent_df.reindex(idx).fillna(0.0)
        
        old_7d = old_sent_df["sentiment"].shift(1).rolling(window=7).mean().fillna(0.0)
        new_7d = new_sent_df["sentiment"].shift(1).rolling(window=7).mean().fillna(0.0)
        
        old_30d = old_sent_df["sentiment"].shift(1).rolling(window=30).mean().fillna(0.0)
        new_30d = new_sent_df["sentiment"].shift(1).rolling(window=30).mean().fillna(0.0)
        
        diff_7d = (new_7d - old_7d).abs()
        diff_30d = (new_30d - old_30d).abs()
        
        changed_mask = (diff_7d > 1e-4) | (diff_30d > 1e-4)
        changed_count = changed_mask.sum()
        total_count = len(changed_mask)
        pct_changed = changed_count / total_count * 100 if total_count > 0 else 0
        
        print(f"\n{ticker} Feature Impact:")
        print(f"Total rows: {total_count}")
        print(f"Changed rows: {changed_count} ({pct_changed:.2f}%)")
        
        if changed_count > 0:
            changed_diff = diff_7d[changed_mask]
            print(f"Mean Abs Diff (7d): {changed_diff.mean():.4f}")
            print(f"Median Abs Diff (7d): {changed_diff.median():.4f}")
            print(f"P95 Abs Diff (7d): {changed_diff.quantile(0.95):.4f}")
            print(f"Max Abs Diff (7d): {changed_diff.max():.4f}")
            
            affected_dates = changed_mask[changed_mask].index
            print(f"Earliest affected: {affected_dates.min().date()}")
            print(f"Latest affected: {affected_dates.max().date()}")
        else:
            print("No features changed.")
            
def lookahead_bias_test():
    print("\n" + "="*60)
    print("LOOKAHEAD BIAS TEST")
    print("="*60)
    # The actual implementation in engineering.py:
    # df["sentiment_7d_avg"] = df["sentiment"].shift(1).rolling(window=7).mean()
    # Let's mock a DataFrame
    dates = pd.date_range('2024-01-01', '2024-01-10', freq='D')
    news_docs = [
        {"published_at": datetime(2024, 1, 2, 10, 0), "compound": 0.5},   # Market hours
        {"published_at": datetime(2024, 1, 2, 20, 0), "compound": 0.9},   # Post market
        {"published_at": datetime(2024, 1, 3, 23, 59), "compound": -0.8}, # Midnight boundary
        {"published_at": datetime(2024, 1, 5, 12, 0), "compound": 0.3},   
    ]
    
    sent_df = _prepare_sentiment_data(news_docs)
    df = pd.DataFrame(index=dates)
    df = df.join(sent_df, how="left").fillna(0.0)
    df["sentiment_7d_avg"] = df["sentiment"].shift(1).rolling(window=7).mean().fillna(0.0)
    
    for i, date in enumerate(dates):
        print(f"{date.date()}: daily={df.iloc[i]['sentiment']:.4f}, 7d_avg={df.iloc[i]['sentiment_7d_avg']:.4f}")
    
    print("\nObservation:")
    print("Since news_df['date'] is grouped by dt.normalize() and joined to the market date index,")
    print("and then shifted by 1 day using shift(1), an article on Jan 2 is assigned to the Jan 2 daily average,")
    print("but only becomes available to models on Jan 3. Market/Post-market times are ignored because dt.normalize()")
    print("strips the time. This means post-market news on Jan 2 is used as a predictor for Jan 3, which is correct.")
    print("However, market-hours news on Jan 2 is ALSO used as a predictor for Jan 3, which is slightly delayed (safe but lags).")

if __name__ == '__main__':
    get_sentiment_impact()
    lookahead_bias_test()
