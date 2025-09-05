import pandas as pd
import pandas_ta as ta
import yfinance as yf
from pymongo import MongoClient
from sklearn.model_selection import train_test_split, GridSearchCV
from xgboost import XGBClassifier
from sklearn.metrics import classification_report
from imblearn.over_sampling import SMOTE
import joblib
import json
from dotenv import load_dotenv
import os

# --- SETUP ---
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

def create_dataset(ticker, client):
    """
    Pulls data from MongoDB and engineers advanced features.
    """
    db = client['stock_market_db']
    
    prices_df = pd.DataFrame(list(db.historical_data.find({'ticker': ticker})))
    if prices_df.empty:
        return pd.DataFrame()
        
    prices_df.set_index('date', inplace=True)
    prices_df.sort_index(inplace=True)

    nifty_df = yf.download('^NSEI', start=prices_df.index.min(), end=prices_df.index.max(), progress=False, auto_adjust=True)
    
    prices_df['return'] = prices_df['close'].pct_change()
    nifty_df['nifty_return'] = nifty_df['Close'].pct_change()
    df = prices_df.join(nifty_df['nifty_return'], how='left')
    df['outperformance'] = df['return'] - df['nifty_return']
    
    news_df = pd.DataFrame(list(db.news_articles.find({'ticker': ticker})))
    if not news_df.empty:
        news_df['date'] = pd.to_datetime(news_df['published_at'].dt.date)
        sentiment_df = news_df.groupby('date')['sentiment'].apply(lambda x: x.str['score'].mean()).to_frame()
        df = df.join(sentiment_df, how='left')
    else:
        df['sentiment'] = 0.0

    df.fillna(0, inplace=True)
    
    df.ta.rsi(length=14, append=True)
    df.ta.macd(append=True)
    df.ta.bbands(append=True)
    df.ta.atr(append=True)
    df['sentiment_7d_avg'] = df['sentiment'].rolling(window=7).mean()
    df['price_change_1d'] = df['close'].pct_change(1)
    df['price_change_5d'] = df['close'].pct_change(5)
    df['market_correlation'] = df['return'].rolling(window=30).corr(df['nifty_return'])
    
    future_price_5d = df['close'].shift(-5)
    price_change = (future_price_5d - df['close']) / df['close']
    
    df['target'] = 1 
    df.loc[price_change > 0.02, 'target'] = 2
    df.loc[price_change < -0.02, 'target'] = 0
    
    df.dropna(inplace=True)
    return df

def train_model(df, ticker):
    """
    Performs hyperparameter tuning and saves the model and features into separate directories.
    """
    print(f"--- Training and Tuning model for {ticker} ---")
    
    MODELS_DIR = "models"
    FEATURES_DIR = "features"
    os.makedirs(MODELS_DIR, exist_ok=True)
    os.makedirs(FEATURES_DIR, exist_ok=True)

    # --- CORRECTED LINE ---
    # 'outperformance' is now REMOVED from the exclusion list, so it will be used as a feature.
    features = [col for col in df.columns if col not in ['_id', 'ticker', 'target', 'open', 'high', 'low', 'close', 'volume', 'sentiment', 'published_at', 'return']]
    
    features_filename = os.path.join(FEATURES_DIR, f"features_{ticker}.json")
    with open(features_filename, 'w') as f:
        json.dump(features, f)
    
    X = df[features]
    y = df['target']

    if len(y.unique()) < 3:
        print(f"Warning: Not enough class diversity for {ticker}. Skipping training.")
        return

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, shuffle=False)
    smote = SMOTE(random_state=42)
    X_train_resampled, y_train_resampled = smote.fit_resample(X_train, y_train)
    
    xgb = XGBClassifier(objective='multi:softmax', num_class=3, eval_metric='mlogloss', use_label_encoder=False)
    
    param_grid = {
        'max_depth': [3, 5],
        'learning_rate': [0.01, 0.1],
        'n_estimators': [100, 200],
        'gamma': [0.1, 0.2]
    }
    
    grid_search = GridSearchCV(estimator=xgb, param_grid=param_grid, cv=3, scoring='f1_weighted', verbose=0, n_jobs=-1)
    grid_search.fit(X_train_resampled, y_train_resampled)
    
    best_model = grid_search.best_estimator_
    y_pred = best_model.predict(X_test)
    
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred, target_names=['SELL', 'HOLD', 'BUY'], zero_division=0))

    model_filename = os.path.join(MODELS_DIR, f"model_{ticker}.joblib")
    joblib.dump(best_model, model_filename)
    print(f"Model saved as {model_filename}\n")

def run(tickers_to_process):
    """
    Main function to run the training pipeline for a given list of tickers.
    """
    client = MongoClient(MONGO_URI)
    for ticker in tickers_to_process:
        print(f"--- Processing {ticker} ---")
        dataset = create_dataset(ticker, client)
        
        if not dataset.empty and not dataset['target'].value_counts().empty:
            train_model(dataset, ticker)
        else:
            print(f"Could not create a valid dataset for {ticker}. Skipping.")
    client.close()

if __name__ == "__main__":
    from nifty50 import TICKERS
    run(TICKERS)

