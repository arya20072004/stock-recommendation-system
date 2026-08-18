import os
import sys
import pandas as pd
import numpy as np
import unittest.mock
from pymongo import MongoClient

# Mock optuna before importing trainer
sys.modules['optuna'] = unittest.mock.MagicMock()

sys.path.append('C:/Users/aryab/Coding/stock_recommendations')
from src.ml.trainer import create_dataset
from src.features.v1.engineering import build_feature_row
from src.data.nifty50 import TICKERS
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
client = MongoClient(mongo_uri, tlsAllowInvalidCertificates=True)
db = client['stock_market_db']

total_tested = 0
tickers_with_unexpected_nans = 0
total_warmup_nans = 0
total_unexpected_nans = 0
total_alignment_nans = 0
total_row_filtering_nans = 0
nans_reach_estimator = False

for ticker in TICKERS:
    try:
        # First, we need to intercept before dropna to see warmup NaNs.
        # However, create_dataset handles it internally. 
        # So we just evaluate the output of create_dataset for UNEXPECTED NaNs.
        df_final = create_dataset(ticker, client)
        if df_final.empty:
            continue
            
        total_tested += 1
        
        # Check required columns used by model
        # To get the exact columns passed to the estimator:
        from src.ml.trainer import _make_feature_list
        features = _make_feature_list(df_final)
        
        # Are there NaNs in the final features?
        has_nans = df_final[features].isna().any().any()
        if has_nans:
            tickers_with_unexpected_nans += 1
            nans_reach_estimator = True
            total_unexpected_nans += df_final[features].isna().sum().sum()
            
    except Exception as e:
        print(f"Error on {ticker}: {e}")

print(f"TOTAL_TICKERS_TESTED={total_tested}")
print(f"TICKERS_WITH_UNEXPECTED_NANS={tickers_with_unexpected_nans}")
print(f"TOTAL_UNEXPECTED_NANS={total_unexpected_nans}")
print(f"NANS_REACH_ESTIMATOR={'YES' if nans_reach_estimator else 'NO'}")

# To classify the NaNs before dropna, we can run the pipeline parts manually for one ticker
# or monkey patch dropna. Let's patch dropna to count them!
