import os
import sys
import json
import logging
from pymongo import MongoClient
import joblib

sys.path.append("c:/Users/aryab/Coding/stock_recommendations")
MODELS_DIR = "c:/Users/aryab/Coding/stock_recommendations/saved_models"
FEATURES_DIR = "c:/Users/aryab/Coding/stock_recommendations/saved_features"

def get_db():
    from dotenv import load_dotenv
    load_dotenv("c:/Users/aryab/Coding/stock_recommendations/.env")
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    return client["stock_market_db"]

db = get_db()
all_records = list(db.model_registry.find())

def audit_ticker(ticker):
    print(f"--- AUDITING {ticker} ---")
    cands = [r for r in all_records if r.get("ticker") == ticker and r.get("status") == "CANDIDATE"]
    print(f"Candidates found: {len(cands)}")
    
    if not cands:
        return
        
    c = sorted(cands, key=lambda x: x.get("trained_at", ""), reverse=True)[0]
    
    version = c.get("version")
    model_hash = c.get("model_hash")
    feature_hash = c.get("feature_hash")
    pipeline_hash = c.get("feature_pipeline_hash")
    print(f"Selected Candidate Version: {version}")
    print(f"Pipeline Hash: {pipeline_hash}")
    
    m_path = os.path.join(MODELS_DIR, f"model_{ticker}_{version}.joblib")
    f_path = os.path.join(FEATURES_DIR, f"features_{ticker}_{version}.json")
    
    print(f"Model Artifact Exists: {os.path.exists(m_path)}")
    print(f"Feature Artifact Exists: {os.path.exists(f_path)}")
    
    try:
        model = joblib.load(m_path)
        print("Model Loadable: YES")
    except Exception as e:
        print(f"Model Loadable: NO ({e})")
        
    try:
        with open(f_path, 'r') as f:
            features = json.load(f)
        print("Feature Artifact Loadable: YES")
    except Exception as e:
        print(f"Feature Artifact Loadable: NO ({e})")
        
    prov = c.get("provenance_status")
    print(f"Provenance Status: {prov}")
    print(f"CV Score: {c.get('metrics', {}).get('optuna', {}).get('best_value')}")
    print(f"F1 Macro: {c.get('metrics', {}).get('f1_macro')}")
    print(f"Dataset Row Count: {c.get('dataset_row_count')}")
    print(f"Training Cutoff: {c.get('dataset_date_end')}")

audit_ticker("RELIANCE.NS")
audit_ticker("ADANIENT.NS")
