import os
import json
import logging
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import pymongo
import joblib

# Setup isolated environment before importing trainer
load_dotenv()
os.environ["ENFORCE_SEEDS"] = "1"
os.environ["MODELS_DIR"] = "artifacts/model_ab/candidate_same_cutoff/models"
os.environ["FEATURES_DIR"] = "artifacts/model_ab/candidate_same_cutoff/features"
os.makedirs(os.environ["MODELS_DIR"], exist_ok=True)
os.makedirs(os.environ["FEATURES_DIR"], exist_ok=True)

from src.ml.trainer import run, create_dataset

def load_metrics(filepath):
    with open(filepath) as f:
        return json.load(f)

def run_canary_double_test(ticker):
    print(f"--- Running Reproducibility Control for {ticker} ---")
    
    baseline_metrics_file = f"saved_models/{ticker}_metrics.json"
    if not os.path.exists(baseline_metrics_file):
        raise Exception(f"Baseline metrics not found: {baseline_metrics_file}")
    
    baseline_metrics = load_metrics(baseline_metrics_file)
    cutoff = baseline_metrics["data_fingerprint"]["feature_date_max"]
    print(f"Baseline cutoff: {cutoff}")
    
    os.environ["TRAINING_CUTOFF_DATE"] = cutoff
    
    # Run 1
    print("Executing Run 1...")
    run([ticker])
    run1_metrics = load_metrics(f"{os.environ['MODELS_DIR']}/{ticker}_metrics.json")
    
    # Move models temporarily
    os.rename(f"{os.environ['MODELS_DIR']}/model_{ticker}.joblib", f"{os.environ['MODELS_DIR']}/model_{ticker}_run1.joblib")
    
    # Run 2
    print("Executing Run 2...")
    run([ticker])
    run2_metrics = load_metrics(f"{os.environ['MODELS_DIR']}/{ticker}_metrics.json")
    os.rename(f"{os.environ['MODELS_DIR']}/model_{ticker}.joblib", f"{os.environ['MODELS_DIR']}/model_{ticker}_run2.joblib")
    
    # Compare
    fp1 = run1_metrics["data_fingerprint"]
    fp2 = run2_metrics["data_fingerprint"]
    
    for key in fp1:
        if fp1[key] != fp2[key]:
            raise Exception(f"Fingerprint mismatch on {key}: {fp1[key]} vs {fp2[key]}")
            
    print("Fingerprints match.")
    
    if run1_metrics["f1_macro"] != run2_metrics["f1_macro"]:
        raise Exception(f"Macro F1 mismatch: {run1_metrics['f1_macro']} vs {run2_metrics['f1_macro']}")
    print("Macro F1 match.")
    
    # Probability matrix compare
    client = pymongo.MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
    df = create_dataset(ticker, client)
    
    with open(f"{os.environ['FEATURES_DIR']}/features_{ticker}.json") as f:
        features = json.load(f)
        
    X = df[features]
    split_index = int(len(X) * 0.8)
    X_test = X.iloc[split_index:]
    
    m1 = joblib.load(f"{os.environ['MODELS_DIR']}/model_{ticker}_run1.joblib")
    m2 = joblib.load(f"{os.environ['MODELS_DIR']}/model_{ticker}_run2.joblib")
    
    p1 = m1.predict_proba(X_test)
    p2 = m2.predict_proba(X_test)
    
    diff = np.abs(p1 - p2).max()
    print(f"Max proba diff: {diff}")
    
    if diff > 1e-6:
        raise Exception("Probability matrices differ by more than 1e-6")
        
    print("BEHAVIORAL_REPRODUCIBILITY passed.")
    # Restore model
    os.rename(f"{os.environ['MODELS_DIR']}/model_{ticker}_run1.joblib", f"{os.environ['MODELS_DIR']}/model_{ticker}.joblib")

if __name__ == "__main__":
    run_canary_double_test("RELIANCE.NS")
