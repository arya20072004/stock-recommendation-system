import os
import json
import logging
from datetime import datetime
import pymongo
from dotenv import load_dotenv

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

def run_canaries(tickers):
    report = {
        "metadata": {
            "timestamp": datetime.utcnow().isoformat(),
            "models_dir": os.environ["MODELS_DIR"],
            "features_dir": os.environ["FEATURES_DIR"]
        },
        "results": {},
        "overall_classification": ""
    }
    
    canary_gate = "PASS"
    
    for ticker in tickers:
        print(f"--- Processing {ticker} ---")
        
        baseline_metrics_file = f"saved_models/{ticker}_metrics.json"
        if not os.path.exists(baseline_metrics_file):
            print(f"Baseline not found for {ticker}")
            canary_gate = "FAIL"
            continue
            
        baseline_metrics = load_metrics(baseline_metrics_file)
        cutoff = baseline_metrics["data_fingerprint"]["feature_date_max"]
        
        os.environ["TRAINING_CUTOFF_DATE"] = cutoff
        print(f"Executing Candidate A for {ticker} with cutoff {cutoff}...")
        
        try:
            run([ticker])
        except Exception as e:
            print(f"Training failed: {e}")
            canary_gate = "FAIL"
            continue
            
        candidate_metrics = load_metrics(f"{os.environ['MODELS_DIR']}/{ticker}_metrics.json")
        
        # Verify structural counts
        checks = {
            "total_rows_after_features": candidate_metrics["total_rows_after_features"] == baseline_metrics["total_rows_after_features"],
            "train_size": candidate_metrics["train_size"] == baseline_metrics["train_size"],
            "test_size": candidate_metrics["test_size"] == baseline_metrics["test_size"],
            "feature_date_min": candidate_metrics["data_fingerprint"]["feature_date_min"] == baseline_metrics["data_fingerprint"]["feature_date_min"],
            "feature_date_max": candidate_metrics["data_fingerprint"]["feature_date_max"] == baseline_metrics["data_fingerprint"]["feature_date_max"],
        }
        
        if not all(checks.values()):
            print(f"Structural verification failed for {ticker}: {checks}")
            canary_gate = "FAIL"
            
        # Metric comparison
        b_f1 = baseline_metrics["f1_macro"]
        c_f1 = candidate_metrics["f1_macro"]
        delta_f1 = c_f1 - b_f1
        
        classif = "IMPROVED"
        if delta_f1 <= -0.05:
            classif = "SEVERE_REGRESSION"
            canary_gate = "FAIL"
        elif -0.05 < delta_f1 <= -0.02:
            classif = "REGRESSED"
            if canary_gate == "PASS":
                canary_gate = "REVIEW"
        elif -0.02 < delta_f1 < 0.02:
            classif = "ROUGHLY_STABLE"
            
        # Class collapse
        b_buy = baseline_metrics["per_class_metrics"]["BUY"]["recall"]
        c_buy = candidate_metrics["per_class_metrics"]["BUY"]["recall"]
        
        b_sell = baseline_metrics["per_class_metrics"]["SELL"]["recall"]
        c_sell = candidate_metrics["per_class_metrics"]["SELL"]["recall"]
        
        if c_buy == 0.0 or c_sell == 0.0:
            print(f"Class collapse on {ticker}")
            canary_gate = "FAIL"
            
        report["results"][ticker] = {
            "cutoff": cutoff,
            "structural_verification": all(checks.values()),
            "structural_checks": checks,
            "baseline_f1_macro": b_f1,
            "candidate_f1_macro": c_f1,
            "delta_f1": delta_f1,
            "classification": classif,
            "buy_recall": {"baseline": b_buy, "candidate": c_buy},
            "sell_recall": {"baseline": b_sell, "candidate": c_sell},
            "fingerprints": candidate_metrics["data_fingerprint"]
        }
        print(f"{ticker}: {classif} (delta: {delta_f1:.4f})")
        
    report["overall_classification"] = canary_gate
    
    os.makedirs("artifacts/model_ab/reports", exist_ok=True)
    with open("artifacts/model_ab/reports/canary_report.json", "w") as f:
        json.dump(report, f, indent=4)
        
    print(f"\n--- CANARY EXECUTION FINISHED ---")
    print(f"OVERALL RESULT: {canary_gate}")

if __name__ == "__main__":
    run_canaries(["RELIANCE.NS", "TCS.NS", "SBIN.NS", "HDFCBANK.NS", "TITAN.NS"])
