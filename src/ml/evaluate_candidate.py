"""
evaluate_candidate.py

Candidate Evaluator Gate.
Strictly read-only evidence generator for Active vs Candidate models
using the immutable frozen evaluation dataset.
"""

import os
import glob
import json
import argparse
import hashlib
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import joblib

from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix
from scipy.stats import chi2

from src.data.nifty50 import TICKERS
from src.features.router import resolve_feature_pipeline, get_feature_pipeline_hash
from src.ml.confidence import compute_confidence_tier

MODELS_DIR = "saved_models"
FEATURES_DIR = "saved_features"
EVALUATIONS_DIR = "saved_evaluations"

def load_manifest(manifest_path):
    with open(manifest_path, "r", encoding="utf-8") as f:
        return json.load(f)

def select_current_candidate(db, ticker, cutoff):
    """
    Finds the current generation candidate model for a ticker using the authoritative MongoDB registry.
    Requires exact match on ticker, status=CANDIDATE, and dataset_date_end=cutoff.
    """
    docs = list(db.model_registry.find({
        "ticker": ticker,
        "status": "CANDIDATE",
        "dataset_date_end": cutoff
    }))
    
    if len(docs) == 0:
        return None
    elif len(docs) > 1:
        raise ValueError(f"DUPLICATE CANDIDATES: Found {len(docs)} current-generation candidates for {ticker}")
        
    doc = docs[0]
    return doc.get("model_hash") or doc.get("version")

def load_frozen_dataset():
    EXPECTED_DATASET_HASH = "b4c8b5075e70"
    
    metadata_path = os.path.join(EVALUATIONS_DIR, f"metadata_v1_{EXPECTED_DATASET_HASH}.json")
    if not os.path.exists(metadata_path):
        raise FileNotFoundError(f"Missing canonical metadata manifest: {metadata_path}")
        
    metadata = load_manifest(metadata_path)
    
    if not metadata.get("evaluation_dataset_hash", "").startswith(EXPECTED_DATASET_HASH):
        raise ValueError("Metadata identity mismatch")
        
    if metadata.get("evaluation_start_date") != "2025-08-25" or metadata.get("evaluation_end_date") != "2026-08-05":
        raise ValueError("Unexpected evaluation date range in metadata")
        
    dataset_path = os.path.join(EVALUATIONS_DIR, f"eval_dataset_v1_{EXPECTED_DATASET_HASH}.parquet")
    if not os.path.exists(dataset_path):
        raise FileNotFoundError(f"Missing canonical parquet dataset: {dataset_path}")
        
    df = pd.read_parquet(dataset_path)
    
    if "target" in df.columns and df["target"].isna().any():
        raise ValueError("Dataset is not fully mature")
        
    if "future_return" in df.columns and df["future_return"].isna().any():
        raise ValueError("Dataset is not fully mature")
        
    return df, metadata

def generate_report(ticker, active_info, candidate_info, metadata, active_metrics, candidate_metrics, stats, economic, robustness):
    
    # Decide verdict based on simple heuristic (for evidence only)
    # The actual promotion is NOT done here.
    verdict = "INCONCLUSIVE"
    explanation = "Pending human review."
    
    if candidate_metrics["actionable_count"] == 0:
        verdict = "FAIL"
        explanation = "Candidate produced ZERO actionable predictions."

    elif candidate_metrics["actionable_precision"] < active_metrics["actionable_precision"]:
        verdict = "FAIL"
        explanation = "Candidate actionable precision is below the Active model."

    elif economic["candidate_return"] < economic["active_return"]:
        verdict = "FAIL"
        explanation = "Candidate simulated actionable cumulative return is below the Active model."

    else:
        # Mandatory Policy B gate passed.
        # Further evidence determines whether the result is PASS or INCONCLUSIVE.
        if (
            candidate_metrics["actionable_precision"] > active_metrics["actionable_precision"]
            and economic["candidate_return"] > economic["active_return"]
            and stats["mcnemar_pvalue"] is not None
            and stats["mcnemar_pvalue"] < 0.05
        ):
            verdict = "PASS"
            explanation = (
                "Candidate statistically and economically outperforms "
                "Active model on actionable precision."
            )
        else:
            verdict = "INCONCLUSIVE"
            explanation = (
                "No mandatory Policy B failure condition was triggered, "
                "but remaining evidence is insufficient for a definitive PASS."
            )

    report = f"""# Candidate Evaluation Report: {ticker}

## 1. Identity
- **Ticker**: {ticker}
- **Active Model Hash**: `{active_info.get('model_version', active_info.get('model_hash'))}`
- **Candidate Model Hash**: `{candidate_info.get('model_hash', candidate_info.get('model_version'))}`
- **Evaluation Timestamp**: {datetime.now(timezone.utc).isoformat()}

## 2. Dataset
- **Evaluation Dataset Version**: `{metadata.get('evaluation_dataset_version', 'UNKNOWN')}`
- **Evaluation Dataset Hash**: `{metadata.get('evaluation_dataset_hash', 'UNKNOWN')}`
- **Evaluation Start Date**: {metadata.get('evaluation_start_date', 'UNKNOWN')}
- **Evaluation End Date**: {metadata.get('evaluation_end_date', 'UNKNOWN')}
- **Evaluation Rows (Ticker)**: {stats['total_rows']}

## 3. Contract
- **Prediction Horizon**: 10 sessions
- **Active Feature Pipeline Hash**: `{active_info['feature_pipeline_hash']}`
- **Candidate Feature Pipeline Hash**: `{candidate_info['feature_pipeline_hash']}`
- **Feature Schema Compatibility**: PASS

## 4. ML Metrics (Actionable Signals Only)
| Metric | Active | Candidate | Delta |
|--------|--------|-----------|-------|
| Actionable Precision | {active_metrics['actionable_precision']:.4f} | {candidate_metrics['actionable_precision']:.4f} | {(candidate_metrics['actionable_precision'] - active_metrics['actionable_precision']):.4f} |
| Actionable Recall | {active_metrics['actionable_recall']:.4f} | {candidate_metrics['actionable_recall']:.4f} | {(candidate_metrics['actionable_recall'] - active_metrics['actionable_recall']):.4f} |
| Actionable Count | {active_metrics['actionable_count']} | {candidate_metrics['actionable_count']} | {candidate_metrics['actionable_count'] - active_metrics['actionable_count']} |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: {'APPLICABLE' if stats['mcnemar_pvalue'] is not None else 'NOT APPLICABLE'}
- **Test Statistic**: {stats['mcnemar_statistic']}
- **p-value**: {stats['mcnemar_pvalue']}
- **Actionable Sample Size (Intersection)**: {stats['actionable_intersection']}
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: {economic['active_return']*100:.2f}%
- **Candidate Return**: {economic['candidate_return']*100:.2f}%
- **Delta**: {(economic['candidate_return'] - economic['active_return'])*100:.2f}%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {active_metrics['class_distribution']}
- **Candidate Class Distribution**: {candidate_metrics['class_distribution']}

## 8. Final Decision
### **Verdict**: {verdict}
**Explanation**: {explanation}

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
"""
    
    report_path = os.path.join(EVALUATIONS_DIR, f"evaluation_report_{ticker}_{candidate_info.get('model_hash', candidate_info.get('model_version'))}.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
        
    print(f"[{ticker}] Evaluation Complete -> {verdict}")
    return verdict

def evaluate_ticker(db, ticker, df_eval, metadata, cutoff):
    active_manifest_path = os.path.join(MODELS_DIR, f"{ticker}_active.json")
    if not os.path.exists(active_manifest_path):
        print(f"[{ticker}] Active manifest missing.")
        return None
        
    active_info = load_manifest(active_manifest_path)
    active_version = active_info.get("model_version")
    
    candidate_version = select_current_candidate(db, ticker, cutoff)
    if not candidate_version:
        print(f"[{ticker}] No candidate version found.")
        return None
        
    candidate_metrics_path = os.path.join(MODELS_DIR, f"metrics_{ticker}_{candidate_version}.json")
    candidate_info = load_manifest(candidate_metrics_path)
    
    # 4. Feature Compatibility
    if active_info["feature_pipeline_hash"] != candidate_info["feature_pipeline_hash"]:
        print(f"[{ticker}] INELIGIBLE: Feature Pipeline Schema Mismatch.")
        return "INELIGIBLE"
        
    # Load Models & Features
    with open(os.path.join(FEATURES_DIR, f"features_{ticker}_{active_version}.json"), "r") as f:
        active_features = json.load(f)
        
    with open(os.path.join(FEATURES_DIR, f"features_{ticker}_{candidate_version}.json"), "r") as f:
        candidate_features = json.load(f)
        
    # Check dataset has all required columns
    missing_active = [f for f in active_features if f not in df_eval.columns]
    missing_candidate = [f for f in candidate_features if f not in df_eval.columns]
    if missing_active or missing_candidate:
        print(f"[{ticker}] INELIGIBLE: Missing features in frozen dataset.")
        return "INELIGIBLE"
        
    active_model = joblib.load(os.path.join(MODELS_DIR, f"model_{ticker}_{active_version}.joblib"))
    candidate_model = joblib.load(os.path.join(MODELS_DIR, f"model_{ticker}_{candidate_version}.joblib"))
    
    # Pipeline for thresholds
    pipeline_version = active_info.get("feature_pipeline_version", "v1")
    eng = resolve_feature_pipeline(pipeline_version)
    
    # Data slices
    ticker_df = df_eval[df_eval["ticker"] == ticker].copy()
    if ticker_df.empty:
        print(f"[{ticker}] No rows in evaluation dataset.")
        return "INCONCLUSIVE"
        
    # Inference
    X_active = ticker_df[active_features].values
    X_candidate = ticker_df[candidate_features].values
    y_true = ticker_df["target"].values
    future_returns = ticker_df["future_return"].values
    
    thresholds = eng.TICKER_CLASS_THRESHOLDS.get(ticker)
    
    # Active Predictions
    active_proba = active_model.predict_proba(X_active)
    active_preds = np.array([eng.apply_threshold_calibration(p, thresholds) for p in active_proba])
    active_max_prob = active_proba.max(axis=1)
    active_sorted = np.sort(active_proba, axis=1)[:, ::-1]
    active_margin = active_sorted[:, 0] - active_sorted[:, 1]
    
    # Candidate Predictions
    candidate_proba = candidate_model.predict_proba(X_candidate)
    candidate_preds = np.array([eng.apply_threshold_calibration(p, thresholds) for p in candidate_proba])
    candidate_max_prob = candidate_proba.max(axis=1)
    candidate_sorted = np.sort(candidate_proba, axis=1)[:, ::-1]
    candidate_margin = candidate_sorted[:, 0] - candidate_sorted[:, 1]
    
    active_metrics = {"actionable_count": 0, "actionable_correct": 0, "class_distribution": {0:0, 1:0, 2:0}}
    candidate_metrics = {"actionable_count": 0, "actionable_correct": 0, "class_distribution": {0:0, 1:0, 2:0}}
    
    economic = {"active_return": 0.0, "candidate_return": 0.0}
    
    paired_outcomes = []
    
    # Evaluate Actionability
    for i in range(len(ticker_df)):
        true_label = y_true[i]
        ret = future_returns[i]
        
        # Active Actionable
        a_pred = active_preds[i]
        active_metrics["class_distribution"][a_pred] += 1
        a_conf = compute_confidence_tier(ticker, active_max_prob[i], active_margin[i], active_info.get("f1_macro", 0.0))
        a_is_actionable = (a_conf["tier"] in ["MEDIUM", "HIGH", "VERY_HIGH"]) and a_pred in [0, 2]
        a_correct = False
        
        if a_is_actionable:
            active_metrics["actionable_count"] += 1
            if a_pred == true_label:
                active_metrics["actionable_correct"] += 1
                a_correct = True
                
            # Economic validation (PnL)
            if a_pred == 2: # BUY
                economic["active_return"] += ret
            elif a_pred == 0: # SELL
                economic["active_return"] -= ret
                
        # Candidate Actionable
        c_pred = candidate_preds[i]
        candidate_metrics["class_distribution"][c_pred] += 1
        c_conf = compute_confidence_tier(ticker, candidate_max_prob[i], candidate_margin[i], candidate_info.get("f1_macro", 0.0))
        c_is_actionable = (c_conf["tier"] in ["MEDIUM", "HIGH", "VERY_HIGH"]) and c_pred in [0, 2]
        c_correct = False
        
        if c_is_actionable:
            candidate_metrics["actionable_count"] += 1
            if c_pred == true_label:
                candidate_metrics["actionable_correct"] += 1
                c_correct = True
                
            if c_pred == 2:
                economic["candidate_return"] += ret
            elif c_pred == 0:
                economic["candidate_return"] -= ret
                
        # Paired outcome for McNemar's
        if a_is_actionable and c_is_actionable:
            paired_outcomes.append((a_correct, c_correct))
            
    # Calculate Precision
    active_metrics["actionable_precision"] = (active_metrics["actionable_correct"] / active_metrics["actionable_count"]) if active_metrics["actionable_count"] > 0 else 0.0
    candidate_metrics["actionable_precision"] = (candidate_metrics["actionable_correct"] / candidate_metrics["actionable_count"]) if candidate_metrics["actionable_count"] > 0 else 0.0
    
    # Calculate Actionable Recall (out of ALL actual BUY/SELLs)
    actual_actionable = np.sum((y_true == 0) | (y_true == 2))
    active_metrics["actionable_recall"] = active_metrics["actionable_correct"] / actual_actionable if actual_actionable > 0 else 0.0
    candidate_metrics["actionable_recall"] = candidate_metrics["actionable_correct"] / actual_actionable if actual_actionable > 0 else 0.0
    
    # McNemar's Test
    stats = {
        "total_rows": len(ticker_df),
        "actionable_intersection": len(paired_outcomes),
        "mcnemar_statistic": None,
        "mcnemar_pvalue": None
    }
    
    if len(paired_outcomes) > 20: # Require minimum sample size for McNemar's
        b = sum(1 for a, c in paired_outcomes if a and not c) # Active correct, Candidate wrong
        c = sum(1 for a, c in paired_outcomes if not a and c) # Active wrong, Candidate correct
        
        try:
            # McNemar with continuity correction
            statistic = (abs(b - c) - 1)**2 / (b + c) if (b + c) > 0 else 0
            pvalue = chi2.sf(statistic, 1)
            stats["mcnemar_statistic"] = float(statistic)
            stats["mcnemar_pvalue"] = float(pvalue)
        except Exception:
            pass

    return generate_report(ticker, active_info, candidate_info, metadata, active_metrics, candidate_metrics, stats, economic, robustness={})

def main():
    parser = argparse.ArgumentParser(description="Candidate Model Evaluator Gate")
    parser.add_argument("--ticker", type=str, help="Evaluate a specific ticker")
    parser.add_argument("--all", action="store_true", help="Evaluate all 51 configured tickers")
    args = parser.parse_args()
    
    try:
        from dotenv import load_dotenv
        from pymongo import MongoClient
        load_dotenv()
        uri = os.environ.get('MONGO_URI')
        client = MongoClient(uri)
        db = client.stock_market_db
    except Exception as e:
        print(f"EVALUATION BLOCKED: Failed to connect to MongoDB - {e}")
        return

    cutoff = "2025-08-07 00:00:00"
    
    try:
        df_eval, metadata = load_frozen_dataset()
    except Exception as e:
        print(f"EVALUATION BLOCKED: Failed to load frozen dataset - {e}")
        return
        
    os.makedirs(EVALUATIONS_DIR, exist_ok=True)
    
    if args.all:
        for ticker in TICKERS:
            evaluate_ticker(db, ticker, df_eval, metadata, cutoff)
    elif args.ticker:
        evaluate_ticker(db, args.ticker, df_eval, metadata, cutoff)
    else:
        print("Specify --ticker <TICKER> or --all")

if __name__ == "__main__":
    main()
