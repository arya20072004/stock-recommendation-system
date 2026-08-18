import os
import sys
import json
import logging
from pymongo import MongoClient
import math

sys.path.append("c:/Users/aryab/Coding/stock_recommendations")
from src.data.nifty50 import TICKERS

def get_db():
    from dotenv import load_dotenv
    load_dotenv("c:/Users/aryab/Coding/stock_recommendations/.env")
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    return client["stock_market_db"]

db = get_db()
all_records = list(db.model_registry.find())
candidates = [r for r in all_records if r.get("status") == "CANDIDATE"]
actives = [r for r in all_records if r.get("status") == "ACTIVE"]

MODELS_DIR = "c:/Users/aryab/Coding/stock_recommendations/saved_models"
FEATURES_DIR = "c:/Users/aryab/Coding/stock_recommendations/saved_features"

EXPECTED_TICKERS = 51
CORRECTED_PIPELINE_HASH = "f4891c1b2172b8e024b176cd221cf33c8b5e006acd567d95f2768cb7bf72384e"

active_legacy_hash_count = 0
active_corrected_hash_count = 0

for a in actives:
    ph = a.get("feature_pipeline_hash")
    if ph == CORRECTED_PIPELINE_HASH:
        active_corrected_hash_count += 1
    else:
        active_legacy_hash_count += 1

def extract_metric(c, key_path):
    d = c.get("metrics", {})
    for k in key_path:
        if isinstance(d, dict):
            d = d.get(k, None)
        else:
            return None
    return d

def sort_key(x):
    cv = extract_metric(x, ["optuna", "best_value"])
    if cv is None or math.isnan(cv) or math.isinf(cv):
        cv = -1.0
    t = x.get("trained_at", "")
    return (cv, t)

results = []
very_low_conf_count = 0

for ticker in TICKERS:
    t_cands = [c for c in candidates if c.get("ticker") == ticker]
    active_cand = next((c for c in actives if c.get("ticker") == ticker), None)
    
    eligible_cands = []
    
    for c in t_cands:
        version = c.get("version")
        status = c.get("status")
        model_path = os.path.join(MODELS_DIR, f"model_{ticker}_{version}.joblib")
        feature_path = os.path.join(FEATURES_DIR, f"features_{ticker}_{version}.json")
        model_exists = os.path.exists(model_path)
        feature_exists = os.path.exists(feature_path)
        
        prov = c.get("provenance_status") == "COMPLETE"
        dh = bool(c.get("dataset_hash"))
        fpv = bool(c.get("feature_pipeline_version"))
        fph = bool(c.get("feature_pipeline_hash"))
        
        cv_score = extract_metric(c, ["optuna", "best_value"])
        cv_valid = cv_score is not None and not math.isnan(cv_score) and not math.isinf(cv_score)
        
        # Only select candidates matching the new corrected hash
        is_corrected = c.get("feature_pipeline_hash") == CORRECTED_PIPELINE_HASH
        
        if c.get("metrics", {}).get("f1_macro", 1.0) < 0.30:
            very_low_conf_count += 1
            
        failures = []
        if status != "CANDIDATE": failures.append("not CANDIDATE")
        if not model_exists: failures.append("no model artifact")
        if not feature_exists: failures.append("no feature artifact")
        if not prov: failures.append("provenance not COMPLETE")
        if not dh: failures.append("no dataset_hash")
        if not fpv: failures.append("no feature_pipeline_version")
        if not fph: failures.append("no feature_pipeline_hash")
        if not cv_valid: failures.append("CV score invalid or missing")
        if not is_corrected: failures.append("WRONG_PIPELINE_HASH")
        
        is_eligible = len(failures) == 0
        
        c["failures"] = failures
        if is_eligible:
            eligible_cands.append(c)

    if not eligible_cands:
        results.append({
            "ticker": ticker,
            "active_version": active_cand.get("version") if active_cand else "NONE",
            "candidate_version": "NONE",
            "active_score": extract_metric(active_cand, ["optuna", "best_value"]) if active_cand else "NONE",
            "candidate_score": "NONE",
            "selection_rule": "CANONICAL_CV_CHAMPION_V1",
            "quality_status": "NONE",
            "confidence_status": "NONE",
            "pipeline_compatibility": "FAIL",
            "dataset_compatibility": "FAIL",
            "promotion_status": "PROMOTION_INELIGIBLE",
            "blocking_reason": ";".join(t_cands[-1]["failures"]) if t_cands else "NO_CANDIDATE"
        })
        continue

    # Sort logic to match canonical script
    groups = {}
    for c in eligible_cands:
        k = sort_key(c)
        if k not in groups: groups[k] = []
        groups[k].append(c)
        
    final_sorted = []
    for k in sorted(groups.keys(), reverse=True):
        final_sorted.extend(sorted(groups[k], key=lambda x: x.get("version")))
        
    selected = final_sorted[0]
    
    cv_score = extract_metric(selected, ["optuna", "best_value"])
    f1 = selected.get("metrics", {}).get("f1_macro", 0.0)
    conf = "VERY_LOW_CONFIDENCE" if f1 < 0.3 else "NORMAL"
    
    promotion_status = "PROMOTION_ELIGIBLE"
    blocking_reason = "NONE"
    
    if ticker == "RELIANCE.NS":
        promotion_status = "PROMOTION_BLOCKED"
        blocking_reason = "RELIANCE_REQUIRES_SEPARATE_REVIEW"
        
    results.append({
        "ticker": ticker,
        "active_version": active_cand.get("version") if active_cand else "NONE",
        "candidate_version": selected.get("version"),
        "active_score": extract_metric(active_cand, ["optuna", "best_value"]) if active_cand else "NONE",
        "candidate_score": cv_score,
        "selection_rule": "CANONICAL_CV_CHAMPION_V1",
        "quality_status": "HIGHEST_CV",
        "confidence_status": conf,
        "pipeline_compatibility": "PASS",
        "dataset_compatibility": "PASS",
        "promotion_status": promotion_status,
        "blocking_reason": blocking_reason
    })

num_eligible = sum(1 for r in results if r["promotion_status"] == "PROMOTION_ELIGIBLE")
num_ineligible = sum(1 for r in results if r["promotion_status"] == "PROMOTION_INELIGIBLE")
num_blocked = sum(1 for r in results if r["promotion_status"] == "PROMOTION_BLOCKED")
num_unresolved = sum(1 for r in results if r["promotion_status"] == "SELECTION_UNRESOLVED")

with open("scratch/diagnostic_output.json", "w") as f:
    json.dump({
        "active_legacy_hash_count": active_legacy_hash_count,
        "active_corrected_hash_count": active_corrected_hash_count,
        "results": results,
        "num_eligible": num_eligible,
        "num_ineligible": num_ineligible,
        "num_blocked": num_blocked,
        "num_unresolved": num_unresolved,
        "very_low_conf_count": very_low_conf_count
    }, f, indent=2)

print("Diagnostic run complete.")
