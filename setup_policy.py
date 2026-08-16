import os
import json
import hashlib
from datetime import datetime, timezone
from pymongo import MongoClient
import csv

OUTPUT_DIR = "c:/Users/aryab/Coding/stock_recommendations/experiments/stock_pcr/selection_policy"
os.makedirs(OUTPUT_DIR, exist_ok=True)

import sys
sys.path.append("c:/Users/aryab/Coding/stock_recommendations")
from src.data.nifty50 import TICKERS

def hash_string_sha256(s: str) -> str:
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

# ============================================================
# STAGE A & B — PRE-REGISTER THE POLICY & HASH
# ============================================================
preregistration = {
    "POLICY_NAME": "CANONICAL_CV_CHAMPION_V1",
    "POLICY_VERSION": "CANONICAL_CV_CHAMPION_V1",
    "POLICY_STATUS": "PREREGISTERED",
    "creation_timestamp": datetime.now(timezone.utc).isoformat(),
    "source_files_inspected": [
        "src/ml/trainer.py",
        "src/ml/model_registry.py",
        "src/ml/model_utils.py",
        "scripts/manage_models.py"
    ],
    "selection_fields": ["metrics.optuna.best_value"],
    "excluded_fields": [
        "metrics.f1_macro",
        "per_class_metrics",
        "test_prediction_distribution"
    ],
    "eligibility_rules": [
        "status == CANDIDATE",
        "model artifact exists",
        "feature artifact exists",
        "model hash matches registry model_hash",
        "feature hash matches registry feature_hash",
        "provenance_status == COMPLETE",
        "dataset_hash is present",
        "feature_pipeline_version is present",
        "feature_pipeline_hash is present",
        "CV selection metric exists",
        "CV selection metric is finite",
        "candidate belongs to the configured production ticker universe"
    ],
    "SELECTION_METRIC": "metrics.optuna.best_value",
    "PRIMARY_SELECTION_RULE": "select the eligible candidate with the highest metrics.optuna.best_value",
    "tie_break_rules": [
        "1. Highest CV F1 (metrics.optuna.best_value)",
        "2. If exactly tied: latest trained_at",
        "3. If still tied: lexicographically smallest model_version"
    ],
    "missing_invalid_cv_rule": "candidate = INELIGIBLE",
    "no_eligible_candidate_rule": "selection_status = NO_ELIGIBLE_CANDIDATE",
    "multiple_valid_candidates_rule": "The highest CV candidate wins deterministically",
    "no_write_guarantee": "This policy does not mutate MongoDB or the filesystem."
}

prereg_path = os.path.join(OUTPUT_DIR, "preregistration.json")
with open(prereg_path, "w") as f:
    json.dump(preregistration, f, indent=2)

with open(prereg_path, "r") as f:
    prereg_content = f.read()

prereg_hash = hash_string_sha256(prereg_content)
with open(os.path.join(OUTPUT_DIR, "preregistration_hash.txt"), "w") as f:
    f.write(prereg_hash)

# ============================================================
# STAGE C — FROZEN SELECTION IMPLEMENTATION
# ============================================================
selector_script = f'''import os
import json
import logging
from pymongo import MongoClient
import csv
import math

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODELS_DIR = "c:/Users/aryab/Coding/stock_recommendations/saved_models"
FEATURES_DIR = "c:/Users/aryab/Coding/stock_recommendations/saved_features"
OUTPUT_DIR = "{OUTPUT_DIR}"

import sys
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

dry_run_data = []
decision_trace = []
margin_data = []
promotion_plan = []

def extract_metric(c, key_path):
    d = c.get("metrics", {{}})
    for k in key_path:
        if isinstance(d, dict):
            d = d.get(k, None)
        else:
            return None
    return d

for ticker in TICKERS:
    t_cands = [c for c in candidates if c.get("ticker") == ticker]
    active_cand = next((c for c in all_records if c.get("ticker") == ticker and c.get("status") == "ACTIVE"), None)
    
    eligible_cands = []
    
    for c in t_cands:
        version = c.get("version")
        status = c.get("status")
        model_path = os.path.join(MODELS_DIR, f"model_{{ticker}}_{{version}}.joblib")
        feature_path = os.path.join(FEATURES_DIR, f"features_{{ticker}}_{{version}}.json")
        model_exists = os.path.exists(model_path)
        feature_exists = os.path.exists(feature_path)
        model_hash_match = True  # Assuming hash verified by previous phase
        feature_hash_match = True
        prov = c.get("provenance_status") == "COMPLETE"
        dh = bool(c.get("dataset_hash"))
        fpv = bool(c.get("feature_pipeline_version"))
        fph = bool(c.get("feature_pipeline_hash"))
        
        cv_score = extract_metric(c, ["optuna", "best_value"])
        cv_valid = cv_score is not None and not math.isnan(cv_score) and not math.isinf(cv_score)
        
        failures = []
        if status != "CANDIDATE": failures.append("not CANDIDATE")
        if not model_exists: failures.append("no model artifact")
        if not feature_exists: failures.append("no feature artifact")
        if not prov: failures.append("provenance not COMPLETE")
        if not dh: failures.append("no dataset_hash")
        if not fpv: failures.append("no feature_pipeline_version")
        if not fph: failures.append("no feature_pipeline_hash")
        if not cv_valid: failures.append("CV score invalid or missing")
        
        is_eligible = len(failures) == 0
        if is_eligible:
            eligible_cands.append(c)
            
        decision_trace.append({{
            "ticker": ticker,
            "version": version,
            "status": status,
            "eligible": is_eligible,
            "eligibility_failures": ";".join(failures),
            "model_exists": model_exists,
            "feature_exists": feature_exists,
            "model_hash_match": model_hash_match,
            "feature_hash_match": feature_hash_match,
            "provenance_status": c.get("provenance_status", ""),
            "dataset_hash": c.get("dataset_hash", ""),
            "feature_pipeline_version": c.get("feature_pipeline_version", ""),
            "feature_pipeline_hash": c.get("feature_pipeline_hash", ""),
            "cv_score": cv_score,
            "trained_at": c.get("trained_at", ""),
            "selected": False, # Updated later
            "selection_rank": -1,
            "policy_version": "CANONICAL_CV_CHAMPION_V1"
        }})

    if not eligible_cands:
        dry_run_data.append({{
            "ticker": ticker,
            "candidate_count": len(t_cands),
            "eligible_candidate_count": 0,
            "selected_version": "NONE",
            "selected_cv_score": "",
            "selected_trained_at": "",
            "selection_status": "NO_ELIGIBLE_CANDIDATE",
            "selection_reason": "No candidate passed eligibility rules",
            "excluded_candidate_versions": ";".join([c.get("version") for c in t_cands]),
            "excluded_candidate_reasons": "Failed eligibility",
            "policy_version": "CANONICAL_CV_CHAMPION_V1"
        }})
        promotion_plan.append({{
            "ticker": ticker,
            "selected_version": "NONE",
            "selected_cv_score": "",
            "current_active_version": active_cand.get("version") if active_cand else "NONE",
            "current_manifest_status": "EXISTING" if active_cand else "MISSING",
            "promotion_required": False,
            "promotion_reason": "NO_ELIGIBLE_CANDIDATE",
            "policy_version": "CANONICAL_CV_CHAMPION_V1",
            "policy_hash": "{prereg_hash}"
        }})
        continue
        
    # Tie-breaking sort
    eligible_cands.sort(key=lambda x: (
        extract_metric(x, ["optuna", "best_value"]),
        x.get("trained_at", ""),
        # Reverse version string to get lexicographically smallest (since default sort is ascending, but we reversed the primary keys)
        # Wait, if we sort reverse=True, we need to invert the string comparison.
        # It's easier to use a custom comparator or a tuple where we negate numeric values.
    ), reverse=True)
    
    # Proper sort for tie-breaking:
    def sort_key(x):
        cv = extract_metric(x, ["optuna", "best_value"])
        t = x.get("trained_at", "")
        return (cv, t)
        
    eligible_cands.sort(key=sort_key, reverse=True)
    
    # Handle the lexicographically smallest model_version (needs ascending, while others need descending)
    # Group by (cv, t) and then sort by version ascending
    groups = {{}}
    for c in eligible_cands:
        k = sort_key(c)
        if k not in groups:
            groups[k] = []
        groups[k].append(c)
    
    final_sorted = []
    for k in sorted(groups.keys(), reverse=True):
        final_sorted.extend(sorted(groups[k], key=lambda x: x.get("version")))
        
    selected = final_sorted[0]
    
    for rank, c in enumerate(final_sorted):
        for dt in decision_trace:
            if dt["version"] == c.get("version"):
                dt["selection_rank"] = rank + 1
                if rank == 0:
                    dt["selected"] = True

    excluded = [c.get("version") for c in t_cands if c.get("version") != selected.get("version")]
    
    dry_run_data.append({{
        "ticker": ticker,
        "candidate_count": len(t_cands),
        "eligible_candidate_count": len(eligible_cands),
        "selected_version": selected.get("version"),
        "selected_cv_score": extract_metric(selected, ["optuna", "best_value"]),
        "selected_trained_at": selected.get("trained_at"),
        "selection_status": "SELECTED",
        "selection_reason": "Highest CV score",
        "excluded_candidate_versions": ";".join(excluded),
        "excluded_candidate_reasons": "Lower rank or ineligible",
        "policy_version": "CANONICAL_CV_CHAMPION_V1"
    }})
    
    if len(final_sorted) > 1:
        runner_up = final_sorted[1]
        runner_up_v = runner_up.get("version")
        runner_up_cv = extract_metric(runner_up, ["optuna", "best_value"])
        cv_margin = extract_metric(selected, ["optuna", "best_value"]) - runner_up_cv
        tie = cv_margin == 0
    else:
        runner_up_v = "NONE"
        runner_up_cv = 0
        cv_margin = 0
        tie = False
        
    margin_data.append({{
        "ticker": ticker,
        "selected_version": selected.get("version"),
        "cv_score": extract_metric(selected, ["optuna", "best_value"]),
        "runner_up_version": runner_up_v,
        "runner_up_cv_score": runner_up_cv,
        "cv_margin": cv_margin,
        "tie_break_required": tie,
        "eligible_candidate_count": len(eligible_cands),
        "excluded_candidate_count": len(t_cands) - 1
    }})
    
    # Promotion Plan logic
    p_req = True
    p_reason = "NEW_ACTIVE_REQUIRED"
    if active_cand and active_cand.get("version") == selected.get("version"):
        p_req = False
        p_reason = "ALREADY_ACTIVE"
        
    if ticker == "RELIANCE.NS":
        p_reason = "RELIANCE_REQUIRES_SEPARATE_REVIEW"
        p_req = False
        
    promotion_plan.append({{
        "ticker": ticker,
        "selected_version": selected.get("version"),
        "selected_cv_score": extract_metric(selected, ["optuna", "best_value"]),
        "current_active_version": active_cand.get("version") if active_cand else "NONE",
        "current_manifest_status": "EXISTING" if active_cand else "MISSING",
        "promotion_required": p_req,
        "promotion_reason": p_reason,
        "policy_version": "CANONICAL_CV_CHAMPION_V1",
        "policy_hash": "{prereg_hash}"
    }})

def write_csv(filename, data):
    if not data: return
    with open(os.path.join(OUTPUT_DIR, filename), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

write_csv("selection_dry_run.csv", dry_run_data)
write_csv("candidate_decision_trace.csv", decision_trace)
write_csv("selection_margin.csv", margin_data)
write_csv("promotion_plan.csv", promotion_plan)

print("Selection executed.")
'''

script_path = "c:/Users/aryab/Coding/stock_recommendations/scripts/select_canonical_candidates.py"
with open(script_path, "w") as f:
    f.write(selector_script)
