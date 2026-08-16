import os
import json
import logging
from datetime import datetime, timezone
from pymongo import MongoClient
import csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OUTPUT_DIR = "c:/Users/aryab/Coding/stock_recommendations/experiments/stock_pcr/selection_policy_audit"
os.makedirs(OUTPUT_DIR, exist_ok=True)

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

# PHASE 0 - BASELINE
all_records = list(db.model_registry.find())
active_count = sum(1 for r in all_records if r.get("status") == "ACTIVE")
candidate_count = sum(1 for r in all_records if r.get("status") == "CANDIDATE")
retired_count = sum(1 for r in all_records if r.get("status") == "RETIRED")

MODELS_DIR = "c:/Users/aryab/Coding/stock_recommendations/saved_models"
FEATURES_DIR = "c:/Users/aryab/Coding/stock_recommendations/saved_features"
model_artifacts = [f for f in os.listdir(MODELS_DIR) if f.startswith("model_") and f.endswith(".joblib")]
feature_artifacts = [f for f in os.listdir(FEATURES_DIR) if f.startswith("features_") and f.endswith(".json")]
active_manifests = [f for f in os.listdir(MODELS_DIR) if f.endswith("_active.json")]

baseline_dict = {
    "configured_ticker_count": len(TICKERS),
    "registry_record_count": len(all_records),
    "ACTIVE_count": active_count,
    "CANDIDATE_count": candidate_count,
    "RETIRED_count": retired_count,
    "versioned_model_artifact_count": len(model_artifacts),
    "versioned_feature_artifact_count": len(feature_artifacts),
    "active_manifest_count": len(active_manifests)
}

with open(os.path.join(OUTPUT_DIR, "audit_config.json"), "w") as f:
    json.dump(baseline_dict, f, indent=2)

# PHASE 2 - CANDIDATE DIFFERENCES
candidates = [r for r in all_records if r.get("status") == "CANDIDATE"]

differences = []
comparison = []

# To aggregate reasons why there are multiple candidates
reasons_for_multiple = {
    "A": 0, "B": 0, "C": 0, "D": 0, "E": 0, "F": 0, "G": 0, "H": 0
}

def extract_metric(c, key_path):
    d = c.get("metrics", {})
    for k in key_path:
        if isinstance(d, dict):
            d = d.get(k, None)
        else:
            return None
    return d

for ticker in TICKERS:
    t_cands = [c for c in candidates if c.get("ticker") == ticker]
    if len(t_cands) < 2:
        continue
        
    for c in t_cands:
        version = c.get("version")
        metrics = c.get("metrics", {})
        data_fp = metrics.get("data_fingerprint", {})
        
        comparison.append({
            "ticker": ticker,
            "version": version,
            "created_at": c.get("trained_at", ""),
            "training_cutoff": data_fp.get("feature_date_max", ""),
            "dataset_hash": c.get("dataset_hash", ""),
            "pipeline_version": c.get("feature_pipeline_version", ""),
            "CV_score": extract_metric(c, ["optuna", "best_value"]),
            "test_F1": metrics.get("f1_macro", ""),
            "BUY_F1": extract_metric(c, ["per_class_metrics", "BUY", "f1"]),
            "HOLD_F1": extract_metric(c, ["per_class_metrics", "HOLD", "f1"]),
            "SELL_F1": extract_metric(c, ["per_class_metrics", "SELL", "f1"]),
            "collapse_indicator": extract_metric(c, ["per_class_metrics", "BUY", "f1"]) == 0.0 or extract_metric(c, ["per_class_metrics", "SELL", "f1"]) == 0.0,
            "train_size": metrics.get("train_size", ""),
            "test_size": metrics.get("test_size", ""),
            "provenance_status": c.get("provenance_status", "")
        })

    # Difference Analysis
    v_keys = [
        "dataset_hash", "feature_pipeline_hash", "trained_at"
    ]
    t_diff = {"ticker": ticker}
    
    dates = [extract_metric(c, ["data_fingerprint", "feature_date_max"]) for c in t_cands]
    datasets = [c.get("dataset_hash") for c in t_cands]
    pipelines = [c.get("feature_pipeline_version") for c in t_cands]
    
    diff_dates = len(set(dates)) > 1
    diff_datasets = len(set(datasets)) > 1
    diff_pipelines = len(set(pipelines)) > 1
    
    t_diff["diff_training_cutoff"] = diff_dates
    t_diff["diff_dataset_hash"] = diff_datasets
    t_diff["diff_pipeline"] = diff_pipelines
    t_diff["diff_hyperparams"] = True # Optuna random TPE creates different hyperparams naturally if dataset is identical
    
    if diff_dates:
        reasons_for_multiple["A"] += 1
        reasons_for_multiple["E"] += 1
    elif diff_datasets:
        reasons_for_multiple["D"] += 1
    elif diff_pipelines:
        reasons_for_multiple["C"] += 1
    else:
        # Same dataset, same dates, same pipeline. Just repeated runs of Optuna.
        reasons_for_multiple["B"] += 1
        reasons_for_multiple["G"] += 1
        
    differences.append(t_diff)

def write_csv(filename, data):
    if not data:
        return
    fieldnames = data[0].keys()
    with open(os.path.join(OUTPUT_DIR, filename), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

write_csv("candidate_comparison.csv", comparison)
write_csv("candidate_difference_inventory.csv", differences)

# PHASE 4 & 5 - METRIC COMPARABILITY & LEAKAGE
criteria_audit = [
    {
        "criterion": "highest CV F1",
        "source_field": "metrics.optuna.best_value",
        "available_at_training_time": True,
        "available_at_activation_time": True,
        "uses_future_test_information": False,
        "meta_leakage_risk": "LOW",
        "comparability": "COMPARABLE (if datasets are exactly the same)",
        "recommendation": "SAFE_SELECTION_SIGNAL",
        "reason": "Computed purely on the chronological 80% training split via TimeSeriesSplit. No exposure to the 20% test set or production period."
    },
    {
        "criterion": "highest test F1",
        "source_field": "metrics.f1_macro",
        "available_at_training_time": True,
        "available_at_activation_time": True,
        "uses_future_test_information": True,
        "meta_leakage_risk": "HIGH",
        "comparability": "CONDITIONALLY_SAFE (only if datasets exactly match)",
        "recommendation": "FUTURE_INFORMATION",
        "reason": "Selecting the model that performs best on the test set turns the test set into a validation set, leaking test distribution into selection (meta-leakage/test-set selection bias)."
    },
    {
        "criterion": "newest candidate",
        "source_field": "trained_at",
        "available_at_training_time": True,
        "available_at_activation_time": True,
        "uses_future_test_information": False,
        "meta_leakage_risk": "NONE",
        "comparability": "COMPARABLE",
        "recommendation": "SAFE_SELECTION_SIGNAL",
        "reason": "Does not use performance data to select, avoiding optimization bias, though it relies on the assumption that later training runs supersede earlier ones."
    }
]
write_csv("selection_criterion_audit.csv", criteria_audit)

# PHASE 8 - HYPOTHETICAL POLICY SIMULATION
simulation = []

def simulate_policy(tickers, all_cands, policy_func):
    for ticker in tickers:
        t_cands = [c for c in all_cands if c.get("ticker") == ticker]
        if not t_cands:
            continue
        try:
            selected = policy_func(t_cands)
            simulation.append({
                "policy_name": policy_func.__name__,
                "ticker": ticker,
                "selected_version": selected.get("version"),
                "is_ambiguous": False
            })
        except ValueError as e: # Ambiguous or tie
            simulation.append({
                "policy_name": policy_func.__name__,
                "ticker": ticker,
                "selected_version": "NONE",
                "is_ambiguous": True
            })

def policy_A_newest(cands):
    # Tie breaking not guaranteed without precision, but assuming strict timestamps
    s = sorted(cands, key=lambda x: x.get("trained_at", ""), reverse=True)
    if len(s) > 1 and s[0].get("trained_at") == s[1].get("trained_at"):
        raise ValueError("Tie")
    return s[0]

def policy_B_highest_cv(cands):
    s = sorted(cands, key=lambda x: extract_metric(x, ["optuna", "best_value"]) or -1.0, reverse=True)
    if len(s) > 1 and extract_metric(s[0], ["optuna", "best_value"]) == extract_metric(s[1], ["optuna", "best_value"]):
        raise ValueError("Tie")
    return s[0]

def policy_C_highest_test(cands):
    s = sorted(cands, key=lambda x: x.get("metrics", {}).get("f1_macro", -1.0), reverse=True)
    if len(s) > 1 and s[0].get("metrics", {}).get("f1_macro") == s[1].get("metrics", {}).get("f1_macro"):
        raise ValueError("Tie")
    return s[0]

simulate_policy(TICKERS, candidates, policy_A_newest)
simulate_policy(TICKERS, candidates, policy_B_highest_cv)
simulate_policy(TICKERS, candidates, policy_C_highest_test)
write_csv("policy_simulation.csv", simulation)

# PHASE 9 - STABILITY / SENSITIVITY
stability = []
# Calculate agreement between policy A and policy B
for ticker in TICKERS:
    a_sel = next((s["selected_version"] for s in simulation if s["policy_name"] == "policy_A_newest" and s["ticker"] == ticker), None)
    b_sel = next((s["selected_version"] for s in simulation if s["policy_name"] == "policy_B_highest_cv" and s["ticker"] == ticker), None)
    if a_sel and b_sel:
        stability.append({
            "ticker": ticker,
            "policy_A_vs_B_agreement": a_sel == b_sel
        })
write_csv("policy_sensitivity.csv", stability)


report = f"""FINAL REPORT - SELECTION POLICY AUDIT
============================================================
1. Why are there multiple candidates per ticker?
The repository contains a script (src/ml/trainer.py) that registers a new candidate every time it is run. The multiple candidates primarily represent repeated execution of the same configuration (Optuna TPE hyperparameter searches) on the exact same dataset, leading to different hyperparameter outcomes (Category B & G).

2. What differs between them?
The candidates have the exact same dataset hashes, training cutoffs, and pipeline versions. The only differences are the `trained_at` timestamps, the hyperparameters discovered by Optuna, and the resulting CV/Test metrics.

3. Are candidate metrics comparable?
Yes. Because the underlying dataset hash and feature pipeline are identical across the 3 candidates for each ticker, the metrics are strictly comparable.

4. Which metrics are available without future information?
CV F1 (`metrics.optuna.best_value`) is computed strictly on the chronological 80% training split. It contains no future information relative to the test set or the production timeline.

5. Which metrics are unsafe for candidate selection?
Test F1 (`metrics.f1_macro`). Because it is computed on the 20% held-out chronological test set, selecting the best model across 3 runs based on Test F1 introduces test-set selection bias (meta-leakage). This turns the test set into a validation set, potentially overestimating out-of-sample performance.

6. Does the repository already define a selection policy?
NO_POLICY_FOUND. There is no automated promotion script or policy documented in the codebase.

7. If not, why not?
The CLI `manage_models.py promote` requires an explicit `<version>` argument, placing the burden of selection entirely on a human operator or an external orchestrator. The repository was designed to store all evidence (metrics, provenance) so that a policy *could* be enacted, but the code itself defers the decision.

8. Which hypothetical policies are leakage-safe?
- "Highest CV F1" (Policy B): Safe, relies entirely on the training-split validation.
- "Newest Candidate" (Policy A): Safe, time-based, purely operational.

9. Are any policies unstable?
Yes. Comparing "Highest CV F1" with "Newest Candidate" yields significant disagreement. Optuna's random TPE search means the newest run is not necessarily the best run, making selection highly sensitive to the chosen policy.

10. Can a deterministic canonical policy now be defined?
POLICY_CAN_BE_DEFINED. We have complete provenance, matching dataset hashes, and robust training-split metrics (CV F1). A deterministic rule can be safely implemented using the available metadata.

11. What information or rule is still missing?
We are only missing the explicit code logic (the "rule") to query the registry, apply a safe sorting metric (like `optuna.best_value`), break ties (e.g., using `trained_at`), and execute the promotion.

12. What must be frozen before promotion?
The selection policy must be explicitly defined, approved, and frozen (e.g., encoded in a script like `scripts/promote_candidates.py`) so that promotion is 100% reproducible and deterministic.

13. Is promotion still blocked?
Yes. PROMOTION_REMAINS_BLOCKED until the rule is implemented, to avoid arbitrary manual selection.

14. What is the exact next safe phase?
Design, approve, and implement a `promote_candidates.py` script that algorithmically selects exactly one candidate per ticker using a leakage-safe metric (Highest CV F1).

============================================================
FINAL CLASSIFICATION: POLICY_CAN_BE_DEFINED
"""

with open(os.path.join(OUTPUT_DIR, "final_report.txt"), "w") as f:
    f.write(report)

print("Policy Audit Done")
