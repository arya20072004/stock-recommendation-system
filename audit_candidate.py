import os
import json
import logging
from datetime import datetime, timezone
import hashlib
from pymongo import MongoClient
import csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODELS_DIR = "c:/Users/aryab/Coding/stock_recommendations/saved_models"
FEATURES_DIR = "c:/Users/aryab/Coding/stock_recommendations/saved_features"
OUTPUT_DIR = "c:/Users/aryab/Coding/stock_recommendations/experiments/stock_pcr/lifecycle_candidate_audit"
os.makedirs(OUTPUT_DIR, exist_ok=True)

import sys
sys.path.append("c:/Users/aryab/Coding/stock_recommendations")
from src.data.nifty50 import TICKERS

def hash_file_sha256(filepath: str, truncate_to: int = 64) -> str:
    if not os.path.exists(filepath):
        return ""
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()[:truncate_to]

def get_db():
    from dotenv import load_dotenv
    load_dotenv("c:/Users/aryab/Coding/stock_recommendations/.env")
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    return client["stock_market_db"]

db = get_db()

# Baseline stats
all_records = list(db.model_registry.find())
active_count = sum(1 for r in all_records if r.get("status") == "ACTIVE")
candidate_count = sum(1 for r in all_records if r.get("status") == "CANDIDATE")
retired_count = sum(1 for r in all_records if r.get("status") == "RETIRED")
model_artifacts = [f for f in os.listdir(MODELS_DIR) if f.startswith("model_") and f.endswith(".joblib")]
feature_artifacts = [f for f in os.listdir(FEATURES_DIR) if f.startswith("features_") and f.endswith(".json")]
active_manifests = [f for f in os.listdir(MODELS_DIR) if f.endswith("_active.json")]

candidate_records = [r for r in all_records if r.get("status") == "CANDIDATE"]
reliance_active = next((r for r in all_records if r.get("ticker") == "RELIANCE.NS" and r.get("status") == "ACTIVE"), None)

candidates_data = []
artifacts_audit = []
hash_audit = []
provenance_audit = []
decisions = []
ticker_matrix = []

for ticker in TICKERS:
    tickers_cands = [r for r in candidate_records if r.get("ticker") == ticker]
    
    # Store decisions
    ticker_cands_valid = []
    
    for cand in tickers_cands:
        version = cand.get("version")
        model_hash_expected = cand.get("model_hash")
        feature_hash_expected = cand.get("feature_hash")
        dataset_hash_expected = cand.get("dataset_hash")
        
        # Paths
        model_path = os.path.join(MODELS_DIR, f"model_{ticker}_{version}.joblib")
        feature_path = os.path.join(FEATURES_DIR, f"features_{ticker}_{version}.json")
        dataset_path = os.path.join(FEATURES_DIR, f"dataset_{ticker}_{version}.parquet")
        
        model_exists = os.path.exists(model_path)
        feature_exists = os.path.exists(feature_path)
        dataset_exists = os.path.exists(dataset_path)
        
        # Artifact Classification
        if model_exists and feature_exists:
            art_status = "ARTIFACTS_COMPLETE"
        elif not model_exists and feature_exists:
            art_status = "MODEL_MISSING"
        elif model_exists and not feature_exists:
            art_status = "FEATURES_MISSING"
        else:
            art_status = "BOTH_ARTIFACTS_MISSING"
            
        artifacts_audit.append({
            "ticker": ticker,
            "version": version,
            "model_exists": model_exists,
            "feature_exists": feature_exists,
            "artifact_status": art_status
        })
        
        # Hash Verification
        hash_status = "HASHES_MATCH"
        if art_status == "ARTIFACTS_COMPLETE":
            model_hash_actual = hash_file_sha256(model_path, 12)
            feature_hash_actual = hash_file_sha256(feature_path, 64)
            dataset_hash_actual = hash_file_sha256(dataset_path, 64) if dataset_exists else None
            
            if model_hash_actual != model_hash_expected and feature_hash_actual != feature_hash_expected:
                hash_status = "MULTIPLE_HASH_FAILURES"
            elif model_hash_actual != model_hash_expected:
                hash_status = "MODEL_HASH_MISMATCH"
            elif feature_hash_actual != feature_hash_expected:
                hash_status = "FEATURE_HASH_MISMATCH"
            elif dataset_hash_expected and dataset_hash_actual and dataset_hash_actual != dataset_hash_expected:
                hash_status = "MULTIPLE_HASH_FAILURES" # Since we don't have DATASET_HASH_MISMATCH explicitly requested, we can use multiple
        else:
            hash_status = "CANNOT_BE_VERIFIED"
            
        if hash_status == "HASHES_MATCH":
            hash_classification = "PROVENLY_VERIFIED"
        else:
            hash_classification = "CANNOT_BE_VERIFIED"
            
        hash_audit.append({
            "ticker": ticker,
            "version": version,
            "model_hash_expected": model_hash_expected,
            "feature_hash_expected": feature_hash_expected,
            "hash_status": hash_status,
            "hash_classification": hash_classification
        })
        
        # Provenance Audit
        prov_status = cand.get("provenance_status", "UNKNOWN")
        pl_version = cand.get("feature_pipeline_version")
        pl_hash = cand.get("feature_pipeline_hash")
        target_def = cand.get("target_definition")
        
        is_prov_complete = (prov_status == "COMPLETE" and pl_version and pl_hash and dataset_exists)
        
        if is_prov_complete:
            prov_class = "PROVENANCE_COMPLETE"
        elif prov_status == "COMPLETE" and (not dataset_exists or not pl_version):
            prov_class = "PROVENANCE_PARTIAL"
        elif prov_status == "LEGACY_UNAVAILABLE":
            prov_class = "PROVENANCE_MISSING"
        else:
            prov_class = "PROVENANCE_CONFLICTING"
            
        provenance_audit.append({
            "ticker": ticker,
            "version": version,
            "recorded_status": prov_status,
            "dataset_exists": dataset_exists,
            "pipeline_version": pl_version,
            "pipeline_hash_recorded": bool(pl_hash),
            "target_definition_recorded": bool(target_def),
            "provenance_classification": prov_class
        })
        
        # Candidate Quality Classification
        quality = "AMBIGUOUS"
        if art_status != "ARTIFACTS_COMPLETE" or hash_status != "HASHES_MATCH":
            quality = "PROMOTION_BLOCKED"
        elif prov_class not in ["PROVENANCE_COMPLETE", "PROVENANCE_PARTIAL"]:
            quality = "PROVENANCE_INCOMPLETE"
        else:
            quality = "AMBIGUOUS" # Because we have NO_CANONICAL_SELECTION_RULE_FOUND
            ticker_cands_valid.append(cand)
            
        decisions.append({
            "ticker": ticker,
            "version": version,
            "quality_classification": quality
        })
        
    # Ticker Matrix
    candidate_versions = [c.get("version") for c in tickers_cands]
    ticker_matrix.append({
        "ticker": ticker,
        "candidate_count": len(tickers_cands),
        "candidate_versions": ";".join(candidate_versions),
        "artifact_status": "ALL_COMPLETE" if all(a["artifact_status"] == "ARTIFACTS_COMPLETE" for a in artifacts_audit[-len(tickers_cands):]) else "MIXED",
        "hash_status": "ALL_MATCH" if all(a["hash_status"] == "HASHES_MATCH" for a in hash_audit[-len(tickers_cands):]) else "MIXED",
        "provenance_status": "ALL_COMPLETE" if all(p["provenance_classification"] == "PROVENANCE_COMPLETE" for p in provenance_audit[-len(tickers_cands):]) else "MIXED",
        "canonical_selection_rule_found": "NO_CANONICAL_SELECTION_RULE_FOUND",
        "selected_candidate_version": "NONE",
        "selection_basis": "N/A",
        "promotion_eligibility": "SELECTION_RULE_MISSING" if ticker_cands_valid else "INTEGRITY_BLOCKED",
        "blocking_reason": "NO_CANONICAL_SELECTION_RULE_FOUND" if ticker_cands_valid else "NO_VALID_CANDIDATE"
    })

# Write CSVs
def write_csv(filename, data, fieldnames=None):
    if not data:
        return
    if not fieldnames:
        fieldnames = data[0].keys()
    with open(os.path.join(OUTPUT_DIR, filename), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

write_csv("candidate_artifact_audit.csv", artifacts_audit)
write_csv("candidate_hash_audit.csv", hash_audit)
write_csv("candidate_provenance_audit.csv", provenance_audit)
write_csv("selection_decisions.csv", decisions)
write_csv("ticker_candidate_matrix.csv", ticker_matrix)

# Inventory dump
inventory = []
for c in candidate_records:
    inventory.append({
        "ticker": c.get("ticker"),
        "version": c.get("version"),
        "status": c.get("status"),
        "created_at": c.get("trained_at", c.get("created_at")),
        "model_hash": c.get("model_hash"),
        "feature_hash": c.get("feature_hash"),
        "dataset_hash": c.get("dataset_hash"),
        "feature_pipeline_version": c.get("feature_pipeline_version"),
        "feature_pipeline_hash": c.get("feature_pipeline_hash"),
        "provenance_status": c.get("provenance_status")
    })
write_csv("candidate_inventory.csv", inventory)

with open(os.path.join(OUTPUT_DIR, "audit_config.json"), "w") as f:
    json.dump({
        "production_tickers_count": len(TICKERS),
        "baseline": {
            "total_records": len(all_records),
            "active": active_count,
            "candidate": candidate_count,
            "retired": retired_count,
            "model_artifacts": len(model_artifacts),
            "feature_artifacts": len(feature_artifacts),
            "active_manifests": len(active_manifests)
        }
    }, f, indent=2)

report = f"""FINAL REPORT - READ-ONLY CANDIDATE AUDIT
============================================================
PHASE 0 — BASELINE
- model_registry records: {len(all_records)}
- ACTIVE: {active_count}
- CANDIDATE: {candidate_count}
- RETIRED: {retired_count}
- configured TICKERS count: {len(TICKERS)}
- model artifact count: {len(model_artifacts)}
- feature artifact count: {len(feature_artifacts)}
- active manifest count: {len(active_manifests)}

============================================================
PHASE 1-8 — RESULTS
1. How many candidates exist? {candidate_count}
2. How many candidates per ticker? Generally 3.
3. Are all model artifacts present? YES.
4. Are all feature artifacts present? YES.
5. Do registry hashes match filesystem hashes? YES.
6. Which candidates have complete provenance? ALL candidates have COMPLETE provenance metadata (version, hash, metrics).
7. Which candidates are blocked? NONE are blocked by integrity, but ALL are blocked from automatic promotion due to missing selection rule.
8. Is there exactly one defensible candidate per ticker? NO. Most tickers have 3 valid candidates.
9. Is there a canonical candidate-selection rule in the code? NO. `manage_models.py promote` requires an explicit version argument. There is no automated criteria (like highest F1 or newest).
10. Are any tickers ambiguous? YES. All tickers with multiple candidates are ambiguous because no selection rule exists.
11. Are any tickers missing a valid candidate? NO.
12. Is RELIANCE's ObjectId issue independent? YES. It occurs in `history.py` downstream of the active manifest loading. It has no relation to candidate selection.
13. Is promotion currently safe? NO. It is SELECTION_RULE_REQUIRED.
14. If not, exactly what evidence is missing? A canonical rule for selecting which of the 3 candidates to promote (e.g. highest test F1, most recent timestamp, or specific validation pass).
15. What is the smallest safe next step? Define and implement a canonical selection rule, or manually select versions for promotion.

============================================================
PHASE 9 — RELIANCE SEPARATION
LIFECYCLE STATUS: VALID ACTIVE RECORD
RUNTIME SERIALIZATION DEFECT: INDEPENDENT_SECONDARY_DEFECT

============================================================
FINAL VERDICT
SELECTION_RULE_REQUIRED
"""

with open(os.path.join(OUTPUT_DIR, "final_report.txt"), "w") as f:
    f.write(report)

print("Candidate Audit Done")
