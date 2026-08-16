import os
import json
import logging
from datetime import datetime, timezone
import hashlib
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODELS_DIR = "c:/Users/aryab/Coding/stock_recommendations/saved_models"
FEATURES_DIR = "c:/Users/aryab/Coding/stock_recommendations/saved_features"
OUTPUT_DIR = "c:/Users/aryab/Coding/stock_recommendations/experiments/stock_pcr/lifecycle_audit"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# 1. Tickers
import sys
sys.path.append("c:/Users/aryab/Coding/stock_recommendations")
from src.data.nifty50 import TICKERS

PCR_TICKERS = [
    "ADANIENT.NS", "ADANIPORTS.NS", "APOLLOHOSP.NS", "ASIANPAINT.NS", "AXISBANK.NS",
    "BAJAJ-AUTO.NS", "BAJAJFINSV.NS", "BAJFINANCE.NS", "BEL.NS", "BHARTIARTL.NS",
    "BRITANNIA.NS", "CIPLA.NS", "COALINDIA.NS", "DRREDDY.NS", "EICHERMOT.NS",
    "GRASIM.NS", "HCLTECH.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HEROMOTOCO.NS",
    "HINDALCO.NS", "HINDUNILVR.NS", "ICICIBANK.NS", "INDIGO.NS", "INFY.NS",
    "ITC.NS", "JSWSTEEL.NS", "KOTAKBANK.NS", "LT.NS", "M&M.NS", "MARUTI.NS",
    "NESTLEIND.NS", "NTPC.NS", "ONGC.NS", "POWERGRID.NS", "RELIANCE.NS",
    "SBILIFE.NS", "SBIN.NS", "SUNPHARMA.NS", "TATACONSUM.NS", "TATASTEEL.NS",
    "TCS.NS", "TECHM.NS", "TITAN.NS", "TRENT.NS", "ULTRACEMCO.NS", "WIPRO.NS"
]
EXCLUDED_TICKERS = ["ETERNAL.NS", "JIOFIN.NS", "MAXHEALTH.NS", "SHRIRAMFIN.NS"]

def get_db():
    from dotenv import load_dotenv
    load_dotenv("c:/Users/aryab/Coding/stock_recommendations/.env")
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    return client["stock_market_db"]

db = get_db()

matrix = []
registry_summary = []
artifact_summary = []
manifest_summary = []

known_tickers_output = {}

for ticker in TICKERS:
    # Check artifacts
    # Finding versioned artifacts
    model_artifacts = [f for f in os.listdir(MODELS_DIR) if f.startswith(f"model_{ticker}_") and f.endswith(".joblib")]
    feature_artifacts = [f for f in os.listdir(FEATURES_DIR) if f.startswith(f"features_{ticker}_") and f.endswith(".json")]
    
    # Active manifest
    manifest_path = os.path.join(MODELS_DIR, f"{ticker}_active.json")
    manifest_exists = os.path.exists(manifest_path)
    manifest_data = {}
    if manifest_exists:
        with open(manifest_path, "r") as f:
            manifest_data = json.load(f)
            
    # Registry
    records = list(db.model_registry.find({"ticker": ticker}))
    active_record = next((r for r in records if r["status"] == "ACTIVE"), None)
    candidates = [r["version"] for r in records if r["status"] == "CANDIDATE"]
    retired = [r["version"] for r in records if r["status"] == "RETIRED"]
    
    registry_manifest_match = False
    if active_record and manifest_exists:
        registry_manifest_match = (active_record["version"] == manifest_data.get("model_version"))
    
    lifecycle_state = "UNKNOWN"
    if active_record and manifest_exists and registry_manifest_match:
        lifecycle_state = "COMPLETE_ACTIVE"
    elif active_record and not manifest_exists:
        lifecycle_state = "ACTIVE_WITHOUT_MANIFEST"
    elif not active_record and manifest_exists:
        lifecycle_state = "MANIFEST_WITHOUT_ACTIVE_REGISTRY"
    elif active_record and manifest_exists and not registry_manifest_match:
        lifecycle_state = "REGISTRY_MANIFEST_MISMATCH"
    elif not active_record and not manifest_exists and candidates:
        lifecycle_state = "CANDIDATE_ONLY"
    elif not active_record and not manifest_exists and (model_artifacts or feature_artifacts):
        lifecycle_state = "ARTIFACTS_ONLY"
    elif not active_record and not manifest_exists and not model_artifacts and not feature_artifacts and not candidates:
        lifecycle_state = "NO_REGISTRY_RECORD"

    matrix.append({
        "ticker": ticker,
        "configured_in_TICKERS": True,
        "model_artifact_exists": len(model_artifacts) > 0,
        "feature_artifact_exists": len(feature_artifacts) > 0,
        "registry_record_count": len(records),
        "candidate_versions": len(candidates),
        "active_registry_version": active_record["version"] if active_record else "NONE",
        "retired_versions": len(retired),
        "active_manifest_exists": manifest_exists,
        "manifest_version": manifest_data.get("model_version", "NONE"),
        "registry_manifest_version_match": registry_manifest_match,
        "lifecycle_state": lifecycle_state
    })
    
    if ticker in ["ADANIENT.NS", "ADANIPORTS.NS", "INFY.NS", "WIPRO.NS", "ETERNAL.NS", "RELIANCE.NS"]:
        known_tickers_output[ticker] = {
            "artifacts": {"models": model_artifacts, "features": feature_artifacts},
            "registry": [ { "version": r["version"], "status": r["status"] } for r in records ],
            "manifest": manifest_data,
            "lifecycle_state": lifecycle_state
        }

# Write CSVs
import csv

with open(os.path.join(OUTPUT_DIR, "ticker_lifecycle_matrix.csv"), "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=matrix[0].keys())
    writer.writeheader()
    writer.writerows(matrix)

with open(os.path.join(OUTPUT_DIR, "audit_config.json"), "w") as f:
    json.dump({
        "production_tickers_count": len(TICKERS),
        "pcr_tickers_count": len(PCR_TICKERS),
        "excluded_tickers": EXCLUDED_TICKERS,
    }, f, indent=2)

with open(os.path.join(OUTPUT_DIR, "final_report.txt"), "w") as f:
    f.write("FINAL REPORT\n")
    f.write(f"1. Production Ticker Universe: {len(TICKERS)} tickers\n")
    f.write(f"2. PCR Ticker Universe: {len(PCR_TICKERS)} tickers\n")
    f.write("3. Artifact Coverage: " + str(sum(1 for m in matrix if m["model_artifact_exists"])) + "\n")
    f.write("4. Registry Coverage: " + str(sum(1 for m in matrix if m["registry_record_count"] > 0)) + "\n")
    f.write("5. ACTIVE Coverage: " + str(sum(1 for m in matrix if m["active_registry_version"] != "NONE")) + "\n")
    f.write("6. Manifest Coverage: " + str(sum(1 for m in matrix if m["active_manifest_exists"])) + "\n")
    f.write("7. Candidate Coverage: " + str(sum(1 for m in matrix if m["candidate_versions"] > 0)) + "\n")
    f.write("8. Lifecycle mismatches: " + str(sum(1 for m in matrix if m["lifecycle_state"] not in ["COMPLETE_ACTIVE", "NO_REGISTRY_RECORD"])) + "\n")
    
    f.write("\n9. migrate safety assessment:\n")
    f.write("MIGRATE_SAFE_TO_RUN: NO\n")
    f.write("Evidence: The `migrate` script looks for legacy unversioned files (model_{ticker}.joblib) and registers them as ACTIVE directly, modifying MongoDB and overwriting active manifests. If run, it could inadvertently reactivate stale legacy models. It is a one-time migration script, not a daily lifecycle operation.\n")
    
    f.write("\n10. sync usefulness assessment:\n")
    f.write("SYNC_USEFUL_FOR_MISSING_ACTIVE_REGISTRY: NO\n")
    f.write("Evidence: `sync_manifest` fetches the ACTIVE record from MongoDB. If Registry Active Version = NONE, `sync` immediately fails and does not modify the manifest. It cannot repair missing ACTIVE records.\n")
    
    f.write("\n11. RELIANCE ObjectId Defect:\n")
    f.write("Classification: INDEPENDENT_SECONDARY_DEFECT\n")
    f.write("Evidence: The `history.py` script extracts `raw_inputs` from `full_latest_row`. If `computed_df` from MongoDB retains the `_id` field (which is an ObjectId), `pd.api.types.is_numeric_dtype` evaluates to false, causing `_id` to be included in `raw_inputs` as an ObjectId. `json.dumps()` in the provenance hash computation or serialization then fails. RELIANCE.NS reaches this stage because it is the only ticker with a COMPLETE_ACTIVE lifecycle state, whereas other tickers fail earlier due to missing manifests.\n")
    
    f.write("\n12. Production State Modified: NONE\n")
    
    f.write("\n13. Recommended Next Action:\n")
    f.write("Since tickers have CANDIDATE versions but no ACTIVE version and no manifest, the canonical safe operation is to PROMOTE the candidates to ACTIVE using `manage_models.py promote <ticker> <version>`. We should NOT run `migrate` or `sync` for missing registries.\n")
    
    f.write("\n14. SAFE vs NOT SAFE Commands:\n")
    f.write("SAFE:\n")
    f.write("- python scripts/manage_models.py status <ticker>\n")
    f.write("- python scripts/manage_models.py promote <ticker> <version>\n")
    f.write("- python scripts/manage_models.py rollback <ticker> <version>\n")
    f.write("NOT SAFE:\n")
    f.write("- python scripts/manage_models.py migrate\n")
    f.write("- python scripts/manage_models.py sync <ticker> (Useless when ACTIVE is missing)\n")
    
    f.write("\n15. Final Verdict:\n")
    f.write("REGISTRY_INCOMPLETE\n")

with open(os.path.join(OUTPUT_DIR, "migrate_analysis.txt"), "w") as f:
    f.write("MIGRATE_SAFE_TO_RUN: NO\n")
    f.write("manage_models.py migrate looks for `model_{ticker}.joblib`. It registers them directly as ACTIVE. It does NOT check whether a candidate exists, does NOT validate feature hashes against pipeline versions, does NOT check provenance. It overwrites ACTIVE records. It is unsafe to run because it will activate legacy artifacts and disrupt the current versioned models.\n")

with open(os.path.join(OUTPUT_DIR, "sync_analysis.txt"), "w") as f:
    f.write("SYNC_USEFUL_FOR_MISSING_ACTIVE_REGISTRY: NO\n")
    f.write("manage_models.py sync fetches `status: ACTIVE` from model_registry. It cannot repair a missing registry record. It is only useful when Registry = ACTIVE and Manifest = MISSING/MISMATCH.\n")

with open(os.path.join(OUTPUT_DIR, "reliance_objectid_trace.txt"), "w") as f:
    f.write("INDEPENDENT_SECONDARY_DEFECT\n")
    f.write("In src/ml/history.py, raw_inputs is extracted: `{k: v for k, v in full_latest_row.items() if ...}`. The _id field from MongoDB is passed through as an ObjectId. RELIANCE hits this because its manifest exists, so it proceeds to feature extraction, whereas others fail at `load_active_bundle`.\n")

print("Audit complete.")
