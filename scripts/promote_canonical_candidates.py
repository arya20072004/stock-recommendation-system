import os
import sys
import csv
import json
import logging
import argparse
import hashlib
from datetime import datetime, timezone
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

sys.path.append("c:/Users/aryab/Coding/stock_recommendations")
from src.data.nifty50 import TICKERS
from src.ml.model_registry import promote_model

def hash_file_sha256(filepath: str, truncate_to: int = 64) -> str:
    if not os.path.exists(filepath): return ""
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""): sha256.update(chunk)
    return sha256.hexdigest()[:truncate_to]

def hash_string_sha256(s: str) -> str:
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def get_db():
    from dotenv import load_dotenv
    load_dotenv("c:/Users/aryab/Coding/stock_recommendations/.env")
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    return MongoClient(MONGO_URI)["stock_market_db"]

def read_csv(path):
    with open(path, "r") as f:
        return list(csv.DictReader(f))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="Execute the promotion (DANGEROUS)")
    args = parser.parse_args()

    BASE_DIR = "c:/Users/aryab/Coding/stock_recommendations/experiments/stock_pcr/selection_policy"
    MODELS_DIR = "c:/Users/aryab/Coding/stock_recommendations/saved_models"
    FEATURES_DIR = "c:/Users/aryab/Coding/stock_recommendations/saved_features"
    
    # 1. Load Preregistration
    prereg_path = os.path.join(BASE_DIR, "preregistration.json")
    prereg_hash_path = os.path.join(BASE_DIR, "preregistration_hash.txt")
    with open(prereg_path, "r") as f: prereg_content = f.read()
    with open(prereg_hash_path, "r") as f: recorded_hash = f.read().strip()
    
    if hash_string_sha256(prereg_content) != recorded_hash:
        logger.error("Preregistration hash mismatch.")
        sys.exit(1)
        
    prereg = json.loads(prereg_content)
    if prereg.get("POLICY_NAME") != "CANONICAL_CV_CHAMPION_V1":
        logger.error("Policy version is not CANONICAL_CV_CHAMPION_V1.")
        sys.exit(1)

    # 4. Load Frozen Promotion Plan
    plan_path = os.path.join(BASE_DIR, "promotion_plan.csv")
    plan = read_csv(plan_path)
    
    plan_tickers = [row["ticker"] for row in plan]
    if len(plan) != 51 or len(set(plan_tickers)) != 51 or set(plan_tickers) != set(TICKERS):
        logger.error("Promotion plan ticker universe mismatch.")
        sys.exit(1)

    # Prepare targets
    targets = [row for row in plan if row["ticker"] != "RELIANCE.NS"]
    reliance = next(row for row in plan if row["ticker"] == "RELIANCE.NS")
    
    if reliance["promotion_reason"] != "RELIANCE_REQUIRES_SEPARATE_REVIEW":
        logger.error("RELIANCE.NS is not explicitly excluded/flagged for review.")
        sys.exit(1)

    db = get_db()
    all_records = list(db.model_registry.find())
    
    # Pre-execution validation
    for target in targets:
        ticker = target["ticker"]
        selected_version = target["selected_version"]
        
        # Check current active state
        active_recs = [r for r in all_records if r.get("ticker") == ticker and r.get("status") == "ACTIVE"]
        if active_recs:
            logger.error(f"Unexpected ACTIVE record for {ticker}.")
            sys.exit(1)
            
        cand = next((c for c in all_records if c.get("ticker") == ticker and c.get("version") == selected_version), None)
        if not cand:
            logger.error(f"Selected candidate {ticker}_{selected_version} not found in live registry.")
            sys.exit(1)
            
        if cand.get("status") != "CANDIDATE":
            logger.error(f"Selected candidate {ticker}_{selected_version} is not CANDIDATE.")
            sys.exit(1)
            
        # Verify hashes
        m_path = os.path.join(MODELS_DIR, f"model_{ticker}_{selected_version}.joblib")
        f_path = os.path.join(FEATURES_DIR, f"features_{ticker}_{selected_version}.json")
        if not os.path.exists(m_path) or not os.path.exists(f_path):
            logger.error(f"Missing artifacts for {ticker}_{selected_version}.")
            sys.exit(1)
            
        mh = hash_file_sha256(m_path, 12)
        fh = hash_file_sha256(f_path, 64)
        if mh != cand.get("model_hash") or fh != cand.get("feature_hash"):
            logger.error(f"Hash mismatch for {ticker}_{selected_version}.")
            sys.exit(1)
            
        if cand.get("provenance_status") != "COMPLETE" or not cand.get("dataset_hash") or not cand.get("feature_pipeline_version"):
            logger.error(f"Incomplete provenance for {ticker}_{selected_version}.")
            sys.exit(1)
            
    # Verify RELIANCE is untouched
    rel_live = [r for r in all_records if r.get("ticker") == "RELIANCE.NS" and r.get("status") == "ACTIVE"]
    if not rel_live or rel_live[0].get("version") != reliance["current_active_version"]:
        logger.error("RELIANCE.NS state unexpectedly altered.")
        sys.exit(1)

    logger.info("Pre-execution validation complete. 50 targets perfectly match frozen plan.")

    if not args.execute:
        logger.info("DRY-RUN MODE. Execution authorized ONLY with --execute.")
        sys.exit(0)
        
    logger.info("EXECUTING PROMOTION...")
    # NOTE: Since instructions say "DO NOT RUN --execute during this phase", 
    # we don't expect this path to be hit. But if it were, we'd loop over targets and call promote_model(db, ticker, version).
    for target in targets:
        ticker = target["ticker"]
        selected_version = target["selected_version"]
        success = promote_model(db, ticker, selected_version)
        if not success:
            logger.error(f"Promotion failed for {ticker}_{selected_version}. Aborting remainder.")
            sys.exit(1)
    
    logger.info("PROMOTION COMPLETE.")

if __name__ == "__main__":
    main()
