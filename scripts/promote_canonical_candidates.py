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
from src.features.router import get_feature_pipeline_hash

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

def validate_promotion_plan(plan, all_records):
    CURRENT_CANONICAL_HASH = get_feature_pipeline_hash("v1")
    targets = plan
    
    for target in targets:
        ticker = target["ticker"]
        selected_version = target["selected_version"]
        if selected_version == "NONE":
            raise ValueError(f"Missing candidate version for {ticker}")

        req_fields = ["model_hash", "feature_hash", "feature_pipeline_version", "feature_pipeline_hash"]
        for f in req_fields:
            if not target.get(f) or target.get(f) == "NONE":
                raise ValueError(f"Missing {f} in plan for {ticker}")
            
        cand = next((c for c in all_records if c.get("ticker") == ticker and c.get("version") == selected_version), None)
        if not cand:
            raise ValueError(f"Selected candidate {ticker}_{selected_version} not found in live registry.")
            
        if cand.get("status") != "CANDIDATE":
            raise ValueError(f"Selected candidate {ticker}_{selected_version} is not CANDIDATE.")
            
        # Validate identity fields
        if target["model_hash"] != cand.get("model_hash"):
            raise ValueError(f"Model hash mismatch for {ticker}")
        if target["feature_hash"] != cand.get("feature_hash"):
            raise ValueError(f"Feature hash mismatch for {ticker}")
        if target["feature_pipeline_version"] != cand.get("feature_pipeline_version"):
            raise ValueError(f"Pipeline version mismatch for {ticker}")
        if target["feature_pipeline_hash"] != cand.get("feature_pipeline_hash"):
            raise ValueError(f"Pipeline hash mismatch for {ticker}")

        # Validate against canonical hash
        if target["feature_pipeline_hash"] != CURRENT_CANONICAL_HASH:
            raise ValueError(f"Plan pipeline hash is not canonical for {ticker}")
        if cand.get("feature_pipeline_hash") != CURRENT_CANONICAL_HASH:
            raise ValueError(f"Candidate pipeline hash is not canonical for {ticker}")
        if target["feature_pipeline_version"] != "v1":
            raise ValueError(f"Plan pipeline version is not v1 for {ticker}")
        if cand.get("feature_pipeline_version") != "v1":
            raise ValueError(f"Candidate pipeline version is not v1 for {ticker}")
            
    # Also validate missing, duplicate, unexpected tickers
    plan_tickers = [row["ticker"] for row in plan]
    if len(plan) != 51 or len(set(plan_tickers)) != 51 or set(plan_tickers) != set(TICKERS):
        raise ValueError("Promotion plan ticker universe mismatch.")
        
    return True

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
    
    db = get_db()
    all_records = list(db.model_registry.find())
    
    try:
        validate_promotion_plan(plan, all_records)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
        
    logger.info("Pre-execution validation complete. 51 targets perfectly match frozen plan.")
    
    if not args.execute:
        logger.info("DRY-RUN MODE. Execution authorized ONLY with --execute.")
        sys.exit(0)
        
    logger.info("EXECUTING PROMOTION...")
    # NOTE: Since instructions say "DO NOT RUN --execute during this phase", 
    # we don't expect this path to be hit. But if it were, we'd loop over targets and call promote_model(db, ticker, version).
    for target in plan:
        ticker = target["ticker"]
        selected_version = target["selected_version"]
        success = promote_model(db, ticker, selected_version)
        if not success:
            logger.error(f"Promotion failed for {ticker}_{selected_version}. Aborting remainder.")
            sys.exit(1)
    
    logger.info("PROMOTION COMPLETE.")

if __name__ == "__main__":
    main()
