"""
create_evaluation_dataset.py

Generates a frozen, immutable evaluation dataset for all tickers.
The dataset is temporally isolated strictly AFTER the candidate training cutoff
and BEFORE the label maturity threshold.

It reuses the canonical feature engineering logic from trainer.py to ensure parity,
but saves to a dedicated saved_evaluations/ directory.
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timezone
import pandas as pd
from pymongo import MongoClient

from src.data.nifty50 import TICKERS
from src.ml.trainer import create_dataset, FEATURE_PIPELINE_VERSION
from src.features.router import get_feature_pipeline_hash, resolve_feature_pipeline
from src.ml.confidence import compute_confidence_tier
from src.ml.trainer import _make_feature_list

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

EVALUATIONS_DIR = "saved_evaluations"

# Established in preflight
EVALUATION_START_DATE = "2025-08-25"
EVALUATION_END_DATE = "2026-08-05"

eng = resolve_feature_pipeline("v1")

def generate_frozen_dataset():
    os.makedirs(EVALUATIONS_DIR, exist_ok=True)
    
    # Temporarily override cutoff to the latest available data to allow future_return calculation
    # We set it to 2026-08-19, which is the latest market data.
    original_cutoff = os.environ.get("TRAINING_CUTOFF_DATE")
    os.environ["TRAINING_CUTOFF_DATE"] = "2026-08-19"
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        client = MongoClient(os.getenv("MONGO_URI"))
        
        all_ticker_dfs = []
        
        start_ts = pd.Timestamp(EVALUATION_START_DATE)
        end_ts = pd.Timestamp(EVALUATION_END_DATE)
        
        for ticker in TICKERS:
            logger.info("Generating evaluation rows for %s", ticker)
            df = create_dataset(ticker, client)
            
            if df.empty:
                logger.warning("%s: create_dataset returned empty.", ticker)
                continue
                
            # Filter to the evaluation window strictly
            df = df[(df.index >= start_ts) & (df.index <= end_ts)].copy()
            
            if df.empty:
                logger.warning("%s: No valid rows in the evaluation window.", ticker)
                continue
                
            req_features = _make_feature_list(df)
            
            # Keep only the features + target + close (for PnL) + future_return (for analysis)
            cols_to_keep = req_features + ["target", "close", "future_return"]
            missing = [c for c in cols_to_keep if c not in df.columns]
            if missing:
                logger.warning("%s: Missing columns %s, skipping ticker.", ticker, missing)
                continue
                
            # Add ticker column for multi-ticker dataset identification
            df["ticker"] = ticker
            
            # Reorder columns
            cols = ["ticker", "close", "future_return", "target"] + req_features
            df = df[cols]
            
            all_ticker_dfs.append(df)
            
    finally:
        if original_cutoff is not None:
            os.environ["TRAINING_CUTOFF_DATE"] = original_cutoff
        else:
            del os.environ["TRAINING_CUTOFF_DATE"]
            
    if not all_ticker_dfs:
        raise RuntimeError("Failed to generate any evaluation data.")
        
    final_df = pd.concat(all_ticker_dfs)
    
    # Sort by date and ticker for deterministic hashing
    final_df = final_df.sort_index().sort_values(by=["ticker", "date"] if "date" in final_df.columns else ["ticker"])
    # If index is date, sort_index handles it. We just need to sort by index, then ticker.
    # Since index is date, we can sort by index, then ticker.
    final_df = final_df.reset_index().sort_values(["date", "ticker"]).set_index("date")
    
    temp_path = os.path.join(EVALUATIONS_DIR, "eval_dataset_v1_temp.parquet")
    final_df.to_parquet(temp_path, engine="pyarrow")
    
    # Hash the file
    sha256 = hashlib.sha256()
    with open(temp_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    dataset_hash = sha256.hexdigest()[:64]
    
    dataset_version = f"v1_{dataset_hash[:12]}"
    final_path = os.path.join(EVALUATIONS_DIR, f"eval_dataset_{dataset_version}.parquet")
    os.rename(temp_path, final_path)
    
    # Generate metadata
    metadata = {
        "evaluation_dataset_version": dataset_version,
        "evaluation_dataset_hash": dataset_hash,
        "evaluation_start_date": EVALUATION_START_DATE,
        "evaluation_end_date": EVALUATION_END_DATE,
        "feature_pipeline_version": FEATURE_PIPELINE_VERSION,
        "feature_pipeline_hash": get_feature_pipeline_hash(FEATURE_PIPELINE_VERSION),
        "prediction_horizon": 10,
        "ticker_coverage": int(final_df["ticker"].nunique()),
        "total_rows": int(len(final_df)),
        "generated_at": datetime.now(timezone.utc).isoformat()
    }
    
    metadata_path = os.path.join(EVALUATIONS_DIR, f"metadata_{dataset_version}.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)
        
    logger.info("FROZEN EVALUATION DATASET GENERATED:")
    logger.info("Path: %s", final_path)
    logger.info("Version: %s", dataset_version)
    logger.info("Coverage: %d tickers, %d rows", metadata["ticker_coverage"], metadata["total_rows"])

if __name__ == "__main__":
    generate_frozen_dataset()
