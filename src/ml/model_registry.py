import os
import json
import logging
import hashlib
from datetime import datetime, timezone
import pymongo
from pymongo import MongoClient
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

MODELS_DIR = "saved_models"
FEATURES_DIR = "saved_features"

def hash_file_sha256(filepath: str, truncate_to: int = 64) -> str:
    """Computes SHA256 of a file, optionally truncated to match existing semantics."""
    sha256 = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()[:truncate_to]
    except Exception as exc:
        logger.exception(f"Error hashing file {filepath}: {exc}")
        return ""

def get_active_manifest_path(ticker: str) -> str:
    return os.path.join(MODELS_DIR, f"{ticker}_active.json")

def read_active_manifest(ticker: str) -> Optional[Dict[str, Any]]:
    path = get_active_manifest_path(ticker)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to read manifest for {ticker}: {e}")
        return None

def validate_bundle(ticker: str, version: str, expected_model_hash: str, expected_feature_hash: str) -> bool:
    """Validates that immutable artifacts for a specific version exist and match hashes."""
    model_path = os.path.join(MODELS_DIR, f"model_{ticker}_{version}.joblib")
    features_path = os.path.join(FEATURES_DIR, f"features_{ticker}_{version}.json")
    
    if not os.path.exists(model_path):
        logger.error(f"Missing model artifact for {ticker} version {version}")
        return False
        
    if not os.path.exists(features_path):
        logger.error(f"Missing feature artifact for {ticker} version {version}")
        return False
        
    actual_model_hash = hash_file_sha256(model_path, truncate_to=12) # model_version truncates to 12
    if actual_model_hash != expected_model_hash:
        logger.error(f"Model hash mismatch for {ticker} version {version}: {actual_model_hash} != {expected_model_hash}")
        return False
        
    actual_feature_hash = hash_file_sha256(features_path, truncate_to=64)
    if actual_feature_hash != expected_feature_hash:
        logger.error(f"Feature hash mismatch for {ticker} version {version}: {actual_feature_hash} != {expected_feature_hash}")
        return False
        
    return True

def setup_registry_indexes(db):
    try:
        db.model_registry.create_index([("ticker", pymongo.ASCENDING), ("version", pymongo.ASCENDING)], unique=True)
        
        db.model_registry.create_index(
            [("ticker", pymongo.ASCENDING)], 
            unique=True, 
            partialFilterExpression={"status": "ACTIVE"},
            name="unique_active_per_ticker"
        )
    except Exception as e:
        logger.error(f"Failed to setup indexes for model_registry: {e}")

def register_candidate(db, ticker: str, version: str, model_hash: str, feature_hash: str, metrics: Dict[str, Any]):
    """Registers a new model generation as CANDIDATE."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        db.model_registry.insert_one({
            "ticker": ticker,
            "version": version,
            "status": "CANDIDATE",
            "model_hash": model_hash,
            "feature_hash": feature_hash,
            "metrics": metrics,
            "trained_at": now
        })
        logger.info(f"Registered CANDIDATE model for {ticker} version {version}")
    except pymongo.errors.DuplicateKeyError:
        logger.info(f"CANDIDATE model for {ticker} version {version} already exists. Skipping insertion.")

def update_manifest_atomically(ticker: str, manifest_data: Dict[str, Any]):
    """Safely updates the local active manifest using a temporary file and os.replace()."""
    target_path = get_active_manifest_path(ticker)
    temp_path = f"{target_path}.tmp"
    
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)
        
    os.replace(temp_path, target_path)

def promote_model(db, ticker: str, version: str) -> bool:
    """Explicitly promotes a CANDIDATE or RETIRED model to ACTIVE, demoting the previous."""
    # 1. Validate target
    target_record = db.model_registry.find_one({"ticker": ticker, "version": version})
    if not target_record:
        logger.error(f"No registry record found for {ticker} version {version}")
        return False
        
    if target_record["status"] not in ["CANDIDATE", "RETIRED"]:
        logger.error(f"Cannot promote model with status {target_record['status']}")
        return False
        
    is_valid = validate_bundle(
        ticker, 
        version, 
        target_record["model_hash"], 
        target_record["feature_hash"]
    )
    if not is_valid:
        logger.error("Artifact validation failed. Aborting promotion.")
        return False
        
    # 2. Update MongoDB Lifecycle State (Atomically)
    now = datetime.now(timezone.utc).isoformat()
    
    # We must do this in two steps if transactions are not guaranteed:
    # A) Demote current active to RETIRED
    db.model_registry.update_many(
        {"ticker": ticker, "status": "ACTIVE"},
        {"$set": {"status": "RETIRED", "retired_at": now}}
    )
    
    # B) Promote target to ACTIVE
    try:
        result = db.model_registry.update_one(
            {"ticker": ticker, "version": version},
            {"$set": {"status": "ACTIVE", "promoted_at": now}}
        )
        if result.modified_count == 0:
            logger.error("Failed to set ACTIVE status in registry.")
            return False
    except pymongo.errors.DuplicateKeyError:
        logger.error("Concurrency conflict: Another model is already ACTIVE for this ticker.")
        return False

    # 3. Atomically replace active.json
    manifest_data = {
        "ticker": ticker,
        "model_version": version,
        "model_hash": target_record["model_hash"],
        "feature_hash": target_record["feature_hash"],
        "promoted_at": now
    }
    
    try:
        update_manifest_atomically(ticker, manifest_data)
        logger.info(f"Successfully promoted {ticker} version {version} to ACTIVE.")
        return True
    except Exception as e:
        logger.error(f"MongoDB updated successfully, but failed to write manifest for {ticker}: {e}")
        return False

def sync_manifest(db, ticker: str):
    """Reconciles the filesystem manifest with the intended MongoDB registry state."""
    active_record = db.model_registry.find_one({"ticker": ticker, "status": "ACTIVE"})
    
    if not active_record:
        logger.error(f"No ACTIVE registry record found for {ticker}.")
        return False
        
    version = active_record["version"]
    
    is_valid = validate_bundle(
        ticker, 
        version, 
        active_record["model_hash"], 
        active_record["feature_hash"]
    )
    
    if not is_valid:
        logger.error("ACTIVE registry record points to invalid artifacts. Cannot sync.")
        return False
        
    manifest_data = {
        "ticker": ticker,
        "model_version": version,
        "model_hash": active_record["model_hash"],
        "feature_hash": active_record["feature_hash"],
        "promoted_at": active_record.get("promoted_at", datetime.now(timezone.utc).isoformat())
    }
    
    update_manifest_atomically(ticker, manifest_data)
    logger.info(f"Successfully synced manifest for {ticker} to version {version}.")
    return True
