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
    model_path = os.path.join(MODELS_DIR, f"model_{ticker}_{version}.joblib")
    features_path = os.path.join(FEATURES_DIR, f"features_{ticker}_{version}.json")
    
    if not os.path.exists(model_path):
        return False
    if not os.path.exists(features_path):
        return False
        
    actual_model_hash = hash_file_sha256(model_path, truncate_to=12)
    if actual_model_hash != expected_model_hash:
        return False
        
    actual_feature_hash = hash_file_sha256(features_path, truncate_to=64)
    if actual_feature_hash != expected_feature_hash:
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
    now = datetime.now(timezone.utc).isoformat()
    try:
        db.model_registry.insert_one({
            "ticker": ticker,
            "version": version,
            "status": "CANDIDATE",
            "model_hash": model_hash,
            "feature_hash": feature_hash,
            "feature_pipeline_version": metrics.get("feature_pipeline_version"),
            "feature_pipeline_hash": metrics.get("feature_pipeline_hash"),
            "dataset_hash": metrics.get("dataset_hash"),
            "dataset_row_count": metrics.get("dataset_row_count"),
            "dataset_date_start": metrics.get("dataset_date_start"),
            "dataset_date_end": metrics.get("dataset_date_end"),
            "target_definition": metrics.get("target_definition"),
            "provenance_status": metrics.get("provenance_status", "LEGACY_UNAVAILABLE"),
            "metrics": metrics,
            "trained_at": now
        })
    except pymongo.errors.DuplicateKeyError:
        pass

def update_manifest_atomically(ticker: str, manifest_data: Dict[str, Any]):
    target_path = get_active_manifest_path(ticker)
    temp_path = f"{target_path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)
    os.replace(temp_path, target_path)

def promote_model(db, ticker: str, version: str) -> bool:
    # PHASE 3: Filesystem-First Manifest Staging & PHASE 4: Previous State Capture
    target_record = db.model_registry.find_one({"ticker": ticker, "version": version})
    if not target_record:
        return False
        
    if target_record["status"] not in ["CANDIDATE", "RETIRED"]:
        return False
        
    if not validate_bundle(ticker, version, target_record["model_hash"], target_record["feature_hash"]):
        return False
        
    old_active_record = db.model_registry.find_one({"ticker": ticker, "status": "ACTIVE"})
    if old_active_record and old_active_record["version"] == version:
        return False # Already active, idempotent abort
        
    manifest_path = get_active_manifest_path(ticker)
    manifest_exists = os.path.exists(manifest_path)
    old_manifest_contents = None
    if manifest_exists:
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                old_manifest_contents = f.read()
        except Exception:
            return False

    now = datetime.now(timezone.utc).isoformat()
    manifest_data = {
        "ticker": ticker,
        "model_version": version,
        "model_hash": target_record["model_hash"],
        "feature_hash": target_record["feature_hash"],
        "feature_pipeline_version": target_record.get("feature_pipeline_version", "v1"),
        "feature_pipeline_hash": target_record.get("feature_pipeline_hash"),
        "dataset_hash": target_record.get("dataset_hash", "LEGACY_UNAVAILABLE"),
        "dataset_row_count": target_record.get("dataset_row_count"),
        "dataset_date_start": target_record.get("dataset_date_start"),
        "dataset_date_end": target_record.get("dataset_date_end"),
        "target_definition": target_record.get("target_definition"),
        "provenance_status": target_record.get("provenance_status", "LEGACY_UNAVAILABLE"),
        "f1_macro": target_record.get("metrics", {}).get("f1_macro", 0.0),
        "promoted_at": now
    }
    
    temp_path = f"{manifest_path}.tmp"
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=2)
    except Exception as e:
        return False # Failed to stage, no MongoDB mutation occurred

    # PHASE 5: MongoDB Promotion
    if old_active_record:
        db.model_registry.update_many(
            {"ticker": ticker, "status": "ACTIVE"},
            {"$set": {"status": "RETIRED", "retired_at": now}}
        )
    
    try:
        result = db.model_registry.update_one(
            {"ticker": ticker, "version": version},
            {"$set": {"status": "ACTIVE", "promoted_at": now}}
        )
        if result.modified_count == 0:
            if old_active_record:
                db.model_registry.update_one(
                    {"ticker": ticker, "version": old_active_record["version"]},
                    {"$set": {"status": "ACTIVE"}}
                )
            return False
    except pymongo.errors.DuplicateKeyError:
        return False

    # PHASE 6: Atomic Manifest Replacement
    try:
        os.replace(temp_path, manifest_path)
    except Exception as e:
        # PHASE 7: Explicit Rollback
        db.model_registry.update_one(
            {"ticker": ticker, "version": version},
            {"$set": {"status": target_record["status"]}}
        )
        if old_active_record:
            db.model_registry.update_one(
                {"ticker": ticker, "version": old_active_record["version"]},
                {"$set": {"status": "ACTIVE"}}
            )
            
        # PHASE 8: Rollback Verification
        final_actives = list(db.model_registry.find({"ticker": ticker, "status": "ACTIVE"}))
        if old_active_record:
            if len(final_actives) != 1 or final_actives[0]["version"] != old_active_record["version"]:
                logger.error("RECOVERY_REQUIRED")
        else:
            if len(final_actives) > 0:
                logger.error("RECOVERY_REQUIRED")
                
        # Also ensure manifest was untouched (it shouldn't be touched if os.replace fails)
        if manifest_exists:
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    if f.read() != old_manifest_contents:
                        logger.error("RECOVERY_REQUIRED")
            except Exception:
                logger.error("RECOVERY_REQUIRED")
        else:
            if os.path.exists(manifest_path):
                logger.error("RECOVERY_REQUIRED")

        logger.info("PROMOTION_ROLLED_BACK")
        return False

    # PHASE 10: Post-Promotion Consistency Verification
    final_actives = list(db.model_registry.find({"ticker": ticker, "status": "ACTIVE"}))
    if len(final_actives) != 1 or final_actives[0]["version"] != version:
        logger.error("POST_PROMOTION_VERIFICATION_FAILED")
        return False
        
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            final_manifest = json.load(f)
        if final_manifest.get("model_version") != version:
            logger.error("POST_PROMOTION_VERIFICATION_FAILED")
            return False
    except Exception:
        logger.error("POST_PROMOTION_VERIFICATION_FAILED")
        return False

    return True

def sync_manifest(db, ticker: str):
    """Reconciles the filesystem manifest with the intended MongoDB registry state.
    Used for explicitly resolving split-brain scenarios."""
    active_record = db.model_registry.find_one({"ticker": ticker, "status": "ACTIVE"})
    
    if not active_record:
        # If no active record, we should remove the manifest if it exists
        path = get_active_manifest_path(ticker)
        if os.path.exists(path):
            os.remove(path)
        return True
        
    version = active_record["version"]
    
    is_valid = validate_bundle(
        ticker, 
        version, 
        active_record["model_hash"], 
        active_record["feature_hash"]
    )
    
    if not is_valid:
        return False
        
    manifest_data = {
        "ticker": ticker,
        "model_version": version,
        "model_hash": active_record["model_hash"],
        "feature_hash": active_record["feature_hash"],
        "feature_pipeline_version": active_record.get("feature_pipeline_version", "v1"),
        "feature_pipeline_hash": active_record.get("feature_pipeline_hash"),
        "dataset_hash": active_record.get("dataset_hash", "LEGACY_UNAVAILABLE"),
        "dataset_row_count": active_record.get("dataset_row_count"),
        "dataset_date_start": active_record.get("dataset_date_start"),
        "dataset_date_end": active_record.get("dataset_date_end"),
        "target_definition": active_record.get("target_definition"),
        "provenance_status": active_record.get("provenance_status", "LEGACY_UNAVAILABLE"),
        "f1_macro": active_record.get("metrics", {}).get("f1_macro", 0.0),
        "promoted_at": active_record.get("promoted_at", datetime.now(timezone.utc).isoformat())
    }
    
    update_manifest_atomically(ticker, manifest_data)
    return True
