import os
import json
import logging
import hashlib
from datetime import datetime, timezone
import pymongo
from pymongo import MongoClient
from typing import Dict, Any, Optional, Tuple
from src.features.router import get_feature_pipeline_hash
from src.data.nifty50 import TICKERS

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
        db.model_locks.create_index([("ticker", pymongo.ASCENDING)], unique=True)
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
    import uuid
    from datetime import timedelta

    owner_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=120)

    acquired = False
    try:
        db.model_locks.insert_one({
            "ticker": ticker,
            "owner_id": owner_id,
            "expires_at": expires_at
        })
        acquired = True
    except pymongo.errors.DuplicateKeyError:
        res = db.model_locks.find_one_and_update(
            {"ticker": ticker, "expires_at": {"$lt": now}},
            {"$set": {"owner_id": owner_id, "expires_at": expires_at}},
            return_document=pymongo.ReturnDocument.AFTER
        )
        acquired = res is not None

    if not acquired:
        logger.warning(f"Failed to acquire promotion lock for {ticker}")
        return False

    try:
        active_record = db.model_registry.find_one({"ticker": ticker, "status": "ACTIVE"})
        if active_record and active_record["version"] == version:
            logger.info(f"{ticker} version {version} is already ACTIVE. Syncing manifest.")
            sync_manifest(db, ticker, owner_id)
            return True

        target_record = db.model_registry.find_one({"ticker": ticker, "version": version})
        if not target_record:
            logger.error(f"Version {version} not found for {ticker}")
            return False

        if target_record["status"] not in ["CANDIDATE", "RETIRED"]:
            logger.error(f"Version {version} for {ticker} is not CANDIDATE or RETIRED")
            return False

        if not validate_bundle(ticker, version, target_record["model_hash"], target_record["feature_hash"]):
            logger.error("Bundle validation failed")
            return False

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
            "promoted_at": now.isoformat()
        }

        manifest_path = get_active_manifest_path(ticker)
        temp_path = f"{manifest_path}.tmp"

        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(manifest_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to stage temp manifest: {e}")
            return False

        client = db.client
        try:
            with client.start_session() as session:
                with session.start_transaction():
                    if active_record:
                        db.model_registry.update_many(
                            {"ticker": ticker, "status": "ACTIVE"},
                            {"$set": {"status": "RETIRED", "retired_at": now.isoformat()}},
                            session=session
                        )
                    db.model_registry.update_one(
                        {"ticker": ticker, "version": version},
                        {"$set": {"status": "ACTIVE", "promoted_at": now.isoformat()}},
                        session=session
                    )
        except Exception as e:
            logger.error(f"MongoDB promotion transaction failed: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return False

        lock_doc = db.model_locks.find_one({"ticker": ticker})
        check_now = datetime.now(timezone.utc)

        expires_at_val = lock_doc.get("expires_at") if lock_doc else None
        if expires_at_val and expires_at_val.tzinfo is None:
            expires_at_val = expires_at_val.replace(tzinfo=timezone.utc)

        if not lock_doc or lock_doc.get("owner_id") != owner_id or expires_at_val <= check_now:
            logger.error("CRITICAL: Lost lock ownership before filesystem replacement!")
            return False

        try:
            os.replace(temp_path, manifest_path)
        except Exception as e:
            logger.error(f"Manifest replacement failed: {e}")
            
            try:
                with client.start_session() as session:
                    with session.start_transaction():
                        db.model_registry.update_one(
                            {"ticker": ticker, "version": version},
                            {"$set": {"status": "CANDIDATE"}},
                            session=session
                        )
                        if active_record:
                            db.model_registry.update_many(
                                {"ticker": ticker, "status": "RETIRED", "version": active_record["version"]},
                                {"$set": {"status": "ACTIVE"}},
                                session=session
                            )
            except Exception as rollback_exc:
                logger.error(f"CRITICAL: Failed to rollback database after os.replace failure: {rollback_exc}")
                raise RuntimeError("RECOVERY_REQUIRED") from rollback_exc
            
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return False

        final_active = db.model_registry.find_one({"ticker": ticker, "status": "ACTIVE"})
        final_manifest = read_active_manifest(ticker)

        mongo_ver = final_active["version"] if final_active else None
        fs_ver = final_manifest.get("model_version") if final_manifest else None

        if mongo_ver != fs_ver:
            post_lock_doc = db.model_locks.find_one({"ticker": ticker})
            post_now = datetime.now(timezone.utc)
            post_expires = post_lock_doc.get("expires_at") if post_lock_doc else None
            if post_expires and post_expires.tzinfo is None:
                post_expires = post_expires.replace(tzinfo=timezone.utc)

            if post_lock_doc and post_lock_doc.get("owner_id") == owner_id and post_expires > post_now:
                sync_manifest(db, ticker, owner_id)

                verify_active = db.model_registry.find_one({"ticker": ticker, "status": "ACTIVE"})
                verify_manifest = read_active_manifest(ticker)

                if (verify_active and verify_manifest and
                    verify_active["version"] == verify_manifest.get("model_version")):
                    return True
                return False
            else:
                logger.error("CRITICAL: Lost lock ownership after filesystem replacement (post-promotion race detected)!")
                return False

        return True

    finally:
        db.model_locks.delete_one({
            "ticker": ticker,
            "owner_id": owner_id
        })

def sync_manifest(db, ticker: str, owner_id: Optional[str] = None) -> bool:
    """Reconciles the filesystem manifest with the intended MongoDB registry state.
    Used for explicitly resolving split-brain scenarios."""
    active_records = list(db.model_registry.find({"ticker": ticker, "status": "ACTIVE"}))
    
    if len(active_records) > 1:
        logger.error(f"CRITICAL: Multiple ACTIVE records found for {ticker}")
        return False
        
    active_record = active_records[0] if active_records else None

    # State A/C Check: No-Op if manifest is already correct
    current_manifest = read_active_manifest(ticker)
    
    if not active_record:
        if not current_manifest:
            return True # Both missing, correct
        # Missing active record but manifest exists -> need repair
    else:
        # Check if perfectly matches
        if current_manifest:
            if (current_manifest.get("model_version") == active_record.get("version") and
                current_manifest.get("model_hash") == active_record.get("model_hash") and
                current_manifest.get("feature_hash") == active_record.get("feature_hash") and
                current_manifest.get("feature_pipeline_version") == active_record.get("feature_pipeline_version") and
                current_manifest.get("feature_pipeline_hash") == active_record.get("feature_pipeline_hash")):
                return True

    # Needs repair. Acquire lock if we don't have one.
    import uuid
    from datetime import timedelta
    
    acquired_lock = False
    lock_owner = owner_id
    
    if lock_owner is None:
        lock_owner = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=120)
        try:
            db.model_locks.insert_one({
                "ticker": ticker,
                "owner_id": lock_owner,
                "expires_at": expires_at
            })
            acquired_lock = True
        except pymongo.errors.DuplicateKeyError:
            res = db.model_locks.find_one_and_update(
                {"ticker": ticker, "expires_at": {"$lt": now}},
                {"$set": {"owner_id": lock_owner, "expires_at": expires_at}},
                return_document=pymongo.ReturnDocument.AFTER
            )
            acquired_lock = res is not None
            
        if not acquired_lock:
            logger.warning(f"Failed to acquire reconciliation lock for {ticker}")
            return False

    try:
        # Re-read active record in case it changed while waiting for lock
        active_records = list(db.model_registry.find({"ticker": ticker, "status": "ACTIVE"}))
        if len(active_records) > 1:
            logger.error(f"CRITICAL: Multiple ACTIVE records found for {ticker}")
            return False
            
        active_record = active_records[0] if active_records else None

        if not active_record:
            path = get_active_manifest_path(ticker)
            if os.path.exists(path):
                os.remove(path)
            return True

        req_fields = ["version", "model_hash", "feature_hash", "feature_pipeline_version", "feature_pipeline_hash"]
        for f in req_fields:
            if not active_record.get(f):
                logger.error(f"Missing field {f} in ACTIVE record for {ticker}")
                return False

        version = active_record["version"]
        model_hash = active_record["model_hash"]
        feature_hash = active_record["feature_hash"]
        feature_pipeline_version = active_record["feature_pipeline_version"]
        feature_pipeline_hash = active_record["feature_pipeline_hash"]

        if feature_pipeline_version != "v1":
            logger.error(f"Invalid pipeline version {feature_pipeline_version} for {ticker}")
            return False
            
        canonical_hash = get_feature_pipeline_hash("v1")
        if feature_pipeline_hash != canonical_hash:
            logger.error(f"Invalid pipeline hash {feature_pipeline_hash} for {ticker}")
            return False

        is_valid = validate_bundle(
            ticker,
            version,
            model_hash,
            feature_hash
        )

        if not is_valid:
            logger.error(f"Bundle validation failed for {ticker}")
            return False

        manifest_data = {
            "ticker": ticker,
            "model_version": version,
            "model_hash": model_hash,
            "feature_hash": feature_hash,
            "feature_pipeline_version": feature_pipeline_version,
            "feature_pipeline_hash": feature_pipeline_hash,
            "dataset_hash": active_record.get("dataset_hash", "LEGACY_UNAVAILABLE"),
            "dataset_row_count": active_record.get("dataset_row_count"),
            "dataset_date_start": active_record.get("dataset_date_start"),
            "dataset_date_end": active_record.get("dataset_date_end"),
            "target_definition": active_record.get("target_definition"),
            "provenance_status": active_record.get("provenance_status", "LEGACY_UNAVAILABLE"),
            "f1_macro": active_record.get("metrics", {}).get("f1_macro", 0.0),
            "promoted_at": active_record.get("promoted_at", datetime.now(timezone.utc).isoformat())
        }

        try:
            update_manifest_atomically(ticker, manifest_data)
        except Exception as e:
            logger.error(f"Failed to write manifest for {ticker}: {e}")
            return False
            
        # Post-repair verification
        re_read = read_active_manifest(ticker)
        if not re_read:
            logger.error(f"Failed to re-read manifest after write for {ticker}")
            return False
            
        if (re_read.get("model_version") != version or
            re_read.get("model_hash") != model_hash or
            re_read.get("feature_hash") != feature_hash or
            re_read.get("feature_pipeline_version") != feature_pipeline_version or
            re_read.get("feature_pipeline_hash") != feature_pipeline_hash):
            logger.error(f"Manifest identity mismatch after write for {ticker}")
            return False

        return True
        
    finally:
        if acquired_lock:
            db.model_locks.delete_one({"ticker": ticker, "owner_id": lock_owner})

def reconcile_all_manifests(db) -> bool:
    """
    Reconciles the filesystem active manifests against the authoritative 
    MongoDB registry state for all canonical tickers.
    """
    success = True
    for ticker in TICKERS:
        if not sync_manifest(db, ticker):
            logger.error(f"Reconciliation failed for {ticker}")
            success = False
    return success
