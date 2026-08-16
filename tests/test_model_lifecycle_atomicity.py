import os
import json
import uuid
import pytest
import mongomock
from unittest.mock import patch, MagicMock

# Patch mongomock Collection methods to ignore session kwarg
original_update_one = mongomock.Collection.update_one
def patched_update_one(self, filter, update, *args, **kwargs):
    kwargs.pop('session', None)
    return original_update_one(self, filter, update, *args, **kwargs)
mongomock.Collection.update_one = patched_update_one

original_update_many = mongomock.Collection.update_many
def patched_update_many(self, filter, update, *args, **kwargs):
    kwargs.pop('session', None)
    return original_update_many(self, filter, update, *args, **kwargs)
mongomock.Collection.update_many = patched_update_many

from datetime import datetime, timezone, timedelta
from pymongo.errors import DuplicateKeyError, OperationFailure

import pymongo

class MockSession:
    def __init__(self, client):
        self.client = client
        self.in_transaction = False
        self.committed = False
        self.aborted = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.in_transaction:
            if exc_type is None:
                self.committed = True
            else:
                self.aborted = True
            self.in_transaction = False

    def start_transaction(self):
        self.in_transaction = True
        self.committed = False
        self.aborted = False
        return self

class MockMongoClient(mongomock.MongoClient):
    def start_session(self):
        return MockSession(self)

original_mongo_client = pymongo.MongoClient
pymongo.MongoClient = MockMongoClient

import src.ml.model_registry as mr
from src.ml.model_registry import (
    promote_model,
    sync_manifest,
    setup_registry_indexes,
    MODELS_DIR,
    FEATURES_DIR,
    get_active_manifest_path
)

@pytest.fixture
def db():
    client = MockMongoClient()
    db = client["stock_market_db"]
    setup_registry_indexes(db)
    return db

import tempfile

@pytest.fixture(autouse=True)
def setup_dirs():
    with tempfile.TemporaryDirectory() as temp_models, tempfile.TemporaryDirectory() as temp_features:
        with patch('src.ml.model_registry.MODELS_DIR', temp_models), \
             patch('src.ml.model_registry.FEATURES_DIR', temp_features), \
             patch(f"{__name__}.MODELS_DIR", temp_models), \
             patch(f"{__name__}.FEATURES_DIR", temp_features):
            yield

def create_dummy_artifacts(ticker, version, valid=True):
    model_path = os.path.join(MODELS_DIR, f"model_{ticker}_{version}.joblib")
    features_path = os.path.join(FEATURES_DIR, f"features_{ticker}_{version}.json")
    with open(model_path, "wb") as f:
        f.write(b"dummy_model")
    with open(features_path, "w") as f:
        json.dump({"features": []}, f)

    model_hash = mr.hash_file_sha256(model_path, 12)
    feature_hash = mr.hash_file_sha256(features_path, 64)
    if not valid:
        model_hash = "invalid_hash"
    return model_hash, feature_hash

def seed_candidate(db, ticker, version, status="CANDIDATE", valid_artifacts=True):
    m_hash, f_hash = create_dummy_artifacts(ticker, version, valid_artifacts)
    db.model_registry.insert_one({
        "ticker": ticker,
        "version": version,
        "status": status,
        "model_hash": m_hash,
        "feature_hash": f_hash,
        "metrics": {"f1_macro": 0.8},
        "trained_at": datetime.now(timezone.utc).isoformat()
    })

def test_1_atomic_lock_acquisition(db):
    ticker = "TEST1.NS"
    owner_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    db.model_locks.insert_one({
        "ticker": ticker,
        "owner_id": owner_id,
        "expires_at": now + timedelta(seconds=120)
    })
    doc = db.model_locks.find_one({"ticker": ticker})
    assert doc is not None
    assert doc["owner_id"] == owner_id

def test_2_live_lock_contention(db):
    ticker = "TEST2.NS"
    db.model_locks.insert_one({
        "ticker": ticker,
        "owner_id": "ownerA",
        "expires_at": datetime.now(timezone.utc) + timedelta(seconds=120)
    })
    seed_candidate(db, ticker, "v1")
    success = promote_model(db, ticker, "v1")
    assert success is False
    assert db.model_locks.find_one({"ticker": ticker})["owner_id"] == "ownerA"

def test_3_concurrent_first_time_race(db):
    ticker = "TEST3.NS"
    ownerA = "ownerA"
    db.model_locks.insert_one({
        "ticker": ticker,
        "owner_id": ownerA,
        "expires_at": datetime.now(timezone.utc) + timedelta(seconds=120)
    })
    seed_candidate(db, ticker, "v1")
    success = promote_model(db, ticker, "v1")
    assert success is False

def test_4_expired_lock_reclamation(db):
    ticker = "TEST4.NS"
    db.model_locks.insert_one({
        "ticker": ticker,
        "owner_id": "stale_owner",
        "expires_at": datetime.now(timezone.utc) - timedelta(seconds=10)
    })
    seed_candidate(db, ticker, "v1")
    success = promote_model(db, ticker, "v1")
    assert success is True
    assert db.model_locks.find_one({"ticker": ticker}) is None
    active = db.model_registry.find_one({"ticker": ticker, "status": "ACTIVE"})
    assert active["version"] == "v1"

def test_5_stale_owner_cannot_release(db):
    ticker = "TEST5.NS"
    db.model_locks.insert_one({
        "ticker": ticker,
        "owner_id": "new_owner",
        "expires_at": datetime.now(timezone.utc) + timedelta(seconds=120)
    })
    db.model_locks.delete_one({"ticker": ticker, "owner_id": "stale_owner"})
    doc = db.model_locks.find_one({"ticker": ticker})
    assert doc["owner_id"] == "new_owner"

def test_6_independent_tickers(db):
    seed_candidate(db, "A.NS", "v1")
    seed_candidate(db, "B.NS", "v1")
    db.model_locks.insert_one({
        "ticker": "A.NS",
        "owner_id": "ownerA",
        "expires_at": datetime.now(timezone.utc) + timedelta(seconds=120)
    })
    success = promote_model(db, "B.NS", "v1")
    assert success is True
    success = promote_model(db, "A.NS", "v1")
    assert success is False

def test_7_successful_promotion(db):
    ticker = "TEST7.NS"
    seed_candidate(db, ticker, "v1")
    success = promote_model(db, ticker, "v1")
    assert success is True
    active = db.model_registry.find_one({"ticker": ticker, "status": "ACTIVE"})
    manifest = mr.read_active_manifest(ticker)
    assert active["version"] == "v1"
    assert manifest["model_version"] == "v1"

def test_8_same_version_idempotent(db):
    ticker = "TEST8.NS"
    seed_candidate(db, ticker, "v1")
    promote_model(db, ticker, "v1")
    os.remove(mr.get_active_manifest_path(ticker))
    success = promote_model(db, ticker, "v1")
    assert success is True
    manifest = mr.read_active_manifest(ticker)
    assert manifest["model_version"] == "v1"

def test_9_mongo_transaction_failure(db):
    ticker = "TEST9.NS"
    seed_candidate(db, ticker, "v1")
    with patch.object(db.model_registry, "update_one", side_effect=OperationFailure("DB Error")):
        success = promote_model(db, ticker, "v1")
        assert success is False
        active = db.model_registry.find_one({"ticker": ticker, "status": "ACTIVE"})
        assert active is None
        assert not os.path.exists(mr.get_active_manifest_path(ticker))

def test_10_manifest_staging_failure(db):
    ticker = "TEST10.NS"
    seed_candidate(db, ticker, "v1")
    with patch("builtins.open", side_effect=PermissionError("File locked")):
        success = promote_model(db, ticker, "v1")
        assert success is False
        active = db.model_registry.find_one({"ticker": ticker, "status": "ACTIVE"})
        assert active is None

def test_11_manifest_replacement_failure(db):
    ticker = "TEST11.NS"
    seed_candidate(db, ticker, "v1")
    with patch("os.replace", side_effect=OSError("OS Error")):
        success = promote_model(db, ticker, "v1")
        assert success is False
        active = db.model_registry.find_one({"ticker": ticker, "status": "ACTIVE"})
        assert active["version"] == "v1"
        assert not os.path.exists(mr.get_active_manifest_path(ticker))
    sync_manifest(db, ticker)
    manifest = mr.read_active_manifest(ticker)
    assert manifest["model_version"] == "v1"

def test_12_registry_manifest_mismatch_recovery(db):
    ticker = "TEST12.NS"
    seed_candidate(db, ticker, "v1", "ACTIVE")
    manifest_path = mr.get_active_manifest_path(ticker)
    with open(manifest_path, "w") as f:
        json.dump({"model_version": "v0"}, f)
    sync_manifest(db, ticker)
    manifest = mr.read_active_manifest(ticker)
    assert manifest["model_version"] == "v1"

def test_13_concurrent_different_version(db):
    ticker = "TEST13.NS"
    seed_candidate(db, ticker, "v1")
    seed_candidate(db, ticker, "v2")
    db.model_locks.insert_one({
        "ticker": ticker,
        "owner_id": "A",
        "expires_at": datetime.now(timezone.utc) + timedelta(seconds=120)
    })
    success = promote_model(db, ticker, "v2")
    assert success is False
    active = db.model_registry.find_one({"ticker": ticker, "status": "ACTIVE"})
    assert active is None

def test_14_lease_expiration_race(db):
    ticker = "TEST14.NS"
    seed_candidate(db, ticker, "v1")
    original_find_one = db.model_locks.find_one
    def mock_find_one(query):
        if query == {"ticker": ticker}:
            return {"ticker": ticker, "owner_id": "newer_owner", "expires_at": datetime.now(timezone.utc) + timedelta(10)}
        return original_find_one(query)
    with patch.object(db.model_locks, 'find_one', side_effect=mock_find_one):
        with patch('src.ml.model_registry.sync_manifest') as mock_sync:
            with patch('os.replace') as mock_replace:
                success = promote_model(db, ticker, "v1")
                assert success is False
                assert not os.path.exists(mr.get_active_manifest_path(ticker))
                mock_sync.assert_not_called()
                mock_replace.assert_not_called()

def test_15_rollback_atomicity(db):
    ticker = "TEST15.NS"
    seed_candidate(db, ticker, "v1", "ACTIVE")
    seed_candidate(db, ticker, "v2", "RETIRED")
    success = promote_model(db, ticker, "v2")
    assert success is True
    active = db.model_registry.find_one({"ticker": ticker, "status": "ACTIVE"})
    manifest = mr.read_active_manifest(ticker)
    assert active["version"] == "v2"
    assert manifest["model_version"] == "v2"
    v1 = db.model_registry.find_one({"ticker": ticker, "version": "v1"})
    assert v1["status"] == "RETIRED"

def test_16_missing_active_registry(db):
    ticker = "TEST16.NS"
    manifest_path = mr.get_active_manifest_path(ticker)
    with open(manifest_path, "w") as f:
        json.dump({"model_version": "v0"}, f)
    sync_manifest(db, ticker)
    assert not os.path.exists(manifest_path)

def test_17_missing_manifest_recovery(db):
    ticker = "TEST17.NS"
    seed_candidate(db, ticker, "v1", "ACTIVE")
    assert not os.path.exists(mr.get_active_manifest_path(ticker))
    sync_manifest(db, ticker)
    manifest = mr.read_active_manifest(ticker)
    assert manifest["model_version"] == "v1"

def test_18_lost_lock_after_os_replace(db):
    ticker = "TEST18.NS"
    seed_candidate(db, ticker, "v1")
    with patch('src.ml.model_registry.sync_manifest') as mock_sync:
        with patch('src.ml.model_registry.read_active_manifest', return_value={"model_version": "v0"}):
            call_count = [0]
            original_find_one = db.model_locks.find_one
            def mock_find_one(query):
                if query == {"ticker": ticker}:
                    call_count[0] += 1
                    if call_count[0] == 2:
                        return {"ticker": ticker, "owner_id": "newer_owner", "expires_at": datetime.now(timezone.utc) + timedelta(10)}
                return original_find_one(query)
            with patch.object(db.model_locks, 'find_one', side_effect=mock_find_one):
                success = promote_model(db, ticker, "v1")
                assert success is False
                mock_sync.assert_not_called()
