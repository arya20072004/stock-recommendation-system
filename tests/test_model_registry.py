import pytest
import os
import json
import tempfile
import hashlib
from datetime import datetime
from pymongo import MongoClient
import mongomock
from unittest.mock import patch, MagicMock
import pymongo

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

class MockSession:
    def __init__(self, client):
        self.client = client
        self.in_transaction = False
    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): pass
    def start_transaction(self): return self

class MockMongoClient(mongomock.MongoClient):
    def start_session(self):
        return MockSession(self)

from src.ml.model_registry import (
    register_candidate,
    promote_model,
    sync_manifest,
    setup_registry_indexes,
    get_active_manifest_path,
    validate_bundle
)

@pytest.fixture
def mock_db():
    client = MockMongoClient()
    db = client.stock_market_db
    setup_registry_indexes(db)
    return db

@pytest.fixture
def mock_directories(monkeypatch):
    temp_models = tempfile.mkdtemp()
    temp_features = tempfile.mkdtemp()

    monkeypatch.setattr("src.ml.model_registry.MODELS_DIR", temp_models)
    monkeypatch.setattr("src.ml.model_registry.FEATURES_DIR", temp_features)

    return temp_models, temp_features

def create_mock_artifacts(ticker, version, temp_models, temp_features):
    model_path = os.path.join(temp_models, f"model_{ticker}_{version}.joblib")
    features_path = os.path.join(temp_features, f"features_{ticker}_{version}.json")

    with open(model_path, "wb") as f:
        f.write(b"mock_model_data")
    with open(features_path, "wb") as f:
        f.write(b'["mock", "features"]')

    model_hash = hashlib.sha256(b"mock_model_data").hexdigest()[:12]
    feature_hash = hashlib.sha256(b'["mock", "features"]').hexdigest()[:64]

    return model_hash, feature_hash

def test_register_candidate(mock_db):
    register_candidate(mock_db, "TEST", "v1", "hash1", "hash2", {})
    record = mock_db.model_registry.find_one({"ticker": "TEST"})
    assert record is not None
    assert record["status"] == "CANDIDATE"
    assert record["model_hash"] == "hash1"

def test_promote_candidate(mock_db, mock_directories):
    temp_models, temp_features = mock_directories
    ticker = "TEST"
    version = "v1"

    model_hash, feature_hash = create_mock_artifacts(ticker, version, temp_models, temp_features)

    register_candidate(mock_db, ticker, version, model_hash, feature_hash, {})

    success = promote_model(mock_db, ticker, version)
    assert success is True

    # Check DB
    record = mock_db.model_registry.find_one({"ticker": ticker, "version": version})
    assert record["status"] == "ACTIVE"

    # Check Manifest
    manifest_path = os.path.join(temp_models, f"{ticker}_active.json")
    assert os.path.exists(manifest_path)

    with open(manifest_path, "r") as f:
        manifest = json.load(f)

    assert manifest["model_version"] == version
    assert manifest["model_hash"] == model_hash
    assert manifest["feature_hash"] == feature_hash

def test_promote_invalid_candidate(mock_db, mock_directories):
    temp_models, temp_features = mock_directories
    # missing artifacts
    register_candidate(mock_db, "TEST2", "v2", "h1", "h2", {})
    success = promote_model(mock_db, "TEST2", "v2")
    assert success is False

    record = mock_db.model_registry.find_one({"ticker": "TEST2"})
    assert record["status"] == "CANDIDATE"

def test_rollback(mock_db, mock_directories):
    temp_models, temp_features = mock_directories
    ticker = "TEST"

    # Create v1 and v2
    h1_m, h1_f = create_mock_artifacts(ticker, "v1", temp_models, temp_features)
    register_candidate(mock_db, ticker, "v1", h1_m, h1_f, {})
    promote_model(mock_db, ticker, "v1")

    h2_m, h2_f = create_mock_artifacts(ticker, "v2", temp_models, temp_features)
    register_candidate(mock_db, ticker, "v2", h2_m, h2_f, {})
    promote_model(mock_db, ticker, "v2")

    # Check v1 is retired, v2 is active
    assert mock_db.model_registry.find_one({"version": "v1"})["status"] == "RETIRED"
    assert mock_db.model_registry.find_one({"version": "v2"})["status"] == "ACTIVE"

    # Rollback to v1
    success = promote_model(mock_db, ticker, "v1")
    assert success is True

    assert mock_db.model_registry.find_one({"version": "v1"})["status"] == "ACTIVE"
    assert mock_db.model_registry.find_one({"version": "v2"})["status"] == "RETIRED"

    # Manifest updated
    with open(os.path.join(temp_models, f"{ticker}_active.json"), "r") as f:
        manifest = json.load(f)
    assert manifest["model_version"] == "v1"

def test_sync_manifest(mock_db, mock_directories):
    temp_models, temp_features = mock_directories
    ticker = "SYNC"

    hm, hf = create_mock_artifacts(ticker, "v1", temp_models, temp_features)
    mock_db.model_registry.insert_one({
        "ticker": ticker, "version": "v1", "status": "ACTIVE",
        "model_hash": hm, "feature_hash": hf
    })

    assert not os.path.exists(os.path.join(temp_models, f"{ticker}_active.json"))

    sync_manifest(mock_db, ticker)

    assert os.path.exists(os.path.join(temp_models, f"{ticker}_active.json"))
