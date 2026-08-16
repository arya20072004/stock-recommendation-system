import pytest
import numpy as np
import mongomock
import pymongo
from unittest.mock import MagicMock
import pandas as pd

from src.ml.model_utils import compute_provenance_hash
from src.ml.history import load_active_bundle, generate_and_persist_predictions
from scripts.migrate_phase15 import migrate_legacy_predictions

# ------------------------------------------------------------------
# Setup / Teardown
# ------------------------------------------------------------------

@pytest.fixture(scope="module")
def mongo_client():
    client = mongomock.MongoClient()
    yield client
    client.close()

# ------------------------------------------------------------------
# B — Hash integrity
# ------------------------------------------------------------------

def test_hash_integrity():
    payload = {
        "symbol": "RELIANCE.NS",
        "market_date": "2026-08-01",
        "prediction_horizon": 10,
        "model_version": "abc123def456",
        "feature_pipeline_version": "v1",
        "feature_pipeline_hash": "hash_xyz",
        "feature_columns": ["f1", "f2"],
        "raw_inputs": {"close": 100.5, "open": np.float64(99.0), "missing": np.nan, "inf": np.inf},
        "features": {"f1": np.float32(1.5), "f2": 2}
    }
    
    hash1 = compute_provenance_hash(payload)
    hash2 = compute_provenance_hash(payload)
    assert hash1 == hash2, "identical payload -> identical hash"
    
    # Changed feature
    payload_feat = payload.copy()
    payload_feat["features"] = {"f1": 1.500000000001, "f2": 2}
    assert compute_provenance_hash(payload_feat) != hash1, "changed feature -> changed hash"
    
    # Changed raw input
    payload_raw = payload.copy()
    payload_raw["raw_inputs"] = {"close": 100.5, "open": 99.0, "missing": None, "inf": "Infinity", "extra": 1}
    assert compute_provenance_hash(payload_raw) != hash1, "changed raw input -> changed hash"
    
    # Changed model version
    payload_model = payload.copy()
    payload_model["model_version"] = "diff123"
    assert compute_provenance_hash(payload_model) != hash1, "changed model version -> changed hash"

# ------------------------------------------------------------------
# E — Atomicity
# ------------------------------------------------------------------

def test_atomicity(mongo_client, monkeypatch):
    db = mongo_client["stock_market_db"]
    
    # Mock load_active_bundle to return mock components
    class MockModel:
        feature_importances_ = [0.5, 0.5]
        def predict_proba(self, X):
            return np.array([[0.1, 0.2, 0.7]])
            
    class MockEngineering:
        TICKER_CLASS_THRESHOLDS = {"RELIANCE.NS": {0: 0.33, 1: 0.33, 2: 0.20}}
        @staticmethod
        def apply_threshold_calibration(proba, thresholds):
            return 2
        @staticmethod
        def get_target_return_threshold(ticker, atr):
            return 0.05
        @staticmethod
        def build_feature_row(ticker, client, db):
            df = pd.DataFrame({
                "f1": [1.0], "f2": [2.0], "close": [100.0], "atr_pct": [0.05]
            }, index=pd.DatetimeIndex(["2026-08-10"]))
            return df

    def mock_load(ticker):
        return MockModel(), ["f1", "f2"], "test_ver", MockEngineering(), "v1", "hash1", 0.45
        
    import src.ml.history
    monkeypatch.setattr(src.ml.history, "load_active_bundle", mock_load)
    monkeypatch.setattr(src.ml.history, "TICKERS", ["RELIANCE.NS"])
    
    from datetime import date
    with pytest.raises((RuntimeError, pymongo.errors.OperationFailure)):
        generate_and_persist_predictions(mongo_client, target_market_date=date(2026, 8, 10))
        
    # Verify no half-pair exists
    assert db.prediction_history.count_documents({}) == 0
    assert db.prediction_provenance.count_documents({}) == 0

# ------------------------------------------------------------------
# G — Migration
# ------------------------------------------------------------------

def test_migration(mongo_client):
    db = mongo_client["stock_market_db"]
    
    # Insert legacy record
    db.prediction_history.insert_one({
        "symbol": "TCS.NS",
        "market_date": "2020-01-01",
        "prediction_horizon": 10,
        "status": "EVALUATED"
    })
    
    # Run migration
    migrate_legacy_predictions(mongo_client, apply=True)
    
    # Verify legacy updated
    doc = db.prediction_history.find_one({"symbol": "TCS.NS"})
    assert doc["provenance_status"] == "LEGACY_UNAVAILABLE"
    assert doc["provenance_hash"] == "LEGACY_UNAVAILABLE"
    
    # Idempotent rerun
    migrate_legacy_predictions(mongo_client, apply=True)
    doc2 = db.prediction_history.find_one({"symbol": "TCS.NS"})
    assert doc2["provenance_status"] == "LEGACY_UNAVAILABLE"
    
# ------------------------------------------------------------------
# A, C, D, F — Full pipeline integration
# ------------------------------------------------------------------

def test_provenance_integration(mongo_client, monkeypatch):
    db = mongo_client["stock_market_db"]
    
    # Mock load_active_bundle to return mock components
    class MockModel:
        feature_importances_ = [0.5, 0.5]
        def predict_proba(self, X):
            return np.array([[0.1, 0.2, 0.7]])
            
    class MockEngineering:
        TICKER_CLASS_THRESHOLDS = {"RELIANCE.NS": {0: 0.33, 1: 0.33, 2: 0.20}}
        @staticmethod
        def apply_threshold_calibration(proba, thresholds):
            return 2
        @staticmethod
        def get_target_return_threshold(ticker, atr):
            return 0.05
        @staticmethod
        def build_feature_row(ticker, client, db):
            df = pd.DataFrame({
                "f1": [1.0], "f2": [2.0], "close": [100.0], "atr_pct": [0.05]
            }, index=pd.DatetimeIndex(["2026-08-10"]))
            return df

    def mock_load(ticker):
        return MockModel(), ["f1", "f2"], "test_ver", MockEngineering(), "v1", "hash1", 0.45
        
    import src.ml.history
    monkeypatch.setattr(src.ml.history, "load_active_bundle", mock_load)
    
    # Also patch TICKERS to only run for one ticker to speed up
    monkeypatch.setattr(src.ml.history, "TICKERS", ["RELIANCE.NS"])
    
    # Patch mongomock to ignore session argument since it doesn't support it
    orig_update = mongomock.Collection.update_one
    def mock_update(self, filter, update, **kwargs):
        kwargs.pop("session", None)
        return orig_update(self, filter, update, **kwargs)
        
    orig_insert = mongomock.Collection.insert_one
    def mock_insert(self, document, **kwargs):
        kwargs.pop("session", None)
        return orig_insert(self, document, **kwargs)
        
    orig_find_one = mongomock.Collection.find_one
    def mock_find_one(self, filter, **kwargs):
        kwargs.pop("session", None)
        return orig_find_one(self, filter, **kwargs)
        
    monkeypatch.setattr(mongomock.Collection, "update_one", mock_update)
    monkeypatch.setattr(mongomock.Collection, "insert_one", mock_insert)
    monkeypatch.setattr(mongomock.Collection, "find_one", mock_find_one)
    
    # Generate predictions using the mocked pipeline
    # Note: Because mongomock doesn't support transactions, we need to mock start_session!
    class MockSession:
        def start_transaction(self):
            class DummyCtx:
                def __enter__(self): pass
                def __exit__(self, *args): pass
            return DummyCtx()
        def __enter__(self): return self
        def __exit__(self, *args): pass
    
    monkeypatch.setattr(mongo_client, "start_session", lambda *args, **kwargs: MockSession())
    
    from datetime import date
    generate_and_persist_predictions(mongo_client, target_market_date=date(2026, 8, 10))
        
    hist_docs = list(db.prediction_history.find({"symbol": "RELIANCE.NS"}))
    assert len(hist_docs) == 1
        
    doc = hist_docs[0]
    symbol = doc["symbol"]
    market_date = doc["market_date"]
    horizon = doc["prediction_horizon"]
    
    prov_doc = db.prediction_provenance.find_one({
        "symbol": symbol,
        "market_date": market_date,
        "prediction_horizon": horizon
    })
    
    # A - Provenance generation
    assert prov_doc is not None, "Complete provenance must be persisted"
    assert "raw_inputs" in prov_doc and len(prov_doc["raw_inputs"]) > 0
    assert "features" in prov_doc and len(prov_doc["features"]) > 0
    assert "feature_columns" in prov_doc and len(prov_doc["feature_columns"]) > 0
    assert "model_version" in prov_doc
    assert "feature_pipeline_version" in prov_doc
    assert "feature_pipeline_hash" in prov_doc
    
    # D - Immutability and Idempotency
    # Retrying should succeed idempotently
    generate_and_persist_predictions(mongo_client, target_market_date=date(2026, 8, 10))
    assert db.prediction_history.count_documents({"symbol": "RELIANCE.NS"}) == 1
    
    # Modifying hash should cause integrity error
    db.prediction_provenance.update_one(
        {"_id": prov_doc["_id"]},
        {"$set": {"provenance_hash": "CONFLICTING_HASH"}}
    )
    
    with pytest.raises(pymongo.errors.OperationFailure) as excinfo:
        generate_and_persist_predictions(mongo_client, target_market_date=date(2026, 8, 10))
    
    assert "Provenance collision" in str(excinfo.value)
