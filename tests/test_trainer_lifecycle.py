import pytest
import os
import tempfile
import json
import hashlib
from datetime import datetime, timezone
import pandas as pd
from unittest.mock import patch, MagicMock

import joblib
import mongomock

# The module under test
import src.ml.trainer as trainer
from src.ml.trainer import train_model
from src.features.router import get_feature_pipeline_hash

@pytest.fixture
def mock_db():
    client = mongomock.MongoClient()
    # set up index as required
    client.stock_market_db.model_registry.create_index(
        [("ticker", 1)], unique=True, partialFilterExpression={"status": "ACTIVE"}
    )
    return client

@pytest.fixture
def isolated_dirs(monkeypatch):
    temp_models = tempfile.mkdtemp()
    temp_features = tempfile.mkdtemp()
    
    monkeypatch.setattr(trainer, "MODELS_DIR", temp_models)
    monkeypatch.setattr(trainer, "FEATURES_DIR", temp_features)
    
    return temp_models, temp_features

@patch('src.ml.trainer.optuna')
@patch('src.ml.trainer.XGBClassifier')
def test_trainer_lifecycle_safe_registration(mock_xgb, mock_optuna, mock_db, isolated_dirs):
    """
    Test that training serialization, hashing, versioning, and candidate registration
    occur safely without overwriting the ACTIVE model or manifest.
    """
    temp_models, temp_features = isolated_dirs
    ticker = "RELIANCE.NS"
    old_version = "old_v1"
    
    # 1. Establish an existing ACTIVE model and manifest
    old_model_path = os.path.join(temp_models, f"model_{ticker}_{old_version}.joblib")
    old_features_path = os.path.join(temp_features, f"features_{ticker}_{old_version}.json")
    manifest_path = os.path.join(temp_models, f"{ticker}_active.json")
    
    with open(old_model_path, "wb") as f:
        f.write(b"old_model_data")
    with open(old_features_path, "w") as f:
        json.dump(["f1"], f)
        
    old_model_hash = hashlib.sha256(b"old_model_data").hexdigest()[:12]
    old_feature_hash = hashlib.sha256(json.dumps(["f1"]).encode()).hexdigest()[:64]
    
    manifest_content = {
        "ticker": ticker,
        "model_version": old_version,
        "model_hash": old_model_hash,
        "feature_hash": old_feature_hash,
        "feature_pipeline_version": "v1",
        "feature_pipeline_hash": get_feature_pipeline_hash("v1"),
        "dataset_hash": "LEGACY_UNAVAILABLE",
        "provenance_status": "LEGACY_UNAVAILABLE",
        "promoted_at": datetime.now(timezone.utc).isoformat()
    }
    with open(manifest_path, "w") as f:
        json.dump(manifest_content, f)
        
    mock_db.stock_market_db.model_registry.insert_one({
        "ticker": ticker,
        "version": old_version,
        "status": "ACTIVE",
        "model_hash": old_model_hash,
        "feature_hash": old_feature_hash,
        "feature_pipeline_version": "v1",
        "feature_pipeline_hash": get_feature_pipeline_hash("v1"),
        "dataset_hash": "LEGACY_UNAVAILABLE",
        "provenance_status": "LEGACY_UNAVAILABLE"
    })
    
    # 2. Mock external training dependencies
    mock_study = MagicMock()
    mock_study.best_params = {}
    mock_study.best_value = 0.8
    mock_optuna.create_study.return_value = mock_study
    
    mock_model_instance = MagicMock()
    mock_model_instance.predict_proba.return_value = pd.DataFrame({0: [0.1]*20, 1: [0.8]*20, 2: [0.1]*20}).values
    mock_model_instance.predict.return_value = [1] * 20
    mock_model_instance.feature_importances_ = [1.0]
    
    # Crucial: when joblib.dump is called on best_model, we must write some mock bytes
    # so the sha256 function works in trainer.py
    mock_model_instance.__class__ = mock_xgb
    
    # Intercept joblib.dump to write actual bytes to the temporary path 
    # so the hash function doesn't fail on an empty file.
    original_dump = joblib.dump
    def mock_dump(model, filename, **kwargs):
        with open(filename, "wb") as f:
            f.write(b"new_model_data")
    
    with patch('src.ml.trainer.joblib.dump', side_effect=mock_dump):
        # Provide minimal DataFrame
        dates = pd.date_range("2024-01-01", periods=100)
        df = pd.DataFrame({
            "close": range(100),
            "target": [0, 1, 2] * 33 + [0],
            "f1": [0.1] * 100
        }, index=dates)
        
        # Override constants to avoid skipping CV
        with patch('src.ml.trainer.MIN_ROWS_AFTER_FEATURES', 10), \
             patch('src.ml.trainer.N_SPLITS', 2), \
             patch('src.ml.trainer._make_feature_list', return_value=['f1']):
            # Also mock the XGBClassifier instantiation and return
            mock_xgb.return_value = mock_model_instance
            
            # Execute Training
            train_model(df, ticker, mock_db)
            
    # 3. Assertions
    # A new CANDIDATE should be in the database
    candidates = list(mock_db.stock_market_db.model_registry.find({"ticker": ticker, "status": "CANDIDATE"}))
    assert len(candidates) == 1
    new_version = candidates[0]["version"]
    new_model_hash = candidates[0]["model_hash"]
    new_feature_hash = candidates[0]["feature_hash"]
    
    # 1,2,3. Artifacts exist
    new_model_path = os.path.join(temp_models, f"model_{ticker}_{new_version}.joblib")
    new_features_path = os.path.join(temp_features, f"features_{ticker}_{new_version}.json")
    new_metrics_path = os.path.join(temp_models, f"metrics_{ticker}_{new_version}.json")
    
    assert os.path.exists(new_model_path)
    assert os.path.exists(new_features_path)
    assert os.path.exists(new_metrics_path)
    
    # 4,5. Hashes match exactly
    with open(new_model_path, "rb") as f:
        actual_m_hash = hashlib.sha256(f.read()).hexdigest()[:12]
    assert actual_m_hash == new_model_hash
    
    with open(new_features_path, "rb") as f:
        actual_f_hash = hashlib.sha256(f.read()).hexdigest()[:64]
    assert actual_f_hash == new_feature_hash
    
    # 6. Candidate registry exists (verified above)
    
    # 7. Existing ACTIVE remains ACTIVE
    active_records = list(mock_db.stock_market_db.model_registry.find({"ticker": ticker, "status": "ACTIVE"}))
    assert len(active_records) == 1
    assert active_records[0]["version"] == old_version
    
    # 8. Active manifest remains identically unchanged
    with open(manifest_path, "r") as f:
        new_manifest_content = json.load(f)
    assert new_manifest_content == manifest_content
    
    # 9. Old model artifact byte-for-byte unchanged
    with open(old_model_path, "rb") as f:
        assert hashlib.sha256(f.read()).hexdigest()[:12] == old_model_hash

    # 10. Newly trained version is NOT ACTIVE
    assert new_version != old_version

@patch('src.ml.trainer.optuna')
@patch('src.ml.trainer.XGBClassifier')
def test_trainer_serialization_failure_safe(mock_xgb, mock_optuna, mock_db, isolated_dirs):
    """
    Test that a serialization failure during training does not overwrite
    the ACTIVE artifact or manifest, leaving the system in a clean state.
    """
    temp_models, temp_features = isolated_dirs
    ticker = "RELIANCE.NS"
    old_version = "old_v1"
    
    # Establish ACTIVE state
    old_model_path = os.path.join(temp_models, f"model_{ticker}_{old_version}.joblib")
    manifest_path = os.path.join(temp_models, f"{ticker}_active.json")
    
    with open(old_model_path, "wb") as f:
        f.write(b"active_bytes")
    
    manifest_content = {"model_version": old_version}
    with open(manifest_path, "w") as f:
        json.dump(manifest_content, f)
        
    mock_db.stock_market_db.model_registry.insert_one({"ticker": ticker, "status": "ACTIVE", "version": old_version})
    
    # Force failure on joblib.dump
    original_dump = joblib.dump
    def mock_dump_fail(model, filename, **kwargs):
        raise OSError("Disk full!")

    mock_model_instance = MagicMock()
    mock_model_instance.predict_proba.return_value = pd.DataFrame({0: [0.1]*20, 1: [0.8]*20, 2: [0.1]*20}).values
    mock_model_instance.predict.return_value = [1] * 20
    mock_model_instance.feature_importances_ = [1.0]
    mock_model_instance.__class__ = mock_xgb
    mock_xgb.return_value = mock_model_instance
        
    with patch('src.ml.trainer.joblib.dump', side_effect=mock_dump_fail):
        dates = pd.date_range("2024-01-01", periods=100)
        df = pd.DataFrame({"close": range(100), "target": [0,1,2]*33+[0], "f1": [0.1]*100}, index=dates)
        
        with patch('src.ml.trainer.MIN_ROWS_AFTER_FEATURES', 10), \
             patch('src.ml.trainer.N_SPLITS', 2), \
             patch('src.ml.trainer._make_feature_list', return_value=['f1']):
            
            # Expecting failure
            with pytest.raises(OSError, match="Disk full!"):
                train_model(df, ticker, mock_db)
                
    # Verifications
    # 1. No candidate was accidentally registered
    assert mock_db.stock_market_db.model_registry.count_documents({"status": "CANDIDATE"}) == 0
    
    # 2. ACTIVE state remains intact
    assert mock_db.stock_market_db.model_registry.count_documents({"status": "ACTIVE"}) == 1
    
    # 3. Artifact is untouched
    with open(old_model_path, "rb") as f:
        assert f.read() == b"active_bytes"
        
    # 4. Manifest untouched
    with open(manifest_path, "r") as f:
        assert json.load(f) == manifest_content
