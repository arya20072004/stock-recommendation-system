import pytest
import os
import json
import tempfile
import hashlib
from src.ml.history import load_active_bundle
from src.features.router import get_feature_pipeline_hash

@pytest.fixture
def mock_directories(monkeypatch):
    temp_models = tempfile.mkdtemp()
    temp_features = tempfile.mkdtemp()
    
    monkeypatch.setattr("src.ml.history.MODELS_DIR", temp_models)
    monkeypatch.setattr("src.ml.history.FEATURES_DIR", temp_features)
    
    return temp_models, temp_features

def test_missing_manifest(mock_directories):
    with pytest.raises(FileNotFoundError, match="Active manifest missing"):
        load_active_bundle("RELIANCE.NS")

def test_missing_artifacts(mock_directories):
    temp_models, temp_features = mock_directories
    ticker = "RELIANCE.NS"
    manifest_path = os.path.join(temp_models, f"{ticker}_active.json")
    with open(manifest_path, "w") as f:
        json.dump({
            "model_version": "v1",
            "model_hash": "h1",
            "feature_hash": "h2",
            "feature_pipeline_version": "v1",
            "feature_pipeline_hash": get_feature_pipeline_hash("v1"),
            "dataset_hash": "LEGACY_UNAVAILABLE",
            "provenance_status": "LEGACY_UNAVAILABLE",
            "promoted_at": "2026-08-09T00:00:00Z"
        }, f)
        
    with pytest.raises(FileNotFoundError, match="Model artifact missing"):
        load_active_bundle(ticker)

def test_hash_mismatch(mock_directories):
    temp_models, temp_features = mock_directories
    ticker = "RELIANCE.NS"
    version = "v1"
    
    manifest_path = os.path.join(temp_models, f"{ticker}_active.json")
    with open(manifest_path, "w") as f:
        json.dump({
            "model_version": version,
            "model_hash": "expected_m",
            "feature_hash": "expected_f",
            "feature_pipeline_version": "v1",
            "feature_pipeline_hash": get_feature_pipeline_hash("v1"),
            "dataset_hash": "LEGACY_UNAVAILABLE",
            "provenance_status": "LEGACY_UNAVAILABLE",
            "promoted_at": "2026-08-09T00:00:00Z"
        }, f)
        
    model_path = os.path.join(temp_models, f"model_{ticker}_{version}.joblib")
    with open(model_path, "wb") as f:
        f.write(b"data")
        
    with pytest.raises(ValueError, match="Model hash mismatch"):
        load_active_bundle(ticker)

def test_valid_bundle(mock_directories, monkeypatch):
    temp_models, temp_features = mock_directories
    ticker = "RELIANCE.NS"
    version = "v1"
    
    model_path = os.path.join(temp_models, f"model_{ticker}_{version}.joblib")
    with open(model_path, "wb") as f:
        f.write(b"data")
    actual_m = hashlib.sha256(b"data").hexdigest()[:12]
        
    features_path = os.path.join(temp_features, f"features_{ticker}_{version}.json")
    with open(features_path, "wb") as f:
        f.write(b'["f1", "f2"]')
    actual_f = hashlib.sha256(b'["f1", "f2"]').hexdigest()[:64]
    
    manifest_path = os.path.join(temp_models, f"{ticker}_active.json")
    with open(manifest_path, "w") as f:
        json.dump({
            "model_version": version,
            "model_hash": actual_m,
            "feature_hash": actual_f,
            "feature_pipeline_version": "v1",
            "feature_pipeline_hash": get_feature_pipeline_hash("v1"),
            "dataset_hash": "LEGACY_UNAVAILABLE",
            "provenance_status": "LEGACY_UNAVAILABLE",
            "promoted_at": "2026-08-09T00:00:00Z"
        }, f)
        
    # Mock joblib.load so we don't need a real model
    import joblib
    monkeypatch.setattr(joblib, "load", lambda p: "mock_model")
    
    model, features, ver, eng_mod = load_active_bundle(ticker)
    assert model == "mock_model"
    assert features == ["f1", "f2"]
    assert ver == version
    assert hasattr(eng_mod, "build_feature_row")
