import pytest
import os
import tempfile
import json
import hashlib
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import src.features.router as router
from src.features.router import get_feature_pipeline_hash, resolve_feature_pipeline
from src.ml.trainer import train_model, MODELS_DIR, FEATURES_DIR
from src.ml.history import load_active_bundle
from scripts.migrate_phase14 import run_migration

# ---------------------------------------------------------
# TEST GROUP A - FEATURE PIPELINE HASH
# ---------------------------------------------------------

def test_a1_deterministic_hash():
    hash1 = get_feature_pipeline_hash("v1")
    hash2 = get_feature_pipeline_hash("v1")
    assert hash1 == hash2
    assert len(hash1) == 64

def test_a2_line_ending_normalization(monkeypatch, tmp_path):
    # Create mock v1 directory
    v1_dir = tmp_path / "v1"
    v1_dir.mkdir()
    eng_file = v1_dir / "engineering.py"
    
    # Write LF
    eng_file.write_bytes(b"import os\n\nx = 1\n")
    monkeypatch.setattr(router, "__file__", str(tmp_path / "router.py"))
    
    hash_lf = get_feature_pipeline_hash("v1")
    
    # Write CRLF
    eng_file.write_bytes(b"import os\r\n\r\nx = 1\r\n")
    hash_crlf = get_feature_pipeline_hash("v1")
    
    assert hash_lf == hash_crlf

def test_a3_mathematical_modification_changes_identity(monkeypatch, tmp_path):
    v1_dir = tmp_path / "v1"
    v1_dir.mkdir()
    eng_file = v1_dir / "engineering.py"
    monkeypatch.setattr(router, "__file__", str(tmp_path / "router.py"))
    
    eng_file.write_text("RSI_WINDOW = 14\n")
    hash1 = get_feature_pipeline_hash("v1")
    
    eng_file.write_text("RSI_WINDOW = 21\n")
    hash2 = get_feature_pipeline_hash("v1")
    
    assert hash1 != hash2

def test_a4_router_resolves_v1():
    module = resolve_feature_pipeline("v1")
    assert hasattr(module, "build_feature_row")

def test_a5_unknown_version_fails_closed():
    with pytest.raises(RuntimeError, match="Feature pipeline version 'v99' is unavailable."):
        resolve_feature_pipeline("v99")
    with pytest.raises(RuntimeError, match="Feature pipeline version 'v99' is unavailable."):
        get_feature_pipeline_hash("v99")

# ---------------------------------------------------------
# TEST GROUP B - INFERENCE PIPELINE PARITY
# ---------------------------------------------------------

@pytest.fixture
def mock_directories(monkeypatch, tmp_path):
    temp_models = tmp_path / "models"
    temp_features = tmp_path / "features"
    temp_models.mkdir()
    temp_features.mkdir()
    
    monkeypatch.setattr("src.ml.history.MODELS_DIR", str(temp_models))
    monkeypatch.setattr("src.ml.history.FEATURES_DIR", str(temp_features))
    
    return temp_models, temp_features

def test_b1_valid_pipeline(mock_directories, monkeypatch):
    temp_models, temp_features = mock_directories
    ticker = "RELIANCE.NS"
    version = "v1"
    
    model_path = temp_models / f"model_{ticker}_{version}.joblib"
    model_path.write_bytes(b"data")
    actual_m = hashlib.sha256(b"data").hexdigest()[:12]
    
    features_path = temp_features / f"features_{ticker}_{version}.json"
    features_path.write_bytes(b'["f1", "f2"]')
    actual_f = hashlib.sha256(b'["f1", "f2"]').hexdigest()[:64]
    
    v1_hash = get_feature_pipeline_hash("v1")
    
    manifest_path = temp_models / f"{ticker}_active.json"
    manifest_path.write_text(json.dumps({
        "model_version": version,
        "model_hash": actual_m,
        "feature_hash": actual_f,
        "feature_pipeline_version": "v1",
        "feature_pipeline_hash": v1_hash,
        "dataset_hash": "dummy_dataset",
        "provenance_status": "COMPLETE",
        "promoted_at": "2026-08-09T00:00:00Z"
    }))
    
    import joblib
    monkeypatch.setattr(joblib, "load", lambda p: "mock_model")
    
    model, features, ver, eng_mod, p_ver, p_hash, f1_macro = load_active_bundle(ticker)
    assert model == "mock_model"
    assert features == ["f1", "f2"]
    assert ver == version
    assert hasattr(eng_mod, "build_feature_row")

def test_b2_pipeline_hash_mismatch(mock_directories):
    temp_models, temp_features = mock_directories
    ticker = "RELIANCE.NS"
    version = "v1"
    
    model_path = temp_models / f"model_{ticker}_{version}.joblib"
    model_path.write_bytes(b"data")
    actual_m = hashlib.sha256(b"data").hexdigest()[:12]
    
    features_path = temp_features / f"features_{ticker}_{version}.json"
    features_path.write_bytes(b'["f1"]')
    actual_f = hashlib.sha256(b'["f1"]').hexdigest()[:64]
    
    manifest_path = temp_models / f"{ticker}_active.json"
    manifest_path.write_text(json.dumps({
        "model_version": version,
        "model_hash": actual_m,
        "feature_hash": actual_f,
        "feature_pipeline_version": "v1",
        "feature_pipeline_hash": "wrong_hash",
        "dataset_hash": "LEGACY_UNAVAILABLE",
        "provenance_status": "LEGACY_UNAVAILABLE"
    }))
    
    with pytest.raises(RuntimeError, match="Feature pipeline hash mismatch for RELIANCE.NS. Expected wrong_hash"):
        load_active_bundle(ticker)

def test_b3_unknown_pipeline(mock_directories):
    temp_models, temp_features = mock_directories
    ticker = "RELIANCE.NS"
    version = "v1"
    
    manifest_path = temp_models / f"{ticker}_active.json"
    manifest_path.write_text(json.dumps({
        "model_version": version,
        "model_hash": "h1",
        "feature_hash": "h2",
        "feature_pipeline_version": "v99",
        "feature_pipeline_hash": "somehash",
        "dataset_hash": "LEGACY_UNAVAILABLE",
        "provenance_status": "LEGACY_UNAVAILABLE"
    }))
    
    with pytest.raises(RuntimeError, match="Feature pipeline version 'v99' is unavailable."):
        load_active_bundle(ticker)

def test_b4_mongodb_independence(mock_directories, monkeypatch):
    # Ensure MongoDB throws an error if contacted
    import pymongo
    monkeypatch.setattr(pymongo, "MongoClient", lambda *args, **kwargs: Exception("MongoDB contacted!"))
    
    temp_models, temp_features = mock_directories
    ticker = "RELIANCE.NS"
    version = "v1"
    
    model_path = temp_models / f"model_{ticker}_{version}.joblib"
    model_path.write_bytes(b"data")
    actual_m = hashlib.sha256(b"data").hexdigest()[:12]
    
    features_path = temp_features / f"features_{ticker}_{version}.json"
    features_path.write_bytes(b'["f1"]')
    actual_f = hashlib.sha256(b'["f1"]').hexdigest()[:64]
    
    v1_hash = get_feature_pipeline_hash("v1")
    
    manifest_path = temp_models / f"{ticker}_active.json"
    manifest_path.write_text(json.dumps({
        "model_version": version,
        "model_hash": actual_m,
        "feature_hash": actual_f,
        "feature_pipeline_version": "v1",
        "feature_pipeline_hash": v1_hash,
        "dataset_hash": "dummy_dataset",
        "provenance_status": "COMPLETE"
    }))
    
    import joblib
    monkeypatch.setattr(joblib, "load", lambda p: "mock_model")
    
    # This should succeed completely independent of Mongo
    model, features, ver, eng_mod, p_ver, p_hash, f1_macro = load_active_bundle(ticker)
    assert model == "mock_model"

# ---------------------------------------------------------
# TEST GROUP C, D, E, F, H - DATASET PROVENANCE
# ---------------------------------------------------------

@pytest.fixture
def mock_db():
    import mongomock
    return mongomock.MongoClient()["stock_market_db"]

def test_cdefh_trainer_dataset_archival_and_provenance(mock_directories, mock_db, monkeypatch):
    temp_models, temp_features = mock_directories
    ticker = "RELIANCE.NS"
    
    import src.ml.trainer as trainer_mod
    monkeypatch.setattr(trainer_mod, "MODELS_DIR", str(temp_models))
    monkeypatch.setattr(trainer_mod, "FEATURES_DIR", str(temp_features))
    
    # Provide a minimal DataFrame
    dates = pd.date_range("2024-01-01", periods=200)
    df = pd.DataFrame({
        "close": range(200),
        "target": [0, 1, 2] * 66 + [0, 1],
        "f1": [0.1] * 200
    }, index=dates)
    
    # Patch dependencies for trainer
    mock_study = MagicMock()
    mock_study.best_params = {}
    mock_study.best_value = 0.8
    monkeypatch.setattr(trainer_mod.optuna, "create_study", lambda **k: mock_study)
    
    mock_model_instance = MagicMock()
    mock_model_instance.predict_proba.return_value = pd.DataFrame({0: [0.1]*40, 1: [0.8]*40, 2: [0.1]*40}).values
    mock_model_instance.predict.return_value = [1] * 40
    mock_model_instance.feature_importances_ = [1.0]
    monkeypatch.setattr(trainer_mod, "XGBClassifier", lambda **k: mock_model_instance)
    
    monkeypatch.setattr(trainer_mod, "MIN_ROWS_AFTER_FEATURES", 10)
    monkeypatch.setattr(trainer_mod, "N_SPLITS", 2)
    monkeypatch.setattr(trainer_mod, "_make_feature_list", lambda x: ['f1'])
    
    # Intercept joblib.dump to write actual bytes
    original_dump = trainer_mod.joblib.dump
    def mock_dump(model, filename, **kwargs):
        with open(filename, "wb") as f:
            f.write(b"new_model_data")
    monkeypatch.setattr(trainer_mod.joblib, "dump", mock_dump)
    
    # Execute Training
    client_mock = MagicMock()
    client_mock.__getitem__.return_value = mock_db
    train_model(df, ticker, client_mock)
    
    candidates = list(mock_db.model_registry.find({"ticker": ticker, "status": "CANDIDATE"}))
    assert len(candidates) == 1
    record = candidates[0]
    version = record["version"]
    
    # Group C: Dataset exists and is Parquet
    dataset_path = temp_features / f"dataset_{ticker}_{version}.parquet"
    assert dataset_path.exists()
    
    archived_df = pd.read_parquet(dataset_path)
    assert "f1" in archived_df.columns
    assert "target" in archived_df.columns
    assert len(archived_df) == 200
    assert archived_df.index[0] == pd.Timestamp("2024-01-01")
    assert archived_df.index[-1] == pd.Timestamp("2024-07-18")
    
    # Group D: Physical dataset hash
    with open(dataset_path, "rb") as f:
        physical_hash = hashlib.sha256(f.read()).hexdigest()[:64]
    assert record["dataset_hash"] == physical_hash
    
    # Group E: Metadata matches physical file
    assert record["dataset_row_count"] == len(archived_df)
    assert record["dataset_date_start"] == str(archived_df.index.min())
    assert record["dataset_date_end"] == str(archived_df.index.max())
    
    # Group F: Provenance status is COMPLETE
    assert record["provenance_status"] == "COMPLETE"
    
    # Group H: Consistency - let's check promote and sync
    from src.ml.model_registry import promote_model, sync_manifest
    from src.ml.model_registry import MODELS_DIR as REGISTRY_MODELS_DIR
    monkeypatch.setattr("src.ml.model_registry.MODELS_DIR", str(temp_models))
    monkeypatch.setattr("src.ml.model_registry.FEATURES_DIR", str(temp_features))
    
    promote_model(mock_db, ticker, version)
    
    # Check active manifest
    manifest_path = temp_models / f"{ticker}_active.json"
    assert manifest_path.exists()
    manifest_content = json.loads(manifest_path.read_text())
    
    assert manifest_content["feature_pipeline_version"] == "v1"
    assert manifest_content["feature_pipeline_hash"] == get_feature_pipeline_hash("v1")
    assert manifest_content["dataset_hash"] == physical_hash
    assert manifest_content["dataset_row_count"] == 200
    assert manifest_content["provenance_status"] == "COMPLETE"

# ---------------------------------------------------------
# TEST GROUP G - MIGRATION
# ---------------------------------------------------------

def test_g_migration_script(mock_directories, mock_db, monkeypatch):
    temp_models, temp_features = mock_directories
    monkeypatch.setattr("src.ml.model_registry.MODELS_DIR", str(temp_models))
    monkeypatch.setattr("src.ml.model_registry.FEATURES_DIR", str(temp_features))
    
    import scripts.migrate_phase14 as mig_module
    monkeypatch.setattr(mig_module, "MongoClient", lambda *a, **k: MagicMock(__getitem__=lambda s, k: mock_db))
    
    ticker = "RELIANCE.NS"
    version = "legacy_v1"
    
    # Create the artifact files so sync_manifest doesn't fail
    model_path = temp_models / f"model_{ticker}_{version}.joblib"
    model_path.write_bytes(b"data")
    features_path = temp_features / f"features_{ticker}_{version}.json"
    features_path.write_bytes(b"data")
    
    mh1 = hashlib.sha256(b"data").hexdigest()[:12]
    fh1 = hashlib.sha256(b"data").hexdigest()[:64]
    
    # Insert legacy record
    mock_db.model_registry.insert_one({
        "ticker": ticker,
        "version": version,
        "status": "ACTIVE",
        "model_hash": mh1,
        "feature_hash": fh1
        # Notice no feature_pipeline_version
    })
    
    # Create legacy active manifest
    manifest_path = temp_models / f"{ticker}_active.json"
    manifest_path.write_text(json.dumps({
        "ticker": ticker,
        "model_version": version,
        "model_hash": mh1,
        "feature_hash": fh1
    }))
    
    # First execution
    run_migration()
    
    # Verify G1-G5
    record = mock_db.model_registry.find_one({"ticker": ticker, "version": version})
    assert record["model_hash"] == mh1
    assert record["feature_hash"] == fh1
    assert record["feature_pipeline_version"] == "v1"
    assert record["feature_pipeline_hash"] == get_feature_pipeline_hash("v1")
    assert record["dataset_hash"] == "LEGACY_UNAVAILABLE"
    assert record["provenance_status"] == "LEGACY_UNAVAILABLE"
    
    manifest_content = json.loads(manifest_path.read_text())
    assert manifest_content["feature_pipeline_version"] == "v1"
    assert manifest_content["dataset_hash"] == "LEGACY_UNAVAILABLE"
    assert manifest_content["provenance_status"] == "LEGACY_UNAVAILABLE"
    
    # Second execution (Idempotency - G6)
    run_migration()
    
    records = list(mock_db.model_registry.find({"ticker": ticker}))
    assert len(records) == 1
    assert records[0]["status"] == "ACTIVE"
