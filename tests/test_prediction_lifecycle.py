import pytest
import mongomock
from unittest.mock import patch, MagicMock
from datetime import date
import pandas as pd
import numpy as np
import src.features.v1.engineering
from src.ml.history import generate_and_persist_predictions
from src.pipeline.daily import DailyPipeline
from app import app

try:
    mongomock.ignore_feature('session')
except Exception:
    pass

@pytest.fixture
def mock_db():
    client = mongomock.MongoClient()
    client.start_session = MagicMock()
    db = client['stock_market_db']
    yield client, db

@patch("src.ml.history.load_active_bundle")
@patch("src.ml.history.get_latest_valid_feature_row")
@patch("src.features.v1.engineering.get_target_return_threshold", return_value=0.05)
@patch("src.features.v1.engineering.apply_threshold_calibration", return_value=2) # BUY
@patch("src.ml.history._verify_production_readiness")
def test_newly_generated_begin_as_unvalidated(mock_verify, mock_calib, mock_thresh, mock_get_row, mock_load, mock_db):
    client, db = mock_db
    
    mock_model = MagicMock()
    mock_model.predict_proba.return_value = np.array([[0.1, 0.1, 0.8]])
    mock_model.feature_importances_ = [0.5, 0.5]
    mock_eng = MagicMock()
    mock_eng.TICKER_CLASS_THRESHOLDS = {"RELIANCE.NS": {0: 0.2, 1: 0.5}}
    mock_eng.apply_threshold_calibration.return_value = 2
    mock_eng.get_target_return_threshold.return_value = 0.05
    
    # Needs to return: latest_row, computed_df, market_date
    mock_row = pd.Series({"f1": 1.0, "f2": 2.0})
    mock_df = pd.DataFrame({"close": [100, 105], "atr_pct": [0.02, 0.02]}, index=[pd.Timestamp("2026-08-10"), pd.Timestamp("2026-08-11")])
    mock_eng.build_feature_row.return_value = mock_df
    mock_load.return_value = (mock_model, ["f1", "f2"], "v1", mock_eng, "v1", "phash", 0.9)
    
    mock_get_row.return_value = (pd.Timestamp("2026-08-11"), mock_row)
    
    # To bypass threshold lookup
    with patch("src.ml.history.TICKERS", ["RELIANCE.NS"]):
        with patch("src.ml.history.get_display_signal", return_value="BUY"):
            with patch("src.ml.history.compute_confidence_tier", return_value={"tier": "HIGH", "actionable": True, "f1_macro": 0.8}):
                generate_and_persist_predictions(client, date(2026, 8, 10), date(2026, 8, 11))
    
    records = list(db.prediction_history.find())
    assert len(records) == 1
    assert records[0]["status"] == "UNVALIDATED"
    assert records[0]["model_version"] == "v1"

@patch("src.pipeline.daily.MongoClient")
@patch("src.ml.model_utils.get_model_version")
def test_matching_manifest_validates_successfully(mock_version, mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    mock_version.return_value = "v1"

    db.prediction_history.insert_one({
        "symbol": "RELIANCE.NS", "market_date": "2026-08-11", "prediction_horizon": 10,
        "status": "UNVALIDATED", "model_version": "v1", "confidence": 85,
        "price_at_prediction": 100, "recommendation": "BUY"
    })

    pipeline = DailyPipeline(mongo_uri="mock_uri")
    pipeline.db = db
    pipeline.prediction_target_date = date(2026, 8, 11)
    with patch("src.data.nifty50.TICKERS", ["RELIANCE.NS"]):
        pipeline.validate_prediction_batch()

    record = db.prediction_history.find_one({"symbol": "RELIANCE.NS"})
    assert record["status"] == "PENDING"

@patch("src.pipeline.daily.MongoClient")
@patch("src.ml.model_utils.get_model_version")
def test_validation_failure_no_transition(mock_version, mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    mock_version.return_value = "v2" # Manifest is v2, prediction is v1

    db.prediction_history.insert_one({
        "symbol": "RELIANCE.NS", "market_date": "2026-08-11", "prediction_horizon": 10,
        "status": "UNVALIDATED", "model_version": "v1", "confidence": 85,
        "price_at_prediction": 100, "recommendation": "BUY"
    })

    pipeline = DailyPipeline(mongo_uri="mock_uri")
    pipeline.db = db
    pipeline.prediction_target_date = date(2026, 8, 11)
    with patch("src.data.nifty50.TICKERS", ["RELIANCE.NS"]):
        try:
            pipeline.validate_prediction_batch()
        except RuntimeError:
            pass

    record = db.prediction_history.find_one({"symbol": "RELIANCE.NS"})
    assert record["status"] == "UNVALIDATED"

@patch("src.pipeline.daily.MongoClient")
@patch("src.ml.model_utils.get_model_version")
def test_missing_manifest_fails_safely(mock_version, mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    mock_version.return_value = "unknown"

    db.prediction_history.insert_one({
        "symbol": "RELIANCE.NS", "market_date": "2026-08-11", "prediction_horizon": 10,
        "status": "UNVALIDATED", "model_version": "v1", "confidence": 85,
        "price_at_prediction": 100, "recommendation": "BUY"
    })

    pipeline = DailyPipeline(mongo_uri="mock_uri")
    pipeline.db = db
    pipeline.prediction_target_date = date(2026, 8, 11)
    with patch("src.data.nifty50.TICKERS", ["RELIANCE.NS"]):
        try:
            pipeline.validate_prediction_batch()
        except RuntimeError:
            pass
    
    record = db.prediction_history.find_one({"symbol": "RELIANCE.NS"})
    assert record["status"] == "UNVALIDATED"

def test_api_excludes_unvalidated(mock_db):
    client, db = mock_db
    db.prediction_history.insert_many([
        {"symbol": "RELIANCE.NS", "market_date": "2026-08-11", "status": "UNVALIDATED", "prediction_horizon": 1, "recommendation": "BUY", "confidence": 0.9},
        {"symbol": "TCS.NS", "market_date": "2026-08-11", "status": "PENDING", "prediction_horizon": 1, "recommendation": "BUY", "confidence": 0.9},
        {"symbol": "INFY.NS", "market_date": "2026-08-11", "status": "EVALUATED", "prediction_horizon": 1, "recommendation": "BUY", "confidence": 0.9}
    ])
    
    with patch("app.db", db):
        with app.test_client() as c:
            resp = c.get("/api/recommendations")
            assert resp.status_code == 200
            resp_data = resp.get_json()
    
            symbols = [r["ticker"] for r in resp_data["data"]]
            assert "TCS.NS" in symbols
            assert "INFY.NS" in symbols
            assert "RELIANCE.NS" not in symbols

@patch("src.ml.history.load_active_bundle")
@patch("src.ml.history.get_latest_valid_feature_row")
@patch("src.features.v1.engineering.get_target_return_threshold", return_value=0.05)
@patch("src.features.v1.engineering.apply_threshold_calibration", return_value=2)
@patch("src.ml.history._verify_production_readiness")
def test_retry_idempotency(mock_verify, mock_calib, mock_thresh, mock_get_row, mock_load, mock_db):
    client, db = mock_db
    
    mock_model = MagicMock()
    mock_model.predict_proba.return_value = np.array([[0.1, 0.1, 0.8]])
    mock_model.feature_importances_ = [0.5, 0.5]
    mock_eng = MagicMock()
    mock_eng.TICKER_CLASS_THRESHOLDS = {"RELIANCE.NS": {0: 0.2, 1: 0.5}}
    mock_eng.apply_threshold_calibration.return_value = 2
    mock_eng.get_target_return_threshold.return_value = 0.05
    
    mock_row = pd.Series({"f1": 1.0, "f2": 2.0})
    mock_df = pd.DataFrame({"close": [100, 105], "atr_pct": [0.02, 0.02]}, index=[pd.Timestamp("2026-08-10"), pd.Timestamp("2026-08-11")])
    mock_eng.build_feature_row.return_value = mock_df
    mock_load.return_value = (mock_model, ["f1", "f2"], "v1", mock_eng, "v1", "phash", 0.9)
    
    mock_get_row.return_value = (pd.Timestamp("2026-08-11"), mock_row)
    
    with patch("src.ml.history.TICKERS", ["RELIANCE.NS"]):
        with patch("src.features.v1.engineering.TICKER_CLASS_THRESHOLDS", {"RELIANCE.NS": {0: 0.2, 1: 0.5}}):
            with patch("src.ml.history.get_display_signal", return_value="BUY"):
                with patch("src.ml.history.compute_confidence_tier", return_value={"tier": "HIGH", "actionable": True, "f1_macro": 0.8}):
                    generate_and_persist_predictions(client, date(2026, 8, 10), date(2026, 8, 11))
                    generate_and_persist_predictions(client, date(2026, 8, 10), date(2026, 8, 11)) # Retry
        
    records = list(db.prediction_history.find({"symbol": "RELIANCE.NS"}))
    assert len(records) == 1
    assert records[0]["status"] == "UNVALIDATED"

@patch("src.ml.model_utils.json.load")
@patch("src.ml.model_utils.os.path.exists")
@patch("src.ml.model_utils.open")
def test_model_version_authority_active_manifest(mock_open, mock_exists, mock_json):
    from src.ml.model_utils import get_model_version
    mock_exists.return_value = True
    mock_json.return_value = {"model_version": "manifest_v1"}
    
    ver = get_model_version("RELIANCE.NS")
    assert ver == "manifest_v1"
