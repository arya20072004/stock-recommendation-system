import pytest
from datetime import datetime, date, timedelta, timezone
from unittest.mock import MagicMock
import pandas as pd
import numpy as np
import mongomock

from src.ml.history import generate_and_persist_predictions
from src.pipeline.daily import DailyPipeline

# We will mock load_active_bundle so we don't need real files.
@pytest.fixture
def mock_history_dependencies(monkeypatch):
    def mock_load_active_bundle(ticker):
        model = MagicMock()
        model.predict_proba.return_value = np.array([[0.1, 0.2, 0.7]])
        model.feature_importances_ = np.array([0.5, 0.5])
        
        feature_names = ["f1", "f2"]
        version = "v1"
        
        mock_engineering = MagicMock()
        mock_engineering.TICKER_CLASS_THRESHOLDS = {}
        mock_engineering.apply_threshold_calibration.return_value = 2 # BUY
        mock_engineering.get_target_return_threshold.return_value = 0.05
        
        return model, feature_names, "v1", mock_engineering, "v1", "hash_xyz", 0.45
        
    monkeypatch.setattr("src.ml.history.load_active_bundle", mock_load_active_bundle)
    monkeypatch.setattr("src.ml.history._verify_production_readiness", lambda db: None)
    monkeypatch.setattr("src.ml.history.reconcile_all_manifests", lambda db: True)
    
    # Mock MongoDB client
    client = MagicMock()
    # Mock the transaction context managers
    session_mock = MagicMock()
    client.start_session.return_value.__enter__.return_value = session_mock
    session_mock.start_transaction.return_value.__enter__.return_value = MagicMock()
    
    db = client["stock_market_db"]
    # Provide a fake upserted_id so it counts as generated
    update_result = MagicMock()
    update_result.upserted_id = "fake_id"
    db.prediction_history.update_one.return_value = update_result
    
    db.prediction_provenance.find_one.return_value = None
    
    return client

def _setup_df_for_date(engineering_module, target_date, feature_names):
    # Create a DataFrame where the latest date is target_date
    df = pd.DataFrame(
        {
            "f1": [1.0, 1.0],
            "f2": [2.0, 2.0],
            "close": [100.0, 100.0],
            "atr_pct": [0.02, 0.02]
        },
        index=[pd.Timestamp("2026-08-09"), pd.Timestamp(target_date)]
    )
    df = df[~df.index.duplicated(keep='last')]
    engineering_module.build_feature_row.return_value = df
    return df

def test_group_a_exact_match(mock_history_dependencies, monkeypatch):
    client = mock_history_dependencies
    target = date(2026, 8, 10)
    
    original_load = __import__("src.ml.history").ml.history.load_active_bundle
    
    def customized_load(ticker):
        res = original_load(ticker)
        eng_mod = res[3]
        _setup_df_for_date(eng_mod, target, res[1])
        return res
        
    monkeypatch.setattr("src.ml.history.load_active_bundle", customized_load)
    monkeypatch.setattr("src.ml.history.TICKERS", ["RELIANCE.NS"])
    
    result = generate_and_persist_predictions(client, last_completed_session=date(2026, 8, 9), prediction_target_date=target)
    
    assert result["generated"] == 1
    assert result["skipped"] == 0
    assert result.get("existing", 0) == 0
    assert len(result["stale"]) == 0
    assert result["skipped"] == len(result["stale"])
    assert len(result["errors"]) == 0
    
    db = client["stock_market_db"]
    assert db.prediction_history.update_one.called

def test_group_b_stale_data(mock_history_dependencies, monkeypatch):
    client = mock_history_dependencies
    target = date(2026, 8, 10)
    stale_date = date(2026, 8, 9)
    
    original_load = __import__("src.ml.history").ml.history.load_active_bundle
    
    def customized_load(ticker):
        res = original_load(ticker)
        _setup_df_for_date(res[3], stale_date, res[1])
        return res
        
    monkeypatch.setattr("src.ml.history.load_active_bundle", customized_load)
    monkeypatch.setattr("src.ml.history.TICKERS", ["RELIANCE.NS"])
    
    result = generate_and_persist_predictions(client, last_completed_session=date(2026, 8, 9), prediction_target_date=target)
    
    assert result["generated"] == 0
    assert result["skipped"] == 0
    assert result.get("existing", 0) == 0
    assert "RELIANCE.NS" in result["failed"]
    assert len(result["failed"]) == 1
    assert len(result["errors"]) == 1
    
    db = client["stock_market_db"]
    assert not db.prediction_history.update_one.called
    assert not db.prediction_provenance.insert_one.called

def test_group_c_future_data(mock_history_dependencies, monkeypatch):
    client = mock_history_dependencies
    target = date(2026, 8, 10)
    future_date = date(2026, 8, 11)
    
    original_load = __import__("src.ml.history").ml.history.load_active_bundle
    
    def customized_load(ticker):
        res = original_load(ticker)
        _setup_df_for_date(res[3], future_date, res[1])
        return res
        
    monkeypatch.setattr("src.ml.history.load_active_bundle", customized_load)
    monkeypatch.setattr("src.ml.history.TICKERS", ["RELIANCE.NS"])
    
    result = generate_and_persist_predictions(client, last_completed_session=date(2026, 8, 9), prediction_target_date=target)
    assert len(result["failed"]) == 1
    assert "RELIANCE.NS" in result["failed"]
        
    db = client["stock_market_db"]
    assert not db.prediction_history.update_one.called

def _setup_mock_pipeline():
    pipeline = DailyPipeline.__new__(DailyPipeline)
    client = MagicMock()
    pipeline.client = client
    pipeline.db = client["stock_market_db"]
    pipeline.prediction_target_date = date(2026, 8, 10)
    pipeline.last_completed_session = date(2026, 8, 9)
    pipeline.stages = {}
    pipeline.degraded_stages = []
    pipeline.errors = []
    pipeline.status = "RUNNING"
    pipeline.dry_run = False
    pipeline.run_id = "test_run"
    return pipeline, client

def test_group_d_partial_staleness_and_validation(monkeypatch):
    pipeline, client = _setup_mock_pipeline()
    
    from src.data.nifty50 import TICKERS
    pipeline.stages["PREDICTION_GENERATION"] = {
        "metrics": {
            "generated": 49,
            "skipped": 2,
            "stale": TICKERS[:2],
            "errors": []
        }
    }
    
    fake_preds = [{"symbol": t, "confidence": 90, "price_at_prediction": 100.0, "model_version": "v1", "recommendation": "BUY"} for t in TICKERS[2:]]
    pipeline.db.prediction_history.find.return_value = fake_preds
    
    monkeypatch.setattr("src.ml.model_utils.get_model_version", lambda t: "v1")
    monkeypatch.setattr("src.pipeline.daily.DailyPipeline._verify_ownership", lambda s: None)
    monkeypatch.setattr("src.pipeline.daily.DailyPipeline._update_run_record", lambda s: None)
    
    pipeline.validate_prediction_batch()
    
    assert pipeline.stages["PREDICTION_VALIDATION"]["status"] == "DEGRADED"

def test_group_e_coverage_failure(monkeypatch):
    pipeline, client = _setup_mock_pipeline()
    
    from src.data.nifty50 import TICKERS
    # Say we require 46. 45 valid, 6 stale.
    pipeline.stages["PREDICTION_GENERATION"] = {
        "metrics": {
            "generated": 45,
            "skipped": 6,
            "stale": TICKERS[:6],
            "errors": []
        }
    }
    
    fake_preds = [{"symbol": t, "confidence": 90, "price_at_prediction": 100.0, "model_version": "v1", "recommendation": "BUY"} for t in TICKERS[6:]]
    pipeline.db.prediction_history.find.return_value = fake_preds
    monkeypatch.setattr("src.ml.model_utils.get_model_version", lambda t: "v1")
    monkeypatch.setattr("src.pipeline.daily.DailyPipeline._verify_ownership", lambda s: None)
    monkeypatch.setattr("src.pipeline.daily.DailyPipeline._update_run_record", lambda s: None)
    
    with pytest.raises(RuntimeError, match="Prediction coverage failure"):
        pipeline.validate_prediction_batch()

def test_group_f_all_stale(monkeypatch):
    pipeline, client = _setup_mock_pipeline()
    
    from src.data.nifty50 import TICKERS
    pipeline.stages["PREDICTION_GENERATION"] = {
        "metrics": {
            "generated": 0,
            "skipped": 51,
            "stale": TICKERS,
            "errors": []
        }
    }
    
    pipeline.db.prediction_history.find.return_value = []
    monkeypatch.setattr("src.pipeline.daily.DailyPipeline._verify_ownership", lambda s: None)
    monkeypatch.setattr("src.pipeline.daily.DailyPipeline._update_run_record", lambda s: None)
    
    with pytest.raises(RuntimeError, match="Prediction coverage failure"):
        pipeline.validate_prediction_batch()

def test_group_g_unexplained_missing(monkeypatch):
    pipeline, client = _setup_mock_pipeline()
    
    from src.data.nifty50 import TICKERS
    # 49 generated, 1 stale. 1 completely missing from DB for some reason.
    pipeline.stages["PREDICTION_GENERATION"] = {
        "metrics": {
            "generated": 49,
            "skipped": 1,
            "stale": [TICKERS[0]],
            "errors": []
        }
    }
    
    # Return 49 predictions, missing index 0 and 1. 0 is stale, 1 is unexplained.
    fake_preds = [{"symbol": t, "confidence": 90, "price_at_prediction": 100.0, "model_version": "v1", "recommendation": "BUY"} for t in TICKERS[2:]]
    pipeline.db.prediction_history.find.return_value = fake_preds
    monkeypatch.setattr("src.ml.model_utils.get_model_version", lambda t: "v1")
    monkeypatch.setattr("src.pipeline.daily.DailyPipeline._verify_ownership", lambda s: None)
    monkeypatch.setattr("src.pipeline.daily.DailyPipeline._update_run_record", lambda s: None)
    
    with pytest.raises(RuntimeError, match="Unexplained missing predictions"):
        pipeline.validate_prediction_batch()

def test_group_h_idempotency(mock_history_dependencies, monkeypatch):
    def _setup_mock_history(monkeypatch, target):
        original_load = __import__("src.ml.history").ml.history.load_active_bundle
        
        def customized_load(ticker):
            res = original_load(ticker)
            _setup_df_for_date(res[3], target, res[1])
            return res
            
        monkeypatch.setattr("src.ml.history.load_active_bundle", customized_load)
        monkeypatch.setattr("src.ml.history.TICKERS", ["RELIANCE.NS"])
        monkeypatch.setattr("src.ml.history._verify_production_readiness", lambda db: None)
        monkeypatch.setattr("src.ml.history.reconcile_all_manifests", lambda db: True)

    client = mock_history_dependencies
    target = date(2026, 8, 10)
    _setup_mock_history(monkeypatch, target)
    
    # Simulate DB returning no upserted_id (already exists)
    db = client["stock_market_db"]
    update_result = MagicMock()
    update_result.upserted_id = None
    db.prediction_history.update_one.return_value = update_result
    
    result = generate_and_persist_predictions(client, last_completed_session=date(2026, 8, 9), prediction_target_date=target)
    
    assert result["generated"] == 0
    assert result["skipped"] == 0
    assert result["existing"] == 1
    assert len(result["stale"]) == 0
    assert result["skipped"] == len(result["stale"])

def test_group_i_mixed_stale_idempotent(mock_history_dependencies, monkeypatch):
    client = mock_history_dependencies
    target = date(2026, 8, 10)
    stale_date = date(2026, 8, 9)
    
    original_load = __import__("src.ml.history").ml.history.load_active_bundle
    
    def customized_load(ticker):
        res = original_load(ticker)
        # 1 valid (RELIANCE.NS), 1 stale (TCS.NS), 1 idempotent (INFY.NS)
        if ticker == "TCS.NS":
            _setup_df_for_date(res[3], stale_date, res[1])
        else:
            _setup_df_for_date(res[3], target, res[1])
        return res
        
    monkeypatch.setattr("src.ml.history.load_active_bundle", customized_load)
    monkeypatch.setattr("src.ml.history.TICKERS", ["RELIANCE.NS", "TCS.NS", "INFY.NS"])
    monkeypatch.setattr("src.ml.history._verify_production_readiness", lambda db: None)
    monkeypatch.setattr("src.ml.history.reconcile_all_manifests", lambda db: True)
    
    db = client["stock_market_db"]
    
    def mock_update_one(filter_doc, *args, **kwargs):
        res = MagicMock()
        if filter_doc["symbol"] == "INFY.NS":
            res.upserted_id = None
        else:
            res.upserted_id = "fake_id"
        return res
        
    db.prediction_history.update_one.side_effect = mock_update_one
    
    result = generate_and_persist_predictions(client, last_completed_session=date(2026, 8, 9), prediction_target_date=target)
    
    assert result["generated"] == 1
    assert result["skipped"] == 0
    assert result["existing"] == 1
    assert "TCS.NS" in result["failed"]
    assert len(result["failed"]) == 1
    assert len(result["errors"]) == 1
