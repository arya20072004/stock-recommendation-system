import pytest
import os
import json
from app import app as flask_app
from datetime import datetime, timezone
import pymongo
from unittest import mock
import app
from bson import ObjectId

@pytest.fixture(scope="module")
def client():
    flask_app.config['TESTING'] = True
    with flask_app.test_client() as client:
        with flask_app.app_context():
            yield client

@pytest.fixture(scope="module")
def db():
    from app import db as mongo_db
    yield mongo_db

@pytest.fixture(autouse=True)
def setup_teardown(db):
    db.prediction_history.delete_many({"symbol": {"$regex": "^QUARANTINE_TEST"}})
    yield
    db.prediction_history.delete_many({"symbol": {"$regex": "^QUARANTINE_TEST"}})

def insert_test_data(db):
    base_time = datetime.now(timezone.utc)
    future_date = "2099-01-01"
    db.prediction_history.insert_many([
        {"symbol": "QUARANTINE_TEST_U", "market_date": future_date, "prediction_timestamp": base_time, "prediction_horizon": 10, "confidence": 0.9, "recommendation": "BUY", "status": "UNVALIDATED", "model_version": "v1"},
        {"symbol": "QUARANTINE_TEST_P", "market_date": future_date, "prediction_timestamp": base_time, "prediction_horizon": 10, "confidence": 0.9, "recommendation": "BUY", "status": "PENDING", "model_version": "v1"},
        {"symbol": "QUARANTINE_TEST_E", "market_date": future_date, "prediction_timestamp": base_time, "prediction_horizon": 10, "confidence": 0.9, "recommendation": "BUY", "status": "EVALUATED", "model_version": "v1", "settlement_hash": "dummy", "actual_return": 0.05, "target_return_threshold": 0.02, "actual_class": "BUY"},
        {"symbol": "QUARANTINE_TEST_I", "market_date": future_date, "prediction_timestamp": base_time, "prediction_horizon": 10, "confidence": 0.9, "recommendation": "BUY", "status": "INVALID_PROVENANCE", "model_version": "v1"},
        {"symbol": "QUARANTINE_TEST_F", "market_date": future_date, "prediction_timestamp": base_time, "prediction_horizon": 10, "confidence": 0.9, "recommendation": "BUY", "status": "FAILED", "model_version": "v1"},
    ])

def test_1_unvalidated_hidden(client, db):
    insert_test_data(db)
    app.cache.clear()
    
    resp = client.get('/api/predictions/history?symbol=QUARANTINE_TEST_U')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 0
    assert len(data["data"]) == 0

def test_2_pending_exposed(client, db):
    insert_test_data(db)
    app.cache.clear()
    
    resp = client.get('/api/predictions/history?symbol=QUARANTINE_TEST_P')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 1
    assert data["data"][0]["status"] == "PENDING"

def test_3_evaluated_exposed(client, db):
    insert_test_data(db)
    app.cache.clear()
    
    resp = client.get('/api/predictions/history?symbol=QUARANTINE_TEST_E')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 1
    assert data["data"][0]["status"] == "EVALUATED"

def test_4_invalid_provenance_hidden(client, db):
    insert_test_data(db)
    app.cache.clear()
    
    resp = client.get('/api/predictions/history?symbol=QUARANTINE_TEST_I')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 0
    assert len(data["data"]) == 0
    
    original_tickers = app.TICKERS
    app.TICKERS = ["QUARANTINE_TEST_I"]
    try:
        resp2 = client.get('/api/recommendations')
        assert resp2.status_code == 404
    finally:
        app.TICKERS = original_tickers

def test_5_failed_hidden(client, db):
    insert_test_data(db)
    app.cache.clear()
    
    resp = client.get('/api/predictions/history?symbol=QUARANTINE_TEST_F')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 0
    assert len(data["data"]) == 0

def test_6_mixed_lifecycle_dataset(client, db):
    insert_test_data(db)
    app.cache.clear()
    
    original_tickers = app.TICKERS
    test_tickers = ["QUARANTINE_TEST_U", "QUARANTINE_TEST_P", "QUARANTINE_TEST_E", "QUARANTINE_TEST_I", "QUARANTINE_TEST_F"]
    app.TICKERS = test_tickers
    
    try:
        resp = client.get('/api/stocks/summary')
        assert resp.status_code == 200
        data = resp.get_json()
        
        preds = data.get("data", [])
        assert len(preds) == 2
        
        returned_tickers = [p["ticker"] for p in preds]
        assert "QUARANTINE_TEST_P" in returned_tickers
        assert "QUARANTINE_TEST_E" in returned_tickers
        
        assert "QUARANTINE_TEST_U" not in returned_tickers
        assert "QUARANTINE_TEST_I" not in returned_tickers
        assert "QUARANTINE_TEST_F" not in returned_tickers
    finally:
        app.TICKERS = original_tickers

def test_7_performance_metrics_evaluated_only(client, db):
    insert_test_data(db)
    app.cache.clear()
    
    with mock.patch("src.ml.model_utils.compute_settlement_hash", return_value="dummy"):
        with mock.patch("src.ml.model_utils.reconstruct_settlement_payload", return_value={}):
            with mock.patch("app.analyze_performance") as mock_analyze:
                mock_analyze.return_value = {}
                
                resp = client.get('/api/predictions/performance?model_version=v1')
                assert resp.status_code == 200
                
                called_preds = mock_analyze.call_args[0][0]
                test_preds = [p for p in called_preds if str(p.get("symbol")).startswith("QUARANTINE_TEST")]
                
                assert len(test_preds) == 1
                assert test_preds[0]["status"] == "EVALUATED"
                assert test_preds[0]["symbol"] == "QUARANTINE_TEST_E"

def test_8_performance_total_count(client, db):
    insert_test_data(db)
    app.cache.clear()
    
    with mock.patch("pymongo.collection.Collection.count_documents") as mock_count:
        mock_count.return_value = 5
        
        resp = client.get('/api/predictions/performance?ticker=QUARANTINE_TEST_U')
        assert resp.status_code == 200
        
        calls = mock_count.call_args_list
        
        total_query_call = calls[0][0][0]
        assert "status" in total_query_call
        assert total_query_call["status"] == {"$in": ["PENDING", "EVALUATED"]}
        assert total_query_call["symbol"] == "QUARANTINE_TEST_U"
        
        pending_query_call = calls[1][0][0]
        assert "status" in pending_query_call
        assert pending_query_call["status"] == "PENDING"
        assert pending_query_call["symbol"] == "QUARANTINE_TEST_U"

def test_9_performance_filters(client, db):
    app.cache.clear()
    
    with mock.patch("app.fetch_evaluated_predictions") as mock_fetch:
        mock_fetch.return_value = []
        
        client.get('/api/predictions/performance?ticker=RELIANCE.NS&model_version=v2')
        
        call_query = mock_fetch.call_args[0][1]
        assert "symbol" in call_query
        assert call_query["symbol"] == "RELIANCE.NS"
        assert "model_version" in call_query
        assert call_query["model_version"] == "v2"
        assert "status" not in call_query
