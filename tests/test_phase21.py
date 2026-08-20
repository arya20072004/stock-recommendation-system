import pytest
import os
import json
from app import app as flask_app
from datetime import datetime, timezone
import pymongo
from unittest import mock
import app

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
def clean_db(db):
    db.prediction_history.delete_many({"symbol": {"$in": ["PHASE21_A", "PHASE21_B", "PHASE21_C"]}})
    yield
    db.prediction_history.delete_many({"symbol": {"$in": ["PHASE21_A", "PHASE21_B", "PHASE21_C"]}})

def test_group_a_fully_homogeneous_snapshot(client, db):
    db.prediction_history.insert_many([
        {"symbol": "PHASE21_A", "market_date": "2099-01-02", "prediction_timestamp": datetime.now(timezone.utc), "prediction_horizon": 10, "confidence": 0.9, "recommendation": "BUY", "status": "PENDING"},
        {"symbol": "PHASE21_B", "market_date": "2099-01-02", "prediction_timestamp": datetime.now(timezone.utc), "prediction_horizon": 10, "confidence": 0.9, "recommendation": "SELL", "status": "PENDING"},
        {"symbol": "PHASE21_C", "market_date": "2099-01-02", "prediction_timestamp": datetime.now(timezone.utc), "prediction_horizon": 10, "confidence": 0.9, "recommendation": "HOLD", "status": "PENDING"}
    ])

    app.cache.clear()
    
    original_tickers = app.TICKERS
    app.TICKERS = ["PHASE21_A", "PHASE21_B", "PHASE21_C"]
    try:
        response = client.get('/api/stocks/summary')
        assert response.status_code == 200
        data = response.get_json()
        
        meta = data.get("meta", {})
        assert meta.get("market_date") == "2099-01-02"
        assert meta.get("complete") is True
        assert meta.get("mixed_date") is False
        assert meta.get("missing_tickers") == []
        
        preds = data.get("data", [])
        assert len(preds) == 3
        for p in preds:
            assert p["market_date"] == "2099-01-02"
            
    finally:
        app.TICKERS = original_tickers

def test_group_b_mixed_date_database(client, db):
    db.prediction_history.insert_many([
        {"symbol": "PHASE21_A", "market_date": "2099-01-02", "prediction_timestamp": datetime.now(timezone.utc), "prediction_horizon": 10, "confidence": 0.9, "recommendation": "BUY", "status": "PENDING"},
        {"symbol": "PHASE21_B", "market_date": "2099-01-02", "prediction_timestamp": datetime.now(timezone.utc), "prediction_horizon": 10, "confidence": 0.9, "recommendation": "SELL", "status": "PENDING"},
        {"symbol": "PHASE21_C", "market_date": "2099-01-01", "prediction_timestamp": datetime.now(timezone.utc), "prediction_horizon": 10, "confidence": 0.9, "recommendation": "HOLD", "status": "PENDING"}
    ])

    app.cache.clear()
    
    original_tickers = app.TICKERS
    app.TICKERS = ["PHASE21_A", "PHASE21_B", "PHASE21_C"]
    try:
        response = client.get('/api/stocks/summary')
        assert response.status_code == 200
        data = response.get_json()
        
        meta = data.get("meta", {})
        assert meta.get("market_date") == "2099-01-02"
        assert meta.get("complete") is False
        assert meta.get("mixed_date") is False
        assert "PHASE21_C" in meta.get("missing_tickers")
        
        preds = data.get("data", [])
        assert len(preds) == 2
        for p in preds:
            assert p["market_date"] == "2099-01-02"
            
    finally:
        app.TICKERS = original_tickers

def test_group_c_older_prediction_not_used(client, db):
    db.prediction_history.insert_one({
        "symbol": "PHASE21_C", "market_date": "2099-01-01", "prediction_timestamp": datetime.now(timezone.utc), "prediction_horizon": 10, "confidence": 0.9, "recommendation": "SELL", "status": "PENDING"
    })
    
    # Insert another ticker to anchor the latest market_date to 2099-01-02
    db.prediction_history.insert_one({
        "symbol": "PHASE21_A", "market_date": "2099-01-02", "prediction_timestamp": datetime.now(timezone.utc), "prediction_horizon": 10, "confidence": 0.9, "recommendation": "BUY", "status": "PENDING"
    })

    app.cache.clear()
    
    original_tickers = app.TICKERS
    app.TICKERS = ["PHASE21_A", "PHASE21_C"]
    try:
        response = client.get('/api/stocks/summary')
        assert response.status_code == 200
        data = response.get_json()
        
        meta = data.get("meta", {})
        assert meta.get("market_date") == "2099-01-02"
        
        preds = data.get("data", [])
        returned_tickers = [p["ticker"] for p in preds]
        assert "PHASE21_C" not in returned_tickers
            
    finally:
        app.TICKERS = original_tickers

def test_group_d_individual_details_retains_stale(client, db):
    db.prediction_history.insert_one({
        "symbol": "PHASE21_C", "market_date": "2099-01-01", "prediction_timestamp": datetime.now(timezone.utc), "prediction_horizon": 10, "confidence": 0.9, "recommendation": "SELL", "status": "PENDING"
    })
    
    # We must insert historical data for it to not 404 in details
    db.historical_data.insert_one({"ticker": "PHASE21_C", "date": datetime(2026, 8, 10), "close": 100, "open": 95, "high": 105, "low": 90, "volume": 1000})

    app.cache.clear()
    
    original_tickers = app.TICKERS
    app.TICKERS = ["PHASE21_C"]
    try:
        response = client.get('/api/stocks/PHASE21_C/details')
        assert response.status_code == 200
        data = response.get_json()
        
        pred = data.get("prediction", {})
        assert pred.get("market_date") == "2099-01-01"
        
    finally:
        app.TICKERS = original_tickers
        db.historical_data.delete_many({"ticker": "PHASE21_C"})

def test_group_e_empty_prediction_history(client, db):
    # Ensure no predictions exist for the tickers
    db.prediction_history.delete_many({"symbol": {"$in": ["PHASE21_A", "PHASE21_B"]}})
    # And actually, to be safe, temporarily hide ALL predictions for this test
    # by mocking find_one to return None for get_latest_predictions_snapshot
    app.cache.clear()
    with mock.patch("pymongo.collection.Collection.find_one", return_value=None):
        response = client.get('/api/stocks/summary')
        assert response.status_code == 404
        assert response.get_json().get("error") == "No predictions available."

def test_group_f_no_live_inference(client, db):
    db.prediction_history.insert_many([
        {"symbol": "PHASE21_A", "market_date": "2099-01-02", "prediction_timestamp": datetime.now(timezone.utc), "prediction_horizon": 10, "confidence": 0.9, "recommendation": "BUY", "status": "PENDING"}
    ])

    app.cache.clear()
    
    original_tickers = app.TICKERS
    app.TICKERS = ["PHASE21_A", "PHASE21_B"] # PHASE21_B is missing
    
    # Patch feature extraction. Wait, is src.features.router used in app.py? 
    # If app.py is truly clean of ML, this mock should not be called.
    # We use a generic mock for any ML function that might be called.
    with mock.patch("src.features.router.resolve_feature_pipeline", side_effect=AssertionError("LIVE INFERENCE WAS CALLED"), create=True):
        try:
            response = client.get('/api/stocks/summary')
            assert response.status_code == 200
            data = response.get_json()
            assert "PHASE21_B" in data.get("meta", {}).get("missing_tickers")
        finally:
            app.TICKERS = original_tickers

def test_group_g_recommendations_invariant(client, db):
    db.prediction_history.insert_many([
        {"symbol": "PHASE21_A", "market_date": "2099-01-02", "prediction_timestamp": datetime.now(timezone.utc), "prediction_horizon": 10, "confidence": 0.9, "recommendation": "BUY", "status": "PENDING"},
        {"symbol": "PHASE21_B", "market_date": "2099-01-02", "prediction_timestamp": datetime.now(timezone.utc), "prediction_horizon": 10, "confidence": 0.9, "recommendation": "SELL", "status": "PENDING"},
        {"symbol": "PHASE21_C", "market_date": "2099-01-01", "prediction_timestamp": datetime.now(timezone.utc), "prediction_horizon": 10, "confidence": 0.9, "recommendation": "HOLD", "status": "PENDING"}
    ])

    app.cache.clear()
    
    original_tickers = app.TICKERS
    app.TICKERS = ["PHASE21_A", "PHASE21_B", "PHASE21_C"]
    try:
        response = client.get('/api/recommendations')
        assert response.status_code == 200
        data = response.get_json()
        
        meta = data.get("meta", {})
        assert meta.get("market_date") == "2099-01-02"
        assert meta.get("complete") is False
        assert "PHASE21_C" in meta.get("missing_tickers")
        
        preds = data.get("data", [])
        for p in preds:
            assert p["market_date"] == "2099-01-02"
            
    finally:
        app.TICKERS = original_tickers

def test_group_h_mongodb_failure(client, db):
    app.cache.clear()
    with mock.patch("pymongo.collection.Collection.find_one", side_effect=Exception("Simulated MongoDB failure")):
        response = client.get('/api/stocks/summary')
        assert response.status_code in [500, 503]

