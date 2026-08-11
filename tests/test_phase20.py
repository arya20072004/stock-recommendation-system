import pytest
import os
import re
from datetime import datetime
from pymongo import MongoClient
import app
from app import app as flask_app
import json
from unittest import mock

@pytest.fixture(scope="module")
def client():
    flask_app.config["TESTING"] = True
    with flask_app.test_client() as client:
        yield client

@pytest.fixture(scope="module")
def db():
    client = MongoClient(app.MONGO_URI)
    return client['stock_market_db']

def test_group_a_static_ml_isolation():
    """Group A: Static ML Isolation - Verify app.py has no ML dependencies."""
    app_py_path = os.path.join(os.path.dirname(__file__), "..", "app.py")
    with open(app_py_path, "r") as f:
        content = f.read()

    assert "import xgboost" not in content, "app.py should not import xgboost"
    assert "import joblib" not in content, "app.py should not import joblib"
    assert "load_active_bundle" not in content, "app.py should not import load_active_bundle"
    assert "def get_latest_prediction(" not in content, "app.py should not define get_latest_prediction"

def test_group_b_canonical_recommendation_serving(client, db):
    """Group B: Seed prediction_history and verify canonical values."""
    db.prediction_history.delete_many({"symbol": "RELIANCE.NS"})
    db.historical_data.delete_many({"ticker": "RELIANCE.NS"})

    # Insert dummy historical data for the chart endpoint
    db.historical_data.insert_many([
        {"ticker": "RELIANCE.NS", "date": datetime(2026, 8, 10), "open": 100, "high": 110, "low": 90, "close": 105, "volume": 1000},
        {"ticker": "RELIANCE.NS", "date": datetime(2026, 8, 11), "open": 105, "high": 115, "low": 95, "close": 110, "volume": 1100},
    ])

    # Insert canonical prediction
    db.prediction_history.insert_one({
        "symbol": "RELIANCE.NS",
        "market_date": "2026-08-11",
        "prediction_timestamp": datetime(2026, 8, 11, 20, 0, 0),
        "prediction_horizon": 10,
        "model_version": "v1.0.0-phase20",
        "provenance_hash": "hash_xyz",
        "target_return_threshold": 0.05,
        "recommendation": "BUY",
        "raw_prediction": "BUY",
        "confidence": 88.5,
        "confidence_tier": "HIGH"
    })

    response = client.get('/api/stocks/RELIANCE.NS/details')
    assert response.status_code == 200
    data = response.json

    prediction = data['prediction']
    assert prediction is not None
    assert prediction['recommendation'] == "BUY"
    assert prediction['confidence_tier'] == "HIGH"
    assert prediction['market_date'] == "2026-08-11"
    assert prediction['model_version'] == "v1.0.0-phase20"
    assert prediction['provenance_hash'] == "hash_xyz"
    assert prediction['target_return_threshold'] == 0.05

    # Cleanup
    db.prediction_history.delete_many({"symbol": "RELIANCE.NS"})
    db.historical_data.delete_many({"ticker": "RELIANCE.NS"})

def test_group_c_no_live_inference(client, db):
    """Group C: Patch ML boundaries to raise AssertionError if called."""
    with mock.patch("src.features.router.resolve_feature_pipeline", side_effect=AssertionError("LIVE INFERENCE WAS CALLED")):
        # If any live inference occurs, resolve_feature_pipeline would be called, or xgboost would be used.
        # But since we removed it, it won't be called.

        # Test summary endpoint
        response = client.get('/api/stocks/summary')
        assert response.status_code in [200, 404] # 404 if no predictions in DB, which is valid empty state

        # Test details endpoint
        response = client.get('/api/stocks/RELIANCE.NS/details')
        assert response.status_code in [200, 404]

def test_group_d_frontend_integration():
    """Group D: Check index.html and portfolio.html for legacy API usage."""
    index_html = os.path.join(os.path.dirname(__file__), "..", "templates", "index.html")
    with open(index_html, "r") as f:
        content = f.read()
    assert "/api/stocks/'+encodeURIComponent(ticker)+'/details" in content, "index.html should use /details endpoint"
    assert "fetch(API+'/api/stocks/'+encodeURIComponent(ticker));" not in content, "index.html should not use legacy endpoint"

    portfolio_html = os.path.join(os.path.dirname(__file__), "..", "templates", "portfolio.html")
    with open(portfolio_html, "r") as f:
        content = f.read()
    assert "/api/stocks/summary" in content, "portfolio.html should use /summary endpoint"
    assert "/api/portfolio" not in content, "portfolio.html should not use legacy /api/portfolio endpoint"

def test_group_e_empty_history(client, db):
    """Group E: Empty history -> controlled 404 or empty state without inference."""
    db.prediction_history.delete_many({"symbol": "HDFCBANK.NS"})
    db.historical_data.delete_many({"ticker": "HDFCBANK.NS"})
    db.historical_data.insert_one({"ticker": "HDFCBANK.NS", "date": datetime(2026, 8, 11), "open": 100, "close": 100})

    with mock.patch("src.features.router.resolve_feature_pipeline", side_effect=AssertionError("LIVE INFERENCE WAS CALLED")):
        response = client.get('/api/stocks/HDFCBANK.NS/details')
        assert response.status_code == 200
        data = response.json
        assert data['prediction'] is None # Controlled empty state

    db.historical_data.delete_many({"ticker": "HDFCBANK.NS"})

def test_group_f_stale_prediction(client, db):
    """Group F: Stale canonical prediction should not trigger inference."""
    db.prediction_history.delete_many({"symbol": "INFY.NS"})
    db.historical_data.delete_many({"ticker": "INFY.NS"})

    db.historical_data.insert_one({"ticker": "INFY.NS", "date": datetime(2026, 8, 11), "open": 100, "close": 100})

    # Old prediction
    db.prediction_history.insert_one({
        "symbol": "INFY.NS",
        "market_date": "2026-08-01",
        "prediction_timestamp": datetime(2026, 8, 1, 20, 0, 0),
        "prediction_horizon": 10,
        "recommendation": "SELL"
    })

    with mock.patch("src.features.router.resolve_feature_pipeline", side_effect=AssertionError("LIVE INFERENCE WAS CALLED")):
        response = client.get('/api/stocks/INFY.NS/details')
        assert response.status_code == 200
        data = response.json
        assert data['prediction']['market_date'] == "2026-08-01"
        assert data['prediction']['recommendation'] == "SELL"

    db.prediction_history.delete_many({"symbol": "INFY.NS"})
    db.historical_data.delete_many({"ticker": "INFY.NS"})

def test_group_g_mongodb_failure(client):
    """Group G: MongoDB failure -> controlled failure, no inference."""
    app.cache.clear()
    # We patch db.prediction_history.find_one to simulate PyMongoError
    with mock.patch("pymongo.collection.Collection.find_one", side_effect=Exception("Simulated MongoDB failure")):
        response = client.get('/api/stocks/TCS.NS/details')
        assert response.status_code in [500, 503]

    with mock.patch("pymongo.collection.Collection.find_one", side_effect=Exception("Simulated MongoDB failure")):
        response = client.get('/api/stocks/summary')
        assert response.status_code in [500, 503]
