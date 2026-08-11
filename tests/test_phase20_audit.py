import pytest
from app import app as flask_app
from datetime import datetime, timezone
import pymongo

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

def test_canonical_selection_semantics(client, db):
    db.prediction_history.delete_many({"symbol": "AUDIT_TEST"})

    # Record A: Older market_date, newer prediction_timestamp
    db.prediction_history.insert_one({
        "symbol": "AUDIT_TEST",
        "market_date": "2023-10-10",
        "prediction_timestamp": datetime(2023, 10, 15, tzinfo=timezone.utc),
        "prediction_horizon": 10,
        "recommendation": "SELL"
    })

    # Record B: Newer market_date, older prediction_timestamp
    db.prediction_history.insert_one({
        "symbol": "AUDIT_TEST",
        "market_date": "2023-10-12",
        "prediction_timestamp": datetime(2023, 10, 13, tzinfo=timezone.utc),
        "prediction_horizon": 10,
        "recommendation": "BUY"
    })

    from app import cache
    cache.clear()

    # Check details endpoint
    response = client.get('/api/stocks/AUDIT_TEST/details')
    data = response.get_json()
    endpoint_rec = data.get("prediction", {}).get("recommendation")

    # Direct DB queries for comparison
    doc_single_sort = db.prediction_history.find_one({"symbol": "AUDIT_TEST"}, sort=[("market_date", -1)])
    doc_compound_sort = db.prediction_history.find_one({"symbol": "AUDIT_TEST"}, sort=[("market_date", -1), ("prediction_timestamp", -1)])

    print("\n")
    print(f"Endpoint /details Result: {endpoint_rec}")
    print(f"Single Sort Result: {doc_single_sort['recommendation']}")
    print(f"Compound Sort Result: {doc_compound_sort['recommendation']}")

    # Check summary endpoint
    cache.clear()
    # Mock TICKERS to include AUDIT_TEST
    import app
    original_tickers = app.TICKERS
    app.TICKERS = ["AUDIT_TEST"]
    try:
        response_summary = client.get('/api/stocks/summary')
        data_summary = response_summary.get_json()
        summary_rec = None
        for p in data_summary.get("data", []):
            if p["ticker"] == "AUDIT_TEST":
                summary_rec = p["recommendation"]
        print(f"Endpoint /summary Result: {summary_rec}")
    finally:
        app.TICKERS = original_tickers
