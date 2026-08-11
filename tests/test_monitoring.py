import pytest
from unittest.mock import patch, MagicMock
from src.ml.monitoring import (
    calculate_metrics,
    calculate_financials,
    calculate_prediction_distribution,
    evaluate_model_health,
    analyze_performance,
    get_ticker_performance,
    get_system_health,
    fetch_evaluated_predictions
)
from app import app as flask_app

class MockCursor(list):
    def limit(self, limit):
        return MockCursor(self[:limit])
    def sort(self, *args, **kwargs):
        return self

@pytest.fixture
def mock_db():
    db = MagicMock()
    return db

from src.ml.model_utils import compute_settlement_hash, reconstruct_settlement_payload

def _create_valid_record(overrides=None):
    record = {
        "_id": "test_id",
        "status": "EVALUATED",
        "target_return_threshold": 0.02,
        "market_date": "2026-08-01",
        "symbol": "A",
        "provenance_hash": "dummy_prov",
        "settlement_market_date": "2026-08-10",
        "actual_price": 105.0,
        "actual_return": 0.05,
        "actual_class": "BUY",
        "recommendation_correct": True,
        "raw_prediction_correct": True
    }
    if overrides:
        record.update(overrides)

    canonical = reconstruct_settlement_payload(record)
    record["settlement_hash"] = compute_settlement_hash(canonical)
    return record

def test_fetch_evaluated_accepts_valid_hash(mock_db):
    valid_record = _create_valid_record()
    mock_db.prediction_history.find.return_value = MockCursor([valid_record])

    preds = fetch_evaluated_predictions(mock_db, {})
    assert len(preds) == 1
    assert preds[0]["_id"] == "test_id"

    call_args = mock_db.prediction_history.find.call_args[0][0]
    assert call_args["status"] == "EVALUATED"
    assert call_args["target_return_threshold"] == {"$ne": None}

def test_fetch_evaluated_rejects_missing_hash(mock_db):
    missing_hash_record = _create_valid_record()
    del missing_hash_record["settlement_hash"]

    mock_db.prediction_history.find.return_value = MockCursor([missing_hash_record])

    preds = fetch_evaluated_predictions(mock_db, {})
    assert len(preds) == 0

def test_fetch_evaluated_rejects_invalid_hash(mock_db):
    invalid_hash_record = _create_valid_record()
    invalid_hash_record["settlement_hash"] = "invalid_hash_value"

    mock_db.prediction_history.find.return_value = MockCursor([invalid_hash_record])

    preds = fetch_evaluated_predictions(mock_db, {})
    assert len(preds) == 0

def test_fetch_evaluated_mixed_population(mock_db):
    valid_record = _create_valid_record({"_id": "valid"})
    missing_hash = _create_valid_record({"_id": "missing"})
    del missing_hash["settlement_hash"]
    invalid_hash = _create_valid_record({"_id": "invalid"})
    invalid_hash["settlement_hash"] = "bad"

    mock_db.prediction_history.find.return_value = MockCursor([valid_record, missing_hash, invalid_hash])

    preds = fetch_evaluated_predictions(mock_db, {})
    assert len(preds) == 1
    assert preds[0]["_id"] == "valid"

def test_fetch_evaluated_isolates_reconstruction_failure(mock_db):
    valid_record = _create_valid_record({"_id": "valid"})

    # Create a record that will raise TypeError during json.dumps in compute_settlement_hash
    # We put a non-serializable object into a canonical field
    corrupt_record = _create_valid_record({"_id": "corrupt"})
    corrupt_record["actual_return"] = set([1, 2, 3]) # Sets are not JSON serializable

    mock_db.prediction_history.find.return_value = MockCursor([corrupt_record, valid_record])

    # Should not crash, should skip corrupt and return valid
    preds = fetch_evaluated_predictions(mock_db, {})
    assert len(preds) == 1
    assert preds[0]["_id"] == "valid"

def test_zero_sample_state():
    res = analyze_performance([])
    assert res["status"] == "INSUFFICIENT_DATA"
    assert res["sample_size"] == 0

def test_insufficient_sample_state():
    preds = [{"status": "EVALUATED", "actual_class": "BUY", "recommendation": "BUY"}] * 29
    res = analyze_performance(preds)
    assert res["status"] == "INSUFFICIENT_DATA"
    assert res["sample_size"] == 29

def test_meaningful_sample_state():
    preds = [{"status": "EVALUATED", "actual_class": "BUY", "recommendation": "BUY"}] * 30
    res = analyze_performance(preds)
    assert res["status"] == "MEANINGFUL_SAMPLE"
    assert res["sample_size"] == 30

def test_raw_and_recommendation_evaluated_separately():
    preds = [{
        "actual_class": "SELL",
        "raw_prediction": "BUY", # Incorrect raw
        "recommendation": "SELL" # Correct rec
    }]

    res = analyze_performance(preds)

    raw_acc = res["raw_model"]["classification"]["accuracy"]
    rec_acc = res["recommendation"]["classification"]["accuracy"]

    assert raw_acc == 0.0
    assert rec_acc == 1.0

def test_buy_hold_sell_metrics_and_confusion_matrix():
    preds = [
        {"actual_class": "BUY", "recommendation": "BUY"},
        {"actual_class": "SELL", "recommendation": "HOLD"},
        {"actual_class": "HOLD", "recommendation": "HOLD"}
    ]

    res = analyze_performance(preds)
    rec_cls = res["recommendation"]["classification"]

    assert rec_cls["accuracy"] == round(2/3, 4)

    cm = rec_cls["confusion_matrix"]
    assert cm["BUY"]["BUY"] == 1
    assert cm["SELL"]["HOLD"] == 1
    assert cm["HOLD"]["HOLD"] == 1

    assert rec_cls["precision"]["BUY"] == 1.0
    assert rec_cls["recall"]["BUY"] == 1.0

def test_directional_financials():
    preds = [
        {"recommendation": "BUY", "actual_return": 0.05}, # Hit
        {"recommendation": "BUY", "actual_return": -0.02}, # Miss
        {"recommendation": "SELL", "actual_return": -0.05}, # Hit (return -0.05 means SELL was right)
        {"recommendation": "SELL", "actual_return": 0.02}, # Miss
        {"recommendation": "HOLD", "actual_return": -0.01}, # Actual return should be preserved
        {"recommendation": "HOLD", "actual_return": 0.01}
    ]

    fin = calculate_financials(preds, "recommendation")

    assert fin["BUY"]["average_directional_return"] == 0.015
    assert fin["BUY"]["hit_rate"] == 0.5

    assert fin["SELL"]["average_directional_return"] == 0.015 # (-(-0.05) + -(0.02))/2 = (0.05 - 0.02)/2 = 0.015
    assert fin["SELL"]["hit_rate"] == 0.5

    # HOLD directional return is not fabricated, it returns average_actual_return
    assert fin["HOLD"]["average_actual_return"] == 0.0
    assert "hit_rate" not in fin["HOLD"]

def test_confidence_tier_grouping():
    preds = [
        {"actual_class": "BUY", "recommendation": "BUY", "confidence_tier": "HIGH"},
        {"actual_class": "BUY", "recommendation": "HOLD", "confidence_tier": "LOW"}
    ]

    res = analyze_performance(preds)
    conf = res["confidence_analysis"]

    assert "HIGH" in conf
    assert "LOW" in conf
    assert conf["HIGH"]["recommendation_accuracy"] == 1.0
    assert conf["LOW"]["recommendation_accuracy"] == 0.0

def test_ticker_and_model_version_separation(mock_db):
    mock_db.prediction_history.find.return_value = MockCursor([
        _create_valid_record({"status": "EVALUATED", "target_return_threshold": 0.02, "market_date": "2026-08-01", "symbol": "TCS.NS", "model_version": "v1", "recommendation": "BUY", "actual_class": "BUY"}),
        _create_valid_record({"status": "EVALUATED", "target_return_threshold": 0.02, "market_date": "2026-08-02", "symbol": "TCS.NS", "model_version": "v1", "recommendation": "BUY", "actual_class": "SELL"})
    ])

    perf_tcs_v1 = get_ticker_performance(mock_db, "TCS.NS", "v1")
    assert perf_tcs_v1["lifetime_performance"]["sample_size"] == 2

    call_args = mock_db.prediction_history.find.call_args[0][0]
    assert call_args["symbol"] == "TCS.NS"
    assert call_args["model_version"] == "v1"

def test_rolling_50_window_behavior(mock_db):
    # Insert 60 rows
    docs = []
    for i in range(60):
        docs.append(_create_valid_record({"status": "EVALUATED", "target_return_threshold": 0.02, "market_date": f"2026-01-{i:02d}", "symbol": "TCS.NS", "recommendation": "BUY", "actual_class": "BUY"}))
    mock_db.prediction_history.find.return_value = MockCursor(docs)

    perf = get_ticker_performance(mock_db, "TCS.NS")
    assert perf["lifetime_performance"]["sample_size"] == 60
    assert perf["rolling_performance"]["sample_size"] == 50

def test_health_state_reasoning(mock_db):
    # sample_size = 29 -> INSUFFICIENT_DATA
    mock_db.prediction_history.find.return_value = MockCursor([
        _create_valid_record({"status": "EVALUATED", "target_return_threshold": 0.02, "market_date": "2026-08-01", "symbol": "A", "recommendation": "BUY", "actual_class": "BUY"})
    ] * 29)
    health = get_system_health(mock_db)["health"]
    assert health["state"] == "INSUFFICIENT_DATA"

    # sample_size = 30 -> HEALTHY
    mock_db.prediction_history.find.return_value = MockCursor([
        _create_valid_record({"status": "EVALUATED", "target_return_threshold": 0.02, "market_date": "2026-08-01", "symbol": "A", "recommendation": "BUY", "actual_class": "BUY"})
    ] * 30)
    health = get_system_health(mock_db)["health"]
    assert health["state"] == "HEALTHY"

    # sample_size > 30 with rolling_accuracy = 0.39 -> HEALTHY
    mock_db.prediction_history.find.return_value = MockCursor([
        _create_valid_record({"status": "EVALUATED", "target_return_threshold": 0.02, "market_date": "2026-08-01", "symbol": "C", "recommendation": "BUY", "actual_class": "SELL"})
    ] * 61 + [
        _create_valid_record({"status": "EVALUATED", "target_return_threshold": 0.02, "market_date": "2026-08-01", "symbol": "C", "recommendation": "BUY", "actual_class": "BUY"})
    ] * 39)
    health = get_system_health(mock_db)["health"]
    assert health["state"] == "HEALTHY"

    # sample_size > 30 with rolling_accuracy = 0.49 -> HEALTHY
    mock_db.prediction_history.find.return_value = MockCursor([
        _create_valid_record({"status": "EVALUATED", "target_return_threshold": 0.02, "market_date": "2026-08-01", "symbol": "C", "recommendation": "BUY", "actual_class": "SELL"})
    ] * 51 + [
        _create_valid_record({"status": "EVALUATED", "target_return_threshold": 0.02, "market_date": "2026-08-01", "symbol": "C", "recommendation": "BUY", "actual_class": "BUY"})
    ] * 49)
    health = get_system_health(mock_db)["health"]
    assert health["state"] == "HEALTHY"

    # sample_size > 30 with rolling_accuracy = 0.80 -> HEALTHY
    mock_db.prediction_history.find.return_value = MockCursor([
        _create_valid_record({"status": "EVALUATED", "target_return_threshold": 0.02, "market_date": "2026-08-01", "symbol": "C", "recommendation": "BUY", "actual_class": "SELL"})
    ] * 20 + [
        _create_valid_record({"status": "EVALUATED", "target_return_threshold": 0.02, "market_date": "2026-08-01", "symbol": "C", "recommendation": "BUY", "actual_class": "BUY"})
    ] * 80)
    health = get_system_health(mock_db)["health"]
    assert health["state"] == "HEALTHY"

    # sample_size > 30 with HOLD distribution = 0.96 -> HEALTHY
    mock_db.prediction_history.find.return_value = MockCursor([
        _create_valid_record({"status": "EVALUATED", "target_return_threshold": 0.02, "market_date": "2026-08-01", "symbol": "B", "recommendation": "BUY", "actual_class": "BUY"})
    ] * 4 + [
        _create_valid_record({"status": "EVALUATED", "target_return_threshold": 0.02, "market_date": "2026-08-01", "symbol": "B", "recommendation": "HOLD", "actual_class": "HOLD"})
    ] * 96)
    health = get_system_health(mock_db)["health"]
    assert health["state"] == "HEALTHY"

def test_api_degraded_on_failure():
    with flask_app.test_client() as client:
        with patch('app.fetch_evaluated_predictions', side_effect=Exception("DB Error")):
            resp = client.get('/api/predictions/performance')
            assert resp.status_code == 503
            assert resp.get_json()["state"] == "DEGRADED"

        with patch('app.get_system_health', side_effect=Exception("DB Error")):
            resp = client.get('/api/models/health')
            assert resp.status_code == 503
            assert resp.get_json()["state"] == "DEGRADED"

        with patch('app.get_ticker_performance', side_effect=Exception("DB Error")):
            resp = client.get('/api/models/TCS.NS/performance')
            assert resp.status_code == 503
            assert resp.get_json()["state"] == "DEGRADED"
