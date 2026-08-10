import pytest
from datetime import datetime, timezone
import mongomock
from src.ml.model_utils import compute_settlement_hash, reconstruct_settlement_payload
from src.ml.settlement import evaluate_predictions
from src.ml.monitoring import fetch_evaluated_predictions

@pytest.fixture
def client():
    return mongomock.MongoClient()

@pytest.fixture
def db(client):
    return client["stock_market_db"]

@pytest.fixture
def base_payload():
    return {
        "provenance_hash": "a1b2c3d4e5f6",
        "settlement_market_date": "2023-10-15",
        "actual_price": 105.0,
        "actual_return": 0.05,
        "actual_class": "BUY",
        "recommendation_correct": True,
        "raw_prediction_correct": True
    }

def test_group_a_hash_determinism(base_payload):
    # 1. Identical payload -> identical hash
    hash1 = compute_settlement_hash(base_payload)
    hash2 = compute_settlement_hash(base_payload.copy())
    assert hash1 == hash2

    # 2. Dictionary order changes -> identical hash
    reordered_payload = {k: base_payload[k] for k in reversed(list(base_payload.keys()))}
    hash3 = compute_settlement_hash(reordered_payload)
    assert hash1 == hash3

    # 3. Integer/float normalization
    float_payload = base_payload.copy()
    float_payload["actual_price"] = 105.0  # Kept as float for determinism test
    hash4 = compute_settlement_hash(float_payload)
    assert hash1 == hash4

def test_group_b_hash_sensitivity(base_payload):
    baseline = compute_settlement_hash(base_payload)
    # Mutate provenance hash
    m1 = base_payload.copy(); m1["provenance_hash"] = "tampered"
    assert compute_settlement_hash(m1) != baseline
    # Mutate date
    m2 = base_payload.copy(); m2["settlement_market_date"] = "2023-10-16"
    assert compute_settlement_hash(m2) != baseline

    # Mutate price
    m3 = base_payload.copy(); m3["actual_price"] = 106.0
    assert compute_settlement_hash(m3) != baseline

    # Mutate return
    m4 = base_payload.copy(); m4["actual_return"] = 0.06
    assert compute_settlement_hash(m4) != baseline

    # Mutate class
    m5 = base_payload.copy(); m5["actual_class"] = "HOLD"
    assert compute_settlement_hash(m5) != baseline

    # Mutate correctness
    m6 = base_payload.copy(); m6["recommendation_correct"] = False
    assert compute_settlement_hash(m6) != baseline

    m7 = base_payload.copy(); m7["raw_prediction_correct"] = False
    assert compute_settlement_hash(m7) != baseline

def test_group_c_settlement_integration(client, db):
    market_date_str = "2023-10-01"
    # Insert provenance
    from src.ml.model_utils import compute_provenance_hash, reconstruct_canonical_payload
    prov = {
        "provenance_schema_version": "v3",
        "symbol": "TICKER",
        "market_date": market_date_str,
        "prediction_horizon": 1,
        "model_version": "mv1",
        "recommendation": "BUY",
        "confidence_tier": "HIGH",
        "target_return_threshold": 0.05
    }
    canon = reconstruct_canonical_payload(prov)
    real_prov_hash = compute_provenance_hash(canon)
    prov["provenance_hash"] = real_prov_hash
    db.prediction_provenance.insert_one(prov)
    # Insert history
    hist = {
        "_id": "p1",
        "status": "PENDING",
        "symbol": "TICKER",
        "market_date": market_date_str,
        "prediction_horizon": 1,
        "model_version": "mv1",
        "provenance_hash": real_prov_hash,
        "price_at_prediction": 100.0,
        "raw_prediction": "BUY",
        "recommendation": "BUY",
        "target_return_threshold": 0.05,
        "confidence_tier": "HIGH"
    }
    db.prediction_history.insert_one(hist)
    # Insert market data for settlement
    db.historical_data.insert_one({
        "ticker": "TICKER",
        "date": datetime(2023, 10, 2),
        "close": 110.0
    })
    # Run evaluation
    evaluate_predictions(client, apply=True)
    record = db.prediction_history.find_one({"_id": "p1"})
    assert record["status"] == "EVALUATED"
    assert "settlement_hash" in record
    canonical = reconstruct_settlement_payload(record)
    expected_hash = compute_settlement_hash(canonical)
    assert record["settlement_hash"] == expected_hash

def test_group_d_tampering_detection(db):
    market_date_str = "2023-10-01"
    # Insert valid settled record
    hist = {
        "_id": "p1",
        "status": "EVALUATED",
        "symbol": "TICKER",
        "market_date": market_date_str,
        "prediction_horizon": 1,
        "target_return_threshold": 0.05,
        "provenance_hash": "hash1",
        "settlement_market_date": "2023-10-02",
        "actual_price": 110.0,
        "actual_return": 0.10,
        "actual_class": "BUY",
        "recommendation_correct": True,
        "raw_prediction_correct": True
    }
    # Compute correct hash
    canonical = reconstruct_settlement_payload(hist)
    hist["settlement_hash"] = compute_settlement_hash(canonical)
    db.prediction_history.insert_one(hist)
    # Valid fetch
    res = fetch_evaluated_predictions(db, {})
    assert len(res) == 1
    # Tamper with actual return
    db.prediction_history.update_one({"_id": "p1"}, {"$set": {"actual_return": 0.50}})
    res = fetch_evaluated_predictions(db, {})
    assert len(res) == 0  # Tampered record dropped
    # Fix return, tamper with correctness
    db.prediction_history.update_one({"_id": "p1"}, {"$set": {"actual_return": 0.10, "recommendation_correct": False}})
    res = fetch_evaluated_predictions(db, {})
    assert len(res) == 0  # Tampered record dropped

def test_group_e_valid_monitoring(db):
    hist = {
        "_id": "p1",
        "status": "EVALUATED",
        "target_return_threshold": 0.05,
        "provenance_hash": "hash1",
        "settlement_market_date": "2023-10-02",
        "actual_price": 110.0,
        "actual_return": 0.10,
        "actual_class": "BUY",
        "recommendation_correct": True,
        "raw_prediction_correct": True
    }
    canonical = reconstruct_settlement_payload(hist)
    hist["settlement_hash"] = compute_settlement_hash(canonical)
    db.prediction_history.insert_one(hist)
    res = fetch_evaluated_predictions(db, {})
    assert len(res) == 1

def test_group_f_legacy_compatibility(db):
    # Record without settlement_hash
    hist = {
        "_id": "p1",
        "status": "EVALUATED",
        "target_return_threshold": 0.05,
        "provenance_hash": "hash1",
        "actual_return": 0.10,
        "recommendation_correct": True
    }
    db.prediction_history.insert_one(hist)
    res = fetch_evaluated_predictions(db, {})
    assert len(res) == 1  # Legacy record passed

def test_group_g_missing_corrupt_data(db):
    hist = {
        "_id": "p1",
        "status": "EVALUATED",
        "target_return_threshold": 0.05,
        "provenance_hash": "hash1",
        # Missing other fields
        "settlement_hash": "badhash"
    }
    db.prediction_history.insert_one(hist)
    res = fetch_evaluated_predictions(db, {})
    assert len(res) == 0  # Rejected due to bad hash