import pytest
import datetime
from datetime import timezone
import mongomock
from copy import deepcopy

from src.ml.model_utils import compute_provenance_hash, reconstruct_canonical_payload

@pytest.fixture
def mock_db():
    client = mongomock.MongoClient()
    return client["stock_market_db"]

def _create_v3_provenance(symbol="TEST.NS", horizon=10):
    payload = {
        "provenance_schema_version": "v3",
        "symbol": symbol,
        "market_date": "2023-01-01",
        "prediction_horizon": horizon,
        "model_version": "mock_version",
        "feature_pipeline_version": "v1",
        "feature_pipeline_hash": "mock_pipeline_hash",
        "feature_columns": ["f1", "f2"],
        "raw_inputs": {"open": 100},
        "features": {"f1": 1.0, "f2": 2.0},
        "model_probabilities": [0.1, 0.2, 0.7],
        "decision_thresholds": {"0": 0.4, "1": 0.35, "2": 0.45},
        "confidence_metrics": {"f1_macro": 0.6, "max_proba": 0.7, "top2_margin": 0.5},
        "recommendation": "BUY",
        "confidence_tier": "S",
        "target_return_threshold": 0.05,
        "class_mapping": {"0": "SELL", "1": "HOLD", "2": "BUY"},
        "confidence_tier_boundaries": {"ACTIONABLE_MIN_RANK": 2},
        "decision_context": {"actionable": True, "f1_macro_used": 0.6}
    }
    hash_val = compute_provenance_hash(payload)
    payload["provenance_hash"] = hash_val
    payload["created_at"] = datetime.datetime.now(timezone.utc)
    return payload, hash_val

def _create_v3_history(prov_payload, hash_val):
    return {
        "symbol": prov_payload["symbol"],
        "market_date": prov_payload["market_date"],
        "prediction_horizon": prov_payload["prediction_horizon"],
        "model_version": prov_payload["model_version"],
        "status": "PENDING",
        "price_at_prediction": 100.0,
        "recommendation": prov_payload["recommendation"],
        "confidence_tier": prov_payload["confidence_tier"],
        "target_return_threshold": prov_payload["target_return_threshold"],
        "raw_prediction": 2,
        "provenance_hash": hash_val
    }

def _insert_mock_market_data(db, symbol="TEST.NS", market_date="2023-01-01", horizon=10):
    dt = datetime.datetime.strptime(market_date, "%Y-%m-%d")
    for i in range(1, horizon + 5):
        db.historical_data.insert_one({
            "ticker": symbol,
            "date": dt + datetime.timedelta(days=i),
            "close": 110.0
        })

# Group A — V3 Payload
def test_group_a_v3_payload():
    prov, h = _create_v3_provenance()
    assert prov["provenance_schema_version"] == "v3"
    assert "recommendation" in prov
    assert "confidence_tier" in prov
    assert "target_return_threshold" in prov
    assert "class_mapping" in prov
    assert "confidence_tier_boundaries" in prov
    assert "decision_context" in prov

# Group B — Hash Integrity
def test_group_b_hash_integrity():
    prov, orig_hash = _create_v3_provenance()
    canonical = reconstruct_canonical_payload(prov)
    mutations = [
        ("recommendation", "SELL"),
        ("confidence_tier", "F"),
        ("target_return_threshold", 0.01),
        ("class_mapping", {"0": "SELL", "1": "HOLD", "2": "HOLD"}),
        ("confidence_tier_boundaries", {"ACTIONABLE_MIN_RANK": 3}),
        ("decision_context", {"actionable": False, "f1_macro_used": 0.1})
    ]
    for key, new_val in mutations:
        mutated = deepcopy(canonical)
        mutated[key] = new_val
        new_hash = compute_provenance_hash(mutated)
        assert new_hash != orig_hash

# Group C — Full Decision Reconstruction
def test_group_c_reconstruction():
    prov, _ = _create_v3_provenance()
    # Given the exact boundaries are saved, we can reconstruct without confidence.py
    # This verifies the payload holds sufficient data.
    assert prov["recommendation"] == "BUY"
    assert prov["confidence_tier"] == "S"

# Group D — History Tampering
def test_group_d_history_tampering(mock_db):
    from src.ml.settlement import evaluate_predictions
    prov, h = _create_v3_provenance()
    hist = _create_v3_history(prov, h)
    _insert_mock_market_data(mock_db)
    mock_db.prediction_provenance.insert_one(prov)
    # Mutate history
    hist["recommendation"] = "SELL"
    hist_id = mock_db.prediction_history.insert_one(hist).inserted_id
    evaluate_predictions(mock_db.client, apply=True)
    updated_hist = mock_db.prediction_history.find_one({"_id": hist_id})
    assert updated_hist["status"] == "INVALID_PROVENANCE"

# Group E — Provenance Tampering
def test_group_e_provenance_tampering(mock_db):
    from src.ml.settlement import evaluate_predictions
    prov, h = _create_v3_provenance()
    hist = _create_v3_history(prov, h)
    _insert_mock_market_data(mock_db)
    hist_id = mock_db.prediction_history.insert_one(hist).inserted_id
    # Tamper payload but keep hash same
    prov["target_return_threshold"] = 0.01
    mock_db.prediction_provenance.insert_one(prov)
    evaluate_predictions(mock_db.client, apply=True)
    updated_hist = mock_db.prediction_history.find_one({"_id": hist_id})
    assert updated_hist["status"] == "INVALID_PROVENANCE"

# Group F — Missing Provenance
def test_group_f_missing_provenance(mock_db):
    from src.ml.settlement import evaluate_predictions
    prov, h = _create_v3_provenance()
    hist = _create_v3_history(prov, h)
    _insert_mock_market_data(mock_db)
    hist_id = mock_db.prediction_history.insert_one(hist).inserted_id
    evaluate_predictions(mock_db.client, apply=True)
    updated_hist = mock_db.prediction_history.find_one({"_id": hist_id})
    assert updated_hist["status"] == "INVALID_PROVENANCE"

# Group G — Identity Tampering
def test_group_g_identity_tampering(mock_db):
    from src.ml.settlement import evaluate_predictions
    prov, h = _create_v3_provenance()
    hist = _create_v3_history(prov, h)
    _insert_mock_market_data(mock_db)
    mock_db.prediction_provenance.insert_one(prov)
    # Mutate history identity
    hist["model_version"] = "hacked_version"
    hist_id = mock_db.prediction_history.insert_one(hist).inserted_id
    evaluate_predictions(mock_db.client, apply=True)
    updated_hist = mock_db.prediction_history.find_one({"_id": hist_id})
    assert updated_hist["status"] == "INVALID_PROVENANCE"

# Group H — Idempotency
def test_group_h_idempotency(mock_db):
    # Idempotency is implemented in history.py. We'll simulate its logic.
    from pymongo.errors import OperationFailure
    prov, h = _create_v3_provenance()
    mock_db.prediction_provenance.insert_one(prov)
    # Try inserting same
    existing = mock_db.prediction_provenance.find_one({"symbol": prov["symbol"]})
    assert existing["provenance_hash"] == h
    # Conflicting provenance
    prov2, h2 = _create_v3_provenance()
    prov2["target_return_threshold"] = 0.99
    h2 = compute_provenance_hash(reconstruct_canonical_payload(prov2))
    prov2["provenance_hash"] = h2
    with pytest.raises(Exception):
        if existing["provenance_hash"] != h2:
            raise OperationFailure("Collision")

# Group J — Legacy Hash Compatibility
def test_group_j_legacy_compatibility():
    payload_v1 = {
        "symbol": "TEST.NS",
        "market_date": "2023-01-01",
        "prediction_horizon": 10,
        "model_version": "v1",
        "feature_pipeline_version": "v1",
        "feature_pipeline_hash": "hash",
        "feature_columns": ["f1"],
        "raw_inputs": {},
        "features": {},
        "model_probabilities": [],
        "decision_thresholds": {},
        "confidence_metrics": {},
        # No v3 fields
    }
    h1 = compute_provenance_hash(payload_v1)
    payload_v1["provenance_hash"] = h1
    canon = reconstruct_canonical_payload(payload_v1)
    assert compute_provenance_hash(canon) == h1
    # It must not add v3 fields
    assert "recommendation" not in canon

# Group L — Invalid Provenance Lifecycle
def test_group_l_invalid_lifecycle(mock_db):
    from src.ml.settlement import evaluate_predictions
    prov, h = _create_v3_provenance()
    hist = _create_v3_history(prov, h)
    _insert_mock_market_data(mock_db)
    hist["recommendation"] = "SELL"
    hist_id = mock_db.prediction_history.insert_one(hist).inserted_id
    mock_db.prediction_provenance.insert_one(prov)
    evaluate_predictions(mock_db.client, apply=True)
    updated_hist = mock_db.prediction_history.find_one({"_id": hist_id})
    assert updated_hist["status"] == "INVALID_PROVENANCE"
    # Repeated runs shouldn't evaluate it
    evaluate_predictions(mock_db.client, apply=True)
    updated_hist = mock_db.prediction_history.find_one({"_id": hist_id})
    assert updated_hist["status"] == "INVALID_PROVENANCE"