import pytest
from datetime import datetime, timedelta
import pandas as pd
from unittest.mock import MagicMock, patch

from src.ml.evaluation import evaluate_pending_predictions
from src.ml.model_utils import compute_provenance_hash
from tests.test_settlement import create_mock_db


def _create_prov_pred(pred_dict):
    """Helper to attach a mock provenance payload to prediction record."""
    pred_dict["model_version"] = pred_dict.get("model_version", "mock_ver")
    fake_payload = {
        "provenance_schema_version": "v2",
        "symbol": pred_dict["symbol"],
        "market_date": pred_dict["market_date"],
        "prediction_horizon": pred_dict.get("prediction_horizon", 10),
        "model_version": pred_dict["model_version"]
    }
    pred_dict["provenance_hash"] = compute_provenance_hash(fake_payload)
    return pred_dict


def test_delegation():
    """Test 1: Verify evaluation.py delegates directly to canonical settlement engine."""
    client_mock = MagicMock()
    with patch("src.ml.evaluation.evaluate_predictions") as mock_settle:
        mock_settle.return_value = {"READY_TO_SETTLE": 5, "ERRORS": 0}
        
        # Test with default apply=True
        stats1 = evaluate_pending_predictions(client_mock)
        mock_settle.assert_called_once_with(client_mock, apply=True)
        assert stats1 == {"READY_TO_SETTLE": 5, "ERRORS": 0}

        # Test with apply=False
        mock_settle.reset_mock()
        stats2 = evaluate_pending_predictions(client_mock, apply=False)
        mock_settle.assert_called_once_with(client_mock, apply=False)
        assert stats2 == {"READY_TO_SETTLE": 5, "ERRORS": 0}


def test_legacy_record_quarantined_no_type_error():
    """
    Test 2: Given a legacy record with target_return_threshold = None, verify:
      - no TypeError
      - no fallback threshold assigned
      - no evaluation
      - no mutation of the legacy record (classified as LEGACY_UNSETTLEABLE)
    """
    market_date_str = "2026-08-05"
    market_date = pd.to_datetime(market_date_str).to_pydatetime()

    # Legacy record from August 5, 2026 lacking target_return_threshold and threshold_pct
    legacy_pred = {
        "_id": "legacy_1",
        "symbol": "ADANIENT.NS",
        "market_date": market_date_str,
        "prediction_horizon": 10,
        "price_at_prediction": 3050.0,
        "raw_prediction": "SELL",
        "recommendation": "SELL",
        "target_return_threshold": None,
        "threshold_pct": None,
        "status": "PENDING"
    }

    # Historical data with 15 subsequent sessions (> 10 sessions)
    hist = [{"ticker": "ADANIENT.NS", "date": market_date, "close": 3050.0}]
    for i in range(1, 16):
        hist.append({
            "ticker": "ADANIENT.NS",
            "date": market_date + timedelta(days=i),
            "close": 2990.0
        })

    client_mock, db_mock = create_mock_db([legacy_pred], hist)
    
    # Must run without TypeError and quarantine as LEGACY_UNSETTLEABLE
    stats = evaluate_pending_predictions(client_mock, apply=True)

    assert stats["LEGACY_UNSETTLEABLE"] == 1
    assert stats["READY_TO_SETTLE"] == 0
    assert stats["ERRORS"] == 0
    # Crucially, update_one was NOT called to mutate or evaluate the legacy record
    db_mock.prediction_history.update_one.assert_not_called()


def test_threshold_pct_none_cannot_cause_type_error():
    """
    Test 3: Verify that threshold_pct = None cannot be used as the economic evaluation threshold
    and that the old -threshold failure cannot occur.
    """
    market_date_str = "2026-09-01"
    market_date = pd.to_datetime(market_date_str).to_pydatetime()

    # Current-schema record has valid target_return_threshold, but threshold_pct is explicitly None
    pred = _create_prov_pred({
        "_id": "pred_none_threshold_pct",
        "symbol": "TICKER_X",
        "market_date": market_date_str,
        "prediction_horizon": 10,
        "target_return_threshold": 0.025,
        "threshold_pct": None, # None in document
        "price_at_prediction": 100.0,
        "raw_prediction": "SELL",
        "recommendation": "SELL",
        "status": "PENDING"
    })

    hist = [{"ticker": "TICKER_X", "date": market_date, "close": 100.0}]
    for i in range(1, 11):
        hist.append({
            "ticker": "TICKER_X",
            "date": market_date + timedelta(days=i),
            "close": 95.0 # return -5%, exceeds -2.5% threshold
        })

    client_mock, db_mock = create_mock_db([pred], hist)

    # Must not raise TypeError
    stats = evaluate_pending_predictions(client_mock, apply=True)

    assert stats["READY_TO_SETTLE"] == 1
    assert stats["ERRORS"] == 0
    db_mock.prediction_history.update_one.assert_called_once()
    update_payload = db_mock.prediction_history.update_one.call_args[0][1]["$set"]
    assert update_payload["actual_class"] == "SELL"
    assert update_payload["recommendation_correct"] is True
    assert update_payload["status"] == "EVALUATED"


def test_current_mature_record_delegates_to_canonical():
    """
    Test 4: Given a mature current-schema prediction with target_return_threshold > 0,
    verify that evaluation is delegated to the canonical engine and evaluates accurately.
    """
    market_date_str = "2026-09-01"
    market_date = pd.to_datetime(market_date_str).to_pydatetime()

    pred = _create_prov_pred({
        "_id": "mature_buy_1",
        "symbol": "BUY_TICKER",
        "market_date": market_date_str,
        "prediction_horizon": 10,
        "target_return_threshold": 0.03,
        "price_at_prediction": 100.0,
        "raw_prediction": "BUY",
        "recommendation": "BUY",
        "status": "PENDING"
    })

    hist = [{"ticker": "BUY_TICKER", "date": market_date, "close": 100.0}]
    for i in range(1, 11):
        hist.append({
            "ticker": "BUY_TICKER",
            "date": market_date + timedelta(days=i),
            "close": 105.0 if i == 10 else 100.0 # +5% return
        })

    client_mock, db_mock = create_mock_db([pred], hist)
    stats = evaluate_pending_predictions(client_mock, apply=True)

    assert stats["READY_TO_SETTLE"] == 1
    assert stats["ERRORS"] == 0
    db_mock.prediction_history.update_one.assert_called_once()
    update_payload = db_mock.prediction_history.update_one.call_args[0][1]["$set"]
    assert update_payload["actual_return"] == pytest.approx(0.05)
    assert update_payload["actual_class"] == "BUY"
    assert update_payload["outcome"] == "CORRECT"
    assert "settlement_hash" in update_payload


def test_immature_record_remains_pending():
    """
    Test 5: Verify that an immature current-schema record remains PENDING and is not evaluated.
    """
    market_date_str = "2026-09-20"
    market_date = pd.to_datetime(market_date_str).to_pydatetime()

    pred = _create_prov_pred({
        "_id": "immature_1",
        "symbol": "IMMATURE_TICKER",
        "market_date": market_date_str,
        "prediction_horizon": 10,
        "target_return_threshold": 0.02,
        "price_at_prediction": 100.0,
        "raw_prediction": "BUY",
        "recommendation": "BUY",
        "status": "PENDING"
    })

    # Only 3 trading days have passed (< 10)
    hist = [{"ticker": "IMMATURE_TICKER", "date": market_date, "close": 100.0}]
    for i in range(1, 4):
        hist.append({
            "ticker": "IMMATURE_TICKER",
            "date": market_date + timedelta(days=i),
            "close": 101.0
        })

    client_mock, db_mock = create_mock_db([pred], hist)
    stats = evaluate_pending_predictions(client_mock, apply=True)

    assert stats["NOT_MATURE"] == 1
    assert stats["READY_TO_SETTLE"] == 0
    assert stats["ERRORS"] == 0
    # No updates performed
    db_mock.prediction_history.update_one.assert_not_called()


def test_already_evaluated_record_idempotent():
    """
    Test 6: Verify that if an already evaluated record is processed, it remains unchanged.
    """
    market_date_str = "2026-09-01"
    market_date = pd.to_datetime(market_date_str).to_pydatetime()

    pred = _create_prov_pred({
        "_id": "already_eval_1",
        "symbol": "ALREADY_EVAL",
        "market_date": market_date_str,
        "prediction_horizon": 10,
        "target_return_threshold": 0.02,
        "price_at_prediction": 100.0,
        "raw_prediction": "HOLD",
        "recommendation": "HOLD",
        "status": "PENDING"
    })

    hist = [{"ticker": "ALREADY_EVAL", "date": market_date, "close": 100.0}]
    for i in range(1, 11):
        hist.append({
            "ticker": "ALREADY_EVAL",
            "date": market_date + timedelta(days=i),
            "close": 100.5
        })

    client_mock, db_mock = create_mock_db([pred], hist)
    
    # Mock update_one to simulate record already settled (matched_count = 0)
    db_mock.prediction_history.update_one.return_value.matched_count = 0

    stats = evaluate_pending_predictions(client_mock, apply=True)

    assert stats["READY_TO_SETTLE"] == 1
    assert stats["ALREADY_EVALUATED"] == 1
    assert stats["ERRORS"] == 0


def test_canonical_failure_propagation():
    """
    Test 7: If the canonical settlement engine raises a genuine integrity/production error,
    verify that the compatibility wrapper does not silently swallow it.
    """
    client_mock = MagicMock()
    with patch("src.ml.evaluation.evaluate_predictions") as mock_settle:
        mock_settle.side_effect = RuntimeError("CRITICAL_DATABASE_INTEGRITY_FAILURE")
        
        with pytest.raises(RuntimeError) as exc_info:
            evaluate_pending_predictions(client_mock)
            
        assert "CRITICAL_DATABASE_INTEGRITY_FAILURE" in str(exc_info.value)
