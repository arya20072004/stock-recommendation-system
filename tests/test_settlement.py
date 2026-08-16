import pytest
from datetime import datetime, timezone, timedelta
import pandas as pd
from pytest import approx
from unittest.mock import MagicMock, patch

from src.ml.settlement import evaluate_predictions
from src.features.router import resolve_feature_pipeline

get_target_return_threshold = resolve_feature_pipeline("v1").get_target_return_threshold

def test_canonical_target_threshold():
    """Test get_target_return_threshold works correctly with floats and Series"""
    # 0.02 * 0.75 = 0.015 (for SHRIRAMFIN.NS)
    # 0.005 * 0.75 = 0.00375 -> clipped to 0.01

    assert get_target_return_threshold("SHRIRAMFIN.NS", 0.02) == 0.015
    assert get_target_return_threshold("SHRIRAMFIN.NS", 0.005) == 0.01

    # Test pandas Series
    s = pd.Series([0.02, 0.005])
    res = get_target_return_threshold("SHRIRAMFIN.NS", s)
    assert len(res) == 2
    assert res.iloc[0] == 0.015
    assert res.iloc[1] == 0.01

def create_mock_db(pending_preds, historical_data):
    db_mock = MagicMock()

    # Setup pending predictions
    cursor_mock = MagicMock()
    cursor_mock.sort.return_value = pending_preds
    db_mock.prediction_history.find.return_value = cursor_mock

    # Setup historical data global distinct
    all_dates = sorted(list(set([d["date"] for d in historical_data])))
    db_mock.historical_data.distinct.return_value = all_dates

    # Setup historical data find for specific ticker
    def mock_historical_find(query):
        ticker = query["ticker"]
        date_gt = query["date"]["$gt"]

        matches = [
            d for d in historical_data
            if d["ticker"] == ticker
            and d["date"] > date_gt
            and d.get("close", 0) > 0
        ]

        h_cursor = MagicMock()
        h_cursor.sort.return_value.limit.return_value = sorted(matches, key=lambda x: x["date"])[:query.get("limit", 10)]
        return h_cursor

    db_mock.historical_data.find.side_effect = mock_historical_find

    def mock_historical_find_one(query):
        ticker = query.get("ticker")
        date = query.get("date")
        if ticker and date:
            matches = [d for d in historical_data if d["ticker"] == ticker and d["date"] == date]
            return matches[0] if matches else None
        return None

    db_mock.historical_data.find_one.side_effect = mock_historical_find_one

    # Setup update_one
    update_result = MagicMock()
    update_result.matched_count = 1
    db_mock.prediction_history.update_one.return_value = update_result

    # Phase 18: Mock prediction_provenance
    def mock_provenance_find_one(query):
        h = query.get("provenance_hash")
        for p in pending_preds:
            if p.get("provenance_hash") == h:
                # Return a legacy v2 provenance matching identity
                return {
                    "provenance_schema_version": "v2",
                    "provenance_hash": h,
                    "symbol": p.get("symbol"),
                    "market_date": p.get("market_date"),
                    "prediction_horizon": p.get("prediction_horizon", 10),
                    "model_version": p.get("model_version", "mock_ver")
                }
        return None
    db_mock.prediction_provenance.find_one.side_effect = mock_provenance_find_one

    client_mock = MagicMock()
    client_mock.__getitem__.return_value = db_mock
    return client_mock, db_mock

def test_settlement_maturity_and_classes():
    from src.ml.model_utils import compute_provenance_hash
    market_date_str = "2026-08-01"
    market_date = pd.to_datetime(market_date_str).to_pydatetime()

    def _add_prov(p):
        p["model_version"] = "mock_ver"
        fake_payload = {
            "provenance_schema_version": "v2",
            "symbol": p["symbol"],
            "market_date": p["market_date"],
            "prediction_horizon": p.get("prediction_horizon", 10),
            "model_version": "mock_ver"
        }
        p["provenance_hash"] = compute_provenance_hash(fake_payload)
        return p

    # 1. Mature BUY
    pred1 = _add_prov({
        "_id": "p1", "symbol": "TICKER1", "market_date": market_date_str,
        "prediction_horizon": 10, "target_return_threshold": 0.05,
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    })

    # 2. Mature SELL
    pred2 = _add_prov({
        "_id": "p2", "symbol": "TICKER2", "market_date": market_date_str,
        "prediction_horizon": 10, "target_return_threshold": 0.05,
        "price_at_prediction": 100.0, "raw_prediction": "SELL", "recommendation": "SELL"
    })

    # 3. Mature HOLD (inside threshold)
    pred3 = _add_prov({
        "_id": "p3", "symbol": "TICKER3", "market_date": market_date_str,
        "prediction_horizon": 10, "target_return_threshold": 0.05,
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "HOLD" # recommendation changed
    })

    # 4. Exact boundaries (should be HOLD)
    pred4 = _add_prov({
        "_id": "p4", "symbol": "TICKER4", "market_date": market_date_str,
        "prediction_horizon": 10, "target_return_threshold": 105.0/100.0 - 1.0,
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    })
    pred5 = _add_prov({
        "_id": "p5", "symbol": "TICKER5", "market_date": market_date_str,
        "prediction_horizon": 10, "target_return_threshold": -(95.0/100.0 - 1.0),
        "price_at_prediction": 100.0, "raw_prediction": "SELL", "recommendation": "SELL"
    })

    # 5. Missing Threshold (Legacy)
    pred6 = _add_prov({
        "_id": "p6", "symbol": "TICKER6", "market_date": market_date_str,
        "prediction_horizon": 10, "price_at_prediction": 100.0,
        "raw_prediction": "BUY", "recommendation": "BUY"
    })

    # 6. Not mature (only 5 future sessions global)
    pred7 = _add_prov({
        "_id": "p7", "symbol": "TICKER7", "market_date": market_date_str,
        "prediction_horizon": 10, "target_return_threshold": 0.05,
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    })

    # 7. Missing market data (10 global sessions, but only 9 for this ticker)
    pred8 = _add_prov({
        "_id": "p8", "symbol": "TICKER8", "market_date": market_date_str,
        "prediction_horizon": 10, "target_return_threshold": 0.05,
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    })

    pending_preds = [pred1, pred2, pred3, pred4, pred5, pred6, pred7, pred8]

    historical_data = []

    # Add prediction date (day 0)
    for ticker in ["TICKER1", "TICKER2", "TICKER3", "TICKER4", "TICKER5", "TICKER6", "TICKER8"]:
        historical_data.append({"ticker": ticker, "date": market_date, "close": 100.0})

    # Generate 10 days of data for tickers 1-6
    for i in range(1, 11):
        dt = market_date + timedelta(days=i)

        historical_data.append({"ticker": "TICKER1", "date": dt, "close": 110.0 if i==10 else 100.0}) # +10%
        historical_data.append({"ticker": "TICKER2", "date": dt, "close": 90.0 if i==10 else 100.0}) # -10%
        historical_data.append({"ticker": "TICKER3", "date": dt, "close": 102.0 if i==10 else 100.0}) # +2%
        historical_data.append({"ticker": "TICKER4", "date": dt, "close": 105.0 if i==10 else 100.0}) # +5% exact
        historical_data.append({"ticker": "TICKER5", "date": dt, "close": 95.0 if i==10 else 100.0}) # -5% exact
        historical_data.append({"ticker": "TICKER6", "date": dt, "close": 110.0})

        # TICKER7 only gets 5 days of data total, and global max days will only be 5? No, tickers 1-6 have 10 days.
        # Wait, if TICKER1 has 10 days, global has 10 days!
        # So TICKER7 will be MISSING_MARKET_DATA, not NOT_MATURE, if we have 10 global days.
        # To test NOT_MATURE, we need a prediction with market_date closer to the end!
        if i <= 5:
            historical_data.append({"ticker": "TICKER8", "date": dt, "close": 100.0})

    # For pred8, it has 5 days of data (missing). But global is 10 days. So pred8 is MISSING_MARKET_DATA.
    # Let's make pred7 have a later market date so it is NOT_MATURE.
    market_date_later_str = "2026-08-06"
    market_date_later = pd.to_datetime(market_date_later_str).to_pydatetime()
    pred7["market_date"] = market_date_later_str
    # Dates for pred7
    historical_data.append({"ticker": "TICKER7", "date": market_date_later, "close": 100.0})
    for i in range(1, 6):
        dt = market_date_later + timedelta(days=i)
        historical_data.append({"ticker": "TICKER7", "date": dt, "close": 100.0})

    # Also add an invalid OHLCV row for TICKER1 (close=0) on day 11 to prove it's skipped
    dt_invalid = market_date + timedelta(days=11)
    historical_data.append({"ticker": "TICKER1", "date": dt_invalid, "close": 0.0})

    client_mock, db_mock = create_mock_db(pending_preds, historical_data)

    stats = evaluate_predictions(client_mock, apply=True)

    assert stats["READY_TO_SETTLE"] == 5 # 1, 2, 3, 4, 5
    assert stats["LEGACY_UNSETTLEABLE"] == 1 # 6
    assert stats["NOT_MATURE"] == 1 # 7 (since global dates > 2026-08-06 is only 5 days)
    assert stats["MISSING_MARKET_DATA"] == 1 # 8 (since global dates > 2026-08-01 is 10 days, but T8 only has 5)

    # Check exact calls
    updates = db_mock.prediction_history.update_one.call_args_list
    assert len(updates) == 5

    # T1: BUY
    t1_update = [u[0][1]["$set"] for u in updates if u[0][0]["_id"] == "p1"][0]
    assert t1_update["actual_return"] == approx(0.10)
    assert t1_update["actual_class"] == "BUY"
    assert t1_update["raw_prediction_correct"] is True
    assert t1_update["recommendation_correct"] is True
    assert t1_update["outcome"] == "CORRECT"

    # T2: SELL
    t2_update = [u[0][1]["$set"] for u in updates if u[0][0]["_id"] == "p2"][0]
    assert t2_update["actual_return"] == approx(-0.10)
    assert t2_update["actual_class"] == "SELL"
    assert t2_update["raw_prediction_correct"] is True
    assert t2_update["recommendation_correct"] is True
    assert t2_update["outcome"] == "CORRECT"

    # T3: HOLD
    t3_update = [u[0][1]["$set"] for u in updates if u[0][0]["_id"] == "p3"][0]
    assert round(t3_update["actual_return"], 2) == 0.02
    assert t3_update["actual_class"] == "HOLD"
    assert t3_update["raw_prediction_correct"] is False # predicted BUY
    assert t3_update["recommendation_correct"] is True # recommended HOLD
    assert t3_update["prediction_correct"] is True
    assert t3_update["outcome"] == "CORRECT"

    # T4: Exact Boundary +
    t4_update = [u[0][1]["$set"] for u in updates if u[0][0]["_id"] == "p4"][0]
    assert round(t4_update["actual_return"], 2) == 0.05
    assert t4_update["actual_class"] == "HOLD" # exact match on threshold = HOLD

    # T5: Exact Boundary -
    t5_update = [u[0][1]["$set"] for u in updates if u[0][0]["_id"] == "p5"][0]
    assert round(t5_update["actual_return"], 2) == -0.05
    assert t5_update["actual_class"] == "HOLD" # exact match on -threshold = HOLD

# --- PHASE 23 CANONICAL ECONOMIC PRICE BASIS TESTS ---

def _create_prov_doc(p):
    from src.ml.model_utils import compute_provenance_hash
    p["model_version"] = "mock_ver"
    fake_payload = {
        "provenance_schema_version": "v2",
        "symbol": p["symbol"],
        "market_date": p["market_date"],
        "prediction_horizon": p.get("prediction_horizon", 10),
        "model_version": "mock_ver"
    }
    p["provenance_hash"] = compute_provenance_hash(fake_payload)
    return p

def test_phase23_normal_movement():
    market_date_str = "2026-08-01"
    market_date = pd.to_datetime(market_date_str).to_pydatetime()

    pred = _create_prov_doc({
        "_id": "t1", "symbol": "NORM", "market_date": market_date_str,
        "prediction_horizon": 10, "target_return_threshold": 0.05,
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    })

    hist = [{"ticker": "NORM", "date": market_date, "close": 100.0}]
    for i in range(1, 11):
        hist.append({"ticker": "NORM", "date": market_date + timedelta(days=i), "close": 110.0 if i==10 else 100.0})

    client_mock, db_mock = create_mock_db([pred], hist)
    stats = evaluate_predictions(client_mock, apply=True)
    assert stats["READY_TO_SETTLE"] == 1

    update = db_mock.prediction_history.update_one.call_args[0][1]["$set"]
    assert update["actual_return"] == approx(0.10)
    assert update["actual_class"] == "BUY"
    assert "settlement_hash" in update

def test_phase23_stock_split_defect_reproduction():
    market_date_str = "2026-08-01"
    market_date = pd.to_datetime(market_date_str).to_pydatetime()

    pred = _create_prov_doc({
        "_id": "t2", "symbol": "SPLIT", "market_date": market_date_str,
        "prediction_horizon": 10, "target_return_threshold": 0.05,
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    })

    # DB canonical historical value is 50. Immutable provenance is 100.
    hist = [{"ticker": "SPLIT", "date": market_date, "close": 50.0}]
    for i in range(1, 11):
        hist.append({"ticker": "SPLIT", "date": market_date + timedelta(days=i), "close": 55.0 if i==10 else 50.0})

    client_mock, db_mock = create_mock_db([pred], hist)
    stats = evaluate_predictions(client_mock, apply=True)

    update = db_mock.prediction_history.update_one.call_args[0][1]["$set"]
    # If price_at_prediction was used, return would be 55/100 - 1 = -45%
    # With canonical basis, return is 55/50 - 1 = +10%
    assert update["actual_return"] == approx(0.10)
    assert update["actual_class"] == "BUY"

def test_phase23_prediction_provenance_immutability():
    market_date_str = "2026-08-01"
    market_date = pd.to_datetime(market_date_str).to_pydatetime()

    pred = _create_prov_doc({
        "_id": "t3", "symbol": "IMMUTE", "market_date": market_date_str,
        "prediction_horizon": 10, "target_return_threshold": 0.05,
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    })

    hist = [{"ticker": "IMMUTE", "date": market_date, "close": 50.0}]
    for i in range(1, 11):
        hist.append({"ticker": "IMMUTE", "date": market_date + timedelta(days=i), "close": 50.0})

    client_mock, db_mock = create_mock_db([pred], hist)
    stats = evaluate_predictions(client_mock, apply=True)

    update = db_mock.prediction_history.update_one.call_args[0][1]["$set"]
    assert update["actual_return"] == approx(0.0)
    assert "price_at_prediction" not in update # Not mutated

def test_phase23_missing_prediction_date_history():
    market_date_str = "2026-08-01"
    market_date = pd.to_datetime(market_date_str).to_pydatetime()

    pred = _create_prov_doc({
        "_id": "t4", "symbol": "MISSING", "market_date": market_date_str,
        "prediction_horizon": 10, "target_return_threshold": 0.05,
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    })

    # Missing prediction_date history entirely
    hist = []
    for i in range(1, 11):
        hist.append({"ticker": "MISSING", "date": market_date + timedelta(days=i), "close": 110.0})

    client_mock, db_mock = create_mock_db([pred], hist)
    stats = evaluate_predictions(client_mock, apply=True)

    assert stats["MISSING_MARKET_DATA"] == 1
    assert stats["READY_TO_SETTLE"] == 0
    db_mock.prediction_history.update_one.assert_not_called()

def test_phase23_invalid_canonical_prediction_close():
    market_date_str = "2026-08-01"
    market_date = pd.to_datetime(market_date_str).to_pydatetime()

    pred = _create_prov_doc({
        "_id": "t5", "symbol": "INV", "market_date": market_date_str,
        "prediction_horizon": 10, "target_return_threshold": 0.05,
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    })

    # Zero close
    hist = [{"ticker": "INV", "date": market_date, "close": 0.0}]
    for i in range(1, 11):
        hist.append({"ticker": "INV", "date": market_date + timedelta(days=i), "close": 110.0})

    client_mock, db_mock = create_mock_db([pred], hist)
    stats = evaluate_predictions(client_mock, apply=True)

    assert stats["MISSING_MARKET_DATA"] == 1
    db_mock.prediction_history.update_one.assert_not_called()

def test_phase23_already_evaluated_record():
    client_mock, db_mock = create_mock_db([], [])
    # Add a record that is already EVALUATED
    cursor_mock = MagicMock()
    cursor_mock.sort.return_value = [{"_id": "t7", "status": "EVALUATED"}]
    db_mock.prediction_history.find.return_value = cursor_mock

    # Evaluate predictions looks for PENDING, so this shouldn't even be processed, but let's test apply safely
    stats = evaluate_predictions(client_mock, apply=True)
    assert stats["READY_TO_SETTLE"] == 0

def test_phase23_dividend_adjusted_canonical_series():
    market_date_str = "2026-08-01"
    market_date = pd.to_datetime(market_date_str).to_pydatetime()

    pred = _create_prov_doc({
        "_id": "t9", "symbol": "DIV", "market_date": market_date_str,
        "prediction_horizon": 10, "target_return_threshold": 0.05,
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    })

    # Immutable price=100. Dividend happened, history adjusts down by 2.
    hist = [{"ticker": "DIV", "date": market_date, "close": 98.0}]
    for i in range(1, 11):
        hist.append({"ticker": "DIV", "date": market_date + timedelta(days=i), "close": 107.8 if i==10 else 98.0})

    client_mock, db_mock = create_mock_db([pred], hist)
    evaluate_predictions(client_mock, apply=True)

    update = db_mock.prediction_history.update_one.call_args[0][1]["$set"]
    # 107.8 / 98.0 - 1 = +10%
    assert update["actual_return"] == approx(0.10)

def test_phase23_strong_adversarial_proof():
    market_date_str = "2026-08-01"
    market_date = pd.to_datetime(market_date_str).to_pydatetime()

    pred = _create_prov_doc({
        "_id": "t10", "symbol": "ADV", "market_date": market_date_str,
        "prediction_horizon": 10, "target_return_threshold": 0.05,
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    })

    hist = [{"ticker": "ADV", "date": market_date, "close": 50.0}]
    for i in range(1, 11):
        hist.append({"ticker": "ADV", "date": market_date + timedelta(days=i), "close": 60.0 if i==10 else 50.0})

    client_mock, db_mock = create_mock_db([pred], hist)
    evaluate_predictions(client_mock, apply=True)

    update = db_mock.prediction_history.update_one.call_args[0][1]["$set"]
    # 60 / 50 - 1 = +20%
    assert update["actual_return"] == approx(0.20)
