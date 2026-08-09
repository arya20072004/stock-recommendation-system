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
    
    # Setup update_one
    update_result = MagicMock()
    update_result.matched_count = 1
    db_mock.prediction_history.update_one.return_value = update_result
    
    client_mock = MagicMock()
    client_mock.__getitem__.return_value = db_mock
    return client_mock, db_mock

def test_settlement_maturity_and_classes():
    market_date_str = "2026-08-01"
    market_date = pd.to_datetime(market_date_str).to_pydatetime()
    
    # 1. Mature BUY
    pred1 = {
        "_id": "p1", "symbol": "TICKER1", "market_date": market_date_str, 
        "prediction_horizon": 10, "target_return_threshold": 0.05, 
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    }
    
    # 2. Mature SELL
    pred2 = {
        "_id": "p2", "symbol": "TICKER2", "market_date": market_date_str, 
        "prediction_horizon": 10, "target_return_threshold": 0.05, 
        "price_at_prediction": 100.0, "raw_prediction": "SELL", "recommendation": "SELL"
    }
    
    # 3. Mature HOLD (inside threshold)
    pred3 = {
        "_id": "p3", "symbol": "TICKER3", "market_date": market_date_str, 
        "prediction_horizon": 10, "target_return_threshold": 0.05, 
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "HOLD" # recommendation changed
    }
    
    # 4. Exact boundaries (should be HOLD)
    pred4 = {
        "_id": "p4", "symbol": "TICKER4", "market_date": market_date_str, 
        "prediction_horizon": 10, "target_return_threshold": 105.0/100.0 - 1.0, 
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    }
    pred5 = {
        "_id": "p5", "symbol": "TICKER5", "market_date": market_date_str, 
        "prediction_horizon": 10, "target_return_threshold": -(95.0/100.0 - 1.0), 
        "price_at_prediction": 100.0, "raw_prediction": "SELL", "recommendation": "SELL"
    }
    
    # 5. Missing Threshold (Legacy)
    pred6 = {
        "_id": "p6", "symbol": "TICKER6", "market_date": market_date_str, 
        "prediction_horizon": 10, "price_at_prediction": 100.0, 
        "raw_prediction": "BUY", "recommendation": "BUY"
    }
    
    # 6. Not mature (only 5 future sessions global)
    pred7 = {
        "_id": "p7", "symbol": "TICKER7", "market_date": market_date_str, 
        "prediction_horizon": 10, "target_return_threshold": 0.05, 
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    }
    
    # 7. Missing market data (10 global sessions, but only 9 for this ticker)
    pred8 = {
        "_id": "p8", "symbol": "TICKER8", "market_date": market_date_str, 
        "prediction_horizon": 10, "target_return_threshold": 0.05, 
        "price_at_prediction": 100.0, "raw_prediction": "BUY", "recommendation": "BUY"
    }
    
    pending_preds = [pred1, pred2, pred3, pred4, pred5, pred6, pred7, pred8]
    
    historical_data = []
    
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
