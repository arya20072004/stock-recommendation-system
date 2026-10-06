import pytest
import pymongo.errors
from pymongo import ReturnDocument
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock, ANY

from src.pipeline.daily import DailyPipeline
from src.data.nifty50 import TICKERS
from zoneinfo import ZoneInfo
IST = ZoneInfo("Asia/Kolkata")

@pytest.fixture
def mock_db():
    client = MagicMock()
    db = client["stock_market_db"]
    db.pipeline_runs.find_one.return_value = None
    return client, db

# ======================================================================
# FIX 1: Canonical NSE Trading-Session Resolution Tests
# ======================================================================

def _setup_bhavcopy_mock(mock_fetch, *valid_dates):
    def side_effect(dt):
        if dt.date() in valid_dates:
            import pandas as pd
            return pd.DataFrame([{"foo": "bar"}])
        return None
    mock_fetch.side_effect = side_effect

@patch("src.data.pcr_builder._fetch_bhavcopy")
@patch("src.pipeline.daily.datetime")
@patch("src.pipeline.daily.MongoClient")
def test_resolve_trading_session_normal_weekday(mock_mongo, mock_datetime, mock_fetch, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    wednesday = datetime(2026, 8, 5, 22, 0, 0, tzinfo=IST)
    mock_datetime.now.return_value = wednesday
    mock_datetime.combine = datetime.combine
    mock_datetime.min = datetime.min
    _setup_bhavcopy_mock(mock_fetch, wednesday.date())
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True, force=False)
    pipeline.resolve_trading_session()
    
    assert pipeline.last_completed_session == wednesday.date()
    assert pipeline.prediction_target_date == datetime(2026, 8, 6).date()

@patch("src.data.pcr_builder._fetch_bhavcopy")
@patch("src.pipeline.daily.datetime")
@patch("src.pipeline.daily.MongoClient")
def test_resolve_trading_session_normal_monday(mock_mongo, mock_datetime, mock_fetch, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    monday = datetime(2026, 9, 28, 20, 35, 0, tzinfo=IST)
    mock_datetime.now.return_value = monday
    mock_datetime.combine = datetime.combine
    mock_datetime.min = datetime.min
    _setup_bhavcopy_mock(mock_fetch, monday.date())
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True, force=False)
    pipeline.resolve_trading_session()
    
    assert pipeline.last_completed_session == monday.date()
    assert pipeline.prediction_target_date == datetime(2026, 9, 29).date()

@patch("src.data.pcr_builder._fetch_bhavcopy")
@patch("src.pipeline.daily.datetime")
@patch("src.pipeline.daily.MongoClient")
def test_resolve_trading_session_monday_delayed_bhavcopy(mock_mongo, mock_datetime, mock_fetch, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    monday = datetime(2026, 9, 28, 20, 35, 0, tzinfo=IST)
    mock_datetime.now.return_value = monday
    mock_datetime.combine = datetime.combine
    mock_datetime.min = datetime.min
    mock_fetch.return_value = None  # Bhavcopy unavailable
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True, force=False)
    with pytest.raises(RuntimeError, match="DATA_NOT_YET_AVAILABLE"):
        pipeline.resolve_trading_session()
        
    assert pipeline.status == "BLOCKED"
    assert pipeline.blocked_reason == "DATA_NOT_YET_AVAILABLE"

@patch("src.data.pcr_builder._fetch_bhavcopy")
@patch("src.pipeline.daily.datetime")
@patch("src.pipeline.daily.MongoClient")
def test_resolve_trading_session_weekend(mock_mongo, mock_datetime, mock_fetch, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    saturday = datetime(2026, 8, 8, 10, 0, 0, tzinfo=IST)
    mock_datetime.now.return_value = saturday
    mock_datetime.combine = datetime.combine
    mock_datetime.min = datetime.min
    
    prev_friday = datetime(2026, 8, 7).date()
    _setup_bhavcopy_mock(mock_fetch, prev_friday)
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True, force=True)
    pipeline.resolve_trading_session()
    
    assert pipeline.last_completed_session == prev_friday
    assert pipeline.prediction_target_date == datetime(2026, 8, 10).date()

@patch("src.data.pcr_builder._fetch_bhavcopy")
@patch("src.pipeline.daily.datetime")
@patch("src.pipeline.daily.MongoClient")
def test_resolve_trading_session_holiday(mock_mongo, mock_datetime, mock_fetch, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    # 2026-05-01 is Maharashtra Day (Friday, verified non-session in canonical calendar)
    holiday = datetime(2026, 5, 1, 22, 0, 0, tzinfo=IST)
    mock_datetime.now.return_value = holiday
    mock_datetime.combine = datetime.combine
    mock_datetime.min = datetime.min
    
    prev_thursday = datetime(2026, 4, 30).date()
    _setup_bhavcopy_mock(mock_fetch, prev_thursday) 
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True, force=False)
    pipeline.resolve_trading_session()
    
    assert pipeline.last_completed_session == prev_thursday
    assert pipeline.prediction_target_date == datetime(2026, 5, 4).date()  # Next Monday

@patch("src.data.pcr_builder._fetch_bhavcopy")
@patch("src.pipeline.daily.datetime")
@patch("src.pipeline.daily.MongoClient")
def test_resolve_trading_session_monday_following_friday_unavailable_bhavcopy(mock_mongo, mock_datetime, mock_fetch, mock_db):
    """
    Critical regression test for September 28, 2026 incident:
    Even though Friday Sep 25 Bhavcopy is available, Monday Sep 28 is a valid session.
    When Monday Bhavcopy is unavailable, it MUST NOT fall back to Sep 25.
    """
    client, db = mock_db
    mock_mongo.return_value = client
    
    monday = datetime(2026, 9, 28, 20, 35, 0, tzinfo=IST)
    mock_datetime.now.return_value = monday
    mock_datetime.combine = datetime.combine
    mock_datetime.min = datetime.min
    
    # Friday is valid in mock, but Monday is NOT
    _setup_bhavcopy_mock(mock_fetch, datetime(2026, 9, 25).date())
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True, force=False)
    with pytest.raises(RuntimeError, match="DATA_NOT_YET_AVAILABLE"):
        pipeline.resolve_trading_session()
        
    assert pipeline.status == "BLOCKED"
    assert pipeline.blocked_reason == "DATA_NOT_YET_AVAILABLE"
    assert pipeline.last_completed_session is None

@patch("src.data.pcr_builder._fetch_bhavcopy")
@patch("src.pipeline.daily.datetime")
@patch("src.pipeline.daily.MongoClient")
def test_resolve_trading_session_target_date_correctness(mock_mongo, mock_datetime, mock_fetch, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    monday = datetime(2026, 9, 28, 22, 0, 0, tzinfo=IST)
    mock_datetime.now.return_value = monday
    mock_datetime.combine = datetime.combine
    mock_datetime.min = datetime.min
    _setup_bhavcopy_mock(mock_fetch, monday.date())
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True, force=False)
    pipeline.resolve_trading_session()
    
    assert pipeline.last_completed_session == datetime(2026, 9, 28).date()
    assert pipeline.prediction_target_date == datetime(2026, 9, 29).date()

@patch("src.pipeline.daily.datetime")
@patch("src.pipeline.daily.MongoClient")
def test_resolve_trading_session_before_cutoff_blocked(mock_mongo, mock_datetime, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    early = datetime(2026, 8, 5, 10, 0, 0, tzinfo=IST)
    mock_datetime.now.return_value = early
    mock_datetime.combine = datetime.combine
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True, force=False)
    with pytest.raises(RuntimeError, match="Blocked: It's before 20:00 IST"):
        pipeline.resolve_trading_session()

@patch("src.data.pcr_builder._fetch_bhavcopy")
@patch("src.pipeline.daily.datetime")
@patch("src.pipeline.daily.MongoClient")
def test_resolve_trading_session_before_cutoff_force(mock_mongo, mock_datetime, mock_fetch, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    early = datetime(2026, 8, 5, 10, 0, 0, tzinfo=IST)
    mock_datetime.now.return_value = early
    mock_datetime.combine = datetime.combine
    mock_datetime.min = datetime.min
    
    prev_tuesday = datetime(2026, 8, 4).date()
    _setup_bhavcopy_mock(mock_fetch, prev_tuesday)
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True, force=True)
    pipeline.resolve_trading_session()
    
    assert pipeline.last_completed_session == prev_tuesday

@patch("src.data.pcr_builder._fetch_bhavcopy")
@patch("src.pipeline.daily.datetime")
@patch("src.pipeline.daily.MongoClient")
def test_resolve_trading_session_no_false_missed_sessions(mock_mongo, mock_datetime, mock_fetch, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    # Simulate prior run completed with market_date="2026-09-28" (e.g. from Sep 25)
    db.pipeline_runs.find_one.return_value = {
        "status": "SUCCESS",
        "market_date": "2026-09-28"
    }
    
    monday = datetime(2026, 9, 28, 20, 35, 0, tzinfo=IST)
    mock_datetime.now.return_value = monday
    mock_datetime.combine = datetime.combine
    mock_datetime.min = datetime.min
    mock_datetime.strptime = datetime.strptime
    _setup_bhavcopy_mock(mock_fetch, monday.date())
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True, force=False)
    pipeline.resolve_trading_session()
    
    assert pipeline.last_completed_session == monday.date()
    assert pipeline.prediction_target_date == datetime(2026, 9, 29).date()

# ======================================================================
# FIX 2: Atomic MongoDB Execution Lock Tests
# ======================================================================

@patch("src.pipeline.daily.MongoClient")
def test_concurrency_protection_two_processes(mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    pipeline_a = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True)
    pipeline_b = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True)
    
    db.pipeline_locks.insert_one = MagicMock()
    pipeline_a.acquire_lock()
    
    db.pipeline_locks.insert_one.side_effect = pymongo.errors.DuplicateKeyError("")
    db.pipeline_locks.find_one_and_update.return_value = None
    
    with pytest.raises(RuntimeError, match="Concurrent active pipeline run detected"):
        pipeline_b.acquire_lock()

@patch("src.pipeline.daily.MongoClient")
def test_concurrency_stale_lock_recovery(mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    pipeline_b = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True)
    
    db.pipeline_locks.insert_one.side_effect = pymongo.errors.DuplicateKeyError("")
    db.pipeline_locks.find_one_and_update.return_value = {"run_id": pipeline_b.run_id}
    
    pipeline_b.acquire_lock() 
    assert db.pipeline_locks.find_one_and_update.called

@patch("src.pipeline.daily.MongoClient")
def test_concurrency_wrong_owner_release(mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True)
    pipeline._release_lock()
    
    db.pipeline_locks.update_one.assert_called_with(
        {"lock_id": "daily_production_lock", "owner": pipeline.run_id, "run_id": pipeline.run_id},
        {"$set": {"status": "RUNNING", "completed_at": ANY}}
    )

# ======================================================================
# FIX 3: API Ticker Set Equality
# ======================================================================

@patch("src.pipeline.daily.MongoClient")
def test_api_health_exact_tickers_success(mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True)
    
    mock_data = {"data": [{"ticker": t} for t in TICKERS]}
    with patch("app.app.test_client") as mock_client:
        mock_client.return_value.__enter__.return_value.get.return_value.status_code = 200
        mock_client.return_value.__enter__.return_value.get.return_value.get_json.return_value = mock_data
        pipeline.run_api_health_check()
    
    assert pipeline.stages["API_HEALTH"]["status"] == "SUCCESS"

@patch("src.pipeline.daily.MongoClient")
def test_api_health_unexpected_ticker_fail(mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True)
    
    # 50 correct + 1 unexpected
    mock_data = {"data": [{"ticker": t} for t in TICKERS[:-1]] + [{"ticker": "UNEXPECTED.NS"}]}
    with patch("app.app.test_client") as mock_client:
        mock_client.return_value.__enter__.return_value.get.return_value.status_code = 200
        mock_client.return_value.__enter__.return_value.get.return_value.get_json.return_value = mock_data
        
        with pytest.raises(RuntimeError):
            pipeline.run_api_health_check()
        assert pipeline.stages["API_HEALTH"]["status"] == "FAILED"

@patch("src.pipeline.daily.MongoClient")
def test_api_health_duplicate_ticker_fail(mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True)
    
    mock_data = {"data": [{"ticker": t} for t in TICKERS[:-1]] + [{"ticker": TICKERS[0]}]}
    with patch("app.app.test_client") as mock_client:
        mock_client.return_value.__enter__.return_value.get.return_value.status_code = 200
        mock_client.return_value.__enter__.return_value.get.return_value.get_json.return_value = mock_data
        
        with pytest.raises(RuntimeError):
            pipeline.run_api_health_check()
        assert pipeline.stages["API_HEALTH"]["status"] == "FAILED"

@patch("src.pipeline.daily.MongoClient")
def test_api_health_missing_ticker_fail(mock_mongo, mock_db):
    client, db = mock_db
    mock_mongo.return_value = client
    
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True)
    
    mock_data = {"data": [{"ticker": t} for t in TICKERS[:-1]]}
    with patch("app.app.test_client") as mock_client:
        mock_client.return_value.__enter__.return_value.get.return_value.status_code = 200
        mock_client.return_value.__enter__.return_value.get.return_value.get_json.return_value = mock_data
        
        with pytest.raises(RuntimeError):
            pipeline.run_api_health_check()
        assert pipeline.stages["API_HEALTH"]["status"] == "FAILED"
