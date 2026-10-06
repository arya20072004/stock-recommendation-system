import pytest
from datetime import datetime, date, timedelta
from unittest.mock import patch, MagicMock

from src.data.pcr_builder import build_pcr_history, backfill_full_pcr_history

@pytest.fixture
def mock_client():
    client = MagicMock()
    db = client["stock_market_db"]
    db.pcr_data.bulk_write = MagicMock()
    db.pcr_data.create_index = MagicMock()
    return client

@patch("src.data.pcr_builder._compute_daily_pcr")
def test_empty_pcr_database_bootstrap(mock_compute, mock_client):
    """Test 1: Uninitialized database triggers bootstrap from configured lookback."""
    mock_compute.return_value = [{"underlying": "NIFTY", "date": datetime(2026, 9, 28), "pcr_oi": 1.0}]
    db = mock_client["stock_market_db"]
    db.pcr_data.find_one.return_value = None  # Empty database
    
    # Restrict bootstrap window for fast test
    end_d = date(2026, 9, 28)
    start_d = date(2026, 9, 24)
    result = build_pcr_history(mock_client, start_date=start_d, end_date=end_d)
    
    assert result["candidate_days"] == 3  # Sep 24, Sep 25, Sep 28
    assert mock_compute.call_count == 3

@patch("src.data.pcr_builder._compute_daily_pcr")
def test_incremental_single_missing_session(mock_compute, mock_client):
    """Test 2: Normal increment fetches only the single new session."""
    mock_compute.return_value = [{"underlying": "NIFTY", "date": datetime(2026, 9, 28), "pcr_oi": 1.1}]
    db = mock_client["stock_market_db"]
    db.pcr_data.find_one.return_value = {"underlying": "NIFTY", "date": datetime(2026, 9, 25, 0, 0)}
    
    result = build_pcr_history(mock_client, end_date=date(2026, 9, 28))
    
    assert result["candidate_days"] == 1
    assert mock_compute.call_count == 1
    # Check that the fetched date was indeed Sep 28
    args, _ = mock_compute.call_args
    assert args[0].date() == date(2026, 9, 28)

@patch("src.data.pcr_builder._compute_daily_pcr")
def test_up_to_date_zero_fetches(mock_compute, mock_client):
    """Test 3: If database is already up to date through current session, zero requests are made."""
    db = mock_client["stock_market_db"]
    db.pcr_data.find_one.return_value = {"underlying": "NIFTY", "date": datetime(2026, 9, 25, 0, 0)}
    
    result = build_pcr_history(mock_client, end_date=date(2026, 9, 25))
    
    assert result["candidate_days"] == 0
    assert result["fetched"] == 0
    assert mock_compute.call_count == 0

@patch("src.data.pcr_builder._compute_daily_pcr")
def test_no_weekend_pcr_fetch(mock_compute, mock_client):
    """Test 4: Running on weekend does not attempt PCR collection for non-trading weekend days."""
    db = mock_client["stock_market_db"]
    db.pcr_data.find_one.return_value = {"underlying": "NIFTY", "date": datetime(2026, 9, 25, 0, 0)}
    
    # End date is Sunday Sep 27
    result = build_pcr_history(mock_client, end_date=date(2026, 9, 27))
    
    assert result["candidate_days"] == 0
    assert mock_compute.call_count == 0

@patch("src.data.pcr_builder._compute_daily_pcr")
def test_no_holiday_pcr_fetch(mock_compute, mock_client):
    """Test 5: Canonical exchange holidays are excluded from candidate PCR collection."""
    db = mock_client["stock_market_db"]
    # Apr 30, 2026 is Thursday before Maharashtra Day (May 1, 2026 Friday)
    db.pcr_data.find_one.return_value = {"underlying": "NIFTY", "date": datetime(2026, 4, 30, 0, 0)}
    
    result = build_pcr_history(mock_client, end_date=date(2026, 5, 1))
    
    assert result["candidate_days"] == 0
    assert mock_compute.call_count == 0

@patch("src.data.pcr_builder._compute_daily_pcr")
def test_multiple_missing_dates(mock_compute, mock_client):
    """Test 6: Multiple missing sessions are all identified and fetched."""
    mock_compute.return_value = [{"underlying": "NIFTY", "date": datetime(2026, 9, 23), "pcr_oi": 1.0}]
    db = mock_client["stock_market_db"]
    # Last stored date is Tuesday Sep 22; end date is Friday Sep 25
    db.pcr_data.find_one.return_value = {"underlying": "NIFTY", "date": datetime(2026, 9, 22, 0, 0)}
    
    result = build_pcr_history(mock_client, end_date=date(2026, 9, 25))
    
    assert result["candidate_days"] == 3  # Sep 23 (Wed), Sep 24 (Thu), Sep 25 (Fri)
    assert mock_compute.call_count == 3

@patch("src.data.pcr_builder._compute_daily_pcr")
def test_idempotent_rerun(mock_compute, mock_client):
    """Test 7: Rerunning for a specific date upserts by compound key without duplicating."""
    mock_compute.return_value = [
        {"underlying": "NIFTY", "date": datetime(2026, 9, 28), "pcr_oi": 1.25}
    ]
    
    result = build_pcr_history(mock_client, start_date=date(2026, 9, 28), end_date=date(2026, 9, 28))
    
    assert result["candidate_days"] == 1
    assert result["fetched"] == 1
    db = mock_client["stock_market_db"]
    assert db.pcr_data.bulk_write.call_count == 1
    ops = db.pcr_data.bulk_write.call_args[0][0]
    assert len(ops) == 1
    # Verify upsert filter has underlying and date
    assert ops[0]._filter == {"underlying": "NIFTY", "date": datetime(2026, 9, 28, 0, 0)}

@patch("src.data.pcr_builder._compute_daily_pcr")
def test_no_1326_day_sweep_in_daily_production(mock_compute, mock_client):
    """Test 8: Normal daily invocation does not trigger 1326-day historical sweep."""
    db = mock_client["stock_market_db"]
    db.pcr_data.find_one.return_value = {"underlying": "NIFTY", "date": datetime(2026, 9, 25, 0, 0)}
    
    # Normal daily run targeting last_completed_session = 2026-09-28
    result = build_pcr_history(mock_client, end_date=date(2026, 9, 28))
    
    assert result["candidate_days"] == 1
    assert result["candidate_days"] != 1326

@patch("src.data.pcr_builder._compute_daily_pcr")
def test_explicit_historical_backfill(mock_compute, mock_client):
    """Test 9: Explicit full historical backfill function works as intended."""
    result = backfill_full_pcr_history(
        mock_client,
        start_date=date(2026, 9, 1),
        end_date=date(2026, 9, 4)
    )
    
    # 4 business days: Sep 1, 2, 3, 4
    assert result["candidate_days"] == 4
    assert mock_compute.call_count == 4
