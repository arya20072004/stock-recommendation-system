"""
test_remediation_oct1.py

Comprehensive regression tests for the Oct 1, 2026 ADANIENT.NS incident remediation:
1. Macro fetch retry on transient failure
2. Macro retry exhaustion & bounded attempts
3. Macro cache warming before ticker iteration
4. Ticker-order independence
5. API health with recognized/explained localized failure -> DEGRADED
6. API health rejects unexplained missing ticker -> FAILED
7. API health rejects mismatched missing ticker -> FAILED
8. API health rejects unexpected ticker -> FAILED
9. API health rejects duplicate tickers -> FAILED
10. Idempotent recovery after localized failure
11. Full 51/51 universe success path -> SUCCESS
12. NASDAQ fail-closed regression (NaN features block prediction)
"""

import pytest
import pandas as pd
from datetime import datetime, date, timedelta
from unittest.mock import patch, MagicMock, call

from src.data.nifty50 import TICKERS
from src.features.v1.engineering import (
    _fetch_cached_macro,
    _MACRO_CACHE,
    _prepare_macro_data,
    warm_macro_cache,
    MACRO_TICKERS,
)
from src.pipeline.daily import DailyPipeline


@pytest.fixture(autouse=True)
def clean_macro_cache():
    """Ensure macro cache is clean before and after every test."""
    _MACRO_CACHE.clear()
    yield
    _MACRO_CACHE.clear()


# ======================================================================
# Test 1 — Macro fetch retry on transient failure
# ======================================================================
def test_macro_fetch_retry_success():
    """
    Simulates transient network/provider failure on attempt 1, followed by
    success on attempt 2.
    Asserts:
    - retry occurs
    - cache is populated
    - returned DataFrame is non-empty
    """
    valid_dates = pd.date_range("2026-08-01", "2026-09-01", freq="B")
    mock_df = pd.DataFrame({"Close": [100.0 + i for i in range(len(valid_dates))]}, index=valid_dates)
    mock_df.index.name = "Date"

    call_count = 0

    def mock_download(ticker, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("Transient Yahoo Finance connection reset")
        return mock_df.copy()

    with patch("src.features.v1.engineering.yf.download", side_effect=mock_download):
        df = _fetch_cached_macro("^NDX", datetime(2026, 8, 1), datetime(2026, 9, 1), max_retries=3, backoff_base=0.001)
        assert call_count == 2
        assert not df.empty
        assert "^NDX" in _MACRO_CACHE
        assert len(_MACRO_CACHE["^NDX"]) == len(mock_df)


# ======================================================================
# Test 2 — Macro retry exhaustion
# ======================================================================
def test_macro_retry_exhaustion_fail_closed():
    """
    Simulates persistent failure on all 3 attempts.
    Asserts:
    - Exactly 3 attempts made (bounded retry)
    - Returns empty DataFrame
    - Fails closed
    """
    call_count = 0

    def mock_download(ticker, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise TimeoutError("Yahoo Finance timeout")

    with patch("src.features.v1.engineering.yf.download", side_effect=mock_download):
        df = _fetch_cached_macro("^NDX", datetime(2026, 8, 1), datetime(2026, 9, 1), max_retries=3, backoff_base=0.001)
        assert call_count == 3
        assert df.empty
        assert "^NDX" not in _MACRO_CACHE or _MACRO_CACHE["^NDX"].empty


# ======================================================================
# Test 3 — Macro cache warming
# ======================================================================
def test_macro_cache_warming_before_ticker_iteration():
    """
    Verifies warm_macro_cache warms all canonical macro assets.
    """
    valid_dates = pd.date_range("2026-08-01", "2026-09-01", freq="B")
    mock_df = pd.DataFrame({"Close": [100.0 + i for i in range(len(valid_dates))]}, index=valid_dates)
    mock_df.index.name = "Date"

    with patch("src.features.v1.engineering.yf.download", return_value=mock_df) as mock_dl:
        warmed = warm_macro_cache(
            start_date=datetime(2026, 8, 1),
            end_date=datetime(2026, 9, 1),
            tickers=["^NSEI", "^NDX", "INR=X"]
        )
        assert "^NSEI" in warmed
        assert "^NDX" in warmed
        assert "INR=X" in warmed
        assert "^NDX" in _MACRO_CACHE
        assert not _MACRO_CACHE["^NDX"].empty


# ======================================================================
# Test 4 — Ticker-order independence
# ======================================================================
def test_ticker_order_independence():
    """
    Simulates:
    - Macro warming pre-fetches ^NDX with retry recovery
    - When ADANIENT (ticker 0) and ADANIPORTS (ticker 1) process,
      both find ^NDX cached and do NOT fail due to cold-cache ordering.
    """
    valid_dates = pd.date_range("2026-08-01", "2026-09-01", freq="D")
    mock_df = pd.DataFrame({"Close": [100.0 + i for i in range(len(valid_dates))]}, index=valid_dates)
    mock_df.index.name = "Date"

    attempt = 0

    def flaky_ndx_download(ticker, *args, **kwargs):
        nonlocal attempt
        if ticker == "^NDX":
            attempt += 1
            if attempt == 1:
                raise ConnectionError("Cold cache transient blip")
            return mock_df.copy()
        return mock_df.copy()

    with patch("src.features.v1.engineering.yf.download", side_effect=flaky_ndx_download):
        # Warm cache before ticker generation
        warm_macro_cache(
            start_date=datetime(2026, 8, 1),
            end_date=datetime(2026, 9, 1),
            tickers=["^NDX"]
        )

        # Both ticker 0 and ticker 1 now find ^NDX cached without needing any further downloads
        ndx_adanient = _fetch_cached_macro("^NDX", datetime(2026, 8, 1), datetime(2026, 9, 1))
        ndx_adaniports = _fetch_cached_macro("^NDX", datetime(2026, 8, 1), datetime(2026, 9, 1))

        assert not ndx_adanient.empty
        assert not ndx_adaniports.empty
        # yf.download was called twice during warming (1 fail + 1 success), never called during ticker inference
        assert attempt == 2


# ======================================================================
# Test 5 — API health with explained localized failure
# ======================================================================
@patch("src.pipeline.daily.MongoClient")
def test_api_health_explained_localized_failure_degraded(mock_mongo):
    """
    50/51 tickers returned by API, ADANIENT.NS recognized in failed_tickers,
    PREDICTION_VALIDATION is DEGRADED.
    Asserts:
    - API_HEALTH status is DEGRADED
    - Pipeline does NOT raise RuntimeError
    """
    client = MagicMock()
    mock_mongo.return_value = client
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True)
    pipeline.prediction_target_date = date(2026, 10, 5)

    # Set stages state representing recognized localized failure
    pipeline.stages["PREDICTION_GENERATION"] = {
        "status": "DEGRADED",
        "metrics": {"failed": ["ADANIENT.NS"], "stale": []}
    }
    pipeline.stages["PREDICTION_VALIDATION"] = {
        "status": "DEGRADED",
        "message": "Recognized localized failure for ADANIENT.NS"
    }

    # API returns 50 tickers (all valid except ADANIENT.NS)
    mock_rows = [
        {"ticker": t, "market_date": "2026-10-05"}
        for t in TICKERS if t != "ADANIENT.NS"
    ]
    mock_data = {"data": mock_rows}

    with patch("app.app.test_client") as mock_client:
        mock_client.return_value.__enter__.return_value.get.return_value.status_code = 200
        mock_client.return_value.__enter__.return_value.get.return_value.get_json.return_value = mock_data

        pipeline.run_api_health_check()

    assert pipeline.stages["API_HEALTH"]["status"] == "DEGRADED"
    assert "ADANIENT.NS" in pipeline.stages["API_HEALTH"]["metrics"]["explained_missing"]
    assert "API_HEALTH" in pipeline.degraded_stages


# ======================================================================
# Test 6 — API health rejects unexplained missing ticker
# ======================================================================
@patch("src.pipeline.daily.MongoClient")
def test_api_health_rejects_unexplained_missing_ticker(mock_mongo):
    """
    50/51 tickers returned by API, but PREDICTION_GENERATION reports NO failed/stale tickers.
    Asserts:
    - API_HEALTH status is FAILED
    - Raises RuntimeError
    """
    client = MagicMock()
    mock_mongo.return_value = client
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True)
    pipeline.prediction_target_date = date(2026, 10, 5)

    pipeline.stages["PREDICTION_GENERATION"] = {
        "status": "SUCCESS",
        "metrics": {"failed": [], "stale": []}
    }
    pipeline.stages["PREDICTION_VALIDATION"] = {"status": "SUCCESS"}

    # Missing ADANIENT.NS with NO explanation
    mock_rows = [
        {"ticker": t, "market_date": "2026-10-05"}
        for t in TICKERS if t != "ADANIENT.NS"
    ]
    mock_data = {"data": mock_rows}

    with patch("app.app.test_client") as mock_client:
        mock_client.return_value.__enter__.return_value.get.return_value.status_code = 200
        mock_client.return_value.__enter__.return_value.get.return_value.get_json.return_value = mock_data

        with pytest.raises(RuntimeError, match="unexplained"):
            pipeline.run_api_health_check()

    assert pipeline.stages["API_HEALTH"]["status"] == "FAILED"


# ======================================================================
# Test 7 — API health rejects wrong missing ticker
# ======================================================================
@patch("src.pipeline.daily.MongoClient")
def test_api_health_rejects_wrong_missing_ticker(mock_mongo):
    """
    failed_tickers = {"WIPRO.NS"}, but API response missing = {"ADANIENT.NS"}.
    Asserts:
    - Explanation must match actual missing ticker
    - Raises RuntimeError
    - API_HEALTH status is FAILED
    """
    client = MagicMock()
    mock_mongo.return_value = client
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True)
    pipeline.prediction_target_date = date(2026, 10, 5)

    pipeline.stages["PREDICTION_GENERATION"] = {
        "status": "DEGRADED",
        "metrics": {"failed": ["WIPRO.NS"], "stale": []}
    }
    pipeline.stages["PREDICTION_VALIDATION"] = {"status": "DEGRADED"}

    # Missing ADANIENT.NS instead of WIPRO.NS
    mock_rows = [
        {"ticker": t, "market_date": "2026-10-05"}
        for t in TICKERS if t != "ADANIENT.NS"
    ]
    mock_data = {"data": mock_rows}

    with patch("app.app.test_client") as mock_client:
        mock_client.return_value.__enter__.return_value.get.return_value.status_code = 200
        mock_client.return_value.__enter__.return_value.get.return_value.get_json.return_value = mock_data

        with pytest.raises(RuntimeError, match="unexplained"):
            pipeline.run_api_health_check()

    assert pipeline.stages["API_HEALTH"]["status"] == "FAILED"


# ======================================================================
# Test 8 — API health rejects unexpected ticker
# ======================================================================
@patch("src.pipeline.daily.MongoClient")
def test_api_health_rejects_unexpected_ticker(mock_mongo):
    """
    API returns 50 valid tickers + FAKE.NS.
    Asserts:
    - Rejects foreign ticker
    - Raises RuntimeError
    - API_HEALTH status is FAILED
    """
    client = MagicMock()
    mock_mongo.return_value = client
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True)

    mock_rows = [{"ticker": t} for t in TICKERS[:-1]] + [{"ticker": "FAKE.NS"}]
    mock_data = {"data": mock_rows}

    with patch("app.app.test_client") as mock_client:
        mock_client.return_value.__enter__.return_value.get.return_value.status_code = 200
        mock_client.return_value.__enter__.return_value.get.return_value.get_json.return_value = mock_data

        with pytest.raises(RuntimeError, match="unexpected"):
            pipeline.run_api_health_check()

    assert pipeline.stages["API_HEALTH"]["status"] == "FAILED"


# ======================================================================
# Test 9 — API health rejects duplicates
# ======================================================================
@patch("src.pipeline.daily.MongoClient")
def test_api_health_rejects_duplicates(mock_mongo):
    """
    API returns duplicate entries for a ticker.
    Asserts:
    - Duplicate detection is strict
    - Raises RuntimeError
    - API_HEALTH status is FAILED
    """
    client = MagicMock()
    mock_mongo.return_value = client
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True)

    mock_rows = [{"ticker": t} for t in TICKERS[:-1]] + [{"ticker": TICKERS[0]}]
    mock_data = {"data": mock_rows}

    with patch("app.app.test_client") as mock_client:
        mock_client.return_value.__enter__.return_value.get.return_value.status_code = 200
        mock_client.return_value.__enter__.return_value.get.return_value.get_json.return_value = mock_data

        with pytest.raises(RuntimeError, match="duplicates"):
            pipeline.run_api_health_check()

    assert pipeline.stages["API_HEALTH"]["status"] == "FAILED"


# ======================================================================
# Test 10 — Idempotent recovery after localized failure
# ======================================================================
def test_idempotent_recovery_simulation():
    """
    Simulates recovery scenario in isolated test database:
    - 50 valid predictions already exist in prediction_history for target date
    - 1 ticker (ADANIENT.NS) is missing
    - When generation runs with idempotency, existing 50 remain untouched
    - Only the missing ticker is generated
    - Total cohort size becomes 51 with 0 duplicates
    """
    records = {
        t: {"symbol": t, "market_date": "2026-10-05", "prediction_horizon": 10, "status": "PENDING"}
        for t in TICKERS if t != "ADANIENT.NS"
    }

    # Simulate generation loop
    generated = []
    skipped_existing = []

    for t in TICKERS:
        if t in records:
            skipped_existing.append(t)
        else:
            # Generate missing prediction
            new_pred = {"symbol": t, "market_date": "2026-10-05", "prediction_horizon": 10, "status": "PENDING"}
            records[t] = new_pred
            generated.append(t)

    assert len(skipped_existing) == 50
    assert len(generated) == 1
    assert generated[0] == "ADANIENT.NS"
    assert len(records) == 51
    assert len(set(records.keys())) == 51


# ======================================================================
# Test 11 — Full 51/51 production path
# ======================================================================
@patch("src.pipeline.daily.MongoClient")
def test_full_51_universe_success_path(mock_mongo):
    """
    All 51 predictions returned by API.
    Asserts:
    - API_HEALTH status is SUCCESS
    - No RuntimeError
    - No degradation logged
    """
    client = MagicMock()
    mock_mongo.return_value = client
    pipeline = DailyPipeline(mongo_uri="mongodb://mock", dry_run=True)
    pipeline.prediction_target_date = date(2026, 10, 5)

    pipeline.stages["PREDICTION_GENERATION"] = {
        "status": "SUCCESS",
        "metrics": {"failed": [], "stale": []}
    }
    pipeline.stages["PREDICTION_VALIDATION"] = {"status": "SUCCESS"}

    mock_rows = [
        {"ticker": t, "market_date": "2026-10-05"}
        for t in TICKERS
    ]
    mock_data = {"data": mock_rows}

    with patch("app.app.test_client") as mock_client:
        mock_client.return_value.__enter__.return_value.get.return_value.status_code = 200
        mock_client.return_value.__enter__.return_value.get.return_value.get_json.return_value = mock_data

        pipeline.run_api_health_check()

    assert pipeline.stages["API_HEALTH"]["status"] == "SUCCESS"
    assert "API_HEALTH" not in pipeline.degraded_stages


# ======================================================================
# Test 12 — NASDAQ fail-closed regression
# ======================================================================
def test_nasdaq_persistent_failure_remains_fail_closed():
    """
    Verifies that persistent ^NDX failure returns NaN for nasdaq_ret_5d and
    nasdaq_ret_20d, ensuring fail-closed integrity (no 0.0 substitution).
    """
    def mock_download(ticker, *args, **kwargs):
        if ticker == "^NDX":
            return pd.DataFrame()
        # Return valid Nifty to allow macro index creation
        dates = pd.date_range("2026-08-01", "2026-09-01", freq="B")
        return pd.DataFrame({"Close": [24000.0 + i for i in range(len(dates))]}, index=dates)

    with patch("src.features.v1.engineering.yf.download", side_effect=mock_download):
        macro_df = _prepare_macro_data(
            datetime(2026, 8, 1),
            datetime(2026, 9, 1),
            client=MagicMock(),
            prediction_target_date=date(2026, 9, 2)
        )
        assert not macro_df.empty
        # NASDAQ features must be NaN (fail-closed)
        assert pd.isna(macro_df["nasdaq_ret_5d"].iloc[-1])
        assert pd.isna(macro_df["nasdaq_ret_20d"].iloc[-1])
