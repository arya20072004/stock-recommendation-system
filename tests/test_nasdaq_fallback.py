import pytest
import pandas as pd
from datetime import datetime, date
from unittest.mock import patch, MagicMock
from src.features.v1.engineering import _prepare_macro_data, _MACRO_CACHE

@pytest.fixture(autouse=True)
def clear_cache():
    _MACRO_CACHE.clear()

# Helper mock for yf.download
def get_yf_download_mock(ndx_dates, nse_dates):
    ndx_df = pd.DataFrame({"Close": [100.0 * (1.01**i) for i in range(len(ndx_dates))]}, index=ndx_dates)
    ndx_df.index.name = "Date"
    nse_df = pd.DataFrame({"Close": [200.0 * (1.005**i) for i in range(len(nse_dates))]}, index=nse_dates)
    nse_df.index.name = "Date"
    
    # Needs at least 20 rows to pass _validate_macro_asset
    # We pad the beginning of both dataframes to satisfy the min 20 rows rule
    pad_dates = pd.date_range(end=ndx_dates[0] - pd.Timedelta(days=1), periods=30)
    
    ndx_pad = pd.DataFrame({"Close": [100.0] * 30}, index=pad_dates)
    ndx_df = pd.concat([ndx_pad, ndx_df])
    
    nse_pad = pd.DataFrame({"Close": [200.0] * 30}, index=pad_dates)
    nse_df = pd.concat([nse_pad, nse_df])
    
    def mock_yf(ticker, *args, **kwargs):
        if ticker == "^NDX":
            return ndx_df.copy()
        if ticker == "^NSEI":
            return nse_df.copy()
        return pd.DataFrame({"Close": [10.0] * len(nse_df)}, index=nse_df.index)
        
    return mock_yf

def test_nasdaq_normal_complete():
    """A. Normal Yahoo-complete periods"""
    start = pd.Timestamp("2026-09-01")
    # Both open on 1st to 4th
    dates = [pd.Timestamp(f"2026-09-0{i}") for i in range(1, 5)]
    mock_yf = get_yf_download_mock(dates, dates)
    
    with patch("src.features.v1.engineering.yf.download", side_effect=mock_yf):
        with patch("src.data.session_calendar.is_session", return_value=True):
            df = _prepare_macro_data(datetime(2026, 8, 1), datetime(2026, 9, 4), MagicMock(), prediction_target_date=date(2026, 9, 5))
            # Shift moves Sep 4 -> Sep 5
            # Before shift, Sep 4 macro has valid ndx data
            assert not pd.isna(df.loc[pd.Timestamp("2026-09-05"), "nasdaq_ret_5d"])

def test_nasdaq_one_day_us_holiday():
    """B & D. One-day US holiday / NSE-open / US-closed"""
    # NDX missing on Sep 7
    ndx_dates = [pd.Timestamp("2026-09-04"), pd.Timestamp("2026-09-08")]
    nse_dates = [pd.Timestamp("2026-09-04"), pd.Timestamp("2026-09-07"), pd.Timestamp("2026-09-08")]
    
    mock_yf = get_yf_download_mock(ndx_dates, nse_dates)
    
    with patch("src.features.v1.engineering.yf.download", side_effect=mock_yf):
        with patch("src.data.session_calendar.is_session", return_value=True):
            df = _prepare_macro_data(datetime(2026, 8, 1), datetime(2026, 9, 8), MagicMock(), prediction_target_date=date(2026, 9, 9))
            # target date Sep 8 receives Sep 7 macro state. Sep 7 NDX should be ffilled from Sep 4
            assert pd.Timestamp("2026-09-08") in df.index
            assert not pd.isna(df.loc[pd.Timestamp("2026-09-08"), "nasdaq_ret_5d"])
            # verify it matches the sep 4 value (which shifted into sep 7)
            assert df.loc[pd.Timestamp("2026-09-08"), "nasdaq_ret_5d"] == df.loc[pd.Timestamp("2026-09-07"), "nasdaq_ret_5d"]

def test_nasdaq_multiple_consecutive_closed():
    """C & F. Multiple consecutive US-closed days and beyond stale window"""
    # US closed 4 days, NSE open all 4
    # Our ffill limit is 3, so the 4th day should be NaN
    ndx_dates = [pd.Timestamp("2026-09-01"), pd.Timestamp("2026-09-06")]
    nse_dates = [
        pd.Timestamp("2026-09-01"),
        pd.Timestamp("2026-09-02"),
        pd.Timestamp("2026-09-03"),
        pd.Timestamp("2026-09-04"),
        pd.Timestamp("2026-09-05"),
        pd.Timestamp("2026-09-06")
    ]
    
    mock_yf = get_yf_download_mock(ndx_dates, nse_dates)
    
    with patch("src.features.v1.engineering.yf.download", side_effect=mock_yf):
        with patch("src.data.session_calendar.is_session", return_value=True):
            df = _prepare_macro_data(datetime(2026, 8, 1), datetime(2026, 9, 6), MagicMock(), prediction_target_date=date(2026, 9, 7))
            # Sep 1 -> valid, shifts to Sep 2
            # Sep 2 -> valid (ffill 1), shifts to Sep 3
            # Sep 3 -> valid (ffill 2), shifts to Sep 4
            # Sep 4 -> valid (ffill 3), shifts to Sep 5
            # Sep 5 -> NaN (exceeds ffill 3), shifts to Sep 6
            assert not pd.isna(df.loc[pd.Timestamp("2026-09-05"), "nasdaq_ret_5d"])
            assert pd.isna(df.loc[pd.Timestamp("2026-09-06"), "nasdaq_ret_5d"])

def test_nasdaq_genuine_provider_failure():
    """G. Genuine provider failure"""
    # NDX completely empty
    def mock_yf(ticker, *args, **kwargs):
        if ticker == "^NDX":
            return pd.DataFrame()
        return get_yf_download_mock([pd.Timestamp("2026-09-01")], [pd.Timestamp("2026-09-01")])("^NSEI")
    
    with patch("src.features.v1.engineering.yf.download", side_effect=mock_yf):
        df = _prepare_macro_data(datetime(2026, 8, 1), datetime(2026, 9, 1), MagicMock())
        # Should be NaN due to float("nan") fallback
        # No 0.0 fallback
        assert pd.isna(df.loc[df.index[-1], "nasdaq_ret_5d"])
        assert pd.isna(df.loc[df.index[-1], "nasdaq_ret_20d"])

def test_nasdaq_no_future_leakage():
    """H. No future leakage"""
    # Target prediction is for Sep 8. We fetch data up to Sep 7.
    # What if yf unexpectedly returns Sep 8 data?
    # Our system drops it by indexing on start_date:end_date in _fetch_cached_macro
    # macro = macro.shift(1) prevents Sep 8 macro state from hitting Sep 8 target.
    
    ndx_dates = [pd.Timestamp("2026-09-07"), pd.Timestamp("2026-09-08")]
    nse_dates = [pd.Timestamp("2026-09-07"), pd.Timestamp("2026-09-08")]
    
    mock_yf = get_yf_download_mock(ndx_dates, nse_dates)
    
    with patch("src.features.v1.engineering.yf.download", side_effect=mock_yf):
        df = _prepare_macro_data(datetime(2026, 8, 1), datetime(2026, 9, 7), MagicMock(), prediction_target_date=date(2026, 9, 8))
        
        # Target is Sep 8. The value at Sep 8 should be Sep 7's data.
        # It should not be Sep 8's NDX data.
        # Actually _prepare_macro_data only fetches up to `end_date` (Sep 7).
        assert pd.Timestamp("2026-09-08") in df.index
        # Verify it shifted Sep 7 to Sep 8
        sep_7_val = mock_yf("^NDX").pct_change(5).loc[pd.Timestamp("2026-09-07"), "Close"]
        assert df.loc[pd.Timestamp("2026-09-08"), "nasdaq_ret_5d"] == sep_7_val
