import pytest
import pandas as pd
from datetime import datetime, date, timedelta
from unittest.mock import patch, MagicMock

from src.features.v1.engineering import _prepare_nifty_data, _fetch_cached_macro, _MACRO_CACHE
from src.features.router import get_feature_pipeline_hash
from src.data.nse_index_fallback import fetch_nse_index_close

@pytest.fixture(autouse=True)
def clear_cache():
    _MACRO_CACHE.clear()

def test_historical_t1_t2_gap_recovery():
    start_date = datetime(2026, 8, 20)
    end_date = datetime(2026, 8, 28)

    mock_df = pd.DataFrame({"Close": [24000.0, 24100.0]}, index=[pd.Timestamp("2026-08-26"), pd.Timestamp("2026-08-27")])
    mock_df.index.name = "Date"

    with patch("src.features.v1.engineering.yf.download", return_value=mock_df) as mock_yf:
        with patch("src.data.session_calendar.is_session") as mock_is_session:
            mock_is_session.side_effect = lambda dt: dt in [pd.Timestamp("2026-08-26"), pd.Timestamp("2026-08-27"), pd.Timestamp("2026-08-28")]

            with patch("src.data.nse_index_fallback.fetch_nse_index_close", return_value=24122.6) as mock_nse:
                df = _fetch_cached_macro("^NSEI", start_date, end_date)

                assert pd.Timestamp("2026-08-28") in df.index
                assert df.loc[pd.Timestamp("2026-08-28"), "Close"] == 24122.6
                assert mock_nse.call_count == 1
                mock_nse.assert_called_with(pd.Timestamp("2026-08-28"))

def test_cache_deduplication():
    start_date = datetime(2026, 8, 20)
    end_date = datetime(2026, 8, 28)

    mock_df = pd.DataFrame({"Close": [24000.0, 24100.0]}, index=[pd.Timestamp("2026-08-26"), pd.Timestamp("2026-08-27")])

    with patch("src.features.v1.engineering.yf.download", return_value=mock_df):
        with patch("src.data.session_calendar.is_session") as mock_is_session:
            mock_is_session.side_effect = lambda dt: dt in [pd.Timestamp("2026-08-26"), pd.Timestamp("2026-08-27"), pd.Timestamp("2026-08-28")]
            with patch("src.data.nse_index_fallback.fetch_nse_index_close", return_value=24122.6) as mock_nse:
                for i in range(51):
                    _fetch_cached_macro("^NSEI", start_date + timedelta(days=i%3), end_date)

                assert mock_nse.call_count == 1

def test_weekend_holiday_suppression():
    start_date = datetime(2026, 8, 20)
    end_date = datetime(2026, 8, 28)

    mock_df = pd.DataFrame({"Close": [24000.0, 24100.0]}, index=[pd.Timestamp("2026-08-26"), pd.Timestamp("2026-08-27")])

    with patch("src.features.v1.engineering.yf.download", return_value=mock_df):
        with patch("src.data.session_calendar.is_session", return_value=False):
            with patch("src.data.nse_index_fallback.fetch_nse_index_close") as mock_nse:
                df = _fetch_cached_macro("^NSEI", start_date, end_date)
                assert mock_nse.call_count == 0

def test_fallback_failure():
    start_date = datetime(2026, 8, 20)
    end_date = datetime(2026, 8, 28)
    mock_df = pd.DataFrame({"Close": [24000.0, 24100.0]}, index=[pd.Timestamp("2026-08-26"), pd.Timestamp("2026-08-27")])

    with patch("src.features.v1.engineering.yf.download", return_value=mock_df):
        with patch("src.data.session_calendar.is_session", return_value=True):
            with patch("src.data.nse_index_fallback.fetch_nse_index_close", return_value=None):
                df = _fetch_cached_macro("^NSEI", start_date, end_date)
                assert pd.Timestamp("2026-08-28") not in df.index

def test_overlapping_cache_ranges():
    start1, end1 = datetime(2026, 8, 25), datetime(2026, 8, 27)
    start2, end2 = datetime(2026, 8, 20), datetime(2026, 8, 28)

    with patch("src.features.v1.engineering.yf.download", return_value=pd.DataFrame({"Close": [1.0, 2.0]}, index=[pd.Timestamp("2026-08-26"), pd.Timestamp("2026-08-27")])):
        with patch("src.data.session_calendar.is_session", return_value=False):
            _fetch_cached_macro("^NSEI", start1, end1)

    with patch("src.features.v1.engineering.yf.download", return_value=pd.DataFrame({"Close": [1.0, 2.0, 3.0]}, index=[pd.Timestamp("2026-08-26"), pd.Timestamp("2026-08-27"), pd.Timestamp("2026-08-28")])):
        with patch("src.data.session_calendar.is_session", return_value=False):
            with patch("src.data.nse_index_fallback.fetch_nse_index_close") as mock_nse:
                df = _fetch_cached_macro("^NSEI", start2, end2)
                assert pd.Timestamp("2026-08-28") in df.index
                assert len(df) == 3
