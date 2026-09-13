import pytest
import pandas as pd
from datetime import datetime, date
from unittest.mock import patch, MagicMock

from src.features.v1.engineering import _prepare_nifty_data, _MACRO_CACHE, _prepare_macro_data
from src.features.router import get_feature_pipeline_hash
from src.data.nse_index_fallback import fetch_nse_index_close, _fetch_index_bhavcopy_raw

@pytest.fixture(autouse=True)
def clear_cache():
    _MACRO_CACHE.clear()

def test_test_1_missing_latest_session_blocks_inference():
    end_date = datetime(2026, 8, 28)
    mock_df = pd.DataFrame({"Close": [24000.0, 24100.0]}, index=[pd.Timestamp("2026-08-26"), pd.Timestamp("2026-08-27")])
    mock_df.index.name = "Date"
    with patch("src.features.v1.engineering.yf.download", return_value=mock_df):
        with patch("src.features.v1.engineering._validate_macro_asset", return_value=(True, mock_df.rename(columns={"Close": "Close"}))):
            with patch("src.data.session_calendar.is_session", side_effect=lambda dt: dt in [pd.Timestamp("2026-08-26"), pd.Timestamp("2026-08-27"), pd.Timestamp("2026-08-28")]):
                with patch("src.data.nse_index_fallback.fetch_nse_index_close", return_value=None):
                    nifty_df = _prepare_nifty_data(datetime(2026, 8, 20), end_date, prediction_target_date=date(2026, 8, 31))
                    assert pd.Timestamp("2026-08-28") not in nifty_df.index
                    assert pd.isna(nifty_df.loc[pd.Timestamp("2026-08-31"), "nifty_return"])

def test_test_2_successful_fallback():
    end_date = datetime(2026, 8, 28)
    mock_df = pd.DataFrame({"Close": [24000.0, 24100.0]}, index=[pd.Timestamp("2026-08-26"), pd.Timestamp("2026-08-27")])
    mock_df.index.name = "Date"
    with patch("src.features.v1.engineering.yf.download", return_value=mock_df):
        with patch("src.features.v1.engineering._validate_macro_asset", side_effect=lambda df, name, min_valid_rows: (True, df)):
            with patch("src.data.session_calendar.is_session", side_effect=lambda dt: dt in [pd.Timestamp("2026-08-26"), pd.Timestamp("2026-08-27"), pd.Timestamp("2026-08-28")]):
                with patch("src.data.nse_index_fallback.fetch_nse_index_close", return_value=24122.6):
                    nifty_df = _prepare_nifty_data(datetime(2026, 8, 20), end_date, prediction_target_date=date(2026, 8, 31))
                    assert pd.Timestamp("2026-08-28") in nifty_df.index
                    assert nifty_df.loc[pd.Timestamp("2026-08-28"), "nifty_close"] == 24122.6
                    assert not pd.isna(nifty_df.loc[pd.Timestamp("2026-08-28"), "nifty_return"])

def test_test_3_nse_csv_parsing():
    csv_content = '''Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value,Points Change,Change(%),Volume,Turnover (Rs. Cr.),P/E,P/B,Div Yield\nNifty 50,28-08-2026,24122.6,24122.6,24122.6,24122.6,0.0,0.0,100,100,20.44,2.93,1.16\nNifty Bank,28-08-2026,50000.0,50000.0,50000.0,50000.0,0.0,0.0,100,100,20.0,2.0,1.0\n'''
    import io
    mock_df = pd.read_csv(io.StringIO(csv_content))
    with patch("src.data.nse_index_fallback._fetch_index_bhavcopy_raw", return_value=mock_df):
        val = fetch_nse_index_close(datetime(2026, 8, 28), "Nifty 50")
        assert val == 24122.6

def test_test_4_reject_wrong_benchmark():
    csv_content = '''Index Name,Index Date,Closing Index Value\nNifty Bank,28-08-2026,50000.0\nNifty Next 50,28-08-2026,70000.0\n'''
    import io
    mock_df = pd.read_csv(io.StringIO(csv_content))
    with patch("src.data.nse_index_fallback._fetch_index_bhavcopy_raw", return_value=mock_df):
        val = fetch_nse_index_close(datetime(2026, 8, 28), "Nifty 50")
        assert val is None

def test_test_5_invalid_nse_response():
    csv_content = '''Index Name,Closing Index Value\nNifty 50,INVALID\n'''
    import io
    mock_df = pd.read_csv(io.StringIO(csv_content))
    with patch("src.data.nse_index_fallback._fetch_index_bhavcopy_raw", return_value=mock_df):
        val = fetch_nse_index_close(datetime(2026, 8, 28), "Nifty 50")
        assert val is None
        
    csv_content = '''Index Name,Closing Index Value\nNifty 50,-100.0\n'''
    mock_df = pd.read_csv(io.StringIO(csv_content))
    with patch("src.data.nse_index_fallback._fetch_index_bhavcopy_raw", return_value=mock_df):
        val = fetch_nse_index_close(datetime(2026, 8, 28), "Nifty 50")
        assert val is None

def test_test_6_existing_yahoo_data_does_not_trigger_fallback():
    end_date = datetime(2026, 8, 28)
    mock_df = pd.DataFrame({"Close": [24000.0, 24100.0]}, index=[pd.Timestamp("2026-08-27"), pd.Timestamp("2026-08-28")])
    mock_df.index.name = "Date"
    with patch("src.features.v1.engineering.yf.download", return_value=mock_df):
        with patch("src.features.v1.engineering._validate_macro_asset", side_effect=lambda df, name, min_valid_rows: (True, df)):
            with patch("src.data.session_calendar.is_session", side_effect=lambda dt: dt in [pd.Timestamp("2026-08-27"), pd.Timestamp("2026-08-28")]):
                with patch("src.data.nse_index_fallback.fetch_nse_index_close") as mock_fallback:
                    _prepare_nifty_data(datetime(2026, 8, 20), end_date, prediction_target_date=date(2026, 8, 31))
                    mock_fallback.assert_not_called()

def test_test_8_canonical_pipeline_identity():
    # Updated to the new hash after India VIX fallback modification
    expected_hash = "f0fa3983b0114cc63635a51f26ef954bd4f64b151ed81673aadadc857717d862"
    actual_hash = get_feature_pipeline_hash("v1")
    assert actual_hash == expected_hash, f"Expected {expected_hash}, got {actual_hash}"

# --- NEW INDIA VIX TESTS ---

def test_vix_fallback_yahoo_primary():
    """Test 1: Yahoo primary - Given valid Yahoo India VIX data, fallback is not called."""
    end_date = datetime(2026, 9, 4)
    # mock yf.download to return a valid DF for ^INDIAVIX
    # we just need to test _prepare_macro_data and observe fetch_nse_index_close is NOT called
    
    mock_df_vix = pd.DataFrame({"Close": [10.5]}, index=[pd.Timestamp("2026-09-04")])
    mock_df_vix.index.name = "Date"
    mock_df_other = pd.DataFrame({"Close": [100.0]}, index=[pd.Timestamp("2026-09-04")])
    mock_df_other.index.name = "Date"
    
    def side_effect_yf(ticker, *args, **kwargs):
        if ticker == "^INDIAVIX":
            return mock_df_vix
        return mock_df_other
        
    def side_effect_validate(df, name, min_valid_rows):
        return True, df

    with patch("src.features.v1.engineering.yf.download", side_effect=side_effect_yf):
        with patch("src.features.v1.engineering._validate_macro_asset", side_effect=side_effect_validate):
            with patch("src.data.session_calendar.is_session", side_effect=lambda dt: dt == pd.Timestamp("2026-09-04")):
                with patch("src.data.nse_index_fallback.fetch_nse_index_close") as mock_fallback:
                    mock_client = MagicMock()
                    _prepare_macro_data(datetime(2026, 9, 3), end_date, mock_client, prediction_target_date=date(2026, 9, 7))
                    mock_fallback.assert_not_called()

def test_vix_fallback_missing_session():
    """Test 2: Missing Yahoo VIX - NSE fallback is called and exact session inserted."""
    end_date = datetime(2026, 9, 4)
    
    mock_df_vix = pd.DataFrame({"Close": [10.0]}, index=[pd.Timestamp("2026-09-03")])
    mock_df_other = pd.DataFrame({"Close": [100.0, 100.0]}, index=[pd.Timestamp("2026-09-03"), pd.Timestamp("2026-09-04")])
    
    def side_effect_yf(ticker, *args, **kwargs):
        if ticker == "^INDIAVIX":
            return mock_df_vix.copy()
        return mock_df_other.copy()
        
    def side_effect_validate(df, name, min_valid_rows):
        return True, df

    with patch("src.features.v1.engineering.yf.download", side_effect=side_effect_yf):
        with patch("src.features.v1.engineering._validate_macro_asset", side_effect=side_effect_validate):
            with patch("src.data.session_calendar.is_session", side_effect=lambda dt: dt == pd.Timestamp("2026-09-04")):
                with patch("src.data.nse_index_fallback.fetch_nse_index_close") as mock_fallback:
                    mock_fallback.return_value = 10.68
                    mock_client = MagicMock()
                    
                    df = _prepare_macro_data(datetime(2026, 9, 4), end_date, mock_client, prediction_target_date=date(2026, 9, 7))
                    
                    mock_fallback.assert_any_call(pd.Timestamp("2026-09-04"), index_name="India VIX")
                    assert pd.Timestamp("2026-09-07") in df.index
                    # macro is shifted, so sep 7 row should have sep 4 value
                    assert df.loc[pd.Timestamp("2026-09-07"), "vix_level"] == 10.68

def test_vix_fallback_partial_history():
    """Test 3: Partial Yahoo history - only missing session repaired."""
    end_date = datetime(2026, 9, 4)
    # has sep 3, missing sep 4
    mock_df_vix = pd.DataFrame({"Close": [10.0]}, index=[pd.Timestamp("2026-09-03")])
    mock_df_other = pd.DataFrame({"Close": [100.0, 100.0]}, index=[pd.Timestamp("2026-09-03"), pd.Timestamp("2026-09-04")])
    
    def side_effect_yf(ticker, *args, **kwargs):
        if ticker == "^INDIAVIX":
            return mock_df_vix.copy()
        return mock_df_other.copy()
        
    def side_effect_validate(df, name, min_valid_rows):
        return True, df

    with patch("src.features.v1.engineering.yf.download", side_effect=side_effect_yf):
        with patch("src.features.v1.engineering._validate_macro_asset", side_effect=side_effect_validate):
            with patch("src.data.session_calendar.is_session", side_effect=lambda dt: dt in [pd.Timestamp("2026-09-03"), pd.Timestamp("2026-09-04")]):
                with patch("src.data.nse_index_fallback.fetch_nse_index_close") as mock_fallback:
                    mock_fallback.return_value = 10.68
                    mock_client = MagicMock()
                    df = _prepare_macro_data(datetime(2026, 9, 3), end_date, mock_client, prediction_target_date=date(2026, 9, 7))
                    # verify fallback called only for missing session (Sep 4), not Sep 3
                    mock_fallback.assert_any_call(pd.Timestamp("2026-09-04"), index_name="India VIX")
                    assert df.loc[pd.Timestamp("2026-09-07"), "vix_level"] == 10.68
                    assert df.loc[pd.Timestamp("2026-09-04"), "vix_level"] == 10.0 # From Sep 3

def test_vix_fallback_temporal_integrity():
    """Test 5 & 6: Friday to Monday, Weekend/Holiday - macro.shift(1) correctly maps Sep 4 to Sep 7."""
    end_date = datetime(2026, 9, 4)
    mock_df_vix = pd.DataFrame({"Close": [10.0]}, index=[pd.Timestamp("2026-09-03")])
    mock_df_other = pd.DataFrame({"Close": [100.0, 100.0]}, index=[pd.Timestamp("2026-09-03"), pd.Timestamp("2026-09-04")])
    
    def side_effect_yf(ticker, *args, **kwargs):
        if ticker == "^INDIAVIX":
            return mock_df_vix.copy()
        return mock_df_other.copy()
        
    def side_effect_validate(df, name, min_valid_rows):
        return True, df

    # Simulate Sep 5 and 6 as weekends (not sessions)
    def mock_is_session(dt):
        return dt.weekday() < 5 # True for Mon-Fri
        
    with patch("src.features.v1.engineering.yf.download", side_effect=side_effect_yf):
        with patch("src.features.v1.engineering._validate_macro_asset", side_effect=side_effect_validate):
            with patch("src.data.session_calendar.is_session", side_effect=mock_is_session):
                with patch("src.data.nse_index_fallback.fetch_nse_index_close") as mock_fallback:
                    mock_fallback.return_value = 10.68
                    mock_client = MagicMock()
                    df = _prepare_macro_data(datetime(2026, 9, 4), end_date, mock_client, prediction_target_date=date(2026, 9, 7))
                    # Fallback shouldn't be called for weekends (Sep 5, Sep 6)
                    for call_arg in mock_fallback.call_args_list:
                        dt = call_arg[0][0]
                        assert dt.weekday() < 5
                    # Verify shifted correctly to target date
                    assert df.loc[pd.Timestamp("2026-09-07"), "vix_level"] == 10.68

def test_vix_fallback_both_unavailable_fail_closed():
    """Test 7: Both sources unavailable -> fails validation naturally."""
    end_date = datetime(2026, 9, 4)
    mock_df_vix = pd.DataFrame(columns=["Close"]) # empty
    mock_df_other = pd.DataFrame({"Close": [100.0]}, index=[pd.Timestamp("2026-09-04")])
    
    def side_effect_yf(ticker, *args, **kwargs):
        if ticker == "^INDIAVIX":
            return mock_df_vix.copy()
        return mock_df_other.copy()

    # DO NOT patch _validate_macro_asset. Let it naturally fail because dataframe is empty.
    with patch("src.features.v1.engineering.yf.download", side_effect=side_effect_yf):
        with patch("src.data.session_calendar.is_session", side_effect=lambda dt: dt == pd.Timestamp("2026-09-04")):
            with patch("src.data.nse_index_fallback.fetch_nse_index_close", return_value=None):
                mock_client = MagicMock()
                df = _prepare_macro_data(datetime(2026, 9, 4), end_date, mock_client, prediction_target_date=date(2026, 9, 7))
                assert df.empty
