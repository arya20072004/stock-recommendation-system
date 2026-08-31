import pytest
import pandas as pd
from datetime import datetime, date
from unittest.mock import patch

from src.features.v1.engineering import _prepare_nifty_data, _MACRO_CACHE
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
    expected_hash = "426253a3d8a9dc6a8d6e4210d825d926c393e717f2f334df4a3de1267912328d"
    actual_hash = get_feature_pipeline_hash("v1")
    assert actual_hash == expected_hash, f"Expected {expected_hash}, got {actual_hash}"
