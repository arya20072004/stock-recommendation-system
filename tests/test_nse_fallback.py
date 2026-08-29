import pytest
import pandas as pd
from datetime import datetime, date
from unittest.mock import patch, MagicMock

from src.features.v1.engineering import _prepare_nifty_data
from src.features.router import get_feature_pipeline_hash
from src.data.nse_index_fallback import fetch_nse_index_close, _fetch_index_bhavcopy_raw

def test_test_1_missing_latest_session_blocks_inference():
    # Mock YF to return data without end_date, and NSE fallback fails
    end_date = datetime(2026, 8, 28)
    
    mock_df = pd.DataFrame({"Close": [24000.0, 24100.0]}, index=[pd.Timestamp("2026-08-26"), pd.Timestamp("2026-08-27")])
    mock_df.index.name = "Date"
    
    with patch("src.features.v1.engineering._fetch_cached_macro", return_value=mock_df):
        with patch("src.features.v1.engineering._validate_macro_asset", return_value=(True, mock_df)):
            with patch("src.data.nse_index_fallback.fetch_nse_index_close", return_value=None):
                nifty_df = _prepare_nifty_data(datetime(2026, 8, 20), end_date, prediction_target_date=date(2026, 8, 31))
                # 2026-08-28 should not be in the index (fallback failed)
                assert pd.Timestamp("2026-08-28") not in nifty_df.index
                # Prediction target row should have NaN for nifty_return
                assert pd.isna(nifty_df.loc[pd.Timestamp("2026-08-31"), "nifty_return"])

def test_test_2_successful_fallback():
    end_date = datetime(2026, 8, 28)
    mock_df = pd.DataFrame({"Close": [24000.0, 24100.0]}, index=[pd.Timestamp("2026-08-26"), pd.Timestamp("2026-08-27")])
    mock_df.index.name = "Date"
    
    with patch("src.features.v1.engineering._fetch_cached_macro", return_value=mock_df):
        with patch("src.features.v1.engineering._validate_macro_asset", return_value=(True, mock_df)):
            with patch("src.data.nse_index_fallback.fetch_nse_index_close", return_value=24122.6):
                nifty_df = _prepare_nifty_data(datetime(2026, 8, 20), end_date, prediction_target_date=date(2026, 8, 31))
                
                # 2026-08-28 should be recovered!
                assert pd.Timestamp("2026-08-28") in nifty_df.index
                assert nifty_df.loc[pd.Timestamp("2026-08-28"), "nifty_close"] == 24122.6
                assert not pd.isna(nifty_df.loc[pd.Timestamp("2026-08-28"), "nifty_return"])

def test_test_3_nse_csv_parsing():
    # Mock CSV string representing nsearchives.nseindia.com/content/indices/ind_close_all_...
    csv_content = '''Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value,Points Change,Change(%),Volume,Turnover (Rs. Cr.),P/E,P/B,Div Yield
Nifty 50,28-08-2026,24122.6,24122.6,24122.6,24122.6,0.0,0.0,100,100,20.44,2.93,1.16
Nifty Bank,28-08-2026,50000.0,50000.0,50000.0,50000.0,0.0,0.0,100,100,20.0,2.0,1.0
'''
    import io
    mock_df = pd.read_csv(io.StringIO(csv_content))
    with patch("src.data.nse_index_fallback._fetch_index_bhavcopy_raw", return_value=mock_df):
        val = fetch_nse_index_close(datetime(2026, 8, 28), "Nifty 50")
        assert val == 24122.6

def test_test_4_reject_wrong_benchmark():
    csv_content = '''Index Name,Index Date,Closing Index Value
Nifty Bank,28-08-2026,50000.0
Nifty Next 50,28-08-2026,70000.0
'''
    import io
    mock_df = pd.read_csv(io.StringIO(csv_content))
    with patch("src.data.nse_index_fallback._fetch_index_bhavcopy_raw", return_value=mock_df):
        val = fetch_nse_index_close(datetime(2026, 8, 28), "Nifty 50")
        # Should return None because Nifty 50 is not in the CSV
        assert val is None

def test_test_5_invalid_nse_response():
    # Test non-numeric close
    csv_content = '''Index Name,Closing Index Value
Nifty 50,INVALID
'''
    import io
    mock_df = pd.read_csv(io.StringIO(csv_content))
    with patch("src.data.nse_index_fallback._fetch_index_bhavcopy_raw", return_value=mock_df):
        val = fetch_nse_index_close(datetime(2026, 8, 28), "Nifty 50")
        assert val is None
        
    # Test negative close
    csv_content = '''Index Name,Closing Index Value
Nifty 50,-100.0
'''
    mock_df = pd.read_csv(io.StringIO(csv_content))
    with patch("src.data.nse_index_fallback._fetch_index_bhavcopy_raw", return_value=mock_df):
        val = fetch_nse_index_close(datetime(2026, 8, 28), "Nifty 50")
        assert val is None

def test_test_6_existing_yahoo_data_does_not_trigger_fallback():
    end_date = datetime(2026, 8, 28)
    mock_df = pd.DataFrame({"Close": [24000.0, 24100.0]}, index=[pd.Timestamp("2026-08-27"), pd.Timestamp("2026-08-28")])
    mock_df.index.name = "Date"
    
    with patch("src.features.v1.engineering._fetch_cached_macro", return_value=mock_df):
        with patch("src.features.v1.engineering._validate_macro_asset", return_value=(True, mock_df)):
            with patch("src.data.nse_index_fallback.fetch_nse_index_close") as mock_fallback:
                _prepare_nifty_data(datetime(2026, 8, 20), end_date, prediction_target_date=date(2026, 8, 31))
                # Fallback must not be called
                mock_fallback.assert_not_called()

def test_test_8_canonical_pipeline_identity():
    # Verify the current governed hash.
    expected_hash = "879f04b32ad89e0f6d9e648f8ec2801fba84d6d257bc5a0750eca5aa0685fe2d"
    actual_hash = get_feature_pipeline_hash("v1")
    assert actual_hash == expected_hash, f"Expected {expected_hash}, got {actual_hash}"
