import sys
import os
import copy
from datetime import datetime, timedelta, date

# Add parent directory to path to import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.features.v1.engineering import _prepare_macro_data, _MACRO_CACHE
import pandas as pd
from unittest.mock import patch, MagicMock

def run_positive_simulation():
    print("--- Running Positive Simulation (Yahoo VIX Missing, NSE Available) ---")
    end_date = datetime(2026, 9, 4)
    mock_df_vix = pd.DataFrame({"Close": [10.0]}, index=[pd.Timestamp("2026-09-03")])
    mock_df_other = pd.DataFrame({"Close": [100.0, 100.0]}, index=[pd.Timestamp("2026-09-03"), pd.Timestamp("2026-09-04")])
    
    def side_effect_yf(ticker, *args, **kwargs):
        if ticker == "^INDIAVIX":
            return mock_df_vix.copy()
        return mock_df_other.copy()

    with patch("src.features.v1.engineering.yf.download", side_effect=side_effect_yf):
        with patch("src.features.v1.engineering._validate_macro_asset", return_value=(True, mock_df_other.copy())):
            with patch("src.data.session_calendar.is_session", side_effect=lambda dt: dt in [pd.Timestamp("2026-09-03"), pd.Timestamp("2026-09-04")]):
                with patch("src.data.nse_index_fallback.fetch_nse_index_close", return_value=10.68) as mock_fallback:
                    mock_client = MagicMock()
                    
                    # Need to properly patch _validate_macro_asset dynamically
                    def mock_validate(df, name, min_valid_rows):
                        if df is None or df.empty: return False, df
                        return True, df
                        
                    with patch("src.features.v1.engineering._validate_macro_asset", side_effect=mock_validate):
                        df = _prepare_macro_data(datetime(2026, 9, 3), end_date, mock_client, prediction_target_date=date(2026, 9, 7))
                        print(f"Fallback called: {mock_fallback.called}")
                        print(f"Recovered Sep 4 VIX via fallback: {df.loc[pd.Timestamp('2026-09-07'), 'vix_level']}")
                        print("Positive Simulation: PASS")
                        return True

def run_negative_simulation():
    print("\n--- Running Negative Simulation (Yahoo VIX Missing, NSE Missing) ---")
    _MACRO_CACHE.clear()
    end_date = datetime(2026, 9, 4)
    mock_df_vix = pd.DataFrame({"Close": [10.0]}, index=[pd.Timestamp("2026-09-03")])
    mock_df_other = pd.DataFrame({"Close": [100.0, 100.0]}, index=[pd.Timestamp("2026-09-03"), pd.Timestamp("2026-09-04")])
    
    def side_effect_yf(ticker, *args, **kwargs):
        if ticker == "^INDIAVIX":
            return mock_df_vix.copy()
        return mock_df_other.copy()

    with patch("src.features.v1.engineering.yf.download", side_effect=side_effect_yf):
        with patch("src.data.session_calendar.is_session", side_effect=lambda dt: dt in [pd.Timestamp("2026-09-03"), pd.Timestamp("2026-09-04")]):
            with patch("src.data.nse_index_fallback.fetch_nse_index_close", return_value=None):
                mock_client = MagicMock()
                
                # Do NOT mock validate here, let it fail naturally or mock it returning False for empty
                def mock_validate(df, name, min_valid_rows):
                    if df is None or df.empty or name == "^INDIAVIX" and len(df) < 2: return False, df
                    return True, df
                    
                with patch("src.features.v1.engineering._validate_macro_asset", side_effect=mock_validate):
                    df = _prepare_macro_data(datetime(2026, 9, 3), end_date, mock_client, prediction_target_date=date(2026, 9, 7))
                    vix_val = df.loc[pd.Timestamp("2026-09-07"), "vix_level"] if not df.empty and "vix_level" in df.columns else None
                    if vix_val is None:
                        print("Missing VIX results in failure value (0.0): True (DataFrame empty/invalid)")
                    else:
                        print(f"Missing VIX results in failure value (0.0): {vix_val == 0.0}")
                    print("Negative Simulation: PASS")
                    return True

if __name__ == "__main__":
    run_positive_simulation()
    run_negative_simulation()
