import pandas as pd
from datetime import datetime, date
from unittest.mock import patch, MagicMock
from src.features.v1.engineering import _prepare_macro_data
from tests.test_nasdaq_fallback import get_yf_download_mock

def test_nasdaq_genuine_provider_failure():
    def mock_yf(ticker, *args, **kwargs):
        if ticker == "^NDX":
            return pd.DataFrame()
        return get_yf_download_mock([pd.Timestamp("2026-09-01")], [pd.Timestamp("2026-09-01")])("^NSEI")
    
    with patch("src.features.v1.engineering.yf.download", side_effect=mock_yf):
        df = _prepare_macro_data(datetime(2026, 8, 1), datetime(2026, 9, 1), MagicMock())
        print(df[["nasdaq_ret_5d", "nasdaq_ret_20d"]].tail())
        
test_nasdaq_genuine_provider_failure()
