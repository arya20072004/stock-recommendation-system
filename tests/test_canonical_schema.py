import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

import src.features.v1.engineering as engineering
from src.ml.trainer import _make_feature_list
from src.features.router import get_feature_pipeline_hash

def test_canonical_schema_features():
    # A & B: Ensure canonical list is exactly 57 features and has stock PCR features
    df_mock = pd.DataFrame(columns=[
        "rsi", "macd_hist", "bb_width", "atr", "atr_pct",
        "sentiment_7d_avg", "sentiment_30d_avg", "price_change_1d",
        "price_change_5d", "market_correlation", "outperformance",
        "market_regime", "obv_deviation", "vwap_deviation",
        "relative_volume", "sector_momentum", "sector_momentum_5d",
        "adx", "adx_trending", "nifty_ret_1d", "nifty_ret_5d",
        "nifty_ret_10d", "nifty_ret_20d", "nifty_vol_10d",
        "usdinr_ret_1d", "usdinr_ret_5d", "usdinr_vol_10d",
        "nasdaq_ret_5d", "nasdaq_ret_20d", "crude_ret_1d",
        "crude_ret_5d", "crude_vol_10d", "gold_ret_1d",
        "gold_ret_5d", "gold_vol_10d", "copper_ret_1d",
        "copper_ret_5d", "copper_vol_10d", "vix_level",
        "vix_ret_1d", "vix_chg_5d", "vix_vol_10d", "month_sin",
        "month_cos", "is_month_end", "is_month_start",
        "quarter_end", "is_expiry_week", "in_earnings_season",
        "nifty_pcr_oi", "nifty_pcr_chg_5d", "banknifty_pcr_oi",
        "banknifty_pcr_chg_5d", "nifty_futures_basis",
        "nifty_futures_basis_chg_5d", "stock_pcr_oi",
        "stock_pcr_chg_5d"
    ])
    
    features = _make_feature_list(df_mock)
    assert len(features) == 57, f"Expected 57 features, got {len(features)}"
    assert "stock_pcr_oi" in features
    assert "stock_pcr_chg_5d" in features

@patch("src.features.v1.engineering._prepare_stock_pcr_data")
@patch("src.features.v1.engineering._prepare_sector_data")
@patch("src.features.v1.engineering._prepare_macro_data")
@patch("src.features.v1.engineering._prepare_nifty_data")
def test_zero_fill_path(mock_nifty, mock_macro, mock_sector, mock_pcr):
    # Setup mock returns
    dates = pd.date_range("2026-07-01", periods=50)
    
    mock_db = MagicMock()
    # Mock historical_data.find
    hist_data = [
        {"date": d, "open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 1000.0}
        for d in dates
    ]
    mock_db.historical_data.find.return_value = hist_data

    # Missing PCR data returns empty DataFrame
    mock_pcr.return_value = pd.DataFrame()
    
    mock_sector.return_value = pd.Series([0.01]*50, index=dates, name="sector_return")
    
    mock_macro.return_value = pd.DataFrame({
        "usdinr_close": [80.0]*50,
        "nasdaq_close": [15000.0]*50,
        "crude_close": [70.0]*50,
        "gold_close": [2000.0]*50,
        "copper_close": [4.0]*50,
        "vix_close": [15.0]*50,
    }, index=dates)
    
    mock_nifty.return_value = pd.DataFrame({
        "nifty_close": [20000.0]*50,
        "nifty_return": [0.0]*50,
        "market_regime": [1]*50,
        "nifty_pcr_oi": [1.0]*50,
        "banknifty_pcr_oi": [1.0]*50,
        "nifty_futures_basis": [10.0]*50,
    }, index=dates)

    # Ensure technical indicators don't error out
    df = engineering.build_feature_row(
        ticker="RELIANCE.NS",
        last_completed_session=dates[-2],
        prediction_target_date=dates[-1],
        client=MagicMock(),
        db=mock_db
    )
    
    features = _make_feature_list(df)
    
    assert "stock_pcr_oi" in df.columns
    assert "stock_pcr_chg_5d" in df.columns
    
    assert df["stock_pcr_oi"].iloc[-1] == 0.0
    assert df["stock_pcr_chg_5d"].iloc[-1] == 0.0
    
    assert len(features) == 57

def test_pipeline_hash():
    # G. Pipeline hash consistent
    h = get_feature_pipeline_hash("v1")
    assert h == "f4891c1b2172b8e024b176cd221cf33c8b5e006acd567d95f2768cb7bf72384e"
