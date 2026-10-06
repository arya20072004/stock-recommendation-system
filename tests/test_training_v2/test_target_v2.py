"""
Unit tests for V2 Target & Label Construction.
"""

import numpy as np
import pandas as pd
import pytest

from src.data.nifty50 import TICKERS
from src.ml.training.target import (
    TargetConfig,
    compute_t10_return,
    compute_dynamic_hurdle,
    assign_three_class_labels,
    build_t10_target_series,
)


def test_target_config_locks_horizon():
    """Verifies that TargetConfig strictly enforces H=10."""
    cfg = TargetConfig(horizon=10, theta_min=0.01)
    assert cfg.horizon == 10

    with pytest.raises(ValueError, match="horizon to 10"):
        TargetConfig(horizon=5)

    with pytest.raises(ValueError, match="horizon to 10"):
        TargetConfig(horizon=15)


def test_target_config_theta_min_options():
    """Verifies configurable theta_min (1.00%, 1.25%, 1.50%)."""
    for theta in [0.0100, 0.0125, 0.0150]:
        cfg = TargetConfig(horizon=10, theta_min=theta)
        assert cfg.theta_min == theta

    with pytest.raises(ValueError):
        TargetConfig(horizon=10, theta_min=-0.01)


def test_compute_t10_return():
    """Verifies 10-session return formula R(t, 10) = C(t+10) / C(t) - 1."""
    prices = pd.Series([100.0] * 10 + [110.0] + [90.0] * 5)
    rets = compute_t10_return(prices, horizon=10)

    # Index 0 should look at index 10: 110 / 100 - 1 = +0.10
    assert pytest.approx(rets.iloc[0], rel=1e-5) == 0.10

    # Index 1 should look at index 11: 90 / 100 - 1 = -0.10
    assert pytest.approx(rets.iloc[1], rel=1e-5) == -0.10

    # Tail rows (last 10) should be NaN
    assert rets.iloc[-10:].isna().all()


def test_dynamic_hurdle():
    """Verifies theta = max(alpha * atr_pct, theta_min)."""
    cfg = TargetConfig(horizon=10, theta_min=0.015, alpha=1.0)

    # When ATR is low (0.01), hurdle should be floored at 0.015
    assert compute_dynamic_hurdle(0.01, cfg) == 0.015

    # When ATR is high (0.025), hurdle should scale to 0.025
    assert compute_dynamic_hurdle(0.025, cfg) == 0.025


def test_assign_three_class_labels_exact_boundaries():
    """Verifies exact boundary conditions for 3-class target."""
    hurdle = 0.0200  # 2.0%

    # Exactly +theta -> HOLD (1)
    assert assign_three_class_labels(0.0200, hurdle) == 1

    # Just above +theta -> BUY (2)
    assert assign_three_class_labels(0.0200001, hurdle) == 2

    # Exactly -theta -> HOLD (1)
    assert assign_three_class_labels(-0.0200, hurdle) == 1

    # Just below -theta -> SELL (0)
    assert assign_three_class_labels(-0.0200001, hurdle) == 0

    # Zero return -> HOLD (1)
    assert assign_three_class_labels(0.0, hurdle) == 1

    # NaN return -> NaN
    assert np.isnan(assign_three_class_labels(np.nan, hurdle))


def test_series_label_assignment():
    """Verifies label assignment on pd.Series."""
    returns = pd.Series([0.05, -0.05, 0.01, -0.01, 0.02, -0.02, np.nan])
    hurdle = pd.Series([0.02] * len(returns))

    labels = assign_three_class_labels(returns, hurdle)

    expected = [2.0, 0.0, 1.0, 1.0, 1.0, 1.0, np.nan]
    for actual, exp in zip(labels, expected):
        if np.isnan(exp):
            assert np.isnan(actual)
        else:
            assert actual == exp


def test_all_51_tickers_use_horizon_10():
    """Verifies that build_t10_target_series ignores any legacy horizon overrides for all 51 tickers."""
    for ticker in TICKERS:
        df_dummy = pd.DataFrame({
            "close": np.linspace(100, 200, 30),
            "atr_pct": [0.02] * 30,
        })
        out = build_t10_target_series(df_dummy, ticker=ticker)

        # Future return must be calculated over exactly 10 sessions for all 51 tickers
        assert len(out) == 30
        assert out["future_return"].iloc[-10:].isna().all()
        assert out["future_return"].iloc[:-10].notna().all()
        # Row 0 return: close[10] / close[0] - 1
        expected_ret = (df_dummy["close"].iloc[10] / df_dummy["close"].iloc[0]) - 1.0
        assert pytest.approx(out["future_return"].iloc[0], rel=1e-5) == expected_ret
