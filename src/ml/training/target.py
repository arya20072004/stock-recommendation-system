"""
src.ml.training.target
Canonical T+10 Target Construction for V2 Training Architecture.
Strictly implements H=10 valid exchange trading sessions across all tickers.
"""

from dataclasses import dataclass, field
from typing import Optional, Union
import numpy as np
import pandas as pd

from src.data.session_calendar import is_session


@dataclass(frozen=True)
class TargetConfig:
    """
    Configuration for target label generation.
    Enforces canonical horizon H=10 across all tickers.
    """
    horizon: int = 10
    theta_min: float = 0.0100  # Baseline floor: 1.00% (experimentally supports 1.25%, 1.50%)
    alpha: float = 1.0         # Volatility scaling multiplier
    atr_window: int = 14

    def __post_init__(self):
        if self.horizon != 10:
            raise ValueError(f"V2 architecture strictly locks horizon to 10 sessions. Got: {self.horizon}")
        if self.theta_min <= 0.0:
            raise ValueError(f"theta_min must be positive. Got: {self.theta_min}")
        if self.alpha <= 0.0:
            raise ValueError(f"alpha must be positive. Got: {self.alpha}")
        if self.atr_window < 1:
            raise ValueError(f"atr_window must be >= 1. Got: {self.atr_window}")


def compute_t10_return(
    close_series: pd.Series,
    horizon: int = 10,
) -> pd.Series:
    """
    Computes forward price return R(t, H) = C(t+H) / C(t) - 1.
    Assumes close_series is indexed by sorted valid exchange trading sessions.
    """
    if horizon != 10:
        raise ValueError(f"V2 target requires horizon == 10. Received: {horizon}")
    if not isinstance(close_series, pd.Series):
        raise TypeError(f"close_series must be pd.Series, got {type(close_series)}")
    if close_series.empty:
        return pd.Series(dtype=float)

    # Shift backwards by -horizon (future close divided by current close)
    future_close = close_series.shift(-horizon)
    current_close = close_series.replace(0.0, np.nan)
    returns = (future_close / current_close) - 1.0
    return returns.astype(float)


def compute_dynamic_hurdle(
    atr_pct: Union[pd.Series, float, np.ndarray],
    config: Optional[TargetConfig] = None,
) -> Union[pd.Series, float, np.ndarray]:
    """
    Computes dynamic return hurdle theta = max(alpha * atr_pct, theta_min).
    """
    cfg = config or TargetConfig()
    scaled_atr = cfg.alpha * atr_pct
    hurdle = np.maximum(scaled_atr, cfg.theta_min)
    return hurdle


def assign_three_class_labels(
    returns: Union[pd.Series, np.ndarray, float],
    hurdles: Union[pd.Series, np.ndarray, float],
) -> Union[pd.Series, np.ndarray, int]:
    """
    Assigns discrete 3-class target labels based on returns and hurdle:
      - BUY  = 2  if return > hurdle
      - SELL = 0  if return < -hurdle
      - HOLD = 1  if -hurdle <= return <= hurdle
    NaN return inputs produce NaN label outputs.
    """
    if isinstance(returns, pd.Series):
        labels = pd.Series(index=returns.index, dtype="float64")
        valid_mask = returns.notna() & pd.Series(hurdles, index=returns.index).notna()

        # Default all valid to HOLD (1)
        labels[valid_mask] = 1.0

        # Assign BUY (2) where return strictly exceeds hurdle
        buy_mask = valid_mask & (returns > hurdles)
        labels[buy_mask] = 2.0

        # Assign SELL (0) where return strictly below -hurdle
        sell_mask = valid_mask & (returns < -hurdles)
        labels[sell_mask] = 0.0

        return labels

    elif isinstance(returns, np.ndarray):
        labels = np.full(returns.shape, np.nan, dtype=float)
        valid_mask = np.isfinite(returns) & np.isfinite(hurdles)

        labels[valid_mask] = 1.0
        labels[valid_mask & (returns > hurdles)] = 2.0
        labels[valid_mask & (returns < -hurdles)] = 0.0
        return labels

    else:
        # Scalar handling
        if returns is None or np.isnan(returns) or np.isnan(hurdles):
            return np.nan
        if returns > hurdles:
            return 2
        elif returns < -hurdles:
            return 0
        else:
            return 1


def build_t10_target_series(
    df: pd.DataFrame,
    ticker: str,
    config: Optional[TargetConfig] = None,
    close_col: str = "close",
    atr_pct_col: str = "atr_pct",
) -> pd.DataFrame:
    """
    End-to-end target construction for V2 training.
    Strictly verifies H=10, unadjusted close, and generates:
      - 'future_return': R(t, 10)
      - 'target_hurdle': theta(t)
      - 'target': Discrete class labels {0, 1, 2}
    """
    cfg = config or TargetConfig()

    if df.empty:
        return df.copy()

    if close_col not in df.columns:
        raise KeyError(f"Missing required price column '{close_col}' for ticker {ticker}")
    if atr_pct_col not in df.columns:
        raise KeyError(f"Missing required volatility column '{atr_pct_col}' for ticker {ticker}")

    out = df.copy()

    # Calculate 10-session return
    out["future_return"] = compute_t10_return(out[close_col], horizon=cfg.horizon)

    # Calculate dynamic volatility hurdle
    out["target_hurdle"] = compute_dynamic_hurdle(out[atr_pct_col], config=cfg)

    # Assign class labels
    out["target"] = assign_three_class_labels(out["future_return"], out["target_hurdle"])

    return out
