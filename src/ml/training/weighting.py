"""
src.ml.training.weighting
Class Weighting Strategies for V2 Training Architecture.
Completely replaces SMOTE with cost-sensitive, prior-preserving sample weighting.
"""

from enum import Enum
from typing import Optional, Union, Dict
import numpy as np
import pandas as pd


class WeightingStrategy(str, Enum):
    """Supported class weighting strategies in V2 training."""
    UNIFORM = "uniform"
    INVERSE_FREQUENCY = "inverse_frequency"
    SQRT_INVERSE_FREQUENCY = "sqrt_inverse_frequency"


def compute_inverse_frequency_weights(
    y_train: Union[pd.Series, np.ndarray],
    num_classes: int = 3,
    normalize: bool = True,
) -> np.ndarray:
    """
    Computes exact normalized inverse-frequency sample weights:
      w_k = N / (K * N_k)
    where:
      - N is total training observations
      - K is number of classes (default 3)
      - N_k is count of class k

    Weights are normalized so that mean(weights) == 1.0 (sum(weights) == N).
    If a class is completely absent (N_k == 0), its weight is set to 0.0 without division by zero.
    """
    y_arr = np.asarray(y_train, dtype=int)
    n_total = len(y_arr)

    if n_total == 0:
        return np.array([], dtype=float)

    class_counts = np.bincount(y_arr, minlength=num_classes)
    class_weights = np.zeros(num_classes, dtype=float)

    for k in range(num_classes):
        if class_counts[k] > 0:
            class_weights[k] = n_total / (num_classes * class_counts[k])
        else:
            class_weights[k] = 0.0

    sample_weights = class_weights[y_arr]

    if normalize and sample_weights.sum() > 0:
        sample_weights = sample_weights * (n_total / sample_weights.sum())

    return sample_weights


def compute_sqrt_inverse_frequency_weights(
    y_train: Union[pd.Series, np.ndarray],
    num_classes: int = 3,
    normalize: bool = True,
) -> np.ndarray:
    """
    Computes square-root smoothed inverse-frequency sample weights:
      w_k = sqrt(N / N_k)
    Provides moderate cost-sensitive rebalancing without aggressive over-penalization.
    """
    y_arr = np.asarray(y_train, dtype=int)
    n_total = len(y_arr)

    if n_total == 0:
        return np.array([], dtype=float)

    class_counts = np.bincount(y_arr, minlength=num_classes)
    class_weights = np.zeros(num_classes, dtype=float)

    for k in range(num_classes):
        if class_counts[k] > 0:
            class_weights[k] = np.sqrt(n_total / class_counts[k])
        else:
            class_weights[k] = 0.0

    sample_weights = class_weights[y_arr]

    if normalize and sample_weights.sum() > 0:
        sample_weights = sample_weights * (n_total / sample_weights.sum())

    return sample_weights


def get_sample_weights(
    y_train: Union[pd.Series, np.ndarray],
    strategy: Union[WeightingStrategy, str] = WeightingStrategy.INVERSE_FREQUENCY,
    num_classes: int = 3,
) -> np.ndarray:
    """
    Factory selector for sample weights in V2 training.
    """
    strat = WeightingStrategy(strategy)

    if strat == WeightingStrategy.UNIFORM:
        return np.ones(len(y_train), dtype=float)
    elif strat == WeightingStrategy.INVERSE_FREQUENCY:
        return compute_inverse_frequency_weights(y_train, num_classes=num_classes)
    elif strat == WeightingStrategy.SQRT_INVERSE_FREQUENCY:
        return compute_sqrt_inverse_frequency_weights(y_train, num_classes=num_classes)
    else:
        raise ValueError(f"Unknown weighting strategy: {strategy}")
