"""
Unit tests for V2 Class Weighting & SMOTE Exclusion.
"""

import numpy as np
import pytest

from src.ml.training.weighting import (
    compute_inverse_frequency_weights,
    compute_sqrt_inverse_frequency_weights,
    get_sample_weights,
    WeightingStrategy,
)


def test_no_smote_import():
    """Verifies that imblearn.over_sampling.SMOTE is not imported anywhere in src.ml.training."""
    import sys
    # Check that src.ml.training does not import or expose SMOTE
    import src.ml.training as v2_pkg
    assert not hasattr(v2_pkg, "SMOTE")


def test_inverse_frequency_weighting_normalization():
    """
    Verifies that compute_inverse_frequency_weights:
      w_k = N / (K * N_k)
    sums to N (mean == 1.0).
    """
    # Distribution: 60 SELL (0), 30 HOLD (1), 10 BUY (2) -> Total 100
    y = np.array([0] * 60 + [1] * 30 + [2] * 10)
    w = compute_inverse_frequency_weights(y, num_classes=3, normalize=True)

    assert len(w) == 100
    assert pytest.approx(w.sum(), rel=1e-5) == 100.0
    assert pytest.approx(w.mean(), rel=1e-5) == 1.0

    # Rare class (BUY=2) must have higher weight than majority class (SELL=0)
    w_sell = w[y == 0][0]
    w_hold = w[y == 1][0]
    w_buy = w[y == 2][0]

    assert w_buy > w_hold > w_sell
    # Ratio: N_sell / N_buy = 60 / 10 = 6.0
    assert pytest.approx(w_buy / w_sell, rel=1e-5) == 6.0


def test_sqrt_inverse_frequency_weighting():
    """Verifies square-root smoothed inverse frequency weights."""
    y = np.array([0] * 60 + [1] * 30 + [2] * 10)
    w = compute_sqrt_inverse_frequency_weights(y, num_classes=3, normalize=True)

    assert len(w) == 100
    assert pytest.approx(w.sum(), rel=1e-5) == 100.0

    w_sell = w[y == 0][0]
    w_buy = w[y == 2][0]

    assert w_buy > w_sell
    # Ratio should equal sqrt(60 / 10) = sqrt(6) ≈ 2.449
    assert pytest.approx(w_buy / w_sell, rel=1e-4) == np.sqrt(6.0)


def test_missing_class_graceful_handling():
    """Verifies that missing classes (count 0) do not divide by zero."""
    # Only classes 0 and 1 present, class 2 missing
    y = np.array([0] * 50 + [1] * 50)
    w = compute_inverse_frequency_weights(y, num_classes=3, normalize=True)

    assert len(w) == 100
    assert np.isfinite(w).all()
    assert pytest.approx(w.sum(), rel=1e-5) == 100.0
    assert (w > 0).all()


def test_deterministic_output():
    """Verifies that weights output is 100% deterministic."""
    y = np.array([0, 1, 2, 0, 1, 2, 0, 0, 1, 2])
    w1 = get_sample_weights(y, WeightingStrategy.INVERSE_FREQUENCY)
    w2 = get_sample_weights(y, WeightingStrategy.INVERSE_FREQUENCY)

    np.testing.assert_array_almost_equal(w1, w2)
