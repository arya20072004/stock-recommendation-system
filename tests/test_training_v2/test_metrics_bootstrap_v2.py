"""
Unit tests for V2 Evaluation Metrics & Paired Clustered Bootstrap.
"""

import numpy as np
import pandas as pd
import pytest

from src.ml.training.metrics import (
    compute_classification_metrics,
    compute_probability_metrics,
    compute_economic_metrics,
    compute_expected_calibration_error,
    compute_multiclass_brier_score,
    paired_clustered_bootstrap,
)


def test_classification_metrics_known_confusion():
    """Verifies classification metrics calculation against known ground truth."""
    # 2 BUY (2), 2 HOLD (1), 2 SELL (0)
    y_true = np.array([0, 0, 1, 1, 2, 2])
    y_pred = np.array([0, 1, 1, 1, 2, 0])

    m = compute_classification_metrics(y_true, y_pred)

    # Correct: index 0 (0==0), index 2 (1==1), index 3 (1==1), index 4 (2==2) -> 4/6 = 0.6667
    assert pytest.approx(m["accuracy"], rel=1e-3) == 0.6667
    assert "per_class" in m
    assert "confusion_matrix" in m
    assert m["confusion_matrix"]["SELL"]["SELL"] == 1
    assert m["confusion_matrix"]["BUY"]["BUY"] == 1


def test_ece_and_brier_score():
    """Verifies ECE and Brier score calculations."""
    y_true = np.array([0, 1, 2])
    # Perfect probabilities: 1.0 for true class
    perfect_probs = np.array([
        [1.0, 0.0, 0.0],
        [0.0, 1.0, 0.0],
        [0.0, 0.0, 1.0],
    ])

    brier = compute_multiclass_brier_score(y_true, perfect_probs)
    assert pytest.approx(brier, rel=1e-5) == 0.0

    ece = compute_expected_calibration_error(y_true, perfect_probs)
    assert pytest.approx(ece, rel=1e-5) == 0.0


def test_economic_metrics():
    """Verifies financial metrics calculation."""
    y_pred = np.array([2, 2, 0, 1])  # 2 BUYs, 1 SELL, 1 HOLD
    returns = np.array([0.05, -0.02, -0.04, 0.005])

    econ = compute_economic_metrics(y_pred, returns)

    assert econ["BUY"]["count"] == 2
    assert pytest.approx(econ["BUY"]["mean_return"], rel=1e-4) == 0.015  # (0.05 - 0.02)/2
    assert pytest.approx(econ["BUY"]["win_rate"], rel=1e-4) == 0.50
    assert pytest.approx(econ["BUY"]["loss_rate"], rel=1e-4) == 0.50
    assert econ["SELL"]["count"] == 1
    assert pytest.approx(econ["SELL"]["mean_return"], rel=1e-4) == -0.04


def test_paired_clustered_bootstrap_deterministic():
    """Verifies that paired clustered bootstrap is deterministic and cluster-aware."""
    df = pd.DataFrame({
        "market_date": ["2026-09-01"] * 5 + ["2026-09-02"] * 5,
        "incumbent_correct": [1, 0, 0, 1, 0, 0, 1, 0, 0, 0],
        "challenger_correct": [1, 1, 0, 1, 1, 0, 1, 1, 0, 1],
    })

    b1 = paired_clustered_bootstrap(
        df,
        incumbent_correct_col="incumbent_correct",
        challenger_correct_col="challenger_correct",
        cluster_col="market_date",
        n_bootstrap=500,
        random_seed=42,
    )

    b2 = paired_clustered_bootstrap(
        df,
        incumbent_correct_col="incumbent_correct",
        challenger_correct_col="challenger_correct",
        cluster_col="market_date",
        n_bootstrap=500,
        random_seed=42,
    )

    assert b1["point_estimate"] == b2["point_estimate"]
    assert b1["lower_ci"] == b2["lower_ci"]
    assert b1["upper_ci"] == b2["upper_ci"]
    # Challenger has 7 correct vs Incumbent 3 correct -> delta = +0.40
    assert pytest.approx(b1["point_estimate"], rel=1e-4) == 0.40
    assert b1["n_clusters"] == 2
