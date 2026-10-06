"""
Unit tests for V2 Purged Walk-Forward Validation Engine.
"""

import numpy as np
import pandas as pd
import pytest

from src.ml.training.validation import (
    PurgedWalkForwardCV,
    WalkForwardFold,
)


def test_purged_walk_forward_fold_disjunction():
    """
    Verifies that for every fold:
      1. Train, Purge, Calibration, and Test are pairwise disjoint.
      2. No target overlap occurs across boundaries.
    """
    n_total = 800
    dates = pd.date_range("2022-01-01", periods=n_total, freq="B")
    df = pd.DataFrame({"close": np.linspace(100, 200, n_total)}, index=dates)

    cv = PurgedWalkForwardCV(
        n_splits=3,
        purge_sessions=10,
        calib_sessions=60,
        test_sessions=100,
        min_train_sessions=300,
    )

    folds = list(cv.split(df))
    assert len(folds) >= 1

    for fold in folds:
        # Sets must be pairwise disjoint
        s_train = set(fold.train_indices)
        s_purge = set(fold.purge_indices)
        s_calib = set(fold.calib_indices)
        s_test = set(fold.test_indices)

        assert s_train.isdisjoint(s_purge)
        assert s_train.isdisjoint(s_calib)
        assert s_train.isdisjoint(s_test)
        assert s_calib.isdisjoint(s_test)
        assert s_purge.isdisjoint(s_test)
        assert s_purge.isdisjoint(s_calib)

        # Expanding train: starts at 0
        assert fold.train_indices[0] == 0

        # Purge size must strictly equal 10 sessions
        assert len(fold.purge_indices) == 10

        # Max train index + 10 purge sessions == min calib index
        assert fold.train_indices[-1] + 1 + 10 == fold.calib_indices[0]

        # Max calib index + 1 == min test index
        assert fold.calib_indices[-1] + 1 == fold.test_indices[0]

        # No target overlap: last training session (t_last) label looks forward 10 sessions.
        # It settles at t_last + 10.
        # Calibration starts at t_last + 11.
        # Thus settlement of last train label is STRICTLY BEFORE calibration starts!
        last_train_idx = fold.train_indices[-1]
        calib_start_idx = fold.calib_indices[0]
        assert (last_train_idx + 10) < calib_start_idx


def test_walk_forward_expanding_nature():
    """Verifies that the training window strictly expands in later folds."""
    n_total = 1000
    df = pd.DataFrame({"close": np.arange(n_total)})

    cv = PurgedWalkForwardCV(
        n_splits=3,
        purge_sessions=10,
        calib_sessions=50,
        test_sessions=100,
        min_train_sessions=400,
    )

    folds = list(cv.split(df))
    assert len(folds) == 3

    train_sizes = [f.train_count for f in folds]
    assert train_sizes[0] < train_sizes[1] < train_sizes[2]


def test_insufficient_samples_raises_error():
    """Verifies that an error is raised if samples are insufficient."""
    df_small = pd.DataFrame({"close": np.arange(50)})
    cv = PurgedWalkForwardCV(min_train_sessions=200)

    with pytest.raises(ValueError, match="Insufficient sessions"):
        list(cv.split(df_small))
