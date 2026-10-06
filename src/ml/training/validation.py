"""
src.ml.training.validation
Expanding-Window Purged Walk-Forward Validation Engine for V2 Architecture.
Eliminates target-overlap leakage via strict 10-session purging and dedicated calibration splits.
"""

from dataclasses import dataclass
from typing import List, Tuple, Generator, Optional
import numpy as np
import pandas as pd


@dataclass(frozen=True)
class WalkForwardFold:
    """
    Structured metadata and indices for a single walk-forward validation fold.
    """
    fold_id: int
    train_indices: np.ndarray
    purge_indices: np.ndarray
    calib_indices: np.ndarray
    test_indices: np.ndarray

    train_start: str
    train_end: str
    purge_start: Optional[str]
    purge_end: Optional[str]
    calib_start: Optional[str]
    calib_end: Optional[str]
    test_start: str
    test_end: str

    @property
    def train_count(self) -> int:
        return len(self.train_indices)

    @property
    def calib_count(self) -> int:
        return len(self.calib_indices)

    @property
    def test_count(self) -> int:
        return len(self.test_indices)

    @property
    def purge_count(self) -> int:
        return len(self.purge_indices)


class PurgedWalkForwardCV:
    """
    Expanding-Window Purged Walk-Forward Cross-Validation Splitter.

    Structure per fold:
      [ Expanding Train Window ] -> [ 10-Session Purge ] -> [ Optional Calibration Window ] -> [ Test Window ]

    Guarantees:
      1. Train and Calibration/Test are completely disjoint.
      2. Exactly `purge_sessions` (default 10) rows preceding calibration/test are purged
         so no future target labels from training reach into evaluation periods.
      3. Calibration strictly precedes Test and is non-overlapping.
    """

    def __init__(
        self,
        n_splits: int = 4,
        purge_sessions: int = 10,
        calib_sessions: int = 60,
        test_sessions: int = 120,
        min_train_sessions: int = 400,
    ):
        if n_splits < 1:
            raise ValueError(f"n_splits must be >= 1, got {n_splits}")
        if purge_sessions < 0:
            raise ValueError(f"purge_sessions must be >= 0, got {purge_sessions}")
        if calib_sessions < 0:
            raise ValueError(f"calib_sessions must be >= 0, got {calib_sessions}")
        if test_sessions <= 0:
            raise ValueError(f"test_sessions must be > 0, got {test_sessions}")

        self.n_splits = n_splits
        self.purge_sessions = purge_sessions
        self.calib_sessions = calib_sessions
        self.test_sessions = test_sessions
        self.min_train_sessions = min_train_sessions

    def split(
        self,
        df: pd.DataFrame,
    ) -> Generator[WalkForwardFold, None, None]:
        """
        Yields WalkForwardFold instances for the supplied sorted DataFrame.
        """
        n_samples = len(df)
        total_eval_per_fold = self.purge_sessions + self.calib_sessions + self.test_sessions

        # Ensure we have enough rows for min_train + at least 1 fold
        if n_samples < (self.min_train_sessions + total_eval_per_fold):
            raise ValueError(
                f"Insufficient sessions ({n_samples}) for purged walk-forward with "
                f"min_train={self.min_train_sessions} and total_eval={total_eval_per_fold}"
            )

        # Calculate fold step
        available_test_space = n_samples - self.min_train_sessions - self.purge_sessions - self.calib_sessions
        actual_splits = min(self.n_splits, max(1, available_test_space // self.test_sessions))

        # We anchor backwards from the end of the dataset to ensure the most recent data is tested
        test_starts = []
        for i in range(actual_splits):
            test_end_idx = n_samples - i * self.test_sessions
            test_start_idx = test_end_idx - self.test_sessions
            if test_start_idx < (self.min_train_sessions + self.purge_sessions + self.calib_sessions):
                break
            test_starts.append((test_start_idx, test_end_idx))

        test_starts.reverse()

        for fold_idx, (t_start, t_end) in enumerate(test_starts, start=1):
            test_indices = np.arange(t_start, t_end)

            # Calibration indices sit immediately before test
            if self.calib_sessions > 0:
                c_start = t_start - self.calib_sessions
                c_end = t_start
                calib_indices = np.arange(c_start, c_end)
            else:
                c_start = t_start
                c_end = t_start
                calib_indices = np.array([], dtype=int)

            # Purge indices sit immediately before calibration (or test if calib=0)
            p_end = c_start
            p_start = p_end - self.purge_sessions
            purge_indices = np.arange(p_start, p_end) if self.purge_sessions > 0 else np.array([], dtype=int)

            # Expanding train window starts at 0 and ends at p_start
            train_indices = np.arange(0, p_start)

            # Extract date strings from index
            get_date = lambda idx: str(df.index[idx])[:10] if hasattr(df, "index") else str(idx)

            fold = WalkForwardFold(
                fold_id=fold_idx,
                train_indices=train_indices,
                purge_indices=purge_indices,
                calib_indices=calib_indices,
                test_indices=test_indices,
                train_start=get_date(train_indices[0]),
                train_end=get_date(train_indices[-1]),
                purge_start=get_date(purge_indices[0]) if len(purge_indices) else None,
                purge_end=get_date(purge_indices[-1]) if len(purge_indices) else None,
                calib_start=get_date(calib_indices[0]) if len(calib_indices) else None,
                calib_end=get_date(calib_indices[-1]) if len(calib_indices) else None,
                test_start=get_date(test_indices[0]),
                test_end=get_date(test_indices[-1]),
            )

            yield fold
