"""
src.ml.trainer_v2
V2 Training Pipeline Implementation.
Completely isolated from legacy src/ml/trainer.py.

Key Features:
- Universal H=10 valid session target (zero horizon overrides)
- Zero SMOTE synthetic oversampling
- Cost-sensitive class weighting (inverse frequency / sqrt-inverse)
- Expanding-Window Purged Walk-Forward Cross-Validation
- Dedicated Chronological Probability Calibration
- Strict provenance tracking
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import numpy as np
import pandas as pd
from xgboost import XGBClassifier

from src.ml.training.target import (
    TargetConfig,
    build_t10_target_series,
)
from src.ml.training.weighting import (
    WeightingStrategy,
    get_sample_weights,
)
from src.ml.training.validation import (
    PurgedWalkForwardCV,
    WalkForwardFold,
)
from src.ml.training.calibration import (
    CalibrationMethod,
    get_calibrator,
    MulticlassCalibrator,
)
from src.ml.training.metrics import (
    evaluate_full_metrics_suite,
)
from src.ml.training.provenance import (
    ExperimentConfig,
    ExperimentProvenance,
    compute_config_hash,
)

logger = logging.getLogger(__name__)


class TrainerV2:
    """
    V2 Training Pipeline Orchestrator.
    Executes training, validation, calibration, and artifact generation in strict isolation.
    """

    def __init__(
        self,
        config: Optional[ExperimentConfig] = None,
        output_dir: Optional[Path] = None,
    ):
        self.config = config or ExperimentConfig(experiment_id="DEFAULT_V2")
        self.output_dir = output_dir or Path("artifacts/trainer_v2_output")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def prepare_dataset(
        self,
        raw_df: pd.DataFrame,
        ticker: str,
        feature_columns: List[str],
    ) -> pd.DataFrame:
        """
        Builds the canonical T+10 target and verifies complete feature columns.
        """
        target_cfg = TargetConfig(
            horizon=self.config.target_horizon,
            theta_min=self.config.theta_min,
            alpha=self.config.alpha,
        )

        df = build_t10_target_series(raw_df, ticker=ticker, config=target_cfg)

        # Drop rows where target is NaN (the last H sessions)
        df = df.dropna(subset=["target"] + feature_columns).copy()
        df["target"] = df["target"].astype(int)
        return df

    def train_ticker_fold(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_calib: Optional[np.ndarray] = None,
        y_calib: Optional[np.ndarray] = None,
        hyperparameters: Optional[Dict[str, Any]] = None,
    ) -> Tuple[XGBClassifier, Optional[MulticlassCalibrator]]:
        """
        Fits a single estimator using sample weighting and optional calibration.
        """
        params = hyperparameters or self.config.hyperparameters or {}
        default_xgb_params = {
            "objective": "multi:softprob",
            "num_class": 3,
            "eval_metric": "mlogloss",
            "random_state": self.config.random_seed,
            "n_jobs": 1,
            "n_estimators": 200,
            "max_depth": 4,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "min_child_weight": 8,
            "reg_lambda": 5.0,
            "reg_alpha": 1.0,
        }
        # Merge with user params (user params override defaults)
        fit_params = {**default_xgb_params, **params}
        # Filter non-XGBoost params
        xgb_keys = {
            "objective", "num_class", "eval_metric", "random_state", "n_jobs",
            "n_estimators", "max_depth", "learning_rate", "subsample",
            "colsample_bytree", "min_child_weight", "reg_lambda", "reg_alpha", "gamma"
        }
        clean_params = {k: v for k, v in fit_params.items() if k in xgb_keys}

        model = XGBClassifier(**clean_params)

        # Compute sample weights
        sample_weights = get_sample_weights(
            y_train,
            strategy=self.config.weighting_strategy,
            num_classes=3,
        )

        model.fit(X_train, y_train, sample_weight=sample_weights)

        # Calibrate if calibration method specified and calibration data provided
        calibrator = None
        if self.config.calibration_method != CalibrationMethod.NONE.value and X_calib is not None and len(X_calib) > 0:
            calibrator = get_calibrator(self.config.calibration_method)
            raw_calib_probs = model.predict_proba(X_calib)
            calibrator.fit(raw_calib_probs, y_calib)

        return model, calibrator

    def evaluate_walk_forward(
        self,
        df: pd.DataFrame,
        feature_columns: List[str],
        cv: Optional[PurgedWalkForwardCV] = None,
    ) -> Dict[str, Any]:
        """
        Executes expanding purged walk-forward validation and computes full metrics suite across folds.
        """
        cv_engine = cv or PurgedWalkForwardCV(
            n_splits=3,
            purge_sessions=10,
            calib_sessions=40,
            test_sessions=60,
            min_train_sessions=200,
        )

        fold_reports = []
        all_y_true = []
        all_y_pred = []
        all_probs = []

        X = df[feature_columns].values
        y = df["target"].values
        actual_returns = df["future_return"].values if "future_return" in df.columns else None

        for fold in cv_engine.split(df):
            X_tr, y_tr = X[fold.train_indices], y[fold.train_indices]
            X_ca = X[fold.calib_indices] if len(fold.calib_indices) else None
            y_ca = y[fold.calib_indices] if len(fold.calib_indices) else None
            X_te, y_te = X[fold.test_indices], y[fold.test_indices]

            model, calibrator = self.train_ticker_fold(
                X_tr, y_tr, X_calib=X_ca, y_calib=y_ca
            )

            raw_test_probs = model.predict_proba(X_te)
            if calibrator is not None:
                test_probs = calibrator.calibrate(raw_test_probs)
            else:
                test_probs = raw_test_probs

            test_preds = np.argmax(test_probs, axis=1)

            fold_metrics = evaluate_full_metrics_suite(y_te, test_preds, test_probs)
            fold_reports.append({
                "fold_id": fold.fold_id,
                "train_count": fold.train_count,
                "test_count": fold.test_count,
                "metrics": fold_metrics,
            })

            all_y_true.extend(y_te)
            all_y_pred.extend(test_preds)
            all_probs.extend(test_probs)

        # Aggregate metrics across all out-of-time test folds
        aggregate_metrics = evaluate_full_metrics_suite(
            np.array(all_y_true),
            np.array(all_y_pred),
            np.array(all_probs),
        )

        return {
            "folds": fold_reports,
            "aggregate": aggregate_metrics,
            "config_hash": compute_config_hash(self.config),
        }
