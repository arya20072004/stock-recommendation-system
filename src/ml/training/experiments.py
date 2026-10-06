"""
src.ml.training.experiments
Sequential Experiment Registry & Configuration Management for V2 Architecture.
Covers EXP-1A through EXP-5 with strict lineage tracking.
"""

from typing import Dict, List, Optional
from .provenance import ExperimentConfig


class ExperimentRegistry:
    """
    Authoritative registry of official V2 controlled experiments.
    Prevents confounding by establishing strict single-variable experimental configurations.
    """

    _EXPERIMENTS: Dict[str, ExperimentConfig] = {
        # EXP-1A: Canonical Horizon Unification
        "EXP-1A": ExperimentConfig(
            experiment_id="EXP-1A",
            description="Verify universal H=10 horizon across all 51 tickers under baseline settings.",
            target_horizon=10,
            theta_min=0.0100,
            alpha=1.0,
            weighting_strategy="uniform",
            calibration_method="none",
            feature_variant="canonical_v1",
        ),

        # EXP-1B: Target Threshold Sensitivity (1.00%, 1.25%, 1.50%)
        "EXP-1B-100": ExperimentConfig(
            experiment_id="EXP-1B-100",
            description="Threshold sensitivity floor at theta_min = 1.00% (Baseline).",
            target_horizon=10,
            theta_min=0.0100,
            alpha=1.0,
            weighting_strategy="uniform",
            calibration_method="none",
            feature_variant="canonical_v1",
        ),
        "EXP-1B-125": ExperimentConfig(
            experiment_id="EXP-1B-125",
            description="Threshold sensitivity floor at theta_min = 1.25% (Moderate friction).",
            target_horizon=10,
            theta_min=0.0125,
            alpha=1.0,
            weighting_strategy="uniform",
            calibration_method="none",
            feature_variant="canonical_v1",
        ),
        "EXP-1B-150": ExperimentConfig(
            experiment_id="EXP-1B-150",
            description="Threshold sensitivity floor at theta_min = 1.50% (High institutional friction).",
            target_horizon=10,
            theta_min=0.0150,
            alpha=1.0,
            weighting_strategy="uniform",
            calibration_method="none",
            feature_variant="canonical_v1",
        ),

        # EXP-2: Class Weighting Strategies (Zero SMOTE)
        "EXP-2-INV": ExperimentConfig(
            experiment_id="EXP-2-INV",
            description="Normalized exact inverse-frequency sample weighting.",
            target_horizon=10,
            theta_min=0.0100,
            alpha=1.0,
            weighting_strategy="inverse_frequency",
            calibration_method="none",
            feature_variant="canonical_v1",
        ),
        "EXP-2-SQRT": ExperimentConfig(
            experiment_id="EXP-2-SQRT",
            description="Square-root smoothed inverse-frequency sample weighting.",
            target_horizon=10,
            theta_min=0.0100,
            alpha=1.0,
            weighting_strategy="sqrt_inverse_frequency",
            calibration_method="none",
            feature_variant="canonical_v1",
        ),

        # EXP-3: Post-Hoc Probability Calibration
        "EXP-3-SIGMOID": ExperimentConfig(
            experiment_id="EXP-3-SIGMOID",
            description="One-vs-Rest Platt Sigmoid calibration with vector normalization.",
            target_horizon=10,
            theta_min=0.0100,
            alpha=1.0,
            weighting_strategy="inverse_frequency",
            calibration_method="platt_sigmoid",
            feature_variant="canonical_v1",
        ),
        "EXP-3-TEMP": ExperimentConfig(
            experiment_id="EXP-3-TEMP",
            description="Multiclass Temperature Scaling calibration.",
            target_horizon=10,
            theta_min=0.0100,
            alpha=1.0,
            weighting_strategy="inverse_frequency",
            calibration_method="temperature_scaling",
            feature_variant="canonical_v1",
        ),

        # EXP-4: Macro Regime & Anti-Dip Features
        "EXP-4-REGIME": ExperimentConfig(
            experiment_id="EXP-4-REGIME",
            description="Addition of macro regime signals (nifty_trend_ratio, nifty_structural_ratio, vix_regime).",
            target_horizon=10,
            theta_min=0.0100,
            alpha=1.0,
            weighting_strategy="inverse_frequency",
            calibration_method="platt_sigmoid",
            feature_variant="macro_regime_v2",
        ),
        "EXP-4-ANTIDIP": ExperimentConfig(
            experiment_id="EXP-4-ANTIDIP",
            description="Macro regime + detrended anti-dip-buying interaction signals.",
            target_horizon=10,
            theta_min=0.0100,
            alpha=1.0,
            weighting_strategy="inverse_frequency",
            calibration_method="platt_sigmoid",
            feature_variant="anti_dip_v2",
        ),

        # EXP-5: Hyperparameter Regularization Search
        "EXP-5-OPTUNA": ExperimentConfig(
            experiment_id="EXP-5-OPTUNA",
            description="Regularized Optuna search with max_depth in [3,6] and min_child_weight in [5,15].",
            target_horizon=10,
            theta_min=0.0100,
            alpha=1.0,
            weighting_strategy="inverse_frequency",
            calibration_method="platt_sigmoid",
            feature_variant="canonical_v1",
            hyperparameters={
                "max_depth_bounds": [3, 6],
                "min_child_weight_bounds": [5, 15],
                "learning_rate_bounds": [0.01, 0.2],
                "n_trials": 50,
            },
        ),
    }

    @classmethod
    def get(cls, experiment_id: str) -> ExperimentConfig:
        if experiment_id not in cls._EXPERIMENTS:
            raise KeyError(f"Unknown experiment ID: {experiment_id}. Available: {list(cls._EXPERIMENTS.keys())}")
        return cls._EXPERIMENTS[experiment_id]

    @classmethod
    def list_all(cls) -> List[str]:
        return list(cls._EXPERIMENTS.keys())


def get_experiment_config(experiment_id: str) -> ExperimentConfig:
    return ExperimentRegistry.get(experiment_id)
