"""
src.ml.training
V2 Training Architecture & Experiment Framework.
Completely isolated from legacy incumbent trainer.py.
"""

from .target import (
    compute_t10_return,
    compute_dynamic_hurdle,
    assign_three_class_labels,
    build_t10_target_series,
    TargetConfig,
)
from .weighting import (
    compute_inverse_frequency_weights,
    compute_sqrt_inverse_frequency_weights,
    WeightingStrategy,
)
from .validation import (
    PurgedWalkForwardCV,
    WalkForwardFold,
)
from .calibration import (
    MulticlassCalibrator,
    PlattSigmoidCalibrator,
    TemperatureScalingCalibrator,
    CalibrationMethod,
)
from .metrics import (
    compute_classification_metrics,
    compute_probability_metrics,
    compute_economic_metrics,
    compute_stability_metrics,
    evaluate_full_metrics_suite,
    paired_clustered_bootstrap,
)
from .provenance import (
    ExperimentConfig,
    ExperimentProvenance,
    compute_config_hash,
)
from .experiments import (
    ExperimentRegistry,
    get_experiment_config,
)
from .replay import (
    FrozenReplayDataset,
    ChallengerReplayEngine,
)

__all__ = [
    "compute_t10_return",
    "compute_dynamic_hurdle",
    "assign_three_class_labels",
    "build_t10_target_series",
    "TargetConfig",
    "compute_inverse_frequency_weights",
    "compute_sqrt_inverse_frequency_weights",
    "WeightingStrategy",
    "PurgedWalkForwardCV",
    "WalkForwardFold",
    "MulticlassCalibrator",
    "PlattSigmoidCalibrator",
    "TemperatureScalingCalibrator",
    "CalibrationMethod",
    "compute_classification_metrics",
    "compute_probability_metrics",
    "compute_economic_metrics",
    "compute_stability_metrics",
    "evaluate_full_metrics_suite",
    "paired_clustered_bootstrap",
    "ExperimentConfig",
    "ExperimentProvenance",
    "compute_config_hash",
    "ExperimentRegistry",
    "get_experiment_config",
    "FrozenReplayDataset",
    "ChallengerReplayEngine",
]
