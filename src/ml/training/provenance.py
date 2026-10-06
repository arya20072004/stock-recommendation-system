"""
src.ml.training.provenance
Experiment Metadata, Deterministic Hashing & Provenance Tracking for V2 Architecture.
"""

from dataclasses import dataclass, field, asdict
import hashlib
import json
from typing import Dict, Any, Optional
from datetime import datetime, timezone


@dataclass(frozen=True)
class ExperimentConfig:
    """
    Immutable specification of a single V2 training experiment.
    All variables must be explicitly defined to ensure 100% reproducibility.
    """
    experiment_id: str
    experiment_version: str = "v2.0"
    description: str = ""
    target_horizon: int = 10
    theta_min: float = 0.0100
    alpha: float = 1.0
    weighting_strategy: str = "inverse_frequency"
    calibration_method: str = "platt_sigmoid"
    feature_variant: str = "canonical_v1"
    hyperparameters: Dict[str, Any] = field(default_factory=dict)
    random_seed: int = 42
    dataset_version: str = "v1"

    def __post_init__(self):
        if self.target_horizon != 10:
            raise ValueError(f"V2 architecture locked to target_horizon=10, got {self.target_horizon}")
        if self.theta_min <= 0.0:
            raise ValueError(f"theta_min must be positive, got {self.theta_min}")
        if self.alpha <= 0.0:
            raise ValueError(f"alpha must be positive, got {self.alpha}")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExperimentConfig":
        return cls(**data)


def compute_config_hash(config: ExperimentConfig) -> str:
    """
    Computes deterministic SHA-256 hash of experiment configuration.
    """
    cfg_dict = config.to_dict()
    serialized = json.dumps(cfg_dict, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


@dataclass
class ExperimentProvenance:
    """
    Execution provenance container recording results and artifact hashes.
    """
    config: ExperimentConfig
    config_hash: str
    git_commit: str
    executed_at: str
    dataset_hash: Optional[str] = None
    results: Dict[str, Any] = field(default_factory=dict)
    status: str = "INITIALIZED"  # INITIALIZED, RUNNING, COMPLETED, FAILED

    @classmethod
    def create(
        cls,
        config: ExperimentConfig,
        git_commit: str = "HEAD",
        dataset_hash: Optional[str] = None,
    ) -> "ExperimentProvenance":
        return cls(
            config=config,
            config_hash=compute_config_hash(config),
            git_commit=git_commit,
            executed_at=datetime.now(timezone.utc).isoformat(),
            dataset_hash=dataset_hash,
            status="INITIALIZED",
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "config": self.config.to_dict(),
            "config_hash": self.config_hash,
            "git_commit": self.git_commit,
            "executed_at": self.executed_at,
            "dataset_hash": self.dataset_hash,
            "results": self.results,
            "status": self.status,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)
