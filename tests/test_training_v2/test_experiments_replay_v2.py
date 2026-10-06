"""
Unit tests for V2 Experiment Registry, Challenger Replay & Evaluator.
"""

from pathlib import Path
import numpy as np
import pandas as pd
import pytest

from src.ml.training.provenance import (
    ExperimentConfig,
    compute_config_hash,
)
from src.ml.training.experiments import (
    ExperimentRegistry,
    get_experiment_config,
)
from src.ml.training.replay import (
    FrozenReplayDataset,
    ChallengerReplayEngine,
)
from src.ml.evaluate_challenger_v2 import (
    ChallengerEvaluatorV2,
)


def test_experiment_config_deterministic_hash():
    """Verifies that ExperimentConfig hashing is deterministic and captures changes."""
    c1 = ExperimentConfig(experiment_id="EXP-1A", target_horizon=10, theta_min=0.01)
    c2 = ExperimentConfig(experiment_id="EXP-1A", target_horizon=10, theta_min=0.01)
    c3 = ExperimentConfig(experiment_id="EXP-1A", target_horizon=10, theta_min=0.015)

    assert compute_config_hash(c1) == compute_config_hash(c2)
    assert compute_config_hash(c1) != compute_config_hash(c3)


def test_experiment_registry_official_experiments():
    """Verifies that all official experiments from EXP-1A through EXP-5 are defined in registry."""
    expected = [
        "EXP-1A", "EXP-1B-100", "EXP-1B-125", "EXP-1B-150",
        "EXP-2-INV", "EXP-2-SQRT",
        "EXP-3-SIGMOID", "EXP-3-TEMP",
        "EXP-4-REGIME", "EXP-4-ANTIDIP",
        "EXP-5-OPTUNA",
    ]
    all_exps = ExperimentRegistry.list_all()
    for e in expected:
        assert e in all_exps
        cfg = get_experiment_config(e)
        assert cfg.target_horizon == 10


def test_frozen_replay_dataset_load():
    """Verifies that the frozen replay dataset loads and passes integrity check."""
    p = Path("saved_evaluations/frozen_replay_1083.parquet")
    if not p.exists():
        pytest.skip("Frozen replay parquet file not yet generated in this environment")

    df = FrozenReplayDataset.load()
    assert len(df) == 1083
    assert "symbol" in df.columns
    assert "actual_class" in df.columns
    assert "incumbent_recommendation" in df.columns


class MockChallengerModel:
    """Mock model for testing replay engine and evaluator without training."""
    def predict_proba(self, X):
        n = len(X)
        # Emits mock balanced probability
        return np.tile([0.33, 0.34, 0.33], (n, 1))


def test_challenger_replay_engine_mock():
    """Verifies ChallengerReplayEngine on mock data."""
    # Create small synthetic replay dataframe
    df_eval = pd.DataFrame({
        "symbol": ["RELIANCE.NS", "TCS.NS"],
        "market_date": ["2026-09-01", "2026-09-01"],
        "actual_class": ["SELL", "HOLD"],
        "actual_return": [-0.03, 0.005],
        "incumbent_raw_prediction": ["BUY", "HOLD"],
        "incumbent_recommendation": ["BUY", "HOLD"],
        "incumbent_prob_sell": [0.2, 0.3],
        "incumbent_prob_hold": [0.2, 0.5],
        "incumbent_prob_buy": [0.6, 0.2],
        "feat_f1": [1.0, 2.0],
        "feat_f2": [0.5, 0.8],
    })

    models = {
        "RELIANCE.NS": MockChallengerModel(),
        "TCS.NS": MockChallengerModel(),
    }

    engine = ChallengerReplayEngine(models_dict=models)
    replayed = engine.replay(df_eval)

    assert "challenger_recommendation" in replayed.columns
    assert "challenger_correct" in replayed.columns
    assert len(replayed) == 2


def test_challenger_evaluator_v2_gates():
    """Verifies ChallengerEvaluatorV2 execution and promotion gate checks."""
    # Synthetic paired replay data
    df_paired = pd.DataFrame({
        "symbol": ["TICKER1"] * 10 + ["TICKER2"] * 10,
        "market_date": ["2026-09-01"] * 10 + ["2026-09-02"] * 10,
        "actual_class": ["SELL"] * 12 + ["HOLD"] * 6 + ["BUY"] * 2,
        "actual_return": [-0.03] * 12 + [0.005] * 6 + [0.04] * 2,
        "incumbent_recommendation": ["BUY"] * 10 + ["HOLD"] * 10,
        "challenger_recommendation": ["SELL"] * 10 + ["HOLD"] * 10,
        "incumbent_correct": [0] * 10 + [1] * 6 + [0] * 4,
        "challenger_correct": [1] * 10 + [1] * 6 + [0] * 4,
        "incumbent_prob_sell": [0.1] * 20,
        "incumbent_prob_hold": [0.4] * 20,
        "incumbent_prob_buy": [0.5] * 20,
        "challenger_prob_sell": [0.6] * 10 + [0.2] * 10,
        "challenger_prob_hold": [0.3] * 10 + [0.7] * 10,
        "challenger_prob_buy": [0.1] * 10 + [0.1] * 10,
    })

    evaluator = ChallengerEvaluatorV2(output_dir=Path("saved_evaluations"))
    report = evaluator.evaluate_paired_replay(df_paired, experiment_id="MOCK_TEST_V2")

    assert "gates" in report
    assert "verdict" in report
    assert "mcnemar_test" in report
    assert "clustered_bootstrap" in report
    assert report["total_evaluated_records"] == 20
