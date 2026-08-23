import os
import pytest
from unittest.mock import patch, MagicMock
from src.ml.evaluate_candidate import evaluate_ticker, select_current_candidate, generate_report

def test_evaluator_never_imports_promote_model():
    """Ensure the evaluator script does not import or call promote_model."""
    with open("src/ml/evaluate_candidate.py", "r") as f:
        content = f.read()
    assert "promote_model" not in content, "Evaluator MUST NEVER import or call promote_model()"

def test_missing_candidate_artifact(capsys):
    """Test when no candidate version exists."""
    with patch("src.ml.evaluate_candidate.select_current_candidate", return_value=None), \
         patch("src.ml.evaluate_candidate.os.path.exists", return_value=True), \
         patch("src.ml.evaluate_candidate.load_manifest", return_value={"model_version": "v1"}):
         
         result = evaluate_ticker(MagicMock(), "TEST_TICKER", None, None, "2025-08-07 00:00:00")
         assert result is None
         captured = capsys.readouterr()
         assert "No candidate version found" in captured.out

def test_feature_schema_incompatibility(capsys):
    """Test when feature pipeline hashes differ."""
    with patch("src.ml.evaluate_candidate.select_current_candidate", return_value="v_cand"), \
         patch("src.ml.evaluate_candidate.os.path.exists", return_value=True), \
         patch("src.ml.evaluate_candidate.load_manifest", side_effect=[
             {"model_version": "v_act", "feature_pipeline_hash": "hash1"},
             {"model_version": "v_cand", "feature_pipeline_hash": "hash2"}
         ]):
         
         result = evaluate_ticker(MagicMock(), "TEST_TICKER", None, None, "2025-08-07 00:00:00")
         assert result == "INELIGIBLE"
         captured = capsys.readouterr()
         assert "INELIGIBLE: Feature Pipeline Schema Mismatch" in captured.out

def test_generate_report_zero_actionable():
    """Test scenario with 0 actionable predictions."""
    active = {"model_version": "v1", "feature_pipeline_hash": "hash1"}
    cand = {"model_version": "v2", "feature_pipeline_hash": "hash1"}
    meta = {}
    
    am = {"actionable_precision": 0.5, "actionable_recall": 0.5, "actionable_count": 10, "class_distribution": {0:5, 1:0, 2:5}}
    cm = {"actionable_precision": 0.0, "actionable_recall": 0.0, "actionable_count": 0, "class_distribution": {0:0, 1:100, 2:0}}
    
    stats = {"total_rows": 100, "actionable_intersection": 0, "mcnemar_statistic": None, "mcnemar_pvalue": None}
    eco = {"active_return": 0.1, "candidate_return": 0.0}
    
    with patch("src.ml.evaluate_candidate.os.makedirs"):
        # Just writing report doesn't need to be blocked, but we patch open to prevent actual file writes
        with patch("builtins.open", MagicMock()):
            result = generate_report("TEST", active, cand, meta, am, cm, stats, eco, {})
            assert result == "FAIL", "Should fail if candidate has 0 actionable predictions"

def test_generate_report_ml_improve_economic_deteriorate():
    """Policy B: economic degradation MUST produce FAIL."""
    active = {"model_version": "v1", "feature_pipeline_hash": "hash1"}
    cand = {"model_version": "v2", "feature_pipeline_hash": "hash1"}
    meta = {}

    am = {
        "actionable_precision": 0.5,
        "actionable_recall": 0.5,
        "actionable_count": 10,
        "class_distribution": {}
    }
    cm = {
        "actionable_precision": 0.6,
        "actionable_recall": 0.6,
        "actionable_count": 10,
        "class_distribution": {}
    }

    stats = {
        "total_rows": 100,
        "actionable_intersection": 10,
        "mcnemar_statistic": 4.0,
        "mcnemar_pvalue": 0.04
    }

    eco = {
        "active_return": 0.1,
        "candidate_return": -0.05
    }

    with patch("builtins.open", MagicMock()):
        result = generate_report(
            "TEST", active, cand, meta, am, cm, stats, eco, {}
        )

    assert result == "FAIL"

def test_generate_report_precision_deteriorates_return_improves():
    """Policy B: precision degradation MUST produce FAIL."""
    active = {"model_version": "v1", "feature_pipeline_hash": "hash1"}
    cand = {"model_version": "v2", "feature_pipeline_hash": "hash1"}
    meta = {}

    am = {
        "actionable_precision": 0.60,
        "actionable_recall": 0.5,
        "actionable_count": 10,
        "class_distribution": {}
    }
    cm = {
        "actionable_precision": 0.55,
        "actionable_recall": 0.5,
        "actionable_count": 10,
        "class_distribution": {}
    }

    stats = {
        "total_rows": 100,
        "actionable_intersection": 10,
        "mcnemar_statistic": 4.0,
        "mcnemar_pvalue": 0.01
    }

    eco = {
        "active_return": 0.10,
        "candidate_return": 0.20
    }

    with patch("builtins.open", MagicMock()):
        result = generate_report(
            "TEST", active, cand, meta, am, cm, stats, eco, {}
        )

    assert result == "FAIL"

def test_generate_report_both_metrics_deteriorate():
    """Policy B: degradation of either mandatory metric MUST produce FAIL."""
    active = {"model_version": "v1", "feature_pipeline_hash": "hash1"}
    cand = {"model_version": "v2", "feature_pipeline_hash": "hash1"}
    meta = {}

    am = {
        "actionable_precision": 0.60,
        "actionable_recall": 0.5,
        "actionable_count": 10,
        "class_distribution": {}
    }
    cm = {
        "actionable_precision": 0.55,
        "actionable_recall": 0.5,
        "actionable_count": 10,
        "class_distribution": {}
    }

    stats = {
        "total_rows": 100,
        "actionable_intersection": 10,
        "mcnemar_statistic": 4.0,
        "mcnemar_pvalue": 0.01
    }

    eco = {
        "active_return": 0.10,
        "candidate_return": 0.05
    }

    with patch("builtins.open", MagicMock()):
        result = generate_report(
            "TEST", active, cand, meta, am, cm, stats, eco, {}
        )

    assert result == "FAIL"

def test_generate_report_equal_precision_and_improved_return_not_fail():
    """Policy B: equality in precision is not degradation."""
    active = {"model_version": "v1", "feature_pipeline_hash": "hash1"}
    cand = {"model_version": "v2", "feature_pipeline_hash": "hash1"}
    meta = {}

    am = {
        "actionable_precision": 0.60,
        "actionable_recall": 0.5,
        "actionable_count": 10,
        "class_distribution": {}
    }
    cm = {
        "actionable_precision": 0.60,
        "actionable_recall": 0.5,
        "actionable_count": 10,
        "class_distribution": {}
    }

    stats = {
        "total_rows": 100,
        "actionable_intersection": 10,
        "mcnemar_statistic": 1.0,
        "mcnemar_pvalue": 0.30
    }

    eco = {
        "active_return": 0.10,
        "candidate_return": 0.20
    }

    with patch("builtins.open", MagicMock()):
        result = generate_report(
            "TEST", active, cand, meta, am, cm, stats, eco, {}
        )

    assert result == "INCONCLUSIVE"

def test_generate_report_equal_return_and_improved_precision_not_fail():
    """Policy B: equality in return is not degradation."""
    active = {"model_version": "v1", "feature_pipeline_hash": "hash1"}
    cand = {"model_version": "v2", "feature_pipeline_hash": "hash1"}
    meta = {}

    am = {
        "actionable_precision": 0.60,
        "actionable_recall": 0.5,
        "actionable_count": 10,
        "class_distribution": {}
    }
    cm = {
        "actionable_precision": 0.70,
        "actionable_recall": 0.5,
        "actionable_count": 10,
        "class_distribution": {}
    }

    stats = {
        "total_rows": 100,
        "actionable_intersection": 10,
        "mcnemar_statistic": 1.0,
        "mcnemar_pvalue": 0.30
    }

    eco = {
        "active_return": 0.10,
        "candidate_return": 0.10
    }

    with patch("builtins.open", MagicMock()):
        result = generate_report(
            "TEST", active, cand, meta, am, cm, stats, eco, {}
        )

    assert result == "INCONCLUSIVE"

def test_generate_report_economic_improve_stat_inconclusive():
    active = {"model_version": "v1", "feature_pipeline_hash": "hash1"}
    cand = {"model_version": "v2", "feature_pipeline_hash": "hash1"}
    meta = {}
    
    am = {"actionable_precision": 0.5, "actionable_count": 10, "actionable_recall": 0.5, "class_distribution": {}}
    cm = {"actionable_precision": 0.55, "actionable_count": 10, "actionable_recall": 0.5, "class_distribution": {}}
    
    stats = {"total_rows": 100, "actionable_intersection": 10, "mcnemar_statistic": 1.0, "mcnemar_pvalue": 0.3} # Not significant
    eco = {"active_return": 0.1, "candidate_return": 0.2} # economic improvement
    
    with patch("builtins.open", MagicMock()):
        result = generate_report("TEST", active, cand, meta, am, cm, stats, eco, {})
        assert result == "INCONCLUSIVE", "Should be inconclusive if McNemar p > 0.05"
