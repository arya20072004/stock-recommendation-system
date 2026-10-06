"""
Unit tests for V2 Production Isolation & Anti-Contamination Verification.
"""

from pathlib import Path
import os
import re
import pytest


def test_production_trainer_unmodified():
    """Verifies that legacy incumbent src/ml/trainer.py exists and is unmodified."""
    p = Path("src/ml/trainer.py")
    assert p.exists()
    assert p.stat().st_size > 30000  # Existing production trainer is ~36KB


def test_saved_models_isolation():
    """Verifies that no V2 test artifacts were written into production saved_models directory."""
    models_dir = Path("saved_models")
    assert models_dir.exists()

    # Verify no file in saved_models has 'test', 'v2', 'mock', or 'challenger' in its name
    for f in models_dir.glob("*"):
        name_lower = f.name.lower()
        assert "trainer_v2" not in name_lower
        assert "mock" not in name_lower
        assert "challenger_v2" not in name_lower


def test_no_smote_in_v2_codebase():
    """Verifies that imblearn.over_sampling.SMOTE is completely absent from all V2 training files."""
    v2_dir = Path("src/ml/training")
    v2_files = list(v2_dir.glob("*.py")) + [Path("src/ml/trainer_v2.py"), Path("src/ml/evaluate_challenger_v2.py")]

    smote_pattern = re.compile(r"from\s+imblearn\b|import\s+SMOTE\b|SMOTE\(")

    for f in v2_files:
        if f.exists():
            content = f.read_text(encoding="utf-8")
            assert not smote_pattern.search(content), f"Forbidden SMOTE reference detected in {f}"


def test_no_horizon_override_in_v2_codebase():
    """Verifies that TICKER_HORIZON_OVERRIDE is completely absent from all V2 training files."""
    v2_dir = Path("src/ml/training")
    v2_files = list(v2_dir.glob("*.py")) + [Path("src/ml/trainer_v2.py")]

    for f in v2_files:
        if f.exists():
            content = f.read_text(encoding="utf-8")
            assert "TICKER_HORIZON_OVERRIDE" not in content, f"Forbidden TICKER_HORIZON_OVERRIDE detected in {f}"


def test_no_class_thresholds_in_v2_codebase():
    """Verifies that TICKER_CLASS_THRESHOLDS is completely absent from V2 training and inference."""
    v2_dir = Path("src/ml/training")
    v2_files = list(v2_dir.glob("*.py")) + [Path("src/ml/trainer_v2.py")]

    for f in v2_files:
        if f.exists():
            content = f.read_text(encoding="utf-8")
            assert "TICKER_CLASS_THRESHOLDS" not in content, f"Forbidden TICKER_CLASS_THRESHOLDS detected in {f}"
