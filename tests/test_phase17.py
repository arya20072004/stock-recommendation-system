import pytest
import numpy as np
import mongomock
import pymongo
from datetime import datetime, date, timezone
from src.ml.model_utils import compute_provenance_hash
from src.ml.confidence import compute_confidence_tier

# ------------------------------------------------------------------
# Group D - Hash Integrity
# ------------------------------------------------------------------

def test_group_d_hash_integrity():
    payload = {
        "provenance_schema_version": "v2",
        "symbol": "RELIANCE.NS",
        "market_date": "2026-08-01",
        "prediction_horizon": 10,
        "model_version": "v1",
        "feature_pipeline_version": "v1",
        "feature_pipeline_hash": "hash",
        "feature_columns": ["f1", "f2"],
        "raw_inputs": {"close": 100.5},
        "features": {"f1": 1.5, "f2": 2},
        "model_probabilities": [0.1, 0.2, 0.7],
        "decision_thresholds": {"0": 0.3, "1": 0.3, "2": 0.4},
        "confidence_metrics": {"f1_macro": 0.5, "max_proba": 0.7, "top2_margin": 0.5}
    }
    hash1 = compute_provenance_hash(payload)
    # Probability mutation
    p2 = payload.copy()
    p2["model_probabilities"] = [0.1, 0.21, 0.69]
    assert compute_provenance_hash(p2) != hash1
    # Threshold mutation
    p3 = payload.copy()
    p3["decision_thresholds"] = {"0": 0.3, "1": 0.3, "2": 0.41}
    assert compute_provenance_hash(p3) != hash1
    # Metric mutation
    p4 = payload.copy()
    p4["confidence_metrics"] = {"f1_macro": 0.6, "max_proba": 0.7, "top2_margin": 0.5}
    assert compute_provenance_hash(p4) != hash1
    # Dictionary ordering
    p5 = payload.copy()
    p5["decision_thresholds"] = {"2": 0.4, "0": 0.3, "1": 0.3}
    assert compute_provenance_hash(p5) == hash1

# ------------------------------------------------------------------
# Group C - Confidence Provenance
# ------------------------------------------------------------------

def test_group_c_mutable_metrics_file_independence():
    # Calling compute_confidence_tier directly with f1_macro.
    # It must NOT try to read any file. If it does, and MODELS_DIR doesn't have the file,
    # or the file is invalid, it shouldn't matter because we pass it explicitly.
    confidence = compute_confidence_tier(
        ticker="RELIANCE.NS",
        max_proba=0.7,
        top2_margin=0.5,
        f1_macro=0.45
    )
    assert confidence["f1_macro"] == 0.45
    assert confidence["actionable"] == True

# ------------------------------------------------------------------
# Group H - Legacy Compatibility
# ------------------------------------------------------------------

def test_group_h_legacy_compatibility():
    legacy_payload = {
        "symbol": "RELIANCE.NS",
        "market_date": "2026-08-01",
        "prediction_horizon": 10,
        "model_version": "v1",
        "feature_pipeline_version": "v1",
        "feature_pipeline_hash": "hash",
        "feature_columns": ["f1", "f2"],
        "raw_inputs": {"close": 100.5},
        "features": {"f1": 1.5, "f2": 2}
    }
    # This must compute successfully and not fail due to missing schema_version or decision keys
    legacy_hash = compute_provenance_hash(legacy_payload)
    assert isinstance(legacy_hash, str)
    assert len(legacy_hash) == 64

# ------------------------------------------------------------------
# Group I - Full Reconstruction
# ------------------------------------------------------------------

def test_group_i_full_reconstruction():
    # Given a persisted provenance document:
    persisted_provenance = {
        "provenance_schema_version": "v2",
        "model_probabilities": [0.1, 0.2, 0.7],  # Class 0, 1, 2 (SELL, HOLD, BUY)
        "decision_thresholds": {"0": 0.3, "1": 0.3, "2": 0.4},
        "confidence_metrics": {"f1_macro": 0.45, "max_proba": 0.7, "top2_margin": 0.5}
    }
    # 1. Reconstruct prediction class
    proba = persisted_provenance["model_probabilities"]
    thresholds = persisted_provenance["decision_thresholds"]
    # Apply threshold calibration exactly as history.py does
    scaled_probs = []
    for idx, p in enumerate(proba):
        threshold = thresholds.get(str(idx), 1.0)
        scaled_probs.append(p / threshold if threshold > 0 else 0)
    predicted_class_idx = int(np.argmax(scaled_probs))
    assert predicted_class_idx == 2  # BUY
    predicted_class = ["SELL", "HOLD", "BUY"][predicted_class_idx]
    assert predicted_class == "BUY"
    # 2. Reconstruct confidence
    metrics = persisted_provenance["confidence_metrics"]
    confidence = compute_confidence_tier(
        ticker="ANY",
        max_proba=metrics["max_proba"],
        top2_margin=metrics["top2_margin"],
        f1_macro=metrics["f1_macro"]
    )
    assert confidence["actionable"] is True
    assert confidence["tier_rank"] >= 2