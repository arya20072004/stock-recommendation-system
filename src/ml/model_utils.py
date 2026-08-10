import json
import hashlib
import logging
import os
import numpy as np

logger = logging.getLogger(__name__)

MODELS_DIR = "saved_models"

def get_model_version(ticker: str) -> str:
    """
    Generate a deterministic model version using SHA-256.

    Preferred artifact:
        saved_models/model_<ticker>.ubj

    Fallback:
        saved_models/model_<ticker>.joblib

    The first 12 hexadecimal characters are stored as the version.
    """
    ubj_path = os.path.join(MODELS_DIR, f"model_{ticker}.ubj")
    joblib_path = os.path.join(MODELS_DIR, f"model_{ticker}.joblib")

    if os.path.exists(ubj_path):
        model_path = ubj_path
    elif os.path.exists(joblib_path):
        model_path = joblib_path
    else:
        logger.error("No model artifact found for %s", ticker)
        return "unknown"

    sha256 = hashlib.sha256()

    try:
        with open(model_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()[:12]
    except Exception as exc:
        logger.exception("Error hashing model for %s: %s", ticker, exc)
        return "error"

def _normalize_value(val):
    if isinstance(val, (np.integer, int)):
        return int(val)
    elif isinstance(val, (np.floating, float)):
        if np.isnan(val):
            return None
        if np.isinf(val):
            return "Infinity" if val > 0 else "-Infinity"
        return float(val)
    elif isinstance(val, dict):
        return {str(k): _normalize_value(v) for k, v in val.items()}
    elif isinstance(val, (list, tuple, np.ndarray)):
        return [_normalize_value(v) for v in val]
    return val

def compute_provenance_hash(payload: dict) -> str:
    """
    Computes a deterministic SHA-256 hash for a provenance payload.
    Ensures stable sorting, explicit numpy/float conversion, and handles NaNs.
    """
    normalized = _normalize_value(payload)
    canonical_json = json.dumps(
        normalized,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=True
    )
    return hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()

def reconstruct_canonical_payload(doc: dict) -> dict:
    """
    Extracts the canonical fields for schema-aware hashing verification.
    Filters out database metadata (_id, created_at, provenance_hash).
    """
    schema = doc.get("provenance_schema_version", "v1")

    canonical = {}

    # Base fields present in v1/v2/legacy
    base_keys = [
        "symbol", "market_date", "prediction_horizon", "model_version",
        "feature_pipeline_version", "feature_pipeline_hash", "feature_columns",
        "raw_inputs", "features", "model_probabilities", "decision_thresholds",
        "confidence_metrics"
    ]
    for k in base_keys:
        if k in doc:
            canonical[k] = doc[k]

    if schema in ["v1", "v2"]:
        if "provenance_schema_version" in doc:
            canonical["provenance_schema_version"] = schema
        return canonical

    if schema == "v3":
        canonical["provenance_schema_version"] = "v3"
        v3_keys = [
            "recommendation", "confidence_tier", "target_return_threshold",
            "class_mapping", "confidence_tier_boundaries", "decision_context"
        ]
        for k in v3_keys:
            if k in doc:
                canonical[k] = doc[k]
        return canonical

    return canonical

def reconstruct_settlement_payload(record: dict) -> dict:
    """
    Extracts the canonical fields for Phase 19 settlement verification.
    """
    payload = {}
    keys = [
        "provenance_hash", "settlement_market_date", "actual_price",
        "actual_return", "actual_class", "recommendation_correct",
        "raw_prediction_correct"
    ]
    for k in keys:
        if k in record:
            payload[k] = record[k]
    return payload

def compute_settlement_hash(payload: dict) -> str:
    """
    Computes a deterministic SHA-256 hash for a settlement payload.
    Uses the same normalization as prediction provenance.
    """
    normalized = _normalize_value(payload)
    canonical_json = json.dumps(
        normalized,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=True
    )
    return hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()
