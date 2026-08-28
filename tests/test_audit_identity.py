import os
import pytest
import hashlib
from unittest.mock import MagicMock, patch
from scripts.identity_audit_helper import audit_prediction_identities, _hash_file_sha256

def test_audit_prediction_identities_schema_correctness():
    # Setup mock database
    db = MagicMock()
    
    # 1. Healthy prediction (False Positive test)
    # prediction_history is missing model_hash and feature_hash, which is correct schema.
    p_healthy = {
        "symbol": "TICKER1",
        "model_version": "v123",
        "provenance_hash": "prov1"
    }
    
    # model_registry has actual hashes in metrics
    m_healthy = {
        "ticker": "TICKER1",
        "version": "v123",
        "metrics": {
            "model_hash": "mhash1",
            "feature_hash": "fhash1"
        }
    }
    
    # prediction_provenance has pipeline info
    prov_healthy = {
        "provenance_hash": "prov1",
        "model_version": "v123",
        "feature_pipeline_version": "v1",
        "feature_pipeline_hash": "canonical_pipe_hash"
    }

    # 2. Model version mismatch (True Negative test)
    p_bad_version = {
        "symbol": "TICKER2",
        "model_version": "v999", # Differs from registry
        "provenance_hash": "prov2"
    }
    
    m_bad_version = {
        "ticker": "TICKER2",
        "version": "v234"
    }
    
    # 3. Pipeline hash mismatch (True Negative test)
    p_bad_pipe = {
        "symbol": "TICKER3",
        "model_version": "v345",
        "provenance_hash": "prov3"
    }
    
    m_bad_pipe = {
        "ticker": "TICKER3",
        "version": "v345"
    }
    
    prov_bad_pipe = {
        "provenance_hash": "prov3",
        "model_version": "v345",
        "feature_pipeline_version": "v1",
        "feature_pipeline_hash": "wrong_pipe_hash"
    }

    # Mock DB collections
    def find_one_side_effect(query):
        h = query.get("provenance_hash")
        if h == "prov1":
            return prov_healthy
        elif h == "prov3":
            return prov_bad_pipe
        return None

    db.prediction_provenance.find_one.side_effect = find_one_side_effect
    
    preds = [p_healthy, p_bad_version, p_bad_pipe]
    active_map = {
        "TICKER1": m_healthy,
        "TICKER2": m_bad_version,
        "TICKER3": m_bad_pipe
    }

    # We need to mock os.path.exists and the physical hashing
    with patch("scripts.identity_audit_helper.os.path.exists", return_value=True):
        with patch("scripts.identity_audit_helper._hash_file_sha256") as mock_hash:
            # We mock the physical artifact hashing to return what the registry expects
            # for TICKER1 and TICKER3 so we don't trigger artifact hash mismatches.
            def hash_side_effect(filepath, truncate_to=None):
                if "model_TICKER1" in filepath: return "mhash1"
                if "features_TICKER1" in filepath: return "fhash1"
                return "dummy"
            mock_hash.side_effect = hash_side_effect
            
            with patch("scripts.identity_audit_helper.get_feature_pipeline_hash", return_value="canonical_pipe_hash"):
                results = audit_prediction_identities(db, preds, active_map, canonical_pipe_version="v1")
                
                # 1. False positive check
                # TICKER1 must not trigger a model_hash_mismatch or feature_hash_mismatch
                # just because prediction_history lacks the field.
                # TICKER2 is a version mismatch, so it shouldn't reach hash check
                # TICKER3 has physical hashes mock match.
                assert results["model_hash_mismatches"] == 0, "Regression: Missing hash field caused hash mismatch"
                assert results["feature_hash_mismatches"] == 0, "Regression: Missing hash field caused hash mismatch"
                
                # 2. True negative checks
                assert results["model_version_mismatches"] == 1 # TICKER2
                assert results["pipeline_hash_mismatches"] == 1 # TICKER3
