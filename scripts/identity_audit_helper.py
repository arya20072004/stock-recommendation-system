import os
import hashlib
from src.features.router import get_feature_pipeline_hash

def _hash_file_sha256(filepath, truncate_to=None):
    if not os.path.exists(filepath):
        return None
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    res = sha256.hexdigest()
    if truncate_to:
        res = res[:truncate_to]
    return res

def audit_prediction_identities(db, preds, active_map, canonical_pipe_version="v1", models_dir="saved_models", features_dir="saved_features"):
    """
    Validates prediction provenance against active models without expecting hashes
    in the prediction_history collection (schema-correct).
    Also performs physical artifact verification.
    """
    results = {
        "matching_preds": 0,
        "model_version_mismatches": 0,
        "model_hash_mismatches": 0,
        "feature_hash_mismatches": 0,
        "pipeline_version_mismatches": 0,
        "pipeline_hash_mismatches": 0,
        "prov_issues": 0,
        "malformed_prov": 0,
        "old_pipe_refs": 0,
        "unexpected_pipe_refs": 0,
    }
    
    canonical_pipe_hash = get_feature_pipeline_hash(canonical_pipe_version)
    old_pipeline_hash = "685cb3dbe63d7923126e44c597914c93a7bcebc83c6f6e42017dd1101f7d2c68"
    
    for p in preds:
        ticker = p.get("symbol")
        m = active_map.get(ticker)
        
        if not m:
            continue
            
        # Prediction-to-model linkage via model_version
        p_version = p.get("model_version")
        m_version = m.get("version")
        
        if not p_version or p_version != m_version:
            results["model_version_mismatches"] += 1
            continue
            
        prov_hash = p.get("provenance_hash")
        if not prov_hash:
            results["prov_issues"] += 1
            continue
            
        prov = db.prediction_provenance.find_one({
            "provenance_hash": prov_hash
        })
        
        if not prov:
            results["prov_issues"] += 1
            continue
            
        # Verify provenance points to the correct version
        if prov.get("model_version") != m_version:
            results["malformed_prov"] += 1
            
        # Feature pipeline identity
        prov_pipe_ver = prov.get("feature_pipeline_version")
        prov_pipe_hash = prov.get("feature_pipeline_hash")
        
        if prov_pipe_ver != canonical_pipe_version:
            results["pipeline_version_mismatches"] += 1
            
        if prov_pipe_hash != canonical_pipe_hash:
            results["pipeline_hash_mismatches"] += 1
            if prov_pipe_hash == old_pipeline_hash:
                results["old_pipe_refs"] += 1
            else:
                results["unexpected_pipe_refs"] += 1
                
        # Physical artifact verification against authoritative registry metrics
        reg_metrics = m.get("metrics", {})
        # Note: sometimes they are also at root level in registry, but metrics is safer
        auth_model_hash = reg_metrics.get("model_hash") or m.get("model_hash")
        auth_feat_hash = reg_metrics.get("feature_hash") or m.get("feature_hash")
        
        model_path = os.path.join(models_dir, f"model_{ticker}_{m_version}.joblib")
        feat_path = os.path.join(features_dir, f"features_{ticker}_{m_version}.json")
        
        actual_model_hash = _hash_file_sha256(model_path, truncate_to=12)
        actual_feat_hash = _hash_file_sha256(feat_path, truncate_to=64)
        
        if auth_model_hash and actual_model_hash != auth_model_hash:
            results["model_hash_mismatches"] += 1
            
        if auth_feat_hash and actual_feat_hash != auth_feat_hash:
            results["feature_hash_mismatches"] += 1
                
        results["matching_preds"] += 1
        
    return results
