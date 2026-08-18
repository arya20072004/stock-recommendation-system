import os
import sys
import json
import hashlib
from datetime import datetime
from pprint import pprint

# 1. Expected Tickers
sys.path.append('src')
try:
    from data.nifty50 import TICKERS
except ImportError:
    TICKERS = []

MODELS_DIR = "saved_models"
FEATURES_DIR = "saved_features"

def hash_file_sha256(filepath: str, truncate_to: int = 64) -> str:
    sha256 = hashlib.sha256()
    if not os.path.exists(filepath): return ""
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()[:truncate_to]
    except Exception as exc:
        return ""

def load_json(filepath):
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except:
        return None

def mtime(filepath):
    return os.path.getmtime(filepath) if os.path.exists(filepath) else 0

def format_time(ts):
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

out = {}

# Find latest training run for each ticker by looking at metrics_*.json
# Assume latest by modification time.
latest_versions = {}

for f in os.listdir(MODELS_DIR):
    if f.startswith('metrics_') and f.endswith('.json'):
        parts = f.replace('metrics_', '').replace('.json', '').split('_')
        if len(parts) == 2:
            ticker, version = parts[0], parts[1]
            t = mtime(os.path.join(MODELS_DIR, f))
            if ticker not in latest_versions or t > latest_versions[ticker]['mtime']:
                latest_versions[ticker] = {'version': version, 'mtime': t}

# Now for each expected ticker, gather all details
report_data = []

for ticker in TICKERS:
    # 1. New Candidate Info
    new_version = latest_versions.get(ticker, {}).get('version')
    new_mtime = latest_versions.get(ticker, {}).get('mtime', 0)
    
    # Check if newly trained candidate exists
    model_path = os.path.join(MODELS_DIR, f"model_{ticker}_{new_version}.joblib") if new_version else ""
    metrics_path = os.path.join(MODELS_DIR, f"metrics_{ticker}_{new_version}.json") if new_version else ""
    features_path = os.path.join(FEATURES_DIR, f"features_{ticker}_{new_version}.json") if new_version else ""
    
    model_exists = os.path.exists(model_path)
    metrics_exists = os.path.exists(metrics_path)
    features_exists = os.path.exists(features_path)
    
    metrics_data = load_json(metrics_path) if metrics_exists else {}
    features_data = load_json(features_path) if features_exists else []
    
    # Hashes
    actual_model_hash = hash_file_sha256(model_path, 12) if model_exists else ""
    actual_feature_hash = hash_file_sha256(features_path, 64) if features_exists else ""
    
    expected_feature_hash = metrics_data.get('feature_hash')
    expected_pipeline_hash = metrics_data.get('feature_pipeline_hash')
    
    feature_hash_match = (actual_feature_hash == expected_feature_hash) if expected_feature_hash else False
    
    # Active manifest info
    active_manifest_path = os.path.join(MODELS_DIR, f"{ticker}_active.json")
    active_manifest = load_json(active_manifest_path)
    
    active_version = active_manifest.get('model_version') if active_manifest else None
    
    # Consistency
    is_consistent = model_exists and features_exists and metrics_exists and feature_hash_match
    
    # Stale files
    all_ticker_files = [f for f in os.listdir(MODELS_DIR) if ticker in f] + [f for f in os.listdir(FEATURES_DIR) if ticker in f]
    stale_files = []
    for f in all_ticker_files:
        # Check if it belongs to new_version or active_version
        if new_version and new_version in f: continue
        if active_version and active_version in f: continue
        if "active.json" in f: continue
        stale_files.append(f)
        
    row = {
        'ticker': ticker,
        'new_version': new_version,
        'new_mtime': format_time(new_mtime) if new_mtime else "",
        'model_exists': model_exists,
        'features_exists': features_exists,
        'metrics_exists': metrics_exists,
        'feature_count': len(features_data) if features_exists else 0,
        'f1_macro': metrics_data.get('f1_macro', 0.0) if metrics_data else 0.0,
        'dataset_date_end': metrics_data.get('dataset_date_end'),
        'feature_pipeline_hash': expected_pipeline_hash,
        'active_manifest_exists': bool(active_manifest),
        'active_version': active_version,
        'candidate_is_active': (new_version == active_version) if (new_version and active_version) else False,
        'hash_consistent': feature_hash_match,
        'stale_files': stale_files
    }
    report_data.append(row)

with open("scratch/audit_report_data.json", "w") as f:
    json.dump(report_data, f, indent=2)

print("Audit data generated.")
