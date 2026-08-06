import os
import json
import hashlib
from datetime import datetime
import pymongo
from dotenv import load_dotenv
import subprocess
import sys

def hash_file(filepath):
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        buf = f.read(65536)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(65536)
    return hasher.hexdigest()

def snapshot_dir(directory):
    snapshot = {}
    if not os.path.exists(directory):
        return snapshot
    for root, _, files in os.walk(directory):
        for file in files:
            filepath = os.path.join(root, file)
            rel_path = os.path.relpath(filepath, directory)
            # Normalize path for cross-platform stability
            rel_path = rel_path.replace("\\", "/")
            snapshot[rel_path] = {
                "file_size": os.path.getsize(filepath),
                "sha256": hash_file(filepath)
            }
    return snapshot

def snapshot_prediction_history(db):
    docs = list(db.prediction_history.find(
        {},
        {"symbol": 1, "market_date": 1, "prediction_horizon": 1, "model_version": 1, 
         "recommendation": 1, "raw_prediction": 1, "confidence": 1}
    ))
    
    # Normalize datetimes and floats
    for doc in docs:
        if "_id" in doc:
            del doc["_id"]
        if "market_date" in doc and hasattr(doc["market_date"], "isoformat"):
            doc["market_date"] = doc["market_date"].isoformat()
        if "raw_prediction" in doc:
            if isinstance(doc["raw_prediction"], list):
                doc["raw_prediction"] = [round(float(x), 8) for x in doc["raw_prediction"]]
            elif isinstance(doc["raw_prediction"], (float, int)):
                doc["raw_prediction"] = round(float(doc["raw_prediction"]), 8)
            # if it's a string, just leave it

        if "confidence" in doc and doc["confidence"] is not None:
            try:
                doc["confidence"] = round(float(doc["confidence"]), 8)
            except (ValueError, TypeError):
                pass

    # Sort deterministically
    docs.sort(key=lambda x: (x.get("symbol", ""), x.get("market_date", "")))
    
    # Serialize stable JSON
    stable_json = json.dumps(docs, sort_keys=True, separators=(',', ':'))
    fingerprint = hashlib.sha256(stable_json.encode('utf-8')).hexdigest()
    
    # Collect metadata
    all_dates = [d["market_date"] for d in docs if "market_date" in d]
    all_timestamps = [doc.get("prediction_timestamp", "") for doc in list(db.prediction_history.find({}, {"prediction_timestamp": 1})) if hasattr(doc.get("prediction_timestamp", ""), "isoformat")]
    
    return {
        "document_count": len(docs),
        "fingerprint": fingerprint,
        "min_market_date": min(all_dates) if all_dates else None,
        "max_market_date": max(all_dates) if all_dates else None,
        "newest_prediction_timestamp": max(all_timestamps).isoformat() if all_timestamps and hasattr(max(all_timestamps), "isoformat") else None
    }

def get_git_info():
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
        dirty = subprocess.check_output(["git", "status", "--porcelain"]).decode().strip()
        return {
            "commit": commit,
            "dirty": bool(dirty),
            "modified_files": dirty.split("\n") if dirty else []
        }
    except:
        return {"commit": "unknown", "dirty": False, "modified_files": []}

def main():
    load_dotenv()
    output_dir = "artifacts/model_ab/baseline/"
    os.makedirs(output_dir, exist_ok=True)
    
    print("Snapshotting models...")
    models_snap = snapshot_dir("saved_models")
    print("Snapshotting features...")
    features_snap = snapshot_dir("saved_features")
    
    with open(os.path.join(output_dir, "baseline_hashes.json"), "w") as f:
        json.dump({"saved_models": models_snap, "saved_features": features_snap}, f, indent=4)
        
    # Baseline fleet (to verify what was there)
    with open(os.path.join(output_dir, "baseline_fleet.json"), "w") as f:
        json.dump({
            "models_count": len(models_snap),
            "features_count": len(features_snap)
        }, f, indent=4)
        
    print("Snapshotting prediction history...")
    client = pymongo.MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
    db = client["stock_market_db"]
    ph_snap = snapshot_prediction_history(db)
    
    with open(os.path.join(output_dir, "prediction_history_fingerprint.json"), "w") as f:
        json.dump(ph_snap, f, indent=4)
        
    print("Building manifest...")
    import xgboost
    import sklearn
    import imblearn
    import optuna
    import numpy as np
    import pandas as pd
    import joblib
    import platform
    
    manifest = {
        "git": get_git_info(),
        "python_version": sys.version,
        "os_platform": platform.platform(),
        "architecture": platform.machine(),
        "packages": {
            "xgboost": xgboost.__version__,
            "sklearn": sklearn.__version__,
            "imblearn": imblearn.__version__,
            "optuna": optuna.__version__,
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "pymongo": pymongo.__version__,
            "joblib": joblib.__version__
        },
        "environment": {
            "MODELS_DIR": os.getenv("MODELS_DIR", "saved_models"),
            "FEATURES_DIR": os.getenv("FEATURES_DIR", "saved_features")
        }
    }
    
    with open(os.path.join(output_dir, "experiment_manifest.json"), "w") as f:
        json.dump(manifest, f, indent=4)
        
    print("Baseline snapshot complete.")

if __name__ == "__main__":
    main()
