import os
import json
import hashlib
import pymongo
from dotenv import load_dotenv
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
        if "confidence" in doc and doc["confidence"] is not None:
            try:
                doc["confidence"] = round(float(doc["confidence"]), 8)
            except (ValueError, TypeError):
                pass

    # Sort deterministically
    docs.sort(key=lambda x: (x.get("symbol", ""), x.get("market_date", "")))
    
    stable_json = json.dumps(docs, sort_keys=True, separators=(',', ':'))
    fingerprint = hashlib.sha256(stable_json.encode('utf-8')).hexdigest()
    
    return {
        "document_count": len(docs),
        "fingerprint": fingerprint,
    }

def verify():
    load_dotenv()
    
    baseline_dir = "artifacts/model_ab/baseline"
    with open(f"{baseline_dir}/baseline_hashes.json") as f:
        baseline_hashes = json.load(f)
        
    with open(f"{baseline_dir}/prediction_history_fingerprint.json") as f:
        baseline_ph = json.load(f)
        
    print("Hashing current state...")
    current_models = snapshot_dir("saved_models")
    current_features = snapshot_dir("saved_features")
    
    client = pymongo.MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
    db = client["stock_market_db"]
    current_ph = snapshot_prediction_history(db)
    
    passed = True
    
    added_models = set(current_models.keys()) - set(baseline_hashes["saved_models"].keys())
    removed_models = set(baseline_hashes["saved_models"].keys()) - set(current_models.keys())
    mod_models = [k for k in current_models if k in baseline_hashes["saved_models"] and current_models[k]["sha256"] != baseline_hashes["saved_models"][k]["sha256"]]
    
    added_features = set(current_features.keys()) - set(baseline_hashes["saved_features"].keys())
    removed_features = set(baseline_hashes["saved_features"].keys()) - set(current_features.keys())
    mod_features = [k for k in current_features if k in baseline_hashes["saved_features"] and current_features[k]["sha256"] != baseline_hashes["saved_features"][k]["sha256"]]
    
    print("--- PRODUCTION INTEGRITY REPORT ---")
    print(f"Added models: {len(added_models)}")
    print(f"Removed models: {len(removed_models)}")
    print(f"Modified models: {len(mod_models)}")
    print(f"Added features: {len(added_features)}")
    print(f"Removed features: {len(removed_features)}")
    print(f"Modified features: {len(mod_features)}")
    
    if added_models or removed_models or mod_models or added_features or removed_features or mod_features:
        passed = False
        print("FAIL: production models or features were mutated!")
        
    print(f"Prediction history count: baseline {baseline_ph['document_count']} vs current {current_ph['document_count']}")
    if baseline_ph['document_count'] != current_ph['document_count']:
        passed = False
        print("FAIL: prediction history count changed!")
        
    print(f"Prediction history fingerprint: baseline {baseline_ph['fingerprint']} vs current {current_ph['fingerprint']}")
    if baseline_ph['fingerprint'] != current_ph['fingerprint']:
        passed = False
        print("FAIL: prediction history fingerprint mutated!")
        
    if passed:
        print("ISOLATION PASSED")
    else:
        print("ISOLATION FAILED")

if __name__ == "__main__":
    verify()
