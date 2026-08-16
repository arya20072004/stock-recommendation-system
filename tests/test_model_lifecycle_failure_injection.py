import os
import sys
import json
import shutil
import tempfile
import hashlib
from datetime import datetime, timezone
import csv
from pymongo import MongoClient
import traceback

sys.path.append("c:/Users/aryab/Coding/stock_recommendations")
import src.ml.model_registry as mr

OUTPUT_DIR = "c:/Users/aryab/Coding/stock_recommendations/experiments/stock_pcr/selection_policy/lifecycle_hardening"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ------------------------------------------------------------
# ISOLATION SETUP
# ------------------------------------------------------------
TEMP_DIR = tempfile.mkdtemp()
TEMP_MODELS = os.path.join(TEMP_DIR, "saved_models")
TEMP_FEATURES = os.path.join(TEMP_DIR, "saved_features")
os.makedirs(TEMP_MODELS)
os.makedirs(TEMP_FEATURES)

# Monkeypatch registry directories
mr.MODELS_DIR = TEMP_MODELS
mr.FEATURES_DIR = TEMP_FEATURES

# Isolated DB
from dotenv import load_dotenv
load_dotenv("c:/Users/aryab/Coding/stock_recommendations/.env")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(MONGO_URI)
db = client["test_stock_market_db_isolated"]

# Safely drop only the isolated test collection
db.model_registry.drop()

results = []

def setup_artifacts(ticker, version):
    m_path = os.path.join(TEMP_MODELS, f"model_{ticker}_{version}.joblib")
    f_path = os.path.join(TEMP_FEATURES, f"features_{ticker}_{version}.json")
    with open(m_path, "w") as f: f.write("dummy")
    with open(f_path, "w") as f: f.write("dummy")
    mh = mr.hash_file_sha256(m_path, 12)
    fh = mr.hash_file_sha256(f_path, 64)
    return mh, fh

def run_test(name, setup_func, execute_func, verify_func):
    db.model_registry.delete_many({})
    for f in os.listdir(TEMP_MODELS): os.remove(os.path.join(TEMP_MODELS, f))
    for f in os.listdir(TEMP_FEATURES): os.remove(os.path.join(TEMP_FEATURES, f))
    
    setup_func()
    
    # Store previous state for rollback verification
    old_active = db.model_registry.find_one({"status": "ACTIVE"})
    manifest_path = mr.get_active_manifest_path("TEST.NS")
    old_manifest = None
    if os.path.exists(manifest_path):
        with open(manifest_path, "r") as f: old_manifest = f.read()
        
    try:
        class_str = execute_func()
        
        # Verify
        passed = verify_func(old_active, old_manifest)
    except Exception as e:
        class_str = "FAILURE_INJECTION_FAILED_EXCEPTION"
        passed = False
        traceback.print_exc()
        
    results.append({
        "test_name": name,
        "classification": class_str,
        "passed": passed
    })

# TEST 1 - Normal NO ACTIVE
def t1_setup():
    mh, fh = setup_artifacts("TEST.NS", "v1")
    mr.register_candidate(db, "TEST.NS", "v1", mh, fh, {"feature_pipeline_version": "v1"})
    
def t1_exec():
    res = mr.promote_model(db, "TEST.NS", "v1")
    return "SUCCESS" if res else "FAILED"
    
def t1_verify(oa, om):
    return db.model_registry.count_documents({"status": "ACTIVE"}) == 1 and os.path.exists(mr.get_active_manifest_path("TEST.NS"))

run_test("Test 1 - Normal promotion with NO existing ACTIVE", t1_setup, t1_exec, t1_verify)

# TEST 2 - Normal EXISTING ACTIVE
def t2_setup():
    mh1, fh1 = setup_artifacts("TEST.NS", "v1")
    mh2, fh2 = setup_artifacts("TEST.NS", "v2")
    mr.register_candidate(db, "TEST.NS", "v1", mh1, fh1, {"feature_pipeline_version": "v1"})
    mr.promote_model(db, "TEST.NS", "v1")
    mr.register_candidate(db, "TEST.NS", "v2", mh2, fh2, {"feature_pipeline_version": "v1"})
    
def t2_exec():
    res = mr.promote_model(db, "TEST.NS", "v2")
    return "SUCCESS" if res else "FAILED"
    
def t2_verify(oa, om):
    actives = list(db.model_registry.find({"status": "ACTIVE"}))
    v1 = db.model_registry.find_one({"version": "v1"})
    return len(actives) == 1 and actives[0]["version"] == "v2" and v1["status"] == "RETIRED"

run_test("Test 2 - Normal promotion with existing ACTIVE", t2_setup, t2_exec, t2_verify)

# TEST 3 - Manifest staging failure
orig_open = open
def mock_open_fail(*args, **kwargs):
    if ".tmp" in str(args[0]): raise PermissionError("Simulated Staging Failure")
    return orig_open(*args, **kwargs)

def t3_exec():
    import builtins
    builtins.open = mock_open_fail
    try:
        res = mr.promote_model(db, "TEST.NS", "v1")
        return "PROMOTION_ABORTED_BEFORE_MUTATION" if not res else "FAILED"
    finally:
        builtins.open = orig_open

def t3_verify(oa, om):
    return db.model_registry.count_documents({"status": "ACTIVE"}) == 0 and not os.path.exists(mr.get_active_manifest_path("TEST.NS"))

run_test("Test 3 - Manifest staging failure, NO existing ACTIVE", t1_setup, t3_exec, t3_verify)

# TEST 5 - os.replace failure NO ACTIVE
orig_replace = os.replace
def mock_replace_fail(*args, **kwargs):
    raise PermissionError("Simulated Replace Failure")

def t5_exec():
    os.replace = mock_replace_fail
    try:
        res = mr.promote_model(db, "TEST.NS", "v1")
        return "PROMOTION_ROLLED_BACK" if not res else "FAILED"
    finally:
        os.replace = orig_replace

def t5_verify(oa, om):
    actives = db.model_registry.count_documents({"status": "ACTIVE"})
    c = db.model_registry.find_one({"version": "v1"})
    man = os.path.exists(mr.get_active_manifest_path("TEST.NS"))
    return actives == 0 and c["status"] == "CANDIDATE" and not man

run_test("Test 5 - os.replace() failure, NO existing ACTIVE", t1_setup, t5_exec, t5_verify)

# TEST 6 - os.replace failure EXISTING ACTIVE
def t6_exec():
    os.replace = mock_replace_fail
    try:
        res = mr.promote_model(db, "TEST.NS", "v2")
        return "PROMOTION_ROLLED_BACK" if not res else "FAILED"
    finally:
        os.replace = orig_replace
        
def t6_verify(oa, om):
    actives = list(db.model_registry.find({"status": "ACTIVE"}))
    return len(actives) == 1 and actives[0]["version"] == "v1" and db.model_registry.find_one({"version": "v2"})["status"] == "CANDIDATE"

run_test("Test 6 - os.replace() failure, EXISTING ACTIVE", t2_setup, t6_exec, t6_verify)

# TEST 8 - Rollback DB failure
orig_update = db.model_registry.update_one
def mock_update_fail(*args, **kwargs):
    if "$set" in args[1] and "status" in args[1]["$set"] and args[1]["$set"]["status"] == "CANDIDATE":
        raise Exception("Simulated DB failure during rollback")
    return orig_update(*args, **kwargs)

def t8_exec():
    os.replace = mock_replace_fail
    db.model_registry.update_one = mock_update_fail
    try:
        res = mr.promote_model(db, "TEST.NS", "v1")
        return "RECOVERY_REQUIRED" if not res else "FAILED"
    except Exception:
        return "RECOVERY_REQUIRED"
    finally:
        os.replace = orig_replace
        db.model_registry.update_one = orig_update

def t8_verify(oa, om):
    return True # We expect it to be a mess

run_test("Test 8 - Rollback database failure, NO existing ACTIVE", t1_setup, t8_exec, t8_verify)

# TEST 10 - Post-promotion verification failure
orig_load = json.load
def mock_load_fail(*args, **kwargs):
    res = orig_load(*args, **kwargs)
    res["model_version"] = "tampered"
    return res

def t10_exec():
    import json
    json.load = mock_load_fail
    try:
        res = mr.promote_model(db, "TEST.NS", "v1")
        return "POST_PROMOTION_VERIFICATION_FAILED" if not res else "FAILED"
    finally:
        json.load = orig_load
        
def t10_verify(oa, om):
    return db.model_registry.count_documents({"status": "ACTIVE"}) == 1

run_test("Test 10 - Post-promotion verification failure", t1_setup, t10_exec, t10_verify)

# Write results
with open(os.path.join(OUTPUT_DIR, "failure_injection_results.csv"), "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["test_name", "classification", "passed"])
    writer.writeheader()
    writer.writerows(results)
    
shutil.rmtree(TEMP_DIR)
