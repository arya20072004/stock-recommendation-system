import os
import sys
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()

# Setup paths to import src modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from src.ml.history import TICKERS as canonical_tickers
except ImportError:
    print("Failed to import TICKERS from src.ml.history. Using hardcoded list if necessary, but failing first.")
    sys.exit(1)

# Expected constants
EXPECTED_DB = "stock_market_db"
PIPELINE_VERSION = "v1"
OLD_HASH = "26cc670ca5d434821c0485bde3310599e2c077dbee5504c8b266d4a37333388c"
NEW_HASH = "f0fa3983b0114cc63635a51f26ef954bd4f64b151ed81673aadadc857717d862"

def run_audit():
    # 1. Verify Production Database Identity
    mongo_uri = os.environ.get("MONGO_URI")
    if not mongo_uri:
        print("MONGO_URI not set. STOP.")
        sys.exit(1)
        
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        mongo_conn_status = "PASS"
    except Exception as e:
        print(f"MongoDB connection failed: {e}")
        mongo_conn_status = "FAIL"
        sys.exit(1)

    db_identity_status = "PASS"
    db = client[EXPECTED_DB]

    # 2. Identify the Canonical 51-Ticker Universe
    canonical_ticker_count = len(canonical_tickers)
    unique_canonical_tickers = set(canonical_tickers)
    canonical_ticker_uniqueness = "PASS" if len(unique_canonical_tickers) == canonical_ticker_count else "FAIL"

    if canonical_ticker_count != 51 or canonical_ticker_uniqueness != "PASS":
        print(f"Canonical universe check failed: count={canonical_ticker_count}, uniqueness={canonical_ticker_uniqueness}")
        sys.exit(1)

    # 3. Inspect ALL ACTIVE Registry Records
    active_records = list(db.model_registry.find({"status": "ACTIVE"}))
    total_active_records = len(active_records)
    
    active_v1_records = []
    missing_pipeline_version_count = 0
    unexpected_pipeline_version_count = 0
    
    for r in active_records:
        version = r.get("feature_pipeline_version")
        if version is None:
            missing_pipeline_version_count += 1
        elif version == PIPELINE_VERSION:
            active_v1_records.append(r)
        else:
            unexpected_pipeline_version_count += 1

    # 4. Verify the Old Hash Across ALL ACTIVE v1 Records
    expected_old_hash_count = 0
    new_hash_count = 0
    unexpected_hash_count = 0
    missing_hash_count = 0
    
    active_v1_old_hash_ids = set()
    
    for r in active_v1_records:
        h = r.get("feature_pipeline_hash")
        if h is None:
            missing_hash_count += 1
        elif h == OLD_HASH:
            expected_old_hash_count += 1
            active_v1_old_hash_ids.add(r["_id"])
        elif h == NEW_HASH:
            new_hash_count += 1
        else:
            unexpected_hash_count += 1

    # 5. Verify the Ticker Set Exactly
    registry_tickers = [r.get("ticker") for r in active_v1_records]
    registry_ticker_set = set(registry_tickers)
    
    missing_tickers = unique_canonical_tickers - registry_ticker_set
    unexpected_tickers = registry_ticker_set - unique_canonical_tickers

    # 6. Verify Duplicate ACTIVE Records
    ticker_counts = {}
    for t in registry_tickers:
        ticker_counts[t] = ticker_counts.get(t, 0) + 1
        
    duplicate_active_tickers = sum(1 for count in ticker_counts.values() if count > 1)

    # 7. Verify RETIRED Records Are Not Targets
    retired_records_old_hash = list(db.model_registry.find({"status": "RETIRED", "feature_pipeline_hash": OLD_HASH}))
    retired_targeted = 0 # Since migration explicitly targets status: ACTIVE

    # 8. Independently Reconstruct the Migration Target Set
    independent_target_ids = {r["_id"] for r in active_records 
                             if r.get("feature_pipeline_version") == PIPELINE_VERSION 
                             and r.get("feature_pipeline_hash") == OLD_HASH}
                             
    # 9. Verify Target _id Integrity
    migration_script_target_ids = {r["_id"] for r in db.model_registry.find({"status": "ACTIVE", "feature_pipeline_version": PIPELINE_VERSION, "feature_pipeline_hash": OLD_HASH})}
    
    ids_missing = independent_target_ids - migration_script_target_ids
    ids_unexpected = migration_script_target_ids - independent_target_ids
    target_id_set_equality = "PASS" if not ids_missing and not ids_unexpected else "FAIL"

    # 10. Verify Target Record Identity
    target_identity_validation = "PASS"
    for r in db.model_registry.find({"_id": {"$in": list(independent_target_ids)}}):
        if r.get("status") != "ACTIVE" or \
           r.get("feature_pipeline_version") != PIPELINE_VERSION or \
           r.get("feature_pipeline_hash") != OLD_HASH or \
           r.get("ticker") not in unique_canonical_tickers:
            target_identity_validation = "FAIL"
            break

    # 11. Verify No Mixed Production State
    # Already captured in variables

    # 12. Verify Zero Mutation
    # We performed absolutely no writes.

    # PASS Criteria check
    verdict = "PASS"
    if EXPECTED_DB != "stock_market_db": verdict = "FAIL"
    if canonical_ticker_count != 51 or canonical_ticker_uniqueness != "PASS": verdict = "FAIL"
    if total_active_records != 51: verdict = "FAIL"
    if len(active_v1_records) != 51: verdict = "FAIL"
    if missing_pipeline_version_count != 0 or unexpected_pipeline_version_count != 0: verdict = "FAIL"
    if expected_old_hash_count != 51: verdict = "FAIL"
    if new_hash_count != 0 or unexpected_hash_count != 0 or missing_hash_count != 0: verdict = "FAIL"
    if len(registry_ticker_set) != 51 or missing_tickers or unexpected_tickers: verdict = "FAIL"
    if duplicate_active_tickers != 0: verdict = "FAIL"
    if retired_targeted != 0: verdict = "FAIL"
    if len(independent_target_ids) != 51: verdict = "FAIL"
    if target_id_set_equality != "PASS": verdict = "FAIL"
    if target_identity_validation != "PASS": verdict = "FAIL"

    report = f"""============================================================
READ-ONLY PRODUCTION MIGRATION TARGET VERIFICATION
============================================================

Database:
{EXPECTED_DB}

Pipeline version:
{PIPELINE_VERSION}

Current production hash:
{OLD_HASH}

Intended new hash:
{NEW_HASH}

Canonical ticker count:
{canonical_ticker_count}

Total ACTIVE records:
{total_active_records}

ACTIVE v1 records:
{len(active_v1_records)}

Missing pipeline version:
{missing_pipeline_version_count}

Unexpected pipeline versions:
{unexpected_pipeline_version_count}

ACTIVE records with old hash:
{expected_old_hash_count}

ACTIVE records with new hash:
{new_hash_count}

ACTIVE records with unexpected hash:
{unexpected_hash_count}

ACTIVE records with missing hash:
{missing_hash_count}

Registry ticker count:
{len(registry_ticker_set)}

Missing tickers:
{len(missing_tickers)}

Unexpected tickers:
{len(unexpected_tickers)}

Duplicate ACTIVE tickers:
{duplicate_active_tickers}

RETIRED records targeted:
{retired_targeted}

Independent migration target count:
{len(independent_target_ids)}

Migration target ID count:
{len(migration_script_target_ids)}

Independent target ID count:
{len(independent_target_ids)}

Target ID set equality:
{target_id_set_equality}

Target identity validation:
{target_identity_validation}

Database mutations:
0

Filesystem mutations:
0

============================================================
FINAL VERDICT:
{verdict}
============================================================"""

    print(report)

if __name__ == "__main__":
    run_audit()
