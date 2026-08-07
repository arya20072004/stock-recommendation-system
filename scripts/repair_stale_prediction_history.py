import argparse
import os
import sys
from datetime import datetime
import json
from bson import json_util
from pymongo import MongoClient
from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.ml.model_utils import get_model_version
from src.ml.history import generate_and_persist_predictions

TARGET_TICKERS = [
    "BAJAJFINSV.NS", "BRITANNIA.NS", "EICHERMOT.NS", "GRASIM.NS", 
    "JSWSTEEL.NS", "MAXHEALTH.NS", "SHRIRAMFIN.NS", "TECHM.NS", 
    "TITAN.NS", "ULTRACEMCO.NS"
]
TARGET_MARKET_DATE = "2026-08-07"
TARGET_PREDICTION_HORIZON = 10

def validate_record_for_repair(doc, current_version):
    if doc is None:
        return "MISSING"
    
    # Check if resolved
    if (doc.get("status") != "PENDING" or 
        doc.get("outcome") != "PENDING" or 
        doc.get("actual_price") is not None or 
        doc.get("actual_return") is not None or 
        doc.get("prediction_correct") is not None):
        return "RESOLVED_DO_NOT_TOUCH"
        
    stored_version = doc.get("model_version")
    if stored_version == current_version:
        return "ALREADY_CURRENT"
        
    if stored_version != current_version:
        return "STALE_SAFE_TO_REPAIR"
        
    return "INVALID_STATE"

def main():
    parser = argparse.ArgumentParser(description="Repair stale prediction history after retraining.")
    parser.add_argument('--apply', action='store_true', help="Apply repairs")
    parser.add_argument('--dry-run', action='store_true', help="Dry run mode (default)")
    args = parser.parse_args()

    is_dry_run = not args.apply

    load_dotenv("c:/Users/aryab/Coding/stock_recommendations/.env")
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    db = client['stock_market_db']
    collection = db['prediction_history']

    print(f"--- RUNNING IN {'DRY RUN' if is_dry_run else 'APPLY'} MODE ---")
    
    counts = {
        "STALE_SAFE_TO_REPAIR": 0,
        "ALREADY_CURRENT": 0,
        "MISSING": 0,
        "RESOLVED_DO_NOT_TOUCH": 0,
        "INVALID_STATE": 0
    }
    
    records_to_repair = []
    
    for ticker in TARGET_TICKERS:
        doc = collection.find_one({
            "symbol": ticker,
            "market_date": TARGET_MARKET_DATE,
            "prediction_horizon": TARGET_PREDICTION_HORIZON
        })
        
        current_ver = get_model_version(ticker)
        classification = validate_record_for_repair(doc, current_ver)
        counts[classification] += 1
        
        print(f"\nTarget: {ticker}")
        print(f"  market_date: {TARGET_MARKET_DATE}")
        print(f"  prediction_horizon: {TARGET_PREDICTION_HORIZON}")
        if doc:
            print(f"  existing model_version: {doc.get('model_version')}")
            print(f"  current get_model_version: {current_ver}")
            print(f"  existing recommendation: {doc.get('recommendation')}")
            print(f"  existing raw_prediction: {doc.get('raw_prediction')}")
            print(f"  existing confidence: {doc.get('confidence')}")
            print(f"  status: {doc.get('status')}")
            print(f"  outcome: {doc.get('outcome')}")
            print(f"  actual_price: {doc.get('actual_price')}")
            print(f"  actual_return: {doc.get('actual_return')}")
            print(f"  prediction_correct: {doc.get('prediction_correct')}")
            
            if classification == "STALE_SAFE_TO_REPAIR":
                print(f"  -> SAFE TO REPLACE")
                records_to_repair.append(doc)
            else:
                print(f"  -> DO NOT REPLACE ({classification})")
        else:
            print("  -> MISSING")
            
    print("\n--- CLASSIFICATION COUNTS ---")
    for k, v in counts.items():
        print(f"{k} = {v}")
        
    if is_dry_run:
        print("\nDry run completed. Run with --apply to execute.")
        client.close()
        return
        
    # APPLY MODE
    print("\n--- INITIATING REPAIR (APPLY MODE) ---")
    
    if counts["STALE_SAFE_TO_REPAIR"] != len(TARGET_TICKERS):
        print("ERROR: Not all target tickers are STALE_SAFE_TO_REPAIR. Aborting.")
        sys.exit(1)
        
    # BACKUP PHASE
    backup_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'artifacts', 'prediction_history_repairs'))
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(backup_dir, f"prediction_history_2026-08-07_pre_repair_{timestamp}.json")
    
    backup_data = {
        "repair_execution_timestamp": timestamp,
        "market_date": TARGET_MARKET_DATE,
        "prediction_horizon": TARGET_PREDICTION_HORIZON,
        "targeted_symbols": TARGET_TICKERS,
        "previous_model_versions": {doc['symbol']: doc['model_version'] for doc in records_to_repair},
        "current_model_versions": {doc['symbol']: get_model_version(doc['symbol']) for doc in records_to_repair},
        "documents": records_to_repair
    }
    
    try:
        with open(backup_file, 'w') as f:
            f.write(json_util.dumps(backup_data, indent=2))
        print(f"Backup created successfully at: {backup_file}")
    except Exception as e:
        print(f"ERROR: Failed to create backup. Aborting. {e}")
        sys.exit(1)
        
    # Verify backup
    with open(backup_file, 'r') as f:
        verified_backup = json_util.loads(f.read())
        
    if len(verified_backup["documents"]) != 10:
        print(f"ERROR: Backup contains {len(verified_backup['documents'])} documents, expected 10. Aborting.")
        sys.exit(1)
        
    print("Backup verified (10 documents). Proceeding to targeted delete.")
    
    # TARGETED DELETE
    delete_filter = {
        "symbol": {"$in": TARGET_TICKERS},
        "market_date": TARGET_MARKET_DATE,
        "prediction_horizon": TARGET_PREDICTION_HORIZON
    }
    
    # Pre-delete assert
    match_count = collection.count_documents(delete_filter)
    if match_count != 10:
        print(f"ERROR: Matched count for deletion is {match_count}, expected 10. Aborting.")
        sys.exit(1)
        
    del_result = collection.delete_many(delete_filter)
    print(f"Deleted {del_result.deleted_count} stale records.")
    
    if del_result.deleted_count != 10:
        print(f"ERROR: deleted_count is {del_result.deleted_count}, expected 10. Manual rollback required from {backup_file}")
        sys.exit(1)
        
    # REGENERATE
    print("\n--- REGENERATING PREDICTIONS ---")
    try:
        gen_result = generate_and_persist_predictions(client)
        print(f"Regeneration result: {gen_result['generated']} generated, {gen_result['skipped']} skipped, {gen_result['errors']} errors.")
        if gen_result['generated'] != 10 or gen_result['errors'] > 0:
            print(f"WARNING: Unexpected regeneration result. Expected 10 generated, 0 errors. Manual check required. Rollback backup available at {backup_file}")
    except Exception as e:
        print(f"ERROR: Regeneration failed! {e}")
        print(f"Manual rollback required. Insert the 'documents' array from {backup_file} into prediction_history.")
        sys.exit(1)
        
    print("Repair process finished.")
    client.close()

if __name__ == '__main__':
    main()
