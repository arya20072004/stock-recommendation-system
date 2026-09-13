import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

def get_db():
    load_dotenv()
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(uri)
    return client["stock_market_db"]

def main():
    db = get_db()
    report = {}

    run = db["pipeline_runs"].find_one({"status": "SUCCESS"}, sort=[("started_at", -1)])
    if not run:
        print("No SUCCESS run found.")
        return
        
    run_id = run.get("run_id")
    market_date = run.get("market_date")
    target_date = run.get("target_date") or market_date
    
    report["A_Run_Identity"] = {
        "Run_ID": str(run.get("_id")) + " | " + str(run_id),
        "execution_timestamp": str(run.get("started_at")),
        "target_date": target_date,
        "last_completed_session": market_date,
        "final_status": run.get("status")
    }
    
    report["B_Stage_Results"] = {}
    for stage, data in run.get("stages", {}).items():
        report["B_Stage_Results"][stage] = {
            "status": data.get("status"),
            "errors": data.get("errors", []),
            "metrics": data.get("metrics")
        }

    # Use actual datetime objects
    # Check 2026-08-28, 2026-08-29, 2026-08-31, 2026-09-01
    dates_to_check = [
        datetime(2026, 8, 28),
        datetime(2026, 8, 29),
        datetime(2026, 8, 31),
        datetime(2026, 9, 1),
        datetime(2026, 9, 2)
    ]
    
    report["C_Data_Integrity"] = {}
    for dt in dates_to_check:
        d_str = dt.strftime("%Y-%m-%d")
        hist_count = db["historical_data"].count_documents({"date": dt})
        nifty_count = db["historical_data"].count_documents({"date": dt, "ticker": "^NSEI"})
        report["C_Data_Integrity"][d_str] = {
            "coverage_count": hist_count,
            "nifty_present": nifty_count > 0
        }
        
    nsei_28 = db["historical_data"].count_documents({"date": datetime(2026, 8, 28), "ticker": "^NSEI"})
    report["D_NSE_Recovery"] = {
        "NSEI_2026_08_28_present": nsei_28 > 0
    }
    
    preds = list(db["prediction_history"].find({"run_id": run_id}))
    features_ok = True
    missing_features = []
    for p in preds:
        f = p.get("features", {})
        if f.get("nifty_return") is None or f.get("outperformance") is None or f.get("market_regime") is None:
            features_ok = False
            missing_features.append(p.get("symbol") or p.get("ticker"))
            
    report["F_Predictions"] = {
        "expected": 51,
        "actual": len(preds),
        "target_dates": list(set(str(p.get("target_date")) for p in preds)),
        "duplicates": len(preds) - len(set(p.get("symbol") or p.get("ticker") for p in preds))
    }

    report["E_Feature_Readiness"] = {
        "features_ok": features_ok,
        "missing_count": len(missing_features)
    }
    
    provs = list(db["prediction_provenance"].find({"run_id": run_id}))
    prov_hashes = list(set(p.get("pipeline_hash") or p.get("feature_pipeline_hash") for p in provs))
    
    legacy_provs = list(db["prediction_provenance"].find({"market_date": "2026-08-31"}))
    legacy_hashes = list(set(p.get("pipeline_hash") or p.get("feature_pipeline_hash") for p in legacy_provs))
    
    report["G_Provenance"] = {
        "count": len(provs),
        "current_hash": prov_hashes,
        "legacy_count": len(legacy_provs),
        "legacy_hashes": legacy_hashes
    }
    
    active_models = list(db["model_registry"].find({"status": "ACTIVE"}))
    active_hashes = list(set(m.get("feature_pipeline_hash") for m in active_models))
    
    report["H_Governance"] = {
        "active_DB_record_count": len(active_models),
        "new_hash_references": active_hashes
    }
    
    # Settlement
    cohorts = {}
    if "prediction_history" in db.list_collection_names():
        # Get target dates that are pending/mature
        # Actually there is no 'settlement_observations' collection. The settlement is part of prediction_history
        all_preds = db["prediction_history"].find({})
        for o in all_preds:
            d = str(o.get("target_date"))
            if d not in cohorts:
                cohorts[d] = {"count": 0, "status_counts": {}}
            cohorts[d]["count"] += 1
            st = o.get("settlement_status", "UNKNOWN")
            if st not in cohorts[d]["status_counts"]:
                cohorts[d]["status_counts"][st] = 0
            cohorts[d]["status_counts"][st] += 1
            
    report["J_Settlement"] = {
        "cohorts": cohorts
    }
    
    # Database mutations
    # I can just report current sizes
    
    with open("scratch/audit_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

if __name__ == "__main__":
    main()
