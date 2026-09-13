import os
import json
import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

def default_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    return str(obj)

def main():
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(mongo_uri)
    db = client["stock_market_db"]

    report = {}

    runs = list(db.pipeline_runs.find({"status": "SUCCESS"}))
    report["pipeline_runs"] = []
    sept_8_run = None
    for r in runs:
        if r.get("started_at") and r["started_at"].year == 2026 and r["started_at"].month == 9 and r["started_at"].day == 8:
            r_dump = {
                "run_id": r["run_id"],
                "market_date": r.get("market_date"),
                "status": r["status"],
                "started_at": r["started_at"].isoformat() if r.get("started_at") else None,
                "completed_at": r.get("completed_at").isoformat() if r.get("completed_at") else None,
                "stages": {k: v.get("status") for k, v in r.get("stages", {}).items()}
            }
            report["pipeline_runs"].append(r_dump)
            sept_8_run = r_dump
    
    report["sept_8_run"] = sept_8_run

    preds = list(db.prediction_history.find({"market_date": "2026-09-09", "prediction_horizon": 10}))
    provs = list(db.prediction_provenance.find({"market_date": "2026-09-09", "prediction_horizon": 10}))
    
    provs_by_hash = {p["provenance_hash"]: p for p in provs}

    report["predictions_count"] = len(preds)
    report["provenance_count"] = len(provs)
    
    p_dump = []
    nasdaq_5d_missing = 0
    nasdaq_20d_missing = 0
    max_feature_date = "1900-01-01"
    
    for p in preds:
        prov = provs_by_hash.get(p.get("provenance_hash"), {})
        
        features = prov.get("features", {})
        raw_inputs = prov.get("raw_inputs", {})
        
        n_5d = features.get("nasdaq_ret_5d")
        n_20d = features.get("nasdaq_ret_20d")
        
        if n_5d is None: nasdaq_5d_missing += 1
        if n_20d is None: nasdaq_20d_missing += 1
        
        p_dump.append({
            "symbol": p.get("symbol"),
            "market_date": p.get("market_date"),
            "model_version": p.get("model_version"),
            "status": p.get("status"),
            "outcome": p.get("outcome"),
            "provenance_hash": p.get("provenance_hash"),
            "prov_pipeline_hash": prov.get("feature_pipeline_hash"),
            "prov_pipeline_version": prov.get("feature_pipeline_version"),
            "prov_model_version": prov.get("model_version"),
            "nasdaq_ret_5d": n_5d,
            "nasdaq_ret_20d": n_20d,
            "feature_count": len(features)
        })
    report["predictions"] = p_dump
    report["nasdaq_5d_missing"] = nasdaq_5d_missing
    report["nasdaq_20d_missing"] = nasdaq_20d_missing

    locks = list(db.pipeline_locks.find({}))
    report["locks"] = [
        {
            "lock_id": l.get("lock_id"),
            "owner": l.get("owner"),
            "status": l.get("status"),
            "run_id": l.get("run_id")
        } for l in locks
    ]

    registry = list(db.model_registry.find({"status": "ACTIVE", "model_version": "v1"}))
    report["registry"] = [
        {
            "ticker": r.get("ticker"),
            "model_hash": r.get("model_hash"),
            "feature_hash": r.get("feature_hash"),
            "pipeline_hash": r.get("feature_pipeline_hash"),
            "status": r.get("status")
        } for r in registry
    ]

    # check manifests
    import glob
    manifests = glob.glob("saved_models/*_active.json")
    report["manifests"] = []
    for m_path in manifests:
        with open(m_path, "r") as f:
            m = json.load(f)
            report["manifests"].append({
                "ticker": m.get("ticker"),
                "model_version": m.get("model_version"),
                "feature_pipeline_hash": m.get("feature_pipeline_hash")
            })

    with open("scratch/audit_results_2.json", "w") as f:
        json.dump(report, f, indent=2, default=default_serializer)

if __name__ == "__main__":
    main()
