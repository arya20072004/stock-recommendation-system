import argparse
import csv
import json
import os
import random
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone
import importlib.util

import numpy as np
import optuna
import pandas as pd
from dotenv import load_dotenv, dotenv_values
from pymongo import MongoClient

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.data.nifty50 import TICKERS
from src.features.router import resolve_feature_pipeline
from src.ml import trainer

orig_script_path = os.path.join(PROJECT_ROOT, "scripts", "experiment_stock_pcr.py")
spec = importlib.util.spec_from_file_location("orig_script", orig_script_path)
orig = importlib.util.module_from_spec(spec)
sys.modules["orig_script"] = orig
spec.loader.exec_module(orig)

def parse_date(value):
    return orig.parse_date(value)

def validate_target_cutoff(ticker, client, cutoff_date, horizon, common_df):
    db = client["stock_market_db"]
    trading_dates_docs = list(db.historical_data.find(
        {"ticker": ticker, "date": {"$lte": cutoff_date.to_pydatetime()}},
        {"date": 1, "_id": 0}
    ).sort("date", 1))
    
    trading_dates = [pd.to_datetime(doc["date"]).tz_localize(None) for doc in trading_dates_docs]
    date_to_idx = {d: i for i, d in enumerate(trading_dates)}
    
    violations = 0
    cutoff_d = cutoff_date.date()
    feature_dates = common_df.index
    if len(feature_dates) == 0:
        raise ValueError("common_df is empty")
        
    final_feature_date_max = feature_dates.max().date()
    latest_req_date = None
    
    for fd in feature_dates:
        idx = date_to_idx.get(fd)
        if idx is None:
            violations += 1
            continue
            
        req_idx = idx + horizon
        if req_idx >= len(trading_dates):
            violations += 1
            if fd.date() == final_feature_date_max:
                latest_req_date = "BEYOND_CUTOFF"
        else:
            req_date = trading_dates[req_idx].date()
            if req_date > cutoff_d:
                violations += 1
            if fd.date() == final_feature_date_max:
                latest_req_date = str(req_date)
                
    assert violations == 0, f"Found {violations} leakage violations!"
    
    return {
        "final_feature_date_max": str(final_feature_date_max),
        "latest_required_future_date": latest_req_date,
        "target_leakage_violations": violations,
        "target_cutoff_validation_passed": violations == 0
    }

def main():
    parser = argparse.ArgumentParser(description="Multi-cutoff temporal validation for PCR")
    parser.add_argument("--cutoff-dates", type=parse_date, nargs="+", required=True)
    parser.add_argument("--ticker", help="Run a single eligible ticker as a smoke test.")
    parser.add_argument("--optuna-trials", type=int, default=trainer.N_OPTUNA_TRIALS)
    parser.add_argument("--coverage-csv", default="stock_pcr_coverage_2021-09-22_2026-08-11.csv")
    args = parser.parse_args()

    coverage_path = os.path.abspath(args.coverage_csv)
    if not os.path.exists(coverage_path):
        parser.error(f"Coverage CSV not found: {coverage_path}")

    eligible, excluded = orig.read_eligible_tickers(coverage_path)
    selected = eligible
    if args.ticker:
        if args.ticker not in TICKERS or args.ticker not in eligible:
            parser.error("Invalid smoke test ticker.")
        selected = [args.ticker]

    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
    mongo_uri = os.environ.get("MONGO_URI")
    if not mongo_uri:
        raise RuntimeError("MONGO_URI is missing.")

    feature_pipeline_version = trainer.FEATURE_PIPELINE_VERSION
    feature_module = resolve_feature_pipeline(feature_pipeline_version)
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    run_dir = os.path.join(PROJECT_ROOT, "experiments", "stock_pcr", "multicutoff", run_id)
    os.makedirs(run_dir, exist_ok=True)

    config = {
        "experiment_timestamp": datetime.now(timezone.utc).isoformat(),
        "cutoff_dates": [str(d.date()) for d in args.cutoff_dates],
        "ticker_count": len(selected),
        "included_tickers": selected,
        "optuna_trials": args.optuna_trials
    }
    with open(os.path.join(run_dir, "experiment_config.json"), "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)

    orig.trainer.N_OPTUNA_TRIALS = args.optuna_trials

    all_results = []
    failures = []
    client = MongoClient(mongo_uri, readPreference="primaryPreferred")
    
    total_cutoffs = len(args.cutoff_dates)
    try:
        for c_idx, cutoff in enumerate(args.cutoff_dates, start=1):
            print(f"\n[cutoff {c_idx}/{total_cutoffs}] {cutoff.date()}")
            os.environ["TRAINING_CUTOFF_DATE"] = str(cutoff.date())
            if hasattr(trainer, "_MACRO_CACHE"):
                trainer._MACRO_CACHE.clear()

            for t_idx, ticker in enumerate(selected, start=1):
                print(f"  [cutoff {c_idx}/{total_cutoffs}] [ticker {t_idx}/{len(selected)}] {ticker}")
                try:
                    alignment = orig.build_aligned_datasets(ticker, client, cutoff, feature_module)
                    split_data = orig.split_identically(
                        alignment["common_df"],
                        alignment["control_features"],
                        alignment["experiment_features"],
                    )
                    c_train, c_test, p_train, p_test, y_train, y_test = split_data
                    
                    c_metrics = orig.train_variant(c_train, c_test, y_train, y_test, ticker)
                    p_metrics = orig.train_variant(p_train, p_test, y_train, y_test, ticker)
                    
                    result = orig.build_result(ticker, cutoff, alignment, split_data, c_metrics, p_metrics)
                    
                    # Specific fields required by prompt
                    horizon = trainer.TICKER_HORIZON_OVERRIDE.get(ticker, 10)
                    result["horizon"] = horizon
                    c_dist = json.loads(result["control_prediction_distribution"])
                    p_dist = json.loads(result["pcr_prediction_distribution"])
                    result["prediction_shift"] = sum(abs(c_dist.get(k, 0) - p_dist.get(k, 0)) for k in c_dist)
                    result["delta_cv_score"] = result["pcr_cv_score"] - result["control_cv_score"]
                    
                    val_stats = validate_target_cutoff(ticker, client, cutoff, horizon, alignment["common_df"])
                    result["final_feature_date_max"] = val_stats["final_feature_date_max"]
                    result["latest_required_future_date"] = val_stats["latest_required_future_date"]
                    result["target_leakage_violations"] = val_stats["target_leakage_violations"]
                    result["target_cutoff_validation_passed"] = val_stats["target_cutoff_validation_passed"]
                    
                    all_results.append(result)
                    
                    print(f"    Control F1: {result['control_f1_macro']:.6f} | PCR F1: {result['pcr_f1_macro']:.6f} | Delta: {result['delta_f1_macro']:+.6f}")
                except Exception as exc:
                    failures.append({"cutoff": str(cutoff.date()), "ticker": ticker, "error": str(exc)})
                    print(f"    FAILED: {exc}")
    finally:
        client.close()

    if failures:
        print(f"\nFailures encountered: {len(failures)}")
        for f in failures:
            print(f)

    expected_observations = len(selected) * len(args.cutoff_dates)
    successful_observations = len(all_results)
    failed_observations = len(failures)

    if successful_observations > 0 and successful_observations == expected_observations and failed_observations == 0:
        df = pd.DataFrame(all_results)
        df.to_csv(os.path.join(run_dir, "per_ticker_cutoff_results.csv"), index=False)

        cutoff_summary = []
        for c, grp in df.groupby("cutoff_date"):
            cutoff_summary.append({
                "cutoff_date": c,
                "ticker_count": len(grp),
                "successful_count": len(grp),
                "failed_count": len([f for f in failures if f["cutoff"] == c]),
                "mean_control_f1": float(grp["control_f1_macro"].mean()),
                "mean_pcr_f1": float(grp["pcr_f1_macro"].mean()),
                "mean_delta_f1": float(grp["delta_f1_macro"].mean()),
                "median_delta_f1": float(grp["delta_f1_macro"].median()),
                "improved_tickers": int((grp["delta_f1_macro"] > 0).sum()),
                "worsened_tickers": int((grp["delta_f1_macro"] < 0).sum()),
                "neutral_tickers": int((grp["delta_f1_macro"] == 0).sum()),
                "improvement_rate": float((grp["delta_f1_macro"] > 0).mean()),
                "class_collapse_count": int((grp["class_collapse"] != "").sum()),
                "mean_delta_sell_f1": float(grp["delta_sell_f1"].mean()),
                "mean_delta_hold_f1": float(grp["delta_hold_f1"].mean()),
                "mean_delta_buy_f1": float(grp["delta_buy_f1"].mean()),
            })
        pd.DataFrame(cutoff_summary).to_csv(os.path.join(run_dir, "cutoff_summary.csv"), index=False)

        agg = {
            "level1_per_cutoff": cutoff_summary,
            "level2_per_ticker": {},
            "level3_global": {}
        }

        for t, grp in df.groupby("ticker"):
            agg["level2_per_ticker"][t] = {
                "number_of_cutoffs": len(grp),
                "improvement_count": int((grp["delta_f1_macro"] > 0).sum()),
                "regression_count": int((grp["delta_f1_macro"] < 0).sum()),
                "neutral_count": int((grp["delta_f1_macro"] == 0).sum()),
                "mean_delta_f1": float(grp["delta_f1_macro"].mean()),
                "median_delta_f1": float(grp["delta_f1_macro"].median()),
                "std_delta_f1": float(grp["delta_f1_macro"].std()) if len(grp) > 1 else 0.0,
                "min_delta_f1": float(grp["delta_f1_macro"].min()),
                "max_delta_f1": float(grp["delta_f1_macro"].max()),
                "consistency_rate": float((grp["delta_f1_macro"] > 0).mean()),
                "class_collapse_count": int((grp["class_collapse"] != "").sum())
            }

        agg["level3_global"] = {
            "total_observations": len(df),
            "mean_delta_f1": float(df["delta_f1_macro"].mean()),
            "median_delta_f1": float(df["delta_f1_macro"].median()),
            "std_delta_f1": float(df["delta_f1_macro"].std()),
            "improvement_rate": float((df["delta_f1_macro"] > 0).mean()),
            "regression_rate": float((df["delta_f1_macro"] < 0).mean()),
            "neutral_rate": float((df["delta_f1_macro"] == 0).mean()),
            "mean_delta_sell_f1": float(df["delta_sell_f1"].mean()),
            "mean_delta_hold_f1": float(df["delta_hold_f1"].mean()),
            "mean_delta_buy_f1": float(df["delta_buy_f1"].mean()),
            "total_class_collapses": int((df["class_collapse"] != "").sum())
        }

        with open(os.path.join(run_dir, "aggregate_summary.json"), "w") as f:
            json.dump(agg, f, indent=2)

        with open(os.path.join(run_dir, "final_report.txt"), "w") as f:
            f.write("MULTI-CUTOFF VALIDATION COMPLETED\n")
            f.write(f"Total cutoff dates: {total_cutoffs}\n")
            f.write(f"Total tickers: {len(selected)}\n")
            f.write(f"Total successful observations: {len(all_results)}\n")
            f.write(f"Total failures: {len(failures)}\n")
            f.write(f"Overall improvement rate: {agg['level3_global']['improvement_rate']:.2%}\n")

        print(f"\nSuccessfully completed run in: {run_dir}")
        print(f"Wrote final artifacts to indicate complete success.")
    elif successful_observations > 0:
        df = pd.DataFrame(all_results)
        df.to_csv(os.path.join(run_dir, "partial_results.csv"), index=False)
        print(f"\n[PARTIAL RUN] Completed {successful_observations}/{expected_observations}. Wrote partial_results.csv")
    else:
        print("\nNo successful results to aggregate.")

if __name__ == "__main__":
    main()
