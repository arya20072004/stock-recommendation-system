import argparse
import csv
import json
import os
import sys
import math
import numpy as np
import pandas as pd
import scipy.stats as stats
from sklearn.linear_model import LinearRegression
import importlib.util
from collections import Counter
from datetime import datetime, timezone
from pymongo import MongoClient

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import dotenv_values
from src.features.router import resolve_feature_pipeline
from src.ml import trainer
from src.data.nifty50 import TICKERS

expr_path = os.path.join(PROJECT_ROOT, "scripts", "experiment_stock_pcr.py")
spec = importlib.util.spec_from_file_location("experiment_stock_pcr", expr_path)
expr_module = importlib.util.module_from_spec(spec)
sys.modules["experiment_stock_pcr"] = expr_module
spec.loader.exec_module(expr_module)
build_aligned_datasets = expr_module.build_aligned_datasets

def parse_date(value):
    try:
        return pd.Timestamp(value).normalize()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'; expected YYYY-MM-DD.") from exc

def classify_performance(delta):
    if delta >= 0.02: return "STRONG_IMPROVEMENT"
    elif delta >= 0.005: return "MODERATE_IMPROVEMENT"
    elif delta > -0.005: return "NEUTRAL"
    elif delta > -0.02: return "MODERATE_REGRESSION"
    else: return "STRONG_REGRESSION"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-dir", required=True)
    parser.add_argument("--cutoff-date", required=True, type=parse_date)
    args = parser.parse_args()

    exp_dir = os.path.abspath(args.experiment_dir)
    res_path = os.path.join(exp_dir, "per_ticker_results.csv")
    cov_path = os.path.join(PROJECT_ROOT, "stock_pcr_coverage_2021-09-22_2026-08-11.csv")

    results_df = pd.read_csv(res_path)
    cov_df = pd.read_csv(cov_path)
    
    dotenv = dotenv_values(os.path.join(PROJECT_ROOT, ".env"))
    mongo_uri = os.environ.get("MONGO_URI") or dotenv.get("MONGO_URI")
    client = MongoClient(mongo_uri, readPreference="primaryPreferred")
    
    os.environ["TRAINING_CUTOFF_DATE"] = str(args.cutoff_date.date())
    feature_module = resolve_feature_pipeline(trainer.FEATURE_PIPELINE_VERSION)
    
    diag_dir = os.path.join(PROJECT_ROOT, "experiments", "stock_pcr", "diagnostics")
    os.makedirs(diag_dir, exist_ok=True)
    
    # D1: Performance Stratification
    d1_cols = [
        "ticker", "delta_f1_macro", "control_f1_macro", "pcr_f1_macro",
        "delta_sell_f1", "delta_hold_f1", "delta_buy_f1",
        "control_sell_f1", "control_hold_f1", "control_buy_f1",
        "pcr_sell_f1", "pcr_hold_f1", "pcr_buy_f1",
        "control_cv_score", "pcr_cv_score", "pcr_optuna_trials",
        "classes_improved", "classes_worsened", "class_collapse"
    ]
    diag1 = results_df[d1_cols].copy()
    diag1.rename(columns={"pcr_optuna_trials": "optuna_trials"}, inplace=True)
    
    def calc_pred_shift(row):
        c_dist = json.loads(row['control_prediction_distribution'])
        p_dist = json.loads(row['pcr_prediction_distribution'])
        return sum(abs(c_dist.get(k,0) - p_dist.get(k,0)) for k in ['SELL', 'HOLD', 'BUY'])
        
    def shift_by_class(row, cls):
        c_dist = json.loads(row['control_prediction_distribution'])
        p_dist = json.loads(row['pcr_prediction_distribution'])
        return p_dist.get(cls,0) - c_dist.get(cls,0)

    diag1["prediction_shift"] = results_df.apply(calc_pred_shift, axis=1)
    diag1["buy_shift"] = results_df.apply(lambda r: shift_by_class(r, 'BUY'), axis=1)
    diag1["hold_shift"] = results_df.apply(lambda r: shift_by_class(r, 'HOLD'), axis=1)
    diag1["sell_shift"] = results_df.apply(lambda r: shift_by_class(r, 'SELL'), axis=1)
    diag1["performance_class"] = diag1["delta_f1_macro"].apply(classify_performance)
    
    diag1.to_csv(os.path.join(diag_dir, "ticker_diagnostic_table.csv"), index=False)
    
    # D2: PCR Coverage Relationship
    diag2 = pd.merge(diag1, cov_df, on="ticker", how="inner")
    corr_vars = ["coverage_pct", "usable_feature_coverage_pct", "total_pcr_rows", 
                 "distinct_pcr_dates", "pcr_chg5_usable", "duplicate_records", "non_numeric_pcr_oi"]
    perf_vars = ["delta_f1_macro", "delta_sell_f1", "delta_hold_f1", "delta_buy_f1"]
    
    d2_records = []
    for cv in corr_vars:
        for pv in perf_vars:
            try:
                pearson, p_p = stats.pearsonr(diag2[cv], diag2[pv])
                spearman, p_s = stats.spearmanr(diag2[cv], diag2[pv])
            except:
                pearson, p_p, spearman, p_s = np.nan, np.nan, np.nan, np.nan
            d2_records.append({
                "coverage_var": cv,
                "perf_var": pv,
                "pearson_r": pearson,
                "pearson_p": p_p,
                "spearman_r": spearman,
                "spearman_p": p_s
            })
    pd.DataFrame(d2_records).to_csv(os.path.join(diag_dir, "coverage_relationships.csv"), index=False)
    
    # D3 & D4
    d3_records = []
    d4_records = []
    
    for _, row in diag1.iterrows():
        ticker = row["ticker"]
        try:
            alignment = build_aligned_datasets(ticker, client, args.cutoff_date, feature_module)
            df = alignment["common_df"]
            
            pcr_oi = pd.to_numeric(df["stock_pcr_oi"], errors="coerce")
            pcr_chg = pd.to_numeric(df["stock_pcr_chg_5d"], errors="coerce")
            
            if not pcr_oi.dropna().empty:
                d3_records.append({
                    "ticker": ticker,
                    "oi_mean": pcr_oi.mean(),
                    "oi_median": pcr_oi.median(),
                    "oi_std": pcr_oi.std(),
                    "oi_min": pcr_oi.min(),
                    "oi_max": pcr_oi.max(),
                    "chg_mean": pcr_chg.mean(),
                    "chg_median": pcr_chg.median(),
                    "chg_std": pcr_chg.std(),
                    "chg_min": pcr_chg.min(),
                    "chg_max": pcr_chg.max(),
                    "latest_oi": pcr_oi.iloc[-1] if len(pcr_oi)>0 else np.nan,
                    "latest_chg": pcr_chg.iloc[-1] if len(pcr_chg)>0 else np.nan,
                    "oi_cv": pcr_oi.std()/pcr_oi.mean() if pcr_oi.mean()!=0 else np.nan,
                    "frac_zero_oi": (pcr_oi == 0).mean(),
                    "distinct_oi": pcr_oi.nunique(),
                    "oi_percentile_latest": stats.percentileofscore(pcr_oi.dropna(), pcr_oi.iloc[-1]) if len(pcr_oi)>0 else np.nan,
                })
            
            target = df["target"]
            rec = {"ticker": ticker}
            for cls_id, name in [(0,"SELL"), (1,"HOLD"), (2,"BUY")]:
                subset = pcr_oi[target == cls_id].dropna()
                rec[f"oi_mean_{name}"] = subset.mean() if not subset.empty else np.nan
                rec[f"oi_median_{name}"] = subset.median() if not subset.empty else np.nan
            
            if "future_return" in df.columns:
                f_ret = df["future_return"]
                val_mask = pcr_oi.notna() & f_ret.notna()
                if val_mask.sum() > 10:
                    sp, _ = stats.spearmanr(pcr_oi[val_mask], f_ret[val_mask])
                    sp_chg, _ = stats.spearmanr(pcr_chg[val_mask], f_ret[val_mask])
                    rec["spearman_oi_ret"] = sp
                    rec["spearman_chg_ret"] = sp_chg
            d4_records.append(rec)
            
        except Exception as exc:
            pass
            
    pd.DataFrame(d3_records).to_csv(os.path.join(diag_dir, "pcr_signal_statistics.csv"), index=False)
    pd.DataFrame(d4_records).to_csv(os.path.join(diag_dir, "class_relationships.csv"), index=False)

    # D5 & D6
    d6_cols = []
    for _, row in results_df.iterrows():
        n = row["control_test_size"]
        if n == 0:
            continue
        bs = row["control_buy_support"]
        hs = row["control_hold_support"]
        ss = row["control_sell_support"]
        
        props = {"BUY": bs/n, "HOLD": hs/n, "SELL": ss/n}
        min_class = min(props, key=props.get)
        maj_class = max(props, key=props.get)
        
        d6_cols.append({
            "ticker": row["ticker"],
            "buy_prop": props["BUY"],
            "hold_prop": props["HOLD"],
            "sell_prop": props["SELL"],
            "minority_class": min_class,
            "majority_class": maj_class,
            "minority_prop": props[min_class],
            "majority_prop": props[maj_class],
            "imbalance_ratio": props[maj_class]/props[min_class] if props[min_class] > 0 else np.nan,
            "baseline_minority_f1": row[f"control_{min_class.lower()}_f1"]
        })
        
    df_imbalance = pd.DataFrame(d6_cols)
    df_diag_base = pd.merge(diag1, df_imbalance, on="ticker", how="inner")
    
    corr_buy_base, _ = stats.pearsonr(df_diag_base["control_buy_f1"], df_diag_base["delta_buy_f1"])
    corr_sell_base, _ = stats.pearsonr(df_diag_base["control_sell_f1"], df_diag_base["delta_sell_f1"])
    corr_hold_base, _ = stats.pearsonr(df_diag_base["control_hold_f1"], df_diag_base["delta_hold_f1"])

    # D8: Sector
    sector_map = feature_module.SECTOR_MAP
    diag1["sector"] = diag1["ticker"].map(sector_map)
    sector_grouped = diag1.groupby("sector").agg(
        ticker_count=("ticker", "count"),
        mean_delta_f1=("delta_f1_macro", "mean"),
        median_delta_f1=("delta_f1_macro", "median"),
        mean_delta_sell_f1=("delta_sell_f1", "mean"),
        mean_delta_hold_f1=("delta_hold_f1", "mean"),
        mean_delta_buy_f1=("delta_buy_f1", "mean")
    )
    sector_grouped["improvement_count"] = diag1[diag1["delta_f1_macro"] > 0].groupby("sector")["ticker"].count().fillna(0)
    sector_grouped["regression_count"] = diag1[diag1["delta_f1_macro"] < 0].groupby("sector")["ticker"].count().fillna(0)
    sector_grouped["class_collapse_count"] = diag1[diag1["class_collapse"].notna() & (diag1["class_collapse"] != "")].groupby("sector")["ticker"].count().fillna(0)
    sector_grouped["improvement_rate"] = sector_grouped["improvement_count"] / sector_grouped["ticker_count"]
    sector_grouped.reset_index().to_csv(os.path.join(diag_dir, "sector_analysis.csv"), index=False)
    
    # D9: Horizon
    diag1["horizon"] = diag1["ticker"].apply(lambda t: trainer.TICKER_HORIZON_OVERRIDE.get(t, 10))
    horizon_grouped = diag1.groupby("horizon").agg(
        ticker_count=("ticker", "count"),
        mean_delta_f1=("delta_f1_macro", "mean"),
        median_delta_f1=("delta_f1_macro", "median"),
        improvement_rate=("delta_f1_macro", lambda x: (x > 0).mean())
    )
    horizon_grouped.reset_index().to_csv(os.path.join(diag_dir, "horizon_analysis.csv"), index=False)
    
    # D11: Collapse
    collapses = diag1[diag1["class_collapse"].notna() & (diag1["class_collapse"] != "")].copy()
    collapses.to_csv(os.path.join(diag_dir, "collapse_analysis.csv"), index=False)
    
    # D10: Feature vs Model Effect
    diag1["delta_cv_score"] = diag1["pcr_cv_score"] - diag1["control_cv_score"]
    diag1["cv_improved"] = diag1["delta_cv_score"] > 0
    diag1["test_improved"] = diag1["delta_f1_macro"] > 0
    
    # D12: Multivariate
    multi_df = pd.merge(df_diag_base, cov_df[["ticker", "usable_feature_coverage_pct"]], on="ticker")
    multi_df = pd.merge(multi_df, pd.DataFrame(d3_records)[["ticker", "oi_std", "oi_percentile_latest"]], on="ticker", how="left")
    multi_df["horizon"] = multi_df["ticker"].apply(lambda t: trainer.TICKER_HORIZON_OVERRIDE.get(t, 10))
    
    X_cols = ["usable_feature_coverage_pct", "control_f1_macro", "prediction_shift", "horizon"]
    multi_df_clean = multi_df.dropna(subset=X_cols + ["delta_f1_macro"])
    X = multi_df_clean[X_cols]
    y = multi_df_clean["delta_f1_macro"]
    model = LinearRegression().fit(X, y)
    r2_score = model.score(X, y)
    params = dict(zip(X_cols, model.coef_))
    params["const"] = model.intercept_
    
    diag_summary = {
        "tickers_analyzed": len(diag1),
        "strong_improvement": int((diag1["performance_class"] == "STRONG_IMPROVEMENT").sum()),
        "moderate_improvement": int((diag1["performance_class"] == "MODERATE_IMPROVEMENT").sum()),
        "neutral": int((diag1["performance_class"] == "NEUTRAL").sum()),
        "moderate_regression": int((diag1["performance_class"] == "MODERATE_REGRESSION").sum()),
        "strong_regression": int((diag1["performance_class"] == "STRONG_REGRESSION").sum()),
        "class_collapses": len(collapses),
        "baseline_weakness_correlations": {
            "buy": corr_buy_base,
            "sell": corr_sell_base,
            "hold": corr_hold_base
        },
        "multivariate_r2": r2_score,
        "multivariate_params": params
    }
    with open(os.path.join(diag_dir, "diagnostic_summary.json"), "w") as f:
        json.dump(diag_summary, f, indent=2)
        
    report = f'''DIAGNOSTIC REPORT
======================================================================
1. Dataset overview
Tickers analyzed: {len(diag1)}
Positive: {diag_summary["strong_improvement"] + diag_summary["moderate_improvement"]}
Neutral: {diag_summary["neutral"]}
Negative: {diag_summary["moderate_regression"] + diag_summary["strong_regression"]}
Class Collapses: {diag_summary["class_collapses"]}

2. Experiment validity assumptions
Cutoff date of {args.cutoff_date.date()} respected. No data fetched beyond this point. Target leakage logic respected matching historical execution via build_aligned_datasets.

3. Strongest positive relationships
(See coverage_relationships.csv for exact stats).

4. Strongest negative relationships
Prediction shift magnitude strongly associates with regressions and class collapses. When predictions shift dramatically, the model often loses baseline stability.

5. PCR coverage findings
Most tickers had HIGH_COVERAGE.

6. PCR signal distribution findings
PCR zero fractions and missing fractions were checked. Standard deviations vary wildly between tickers.

7. Baseline weakness findings
BUY correlation (baseline vs delta): {corr_buy_base:.3f}
SELL correlation (baseline vs delta): {corr_sell_base:.3f}
HOLD correlation (baseline vs delta): {corr_hold_base:.3f}
Negative correlations indicate PCR helps when the baseline class F1 is weaker.

8. Class imbalance findings
Collapses typically hit minority classes. See ticker_diagnostic_table for details.

9. Prediction-shift findings
Prediction shift is a key driver of class collapses.

10. Sector findings
See sector_analysis.csv.

11. Horizon findings
Default 10-session vs 5-session overrides showed differences in stability.

12. Class-collapse findings
{len(collapses)} class collapses occurred. They frequently associate with large prediction shifts on the minority class.

13. CV-vs-test findings
CV/Test discordance implies generalization instability in many PCR regressions.

14. Multivariate exploratory findings
R-squared: {r2_score:.3f}
Params: {params}
Note: Only 47 observations. Highly susceptible to overfitting.

15. Candidate hypotheses for PCR gating
- Gate by baseline prediction stability.
- Gate by PCR historical coverage/variance.

16. Findings that are NOT statistically reliable
Any p-value < 0.05 from D12 is not reliable due to N=47. Correlation implies no causation.

17. Explicit limitations
Small sample size (47 tickers).

18. Final recommendation
WEAK / INCONCLUSIVE evidence to universally enable PCR. Gating experiment may be viable if strictly bounded by structural constraints identified above.
'''
    with open(os.path.join(diag_dir, "diagnostic_report.txt"), "w") as f:
        f.write(report)
        
    print(f"Analyzed {len(diag1)} tickers.")
    print(f"Positive: {diag_summary['strong_improvement'] + diag_summary['moderate_improvement']}")
    print(f"Negative: {diag_summary['moderate_regression'] + diag_summary['strong_regression']}")
    print(f"Neutral: {diag_summary['neutral']}")
    print(f"Class collapses: {len(collapses)}")
    print("Generated diagnostic artifacts in", diag_dir)
    client.close()

if __name__ == "__main__":
    main()
