import os
import json
import uuid
import pandas as pd
import numpy as np
from datetime import datetime, timezone

def generate_gating_analysis(csv_path: str):
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    out_dir = os.path.join(os.path.dirname(os.path.dirname(csv_path)), "gating", run_id)
    os.makedirs(out_dir, exist_ok=True)
    
    df = pd.read_csv(csv_path)
    df["cutoff_date"] = pd.to_datetime(df["cutoff_date"])
    df["class_collapse"] = df["class_collapse"].notna()
    df = df.sort_values(["ticker", "cutoff_date"]).reset_index(drop=True)
    
    results = []
    
    for ticker, group in df.groupby("ticker"):
        history = []
        for _, row in group.iterrows():
            cutoff = row["cutoff_date"]
            
            # Historical stats
            obs_count = len(history)
            if obs_count > 0:
                hist_df = pd.DataFrame(history)
                hist_mean_delta = hist_df["delta_f1_macro"].mean()
                hist_median_delta = hist_df["delta_f1_macro"].median()
                hist_consistency = (hist_df["delta_f1_macro"] > 0).mean()
                hist_collapse_rate = hist_df["class_collapse"].mean()
                hist_pred_shift = hist_df["prediction_shift"].mean()
            else:
                hist_mean_delta = 0.0
                hist_median_delta = 0.0
                hist_consistency = 0.0
                hist_collapse_rate = 0.0
                hist_pred_shift = 0.0
                
            delta_cv_score = row["delta_cv_score"]
            
            # Gates
            gates = {}
            # Gate A: Historical Momentum
            gates["Gate_A"] = (hist_mean_delta > 0) if obs_count > 0 else False
            # Gate B: Conservative Historical
            gates["Gate_B"] = (hist_mean_delta > 0 and hist_collapse_rate == 0) if obs_count > 0 else False
            # Gate C: CV-Driven
            gates["Gate_C"] = (delta_cv_score > 0)
            # Gate D: Hybrid
            gates["Gate_D"] = (hist_mean_delta > 0) if obs_count > 0 else (delta_cv_score > 0.005)
            # Gate E: Hybrid Conservative
            gates["Gate_E"] = (hist_mean_delta > 0 and hist_collapse_rate == 0) if obs_count > 0 else (delta_cv_score > 0.005)
            
            res_row = {
                "ticker": ticker,
                "cutoff_date": cutoff.strftime("%Y-%m-%d"),
                "is_t1": obs_count == 0,
                "horizon": row["horizon"],
                "historical_observation_count": obs_count,
                "historical_mean_delta_f1": hist_mean_delta,
                "historical_median_delta_f1": hist_median_delta,
                "historical_consistency": hist_consistency,
                "historical_collapse_rate": hist_collapse_rate,
                "historical_prediction_shift": hist_pred_shift,
                "delta_cv_score": delta_cv_score,
                "control_f1": row["control_f1_macro"],
                "pcr_f1": row["pcr_f1_macro"],
                "actual_delta_f1": row["delta_f1_macro"],
                "class_collapse": row["class_collapse"],
                "target_cutoff_validation_passed": row["target_cutoff_validation_passed"],
                "target_leakage_violations": row["target_leakage_violations"]
            }
            
            for g_name, g_val in gates.items():
                res_row[f"{g_name}_enabled"] = g_val
                res_row[f"{g_name}_f1"] = row["pcr_f1_macro"] if g_val else row["control_f1_macro"]
                res_row[f"{g_name}_delta_vs_control"] = res_row[f"{g_name}_f1"] - row["control_f1_macro"]
                
            results.append(res_row)
            
            # Add to history for next walk-forward step
            history.append({
                "delta_f1_macro": row["delta_f1_macro"],
                "class_collapse": row["class_collapse"],
                "prediction_shift": row["prediction_shift"]
            })
            
    res_df = pd.DataFrame(results)
    res_df.to_csv(os.path.join(out_dir, "gating_observations.csv"), index=False)
    
    # Analyze by gate
    gate_names = ["Gate_A", "Gate_B", "Gate_C", "Gate_D", "Gate_E"]
    summary = []
    
    for split_name, split_df in [("All (T1-T5)", res_df), ("T1 Only (Cold Start)", res_df[res_df["is_t1"]]), ("T2-T5 (Warm)", res_df[~res_df["is_t1"]])]:
        if len(split_df) == 0:
            continue
            
        control_mean = split_df["control_f1"].mean()
        pcr_mean = split_df["pcr_f1"].mean()
        pcr_delta = pcr_mean - control_mean
        pcr_collapse = split_df["class_collapse"].sum()
        
        summary.append({
            "Split": split_name,
            "Strategy": "Control",
            "Mean_F1": control_mean,
            "Delta_vs_Control": 0.0,
            "Enabled_Count": 0,
            "Enabled_Rate": 0.0,
            "Collapse_Count": 0 # Baseline
        })
        summary.append({
            "Split": split_name,
            "Strategy": "PCR Always",
            "Mean_F1": pcr_mean,
            "Delta_vs_Control": pcr_delta,
            "Enabled_Count": len(split_df),
            "Enabled_Rate": 1.0,
            "Collapse_Count": pcr_collapse
        })
        
        for g_name in gate_names:
            g_f1_mean = split_df[f"{g_name}_f1"].mean()
            g_delta = g_f1_mean - control_mean
            g_enabled = split_df[f"{g_name}_enabled"].sum()
            g_collapse = split_df[split_df[f"{g_name}_enabled"]]["class_collapse"].sum()
            
            summary.append({
                "Split": split_name,
                "Strategy": g_name,
                "Mean_F1": g_f1_mean,
                "Delta_vs_Control": g_delta,
                "Enabled_Count": g_enabled,
                "Enabled_Rate": g_enabled / len(split_df),
                "Collapse_Count": g_collapse
            })
            
    sum_df = pd.DataFrame(summary)
    sum_df.to_csv(os.path.join(out_dir, "gating_summary.csv"), index=False)
    
    # Save config
    config = {
        "source_csv": csv_path,
        "gates": {
            "Gate_A": "historical_mean_delta_f1 > 0 (Default Control at T1)",
            "Gate_B": "historical_mean_delta_f1 > 0 AND historical_collapse_rate == 0 (Default Control at T1)",
            "Gate_C": "current_cutoff delta_cv_score > 0",
            "Gate_D": "historical_mean_delta_f1 > 0 OR (T1 AND delta_cv_score > 0.005)"
        },
        "observation_count": len(res_df)
    }
    with open(os.path.join(out_dir, "gating_config.json"), "w") as f:
        json.dump(config, f, indent=2)
        
    print(f"Results saved to {out_dir}")
    print(sum_df.to_string())
    
    # Save JSON summary for easy reading later
    sum_df.to_json(os.path.join(out_dir, "statistical_summary.json"), orient="records", indent=2)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        csv_path = "experiments/stock_pcr/multicutoff/run_20260814_084104_583c0e08/per_ticker_cutoff_results.csv"
    generate_gating_analysis(csv_path)
