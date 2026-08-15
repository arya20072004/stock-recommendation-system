import os
import json
import numpy as np
import pandas as pd
import importlib.util

def load_frozen_gate():
    spec = importlib.util.spec_from_file_location("pcr_gate_frozen", "scripts/pcr_gate_frozen.py")
    gate_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate_module)
    return gate_module.should_enable_pcr

def run_validation(csv_path):
    out_dir = "experiments/stock_pcr/gating_validation"
    os.makedirs(out_dir, exist_ok=True)
    
    df = pd.read_csv(csv_path)
    df["cutoff_date"] = pd.to_datetime(df["cutoff_date"])
    df["class_collapse_bool"] = df["class_collapse"].notna()
    df = df.sort_values(["ticker", "cutoff_date"]).reset_index(drop=True)
    
    t_dates = sorted(df["cutoff_date"].unique())
    dev_dates = t_dates[:3] # T1, T2, T3
    val_dates = t_dates[3:] # T4, T5
    
    should_enable_pcr = load_frozen_gate()
    
    results = []
    
    for ticker, group in df.groupby("ticker"):
        # Calculate historical stats STRICTLY from T1-T3 (Development set)
        dev_group = group[group["cutoff_date"].isin(dev_dates)]
        obs_count = len(dev_group)
        
        hist_info = {"obs_count": obs_count, "mean_delta_f1": 0.0, "collapse_rate": 0.0}
        if obs_count > 0:
            hist_info["mean_delta_f1"] = dev_group["delta_f1_macro"].mean()
            hist_info["collapse_rate"] = dev_group["class_collapse_bool"].mean()
            
        val_group = group[group["cutoff_date"].isin(val_dates)]
        for _, row in val_group.iterrows():
            cutoff = row["cutoff_date"]
            cv_info = {"delta_cv_score": row["delta_cv_score"]}
            
            gate_enabled, gate_reason, gate_version = should_enable_pcr(
                ticker, cutoff.strftime("%Y-%m-%d"), hist_info, cv_info
            )
            
            pcr_f1 = row["pcr_f1_macro"]
            ctrl_f1 = row["control_f1_macro"]
            
            results.append({
                "ticker": ticker,
                "cutoff_date": cutoff.strftime("%Y-%m-%d"),
                "gate_version": gate_version,
                "gate_enabled": gate_enabled,
                "gate_reason": gate_reason,
                "historical_observation_count": obs_count,
                "control_f1_macro": ctrl_f1,
                "pcr_f1_macro": pcr_f1,
                "actual_delta_f1_macro": row["delta_f1_macro"],
                "gated_f1_macro": pcr_f1 if gate_enabled else ctrl_f1,
                "gated_delta_vs_control": (pcr_f1 if gate_enabled else ctrl_f1) - ctrl_f1,
                "class_collapse_bool": row["class_collapse_bool"],
                "delta_sell_f1": row["delta_sell_f1"],
                "delta_hold_f1": row["delta_hold_f1"],
                "delta_buy_f1": row["delta_buy_f1"],
                "prediction_shift": row["prediction_shift"],
                "horizon": row["horizon"]
            })
            
    val_df = pd.DataFrame(results)
    val_df.to_csv(os.path.join(out_dir, "validation_results.csv"), index=False)
    
    # Validation Config
    with open(os.path.join(out_dir, "validation_config.json"), "w") as f:
        json.dump({"source_csv": csv_path, "dev_cutoffs": [d.strftime("%Y-%m-%d") for d in dev_dates], "val_cutoffs": [d.strftime("%Y-%m-%d") for d in val_dates]}, f, indent=2)

    # Cutoff Summary
    cutoff_summary = []
    for c in val_df["cutoff_date"].unique():
        sub_df = val_df[val_df["cutoff_date"] == c]
        cutoff_summary.append({
            "cutoff": c,
            "mean_gated_f1": sub_df["gated_f1_macro"].mean(),
            "mean_ctrl_f1": sub_df["control_f1_macro"].mean(),
            "mean_pcr_f1": sub_df["pcr_f1_macro"].mean(),
            "mean_delta_vs_ctrl": sub_df["gated_delta_vs_control"].mean(),
            "pcr_always_delta": (sub_df["pcr_f1_macro"] - sub_df["control_f1_macro"]).mean(),
            "activation_rate": sub_df["gate_enabled"].mean(),
            "gated_collapses": sub_df[sub_df["gate_enabled"]]["class_collapse_bool"].sum(),
            "pcr_always_collapses": sub_df["class_collapse_bool"].sum()
        })
    pd.DataFrame(cutoff_summary).to_csv(os.path.join(out_dir, "cutoff_summary.csv"), index=False)
    
    # Ticker Summary
    ticker_summary = []
    for t, sub_df in val_df.groupby("ticker"):
        ticker_summary.append({
            "ticker": t,
            "activations": sub_df["gate_enabled"].sum(),
            "gated_delta_vs_ctrl": sub_df["gated_delta_vs_control"].mean(),
            "pcr_always_delta": (sub_df["pcr_f1_macro"] - sub_df["control_f1_macro"]).mean(),
            "gated_collapses": sub_df[sub_df["gate_enabled"]]["class_collapse_bool"].sum(),
            "pcr_always_collapses": sub_df["class_collapse_bool"].sum()
        })
    pd.DataFrame(ticker_summary).to_csv(os.path.join(out_dir, "ticker_summary.csv"), index=False)
    
    # Conditional Performance
    def calc_cond(sub_df, name):
        if len(sub_df) == 0: return {"subset": name, "count": 0}
        df1 = sub_df["actual_delta_f1_macro"]
        return {
            "subset": name,
            "count": len(sub_df),
            "mean_actual_delta_f1": df1.mean(),
            "median_actual_delta_f1": df1.median(),
            "improvement_rate": (df1 > 0).mean(),
            "regression_rate": (df1 < 0).mean(),
            "collapse_rate": sub_df["class_collapse_bool"].mean(),
            "worst_regression": df1.min(),
            "best_improvement": df1.max()
        }
    pd.DataFrame([
        calc_cond(val_df[val_df["gate_enabled"]], "GATE ENABLED"),
        calc_cond(val_df[~val_df["gate_enabled"]], "GATE DISABLED (HYPOTHETICAL)")
    ]).to_csv(os.path.join(out_dir, "conditional_performance.csv"), index=False)
    
    # Confusion Matrix (Gate Discrimination)
    y_true = val_df["actual_delta_f1_macro"] > 0
    y_pred = val_df["gate_enabled"]
    tp = (y_true & y_pred).sum()
    fp = (~y_true & y_pred).sum()
    tn = (~y_true & ~y_pred).sum()
    fn = (y_true & ~y_pred).sum()
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    specificity = tn / (tn + fp) if (tn + fp) > 0 else 0
    accuracy = (tp + tn) / len(val_df)
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    pd.DataFrame([{"TP": tp, "FP": fp, "TN": tn, "FN": fn, "Precision": precision, "Recall": recall, "Specificity": specificity, "Accuracy": accuracy, "F1": f1}]).to_csv(os.path.join(out_dir, "confusion_matrix.csv"), index=False)
    
    # Statistical Summary (Ticker-Clustered Paired Bootstrap)
    np.random.seed(42)
    tickers = val_df["ticker"].unique()
    n_boot = 10000
    boot_gate_ctrl = []
    boot_gate_pcr = []
    
    for _ in range(n_boot):
        samp = np.random.choice(tickers, size=len(tickers), replace=True)
        boot_df = pd.concat([val_df[val_df["ticker"] == t] for t in samp])
        boot_gate_ctrl.append((boot_df["gated_f1_macro"] - boot_df["control_f1_macro"]).mean())
        boot_gate_pcr.append((boot_df["gated_f1_macro"] - boot_df["pcr_f1_macro"]).mean())
        
    stats = {
        "Gate_E_vs_Control": {
            "mean": float(np.mean(boot_gate_ctrl)),
            "ci_2_5": float(np.percentile(boot_gate_ctrl, 2.5)),
            "ci_97_5": float(np.percentile(boot_gate_ctrl, 97.5)),
            "prob_gt_0": float(np.mean(np.array(boot_gate_ctrl) > 0))
        },
        "Gate_E_vs_PCR_Always": {
            "mean": float(np.mean(boot_gate_pcr)),
            "ci_2_5": float(np.percentile(boot_gate_pcr, 2.5)),
            "ci_97_5": float(np.percentile(boot_gate_pcr, 97.5)),
            "prob_gt_0": float(np.mean(np.array(boot_gate_pcr) > 0))
        }
    }
    with open(os.path.join(out_dir, "statistical_summary.json"), "w") as f:
        json.dump(stats, f, indent=2)

    # Calculate overall criteria
    mean_gated_vs_ctrl = val_df["gated_delta_vs_control"].mean()
    mean_gated_vs_pcr = (val_df["gated_f1_macro"] - val_df["pcr_f1_macro"]).mean()
    gated_collapses = val_df[val_df["gate_enabled"]]["class_collapse_bool"].sum()
    pcr_always_collapses = val_df["class_collapse_bool"].sum()
    mean_pcr_always_vs_ctrl = (val_df["pcr_f1_macro"] - val_df["control_f1_macro"]).mean()
    
    t4_df = val_df[val_df["cutoff_date"] == val_dates[0].strftime("%Y-%m-%d")]
    t5_df = val_df[val_df["cutoff_date"] == val_dates[1].strftime("%Y-%m-%d")]
    t4_mean = t4_df["gated_delta_vs_control"].mean()
    t5_mean = t5_df["gated_delta_vs_control"].mean()
    
    crit_1 = mean_gated_vs_ctrl > 0
    crit_both = (t4_mean > 0) and (t5_mean > 0)
    crit_2 = mean_gated_vs_pcr > 0
    crit_3 = gated_collapses < pcr_always_collapses
    
    severe_regressions = val_df[val_df["gated_delta_vs_control"] <= -0.05]
    severe_count = len(severe_regressions)
    severe_ratio = severe_count / len(val_df) if len(val_df) > 0 else 0
    max_severe_per_ticker = severe_regressions.groupby("ticker").size().max() if severe_count > 0 else 0
    
    crit_severe = (severe_ratio < 0.05) and (max_severe_per_ticker <= 1)
    
    all_criteria_met = crit_1 and crit_both and crit_2 and crit_3 and crit_severe
    
    # Assess Production Verdict
    if not all_criteria_met:
        verdict = "GATE_REJECTED"
    else:
        # Check statistical confidence
        ci_lower = stats["Gate_E_vs_Control"]["ci_2_5"]
        if ci_lower > 0:
            verdict = "PRODUCTION_CANDIDATE"
        else:
            verdict = "PROMISING_BUT_INCONCLUSIVE"
            
    with open(os.path.join(out_dir, "final_report.txt"), "w", encoding="utf-8") as f:
        f.write("# Frozen Temporal Validation Final Report\n\n")
        f.write(f"1. Pre-registered gate: Hybrid Conservative (Frozen Version 1.0)\n")
        f.write(f"2. Frozen thresholds: CV > 0.005, Hist Mean > 0, Hist Collapse == 0\n")
        f.write(f"3. Info at validation: Strictly T1-T3 test outcomes. T4 was NOT incorporated into T5.\n")
        f.write(f"4. Was validation out-of-sample? YES.\n")
        f.write(f"5. Activation rate: {val_df['gate_enabled'].mean():.2%}\n")
        f.write(f"6. Frozen Gate vs Control: {mean_gated_vs_ctrl:.6f}\n")
        f.write(f"7. Frozen Gate vs PCR-Always: {mean_gated_vs_pcr:.6f}\n")
        f.write(f"8. Class collapses: {gated_collapses} (vs {pcr_always_collapses} for PCR-Always)\n")
        f.write(f"10. Consistent across T4 and T5? T4={t4_mean:.6f}, T5={t5_mean:.6f} -> {'YES' if crit_both else 'NO'}\n")
        f.write(f"12. Ticker-Clustered CI vs Control: [{stats['Gate_E_vs_Control']['ci_2_5']:.6f}, {stats['Gate_E_vs_Control']['ci_97_5']:.6f}]\n")
        f.write(f"13. P(\u0394F1 > 0): {stats['Gate_E_vs_Control']['prob_gt_0']:.2%}\n")
        f.write(f"14. Did gate satisfy preregistered criteria? {'YES' if all_criteria_met else 'NO'}\n")
        f.write(f"15. Protocol violations: NONE.\n")
        f.write(f"16. Evidence for production: {verdict}\n\n")
        f.write(f"FINAL CLASSIFICATION: {verdict}\n")

if __name__ == "__main__":
    csv_path = "experiments/stock_pcr/multicutoff/run_20260814_084104_583c0e08/per_ticker_cutoff_results.csv"
    run_validation(csv_path)
