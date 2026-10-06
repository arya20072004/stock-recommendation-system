"""
src.ml.training.metrics
Comprehensive Evaluation & Paired Clustered Bootstrap Framework for V2 Architecture.
Covers Classification, Calibration, Financial Usefulness, and Stability.
"""

from typing import Dict, Any, List, Optional, Tuple, Union
import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    log_loss,
    confusion_matrix,
)


def compute_expected_calibration_error(
    y_true: np.ndarray,
    probs: np.ndarray,
    n_bins: int = 10,
) -> float:
    """
    Computes Expected Calibration Error (ECE) across confidence bins:
      ECE = sum_m (|B_m| / N) * |acc(B_m) - conf(B_m)|
    """
    y_true = np.asarray(y_true, dtype=int)
    probs = np.asarray(probs, dtype=float)

    confidences = np.max(probs, axis=1)
    predictions = np.argmax(probs, axis=1)
    accuracies = (predictions == y_true).astype(float)

    bin_boundaries = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    n_total = len(y_true)

    if n_total == 0:
        return 0.0

    for i in range(n_bins):
        in_bin = (confidences > bin_boundaries[i]) & (confidences <= bin_boundaries[i + 1])
        bin_count = np.sum(in_bin)
        if bin_count > 0:
            bin_acc = np.mean(accuracies[in_bin])
            bin_conf = np.mean(confidences[in_bin])
            ece += (bin_count / n_total) * np.abs(bin_acc - bin_conf)

    return float(ece)


def compute_multiclass_brier_score(
    y_true: np.ndarray,
    probs: np.ndarray,
    num_classes: int = 3,
) -> float:
    """
    Computes multi-class Brier score:
      Brier = (1 / N) * sum_i sum_k (p_{i,k} - y_{i,k})^2
    """
    y_true = np.asarray(y_true, dtype=int)
    probs = np.asarray(probs, dtype=float)
    n_samples = len(y_true)

    if n_samples == 0:
        return 0.0

    one_hot = np.zeros((n_samples, num_classes), dtype=float)
    for k in range(num_classes):
        one_hot[y_true == k, k] = 1.0

    squared_diff = np.sum((probs - one_hot) ** 2, axis=1)
    return float(np.mean(squared_diff))


def compute_classification_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    num_classes: int = 3,
) -> Dict[str, Any]:
    """Computes standard 3-class metrics."""
    y_t = np.asarray(y_true, dtype=int)
    y_p = np.asarray(y_pred, dtype=int)
    labels = list(range(num_classes))
    class_names = {0: "SELL", 1: "HOLD", 2: "BUY"}

    acc = float(accuracy_score(y_t, y_p))
    bal_acc = float(balanced_accuracy_score(y_t, y_p))
    macro_f1 = float(f1_score(y_t, y_p, average="macro", zero_division=0))

    cm = confusion_matrix(y_t, y_p, labels=labels)
    cm_dict = {
        class_names[r]: {class_names[c]: int(cm[r, c]) for c in labels}
        for r in labels
    }

    per_class = {}
    for k in labels:
        name = class_names[k]
        tp = cm[k, k]
        fp = cm[:, k].sum() - tp
        fn = cm[k, :].sum() - tp
        support = cm[k, :].sum()

        prec = float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0
        rec = float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0
        f1 = float(2 * prec * rec / (prec + rec)) if (prec + rec) > 0 else 0.0

        per_class[name] = {
            "precision": round(prec, 4),
            "recall": round(rec, 4),
            "f1": round(f1, 4),
            "support": int(support),
            "predicted_count": int(cm[:, k].sum()),
        }

    return {
        "accuracy": round(acc, 4),
        "balanced_accuracy": round(bal_acc, 4),
        "macro_f1": round(macro_f1, 4),
        "per_class": per_class,
        "confusion_matrix": cm_dict,
    }


def compute_probability_metrics(
    y_true: np.ndarray,
    probs: np.ndarray,
    num_classes: int = 3,
) -> Dict[str, Any]:
    """Computes calibration and log-loss metrics."""
    y_t = np.asarray(y_true, dtype=int)
    p = np.asarray(probs, dtype=float)

    # Safe log-loss with clipping
    p_clipped = np.clip(p, 1e-12, 1.0 - 1e-12)
    p_norm = p_clipped / p_clipped.sum(axis=1, keepdims=True)

    try:
        loss = float(log_loss(y_t, p_norm, labels=list(range(num_classes))))
    except Exception:
        loss = float("nan")

    brier = compute_multiclass_brier_score(y_t, p_norm, num_classes=num_classes)
    ece = compute_expected_calibration_error(y_t, p_norm)

    return {
        "log_loss": round(loss, 4),
        "brier_score": round(brier, 4),
        "expected_calibration_error": round(ece, 4),
        "mean_confidence": round(float(np.mean(np.max(p_norm, axis=1))), 4),
    }


def compute_economic_metrics(
    y_pred: np.ndarray,
    actual_returns: np.ndarray,
) -> Dict[str, Any]:
    """Computes financial return profiles conditioned on model recommendations."""
    y_p = np.asarray(y_pred, dtype=int)
    rets = np.asarray(actual_returns, dtype=float)

    class_names = {0: "SELL", 1: "HOLD", 2: "BUY"}
    econ = {}

    for k in [0, 1, 2]:
        mask = (y_p == k)
        k_rets = rets[mask]
        name = class_names[k]

        if len(k_rets) > 0:
            mean_ret = float(np.mean(k_rets))
            median_ret = float(np.median(k_rets))
            win_rate = float(np.mean(k_rets > 0)) if k == 2 else (float(np.mean(k_rets < 0)) if k == 0 else float(np.mean(np.abs(k_rets) <= 0.015)))
            loss_rate = float(np.mean(k_rets < 0))

            econ[name] = {
                "count": int(len(k_rets)),
                "mean_return": round(mean_ret, 4),
                "median_return": round(median_ret, 4),
                "win_rate": round(win_rate, 4),
                "loss_rate": round(loss_rate, 4),
            }
        else:
            econ[name] = {
                "count": 0,
                "mean_return": 0.0,
                "median_return": 0.0,
                "win_rate": 0.0,
                "loss_rate": 0.0,
            }

    return econ


def compute_stability_metrics(
    df: pd.DataFrame,
    y_true_col: str = "actual_class_idx",
    y_pred_col: str = "pred_class_idx",
    ticker_col: str = "symbol",
    cohort_col: str = "market_date",
) -> Dict[str, Any]:
    """Computes cross-sectional and temporal stability metrics."""
    ticker_accs = {}
    for tick, grp in df.groupby(ticker_col):
        acc = (grp[y_pred_col] == grp[y_true_col]).mean()
        ticker_accs[tick] = float(acc)

    cohort_accs = {}
    for c_date, grp in df.groupby(cohort_col):
        acc = (grp[y_pred_col] == grp[y_true_col]).mean()
        cohort_accs[str(c_date)[:10]] = float(acc)

    t_values = list(ticker_accs.values())
    c_values = list(cohort_accs.values())

    return {
        "ticker_min_accuracy": round(min(t_values), 4) if t_values else 0.0,
        "ticker_mean_accuracy": round(float(np.mean(t_values)), 4) if t_values else 0.0,
        "ticker_std_accuracy": round(float(np.std(t_values)), 4) if t_values else 0.0,
        "cohort_min_accuracy": round(min(c_values), 4) if c_values else 0.0,
        "cohort_mean_accuracy": round(float(np.mean(c_values)), 4) if c_values else 0.0,
        "cohort_std_accuracy": round(float(np.std(c_values)), 4) if c_values else 0.0,
    }


def evaluate_full_metrics_suite(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    probs: np.ndarray,
    actual_returns: Optional[np.ndarray] = None,
    df_meta: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """Aggregates all evaluation dimensions into a single unified report dictionary."""
    results = {
        "classification": compute_classification_metrics(y_true, y_pred),
        "probability": compute_probability_metrics(y_true, probs),
    }
    if actual_returns is not None:
        results["economic"] = compute_economic_metrics(y_pred, actual_returns)
    if df_meta is not None:
        results["stability"] = compute_stability_metrics(df_meta)
    return results


def paired_clustered_bootstrap(
    df: pd.DataFrame,
    incumbent_correct_col: str,
    challenger_correct_col: str,
    cluster_col: str = "market_date",
    n_bootstrap: int = 2000,
    ci_level: float = 0.95,
    random_seed: int = 42,
) -> Dict[str, Any]:
    """
    Performs Paired Clustered Bootstrap on accuracy delta (Challenger - Incumbent).
    Resamples clusters with replacement, preserving exact pairing within clusters.
    """
    rng = np.random.default_rng(random_seed)

    clusters = df[cluster_col].unique()
    n_clusters = len(clusters)

    # Point estimate on full dataset
    inc_full = df[incumbent_correct_col].astype(float)
    chal_full = df[challenger_correct_col].astype(float)
    point_estimate_delta = float((chal_full - inc_full).mean())

    if n_clusters < 2:
        return {
            "point_estimate": point_estimate_delta,
            "lower_ci": point_estimate_delta,
            "upper_ci": point_estimate_delta,
            "n_bootstrap": n_bootstrap,
            "cluster_col": cluster_col,
            "n_clusters": n_clusters,
            "p_value_two_sided": 1.0,
        }

    # Pre-aggregate sum and counts per cluster for high performance
    cluster_data = []
    for c in clusters:
        sub = df[df[cluster_col] == c]
        inc_correct = sub[incumbent_correct_col].sum()
        chal_correct = sub[challenger_correct_col].sum()
        count = len(sub)
        cluster_data.append((inc_correct, chal_correct, count))

    inc_arr = np.array([x[0] for x in cluster_data])
    chal_arr = np.array([x[1] for x in cluster_data])
    cnt_arr = np.array([x[2] for x in cluster_data])

    boot_deltas = np.zeros(n_bootstrap, dtype=float)

    for b in range(n_bootstrap):
        sampled_indices = rng.integers(0, n_clusters, size=n_clusters)
        total_obs = cnt_arr[sampled_indices].sum()
        if total_obs > 0:
            chal_acc = chal_arr[sampled_indices].sum() / total_obs
            inc_acc = inc_arr[sampled_indices].sum() / total_obs
            boot_deltas[b] = chal_acc - inc_acc
        else:
            boot_deltas[b] = 0.0

    alpha = 1.0 - ci_level
    lower_pct = (alpha / 2.0) * 100.0
    upper_pct = (1.0 - alpha / 2.0) * 100.0

    lower_ci = float(np.percentile(boot_deltas, lower_pct))
    upper_ci = float(np.percentile(boot_deltas, upper_pct))

    # Two-sided empirical p-value for H0: delta <= 0
    p_val = float(2.0 * min(np.mean(boot_deltas <= 0), np.mean(boot_deltas >= 0)))

    return {
        "point_estimate": round(point_estimate_delta, 6),
        "lower_ci": round(lower_ci, 6),
        "upper_ci": round(upper_ci, 6),
        "n_bootstrap": n_bootstrap,
        "cluster_col": cluster_col,
        "n_clusters": int(n_clusters),
        "p_value_two_sided": round(p_val, 6),
        "statistically_superior": bool(lower_ci > 0.0 and p_val < 0.05),
    }
