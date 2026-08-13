"""Isolated, read-only A/B retraining experiment for stock-level PCR.

CONTROL uses the frozen production feature set. PCR adds only
``stock_pcr_oi`` and ``stock_pcr_chg_5d``. The script creates local artifacts
under ``experiments/stock_pcr`` and never saves models or writes MongoDB.
"""

import argparse
import csv
import json
import os
import random
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone

import numpy as np
import optuna
import pandas as pd
from dotenv import dotenv_values
from imblearn.over_sampling import SMOTE
from pymongo import MongoClient
from sklearn.metrics import accuracy_score, f1_score, precision_recall_fscore_support
from xgboost import XGBClassifier


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.data.nifty50 import TICKERS
from src.features.router import get_feature_pipeline_hash, resolve_feature_pipeline
from src.ml import trainer


DEFAULT_CUTOFF_DATE = "2026-08-10"
RANDOM_STATE = 42
PCR_FEATURES = ("stock_pcr_oi", "stock_pcr_chg_5d")
CONTROL_EXCLUDED_FEATURES = frozenset(PCR_FEATURES)
REQUIRED_COVERAGE_COLUMNS = {
    "ticker",
    "coverage_classification",
    "feature_usability",
}
SELL_BOOST_OVERRIDES = {"TRENT.NS": 2.0}
CLASS_NAMES = {0: "SELL", 1: "HOLD", 2: "BUY"}


def parse_date(value):
    try:
        return pd.Timestamp(value).normalize()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'; expected YYYY-MM-DD."
        ) from exc


def json_safe(value):
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return value


def read_eligible_tickers(coverage_path):
    coverage = pd.read_csv(coverage_path)
    missing = REQUIRED_COVERAGE_COLUMNS - set(coverage.columns)
    if missing:
        raise ValueError(
            f"Coverage CSV is missing required columns: {sorted(missing)}"
        )

    canonical_tickers = set(TICKERS)
    unknown = sorted(set(coverage["ticker"]) - canonical_tickers)
    if unknown:
        raise ValueError(f"Coverage CSV contains non-canonical tickers: {unknown}")

    high_coverage = coverage.loc[
        coverage["coverage_classification"].eq("HIGH_COVERAGE")
        & coverage["feature_usability"].eq("USABLE"),
        "ticker",
    ].tolist()
    eligible = [ticker for ticker in TICKERS if ticker in set(high_coverage)]
    excluded = [ticker for ticker in TICKERS if ticker not in set(eligible)]

    if not eligible:
        raise ValueError("Coverage CSV does not contain any HIGH_COVERAGE/USABLE tickers.")
    return eligible, excluded


def build_aligned_datasets(ticker, client, cutoff_date, feature_module):
    """Use the current production dataset builder, then align shifted PCR by date."""
    source_df = trainer.create_dataset(ticker, client)
    if source_df.empty:
        raise ValueError("Production feature dataset is empty.")

    source_df = source_df.copy()
    source_df.index = pd.to_datetime(source_df.index).tz_localize(None)
    source_df = source_df.loc[source_df.index <= cutoff_date].sort_index()
    if source_df.empty:
        raise ValueError("No production feature rows remain at the requested cutoff.")

    # --- TARGET LEAKAGE FIX ---
    horizon = trainer.TICKER_HORIZON_OVERRIDE.get(ticker, 10)
    db = client["stock_market_db"]
    trading_dates_docs = list(db.historical_data.find(
        {"ticker": ticker, "date": {"$lte": cutoff_date.to_pydatetime()}},
        {"date": 1, "_id": 0}
    ).sort("date", 1))
    
    if not trading_dates_docs:
        raise ValueError("No historical data found in MongoDB up to the cutoff date.")
        
    trading_dates = [pd.to_datetime(doc["date"]).tz_localize(None) for doc in trading_dates_docs]
    invalid_dates = set(trading_dates[-horizon:]) if horizon > 0 else set()
    
    original_rows_before_cutoff = len(source_df)
    source_df = source_df.loc[~source_df.index.isin(invalid_dates)].copy()
    rows_removed_for_cutoff = original_rows_before_cutoff - len(source_df)
    
    print(f"  [Leakage Fix] original_rows_before_cutoff={original_rows_before_cutoff}")
    print(f"  [Leakage Fix] rows_removed_for_cutoff={rows_removed_for_cutoff}")
    print(f"  [Leakage Fix] final_eligible_rows={len(source_df)}")
    print(f"  [Leakage Fix] horizon={horizon}")
    print(f"  [Leakage Fix] cutoff_date={cutoff_date.date()}")

    if source_df.empty:
        raise ValueError("No production feature rows remain after target-leakage cutoff enforcement.")
        
    final_feature_date_min = source_df.index.min().date()
    final_feature_date_max = source_df.index.max().date()
    print(f"  [Leakage Fix] final_feature_date_min={final_feature_date_min}")
    print(f"  [Leakage Fix] final_feature_date_max={final_feature_date_max}")

    # Verify no retained target depends on a price date > cutoff_date
    date_to_idx = {d: i for i, d in enumerate(trading_dates)}
    max_retained_idx = max(date_to_idx[d] for d in source_df.index)
    required_future_idx = max_retained_idx + horizon
    required_future_date = trading_dates[required_future_idx].date()
    print(f"  [Leakage Fix] latest_required_future_date={required_future_date}")

    assert final_feature_date_max <= cutoff_date.date(), "Retained feature date exceeds cutoff_date"
    assert required_future_date <= cutoff_date.date(), f"Target requires future date {required_future_date} > {cutoff_date.date()}"
    # --------------------------

    all_candidate_features = trainer._make_feature_list(source_df)
    control_features = [
        feature for feature in all_candidate_features
        if feature not in CONTROL_EXCLUDED_FEATURES
    ]
    experiment_features = control_features + list(PCR_FEATURES)
    feature_difference = set(experiment_features) - set(control_features)
    if feature_difference != set(PCR_FEATURES) or len(experiment_features) != len(control_features) + 2:
        raise ValueError(
            "Feature integrity failure: experiment minus control is not exactly "
            f"{sorted(PCR_FEATURES)}."
        )
    missing = [feature for feature in experiment_features + ["target"] if feature not in source_df]
    if missing:
        raise ValueError(f"Production dataset is missing required features: {missing}")

    # Rebuild the source series using the production helper. It calculates
    # pcr_oi.diff(5) and then shifts one observation for leakage prevention.
    # Request a short pre-window so the first retained feature rows preserve
    # the production helper's existing five-observation PCR history.
    pcr_df = feature_module._prepare_stock_pcr_data(
        ticker,
        client,
        source_df.index.min() - pd.Timedelta(days=60),
        cutoff_date,
    )
    if pcr_df.empty:
        raise ValueError("Stock PCR helper returned no data for an eligible ticker.")
    pcr_df.index = pd.to_datetime(pcr_df.index).tz_localize(None)
    aligned_pcr = pcr_df.loc[:, list(PCR_FEATURES)].reindex(source_df.index)

    # The current builder already includes these columns. Verify that the
    # independent alignment is identical rather than silently relying on it.
    for feature in PCR_FEATURES:
        existing = pd.to_numeric(source_df[feature], errors="coerce")
        rebuilt = pd.to_numeric(aligned_pcr[feature], errors="coerce")
        overlap = existing.notna() & rebuilt.notna()
        if overlap.any() and not np.allclose(
            existing[overlap].to_numpy(),
            rebuilt[overlap].to_numpy(),
            rtol=0.0,
            atol=1e-12,
        ):
            raise ValueError(
                f"PCR alignment mismatch for {feature}; failing closed."
            )

    base_valid = source_df[control_features + ["target"]].notna().all(axis=1)
    pcr_valid = aligned_pcr[list(PCR_FEATURES)].notna().all(axis=1)
    control_rows_before_alignment = int(base_valid.sum())
    experiment_rows_before_alignment = int((base_valid & pcr_valid).sum())
    common_mask = base_valid & pcr_valid
    common_df = source_df.loc[common_mask].copy()
    common_df.loc[:, list(PCR_FEATURES)] = aligned_pcr.loc[common_mask, list(PCR_FEATURES)]

    if common_df.empty:
        raise ValueError("No common control/PCR rows remain after alignment.")
    if common_df.index.max() > cutoff_date:
        raise ValueError("Cutoff integrity failure after PCR alignment.")

    return {
        "control_features": control_features,
        "experiment_features": experiment_features,
        "common_df": common_df,
        "control_rows_before_alignment": control_rows_before_alignment,
        "experiment_rows_before_alignment": experiment_rows_before_alignment,
        "common_rows_used": int(len(common_df)),
    }


def split_identically(common_df, control_features, experiment_features):
    y = common_df["target"].astype(int).copy()
    control_x = common_df[control_features].copy()
    experiment_x = common_df[experiment_features].copy()
    split_index = int(len(common_df) * 0.8)
    control_train, control_test = control_x.iloc[:split_index], control_x.iloc[split_index:]
    experiment_train, experiment_test = experiment_x.iloc[:split_index], experiment_x.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]

    if control_train.empty or control_test.empty or y_train.nunique() < 2:
        raise ValueError("Production temporal split is not trainable.")
    if len(control_train) < trainer.N_SPLITS * 30:
        raise ValueError("Training set is too small for the production CV configuration.")
    if not (
        control_train.index.equals(experiment_train.index)
        and control_test.index.equals(experiment_test.index)
        and y_train.index.equals(control_train.index)
        and y_test.index.equals(control_test.index)
    ):
        raise ValueError("Temporal split integrity failure: dates are not identical.")
    return control_train, control_test, experiment_train, experiment_test, y_train, y_test


def train_variant(X_train, X_test, y_train, y_test, ticker):
    """Reproduce trainer.py optimization, SMOTE, XGBoost, and calibration."""
    random.seed(RANDOM_STATE)
    np.random.seed(RANDOM_STATE)
    effective_trials = 30 if ticker in trainer.VERY_LOW_CONFIDENCE_TICKERS else trainer.N_OPTUNA_TRIALS
    study = optuna.create_study(
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=RANDOM_STATE),
    )
    study.optimize(
        lambda trial: trainer._optuna_objective(trial, X_train, y_train, ticker),
        n_trials=effective_trials,
        show_progress_bar=False,
    )
    if not study.best_trials:
        raise ValueError("Production Optuna configuration yielded no successful trials.")

    best_params = {
        key: value for key, value in study.best_params.items()
        if key != "smote_minority_ratio"
    }
    label_counts = y_train.value_counts()
    majority_count = label_counts.max()
    minority_count = label_counts.min()
    smote_floors = trainer.TICKER_SMOTE_FLOOR_OVERRIDES.get(ticker, trainer.SMOTE_FLOORS)
    sampling_target = {}
    for label, count in label_counts.items():
        floor = smote_floors.get(int(label), 0.50)
        sampling_target[label] = min(
            max(count, int(majority_count * floor)),
            majority_count,
        )
    smote = SMOTE(
        random_state=RANDOM_STATE,
        sampling_strategy=sampling_target,
        k_neighbors=min(5, int(minority_count) - 1),
    )
    try:
        X_resampled, y_resampled = smote.fit_resample(X_train, y_train)
    except ValueError:
        X_resampled, y_resampled = X_train, y_train

    resampled_counts = pd.Series(y_resampled).value_counts()
    sell_count = resampled_counts.get(0, 1)
    hold_count = resampled_counts.get(1, 1)
    buy_count = resampled_counts.get(2, 1)
    sell_boost = min(
        (hold_count + buy_count) / max(sell_count, 1),
        SELL_BOOST_OVERRIDES.get(ticker, 1.5),
    )
    hold_boost = 1.2 if sell_boost > 1.2 else 1.0
    hold_boost = max(hold_boost, trainer.TICKER_HOLD_WEIGHT_OVERRIDE.get(ticker, 0.0))
    sample_weights = np.where(
        y_resampled == 0,
        sell_boost,
        np.where(y_resampled == 1, hold_boost, 1.0),
    )
    model = XGBClassifier(
        objective="multi:softprob",
        num_class=3,
        eval_metric="mlogloss",
        random_state=RANDOM_STATE,
        n_jobs=-1,
        **best_params,
    )
    model.fit(X_resampled, y_resampled, sample_weight=sample_weights)
    probabilities = model.predict_proba(X_test)
    raw_predictions = probabilities.argmax(axis=1)
    thresholds = trainer.TICKER_CLASS_THRESHOLDS.get(ticker)
    calibrated_predictions = np.array([
        trainer.apply_threshold_calibration(proba, thresholds)
        for proba in probabilities
    ])
    return evaluate_variant(
        y_test,
        raw_predictions,
        calibrated_predictions,
        study.best_value,
        effective_trials,
        best_params,
    )


def evaluate_variant(y_test, raw_predictions, calibrated_predictions, cv_score, trials, best_params):
    precision, recall, f1_values, support = precision_recall_fscore_support(
        y_test,
        calibrated_predictions,
        labels=[0, 1, 2],
        zero_division=0,
    )
    metrics = {
        "f1_macro": float(f1_score(y_test, calibrated_predictions, average="macro", zero_division=0)),
        "raw_argmax_f1_macro": float(f1_score(y_test, raw_predictions, average="macro", zero_division=0)),
        "accuracy": float(accuracy_score(y_test, calibrated_predictions)),
        "raw_prediction_distribution": {
            CLASS_NAMES[key]: int(value)
            for key, value in Counter(raw_predictions).items()
        },
        "prediction_distribution": {
            name: int((calibrated_predictions == class_id).sum())
            for class_id, name in CLASS_NAMES.items()
        },
        "cv_score": float(cv_score),
        "optuna_trials": trials,
        "best_params": best_params,
    }
    for index, class_id in enumerate([0, 1, 2]):
        name = CLASS_NAMES[class_id].lower()
        metrics[f"{name}_precision"] = float(precision[index])
        metrics[f"{name}_recall"] = float(recall[index])
        metrics[f"{name}_f1"] = float(f1_values[index])
        metrics[f"{name}_support"] = int(support[index])
    return metrics


def build_result(ticker, cutoff_date, alignment, split_data, control, pcr):
    control_train, control_test, experiment_train, experiment_test, y_train, y_test = split_data
    deltas = {
        name: pcr[f"{name}_f1"] - control[f"{name}_f1"]
        for name in ("sell", "hold", "buy")
    }
    control_class_f1 = {name: control[f"{name}_f1"] for name in deltas}
    pcr_class_f1 = {name: pcr[f"{name}_f1"] for name in deltas}
    weakest_control = min(control_class_f1, key=control_class_f1.get)
    weakest_pcr = min(pcr_class_f1, key=pcr_class_f1.get)
    collapses = [
        name.upper() for name in deltas
        if pcr_class_f1[name] < 0.10 or deltas[name] <= -0.10
    ]
    result = {
        "ticker": ticker,
        "cutoff_date": str(cutoff_date.date()),
        "control_feature_count": len(alignment["control_features"]),
        "pcr_feature_count": len(alignment["experiment_features"]),
        "control_rows_before_alignment": alignment["control_rows_before_alignment"],
        "experiment_rows_before_alignment": alignment["experiment_rows_before_alignment"],
        "common_rows_used": alignment["common_rows_used"],
        "train_date_min": str(control_train.index.min().date()),
        "train_date_max": str(control_train.index.max().date()),
        "test_date_min": str(control_test.index.min().date()),
        "test_date_max": str(control_test.index.max().date()),
        "control_train_size": len(control_train),
        "pcr_train_size": len(experiment_train),
        "control_test_size": len(control_test),
        "pcr_test_size": len(experiment_test),
        "labels_identical": bool(y_train.equals(y_train) and y_test.equals(y_test)),
        "control_f1_macro": control["f1_macro"],
        "pcr_f1_macro": pcr["f1_macro"],
        "delta_f1_macro": pcr["f1_macro"] - control["f1_macro"],
        "control_raw_argmax_f1_macro": control["raw_argmax_f1_macro"],
        "pcr_raw_argmax_f1_macro": pcr["raw_argmax_f1_macro"],
        "control_accuracy": control["accuracy"],
        "pcr_accuracy": pcr["accuracy"],
        "weakest_class_control": weakest_control.upper(),
        "weakest_class_pcr": weakest_pcr.upper(),
        "weakest_class_f1_before": control_class_f1[weakest_control],
        "weakest_class_f1_after": pcr_class_f1[weakest_control],
        "weakest_class_delta": pcr_class_f1[weakest_control] - control_class_f1[weakest_control],
        "classes_improved": sum(delta > 0 for delta in deltas.values()),
        "classes_worsened": sum(delta < 0 for delta in deltas.values()),
        "class_collapse": ",".join(collapses) if collapses else "",
        "feature_set_difference_verified": True,
    }
    for name in ("sell", "hold", "buy"):
        for suffix in ("precision", "recall", "f1", "support"):
            result[f"control_{name}_{suffix}"] = control[f"{name}_{suffix}"]
            result[f"pcr_{name}_{suffix}"] = pcr[f"{name}_{suffix}"]
        result[f"delta_{name}_f1"] = deltas[name]
    for prefix, metrics in (("control", control), ("pcr", pcr)):
        result[f"{prefix}_prediction_distribution"] = json.dumps(metrics["prediction_distribution"], sort_keys=True)
        result[f"{prefix}_raw_prediction_distribution"] = json.dumps(metrics["raw_prediction_distribution"], sort_keys=True)
        result[f"{prefix}_cv_score"] = metrics["cv_score"]
        result[f"{prefix}_optuna_trials"] = metrics["optuna_trials"]
    return result


def aggregate(results):
    if not results:
        return {}
    frame = pd.DataFrame(results)
    output = {
        "tickers_successful": len(frame),
        "mean_control_f1": float(frame["control_f1_macro"].mean()),
        "mean_pcr_f1": float(frame["pcr_f1_macro"].mean()),
        "mean_delta": float(frame["delta_f1_macro"].mean()),
        "median_control_f1": float(frame["control_f1_macro"].median()),
        "median_pcr_f1": float(frame["pcr_f1_macro"].median()),
        "median_delta": float(frame["delta_f1_macro"].median()),
        "macro_improved": int((frame["delta_f1_macro"] > 0).sum()),
        "macro_worsened": int((frame["delta_f1_macro"] < 0).sum()),
        "macro_unchanged": int((frame["delta_f1_macro"] == 0).sum()),
        "macro_improvement_pct": float((frame["delta_f1_macro"] > 0).mean() * 100),
        "class_collapses": int(frame["class_collapse"].ne("").sum()),
    }
    for name in ("sell", "hold", "buy"):
        delta = frame[f"delta_{name}_f1"]
        output[f"mean_{name}_delta"] = float(delta.mean())
        output[f"{name}_improved"] = int((delta > 0).sum())
        output[f"{name}_worsened"] = int((delta < 0).sum())
    return output


def interpretation(summary):
    if not summary:
        return "NEUTRAL"
    if summary["class_collapses"] or summary["mean_delta"] <= -0.02:
        return "STRONG_NEGATIVE" if summary["mean_delta"] <= -0.02 else "MODERATE_NEGATIVE"
    if summary["mean_delta"] >= 0.02 and summary["macro_improvement_pct"] >= 60:
        return "STRONG_POSITIVE"
    if summary["mean_delta"] >= 0.005 and summary["macro_improved"] > summary["macro_worsened"]:
        return "MODERATE_POSITIVE"
    if summary["mean_delta"] <= -0.005:
        return "MODERATE_NEGATIVE"
    return "NEUTRAL"


def print_summary(attempted, results, failures, summary):
    print("=" * 72)
    print("STOCK PCR RETRAINING EXPERIMENT")
    print("=" * 72)
    print(f"Tickers attempted : {attempted}")
    print(f"Tickers successful: {len(results)}")
    print(f"Tickers failed    : {len(failures)}")
    if summary:
        print(f"Mean control F1   : {summary['mean_control_f1']:.6f}")
        print(f"Mean PCR F1       : {summary['mean_pcr_f1']:.6f}")
        print(f"Mean delta        : {summary['mean_delta']:+.6f}")
        print(f"Median control F1 : {summary['median_control_f1']:.6f}")
        print(f"Median PCR F1     : {summary['median_pcr_f1']:.6f}")
        print(f"Median delta      : {summary['median_delta']:+.6f}")
        print(f"Macro F1 improved : {summary['macro_improved']}")
        print(f"Macro F1 worsened : {summary['macro_worsened']}")
        for name in ("sell", "hold", "buy"):
            print(f"{name.upper()} improved      : {summary[f'{name}_improved']}")
            print(f"{name.upper()} worsened      : {summary[f'{name}_worsened']}")
        print(f"Class collapses   : {summary['class_collapses']}")
        print(f"Interpretation    : {interpretation(summary)}")


def main():
    parser = argparse.ArgumentParser(
        description="Isolated, read-only stock-level PCR A/B retraining experiment."
    )
    parser.add_argument("--cutoff-date", type=parse_date, default=parse_date(DEFAULT_CUTOFF_DATE))
    parser.add_argument("--ticker", help="Run a single eligible ticker as a smoke test.")
    parser.add_argument(
        "--coverage-csv",
        default="stock_pcr_coverage_2021-09-22_2026-08-11.csv",
        help="Read-only coverage-audit CSV used to select eligible tickers.",
    )
    args = parser.parse_args()
    coverage_path = os.path.abspath(args.coverage_csv)
    if not os.path.exists(coverage_path):
        parser.error(f"Coverage CSV not found: {coverage_path}")

    eligible, excluded = read_eligible_tickers(coverage_path)
    selected = eligible
    if args.ticker:
        if args.ticker not in TICKERS:
            parser.error(f"Ticker is not canonical: {args.ticker}")
        if args.ticker not in eligible:
            parser.error(f"Ticker is excluded by coverage audit: {args.ticker}")
        selected = [args.ticker]

    dotenv = dotenv_values(os.path.join(PROJECT_ROOT, ".env"))
    mongo_uri = os.environ.get("MONGO_URI") or dotenv.get("MONGO_URI")
    if not mongo_uri:
        raise RuntimeError("MONGO_URI is not configured.")

    os.environ["TRAINING_CUTOFF_DATE"] = str(args.cutoff_date.date())

    random.seed(RANDOM_STATE)
    np.random.seed(RANDOM_STATE)
    feature_pipeline_version = trainer.FEATURE_PIPELINE_VERSION
    feature_module = resolve_feature_pipeline(feature_pipeline_version)
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    run_dir = os.path.join(PROJECT_ROOT, "experiments", "stock_pcr", run_id)
    os.makedirs(run_dir, exist_ok=False)

    config = {
        "experiment_timestamp": datetime.now(timezone.utc).isoformat(),
        "cutoff_date": str(args.cutoff_date.date()),
        "seed": RANDOM_STATE,
        "feature_pipeline_version": feature_pipeline_version,
        "feature_pipeline_hash": get_feature_pipeline_hash(feature_pipeline_version),
        "optuna_trials": trainer.N_OPTUNA_TRIALS,
        "ticker_count": len(selected),
        "included_tickers": selected,
        "excluded_tickers": excluded,
        "pcr_feature_names": list(PCR_FEATURES),
        "training_configuration": {
            "temporal_split": "first 80% train, final 20% test",
            "n_splits": trainer.N_SPLITS,
            "objective": "trainer._optuna_objective (recency-weighted macro F1 with HOLD penalty)",
            "smote": "trainer SMOTE floors/overrides and k-neighbor logic",
            "threshold_calibration": "feature pipeline TICKER_CLASS_THRESHOLDS",
        },
        "read_only": True,
    }
    with open(os.path.join(run_dir, "experiment_config.json"), "w", encoding="utf-8") as output:
        json.dump(json_safe(config), output, indent=2)

    results = []
    failures = []
    client = MongoClient(mongo_uri, readPreference="primaryPreferred")
    try:
        for position, ticker in enumerate(selected, start=1):
            print(f"[{position}/{len(selected)}] {ticker}")
            try:
                alignment = build_aligned_datasets(ticker, client, args.cutoff_date, feature_module)
                split_data = split_identically(
                    alignment["common_df"],
                    alignment["control_features"],
                    alignment["experiment_features"],
                )
                control_train, control_test, pcr_train, pcr_test, y_train, y_test = split_data
                control_metrics = train_variant(control_train, control_test, y_train, y_test, ticker)
                pcr_metrics = train_variant(pcr_train, pcr_test, y_train, y_test, ticker)
                result = build_result(
                    ticker,
                    args.cutoff_date,
                    alignment,
                    split_data,
                    control_metrics,
                    pcr_metrics,
                )
                results.append(result)
                print(f"  Control F1: {result['control_f1_macro']:.6f}")
                print(f"  PCR F1    : {result['pcr_f1_macro']:.6f}")
                print(f"  Delta     : {result['delta_f1_macro']:+.6f}")
            except Exception as exc:
                failures.append({"ticker": ticker, "error": str(exc)})
                print(f"  FAILED: {exc}")
    finally:
        client.close()

    if results:
        with open(os.path.join(run_dir, "per_ticker_results.csv"), "w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=list(results[0].keys()))
            writer.writeheader()
            writer.writerows(results)
    summary = aggregate(results)
    summary_payload = {
        "run_id": run_id,
        "attempted": len(selected),
        "successful": len(results),
        "failed": failures,
        "aggregate": summary,
        "interpretation": interpretation(summary),
        "top_10_improvements": sorted(results, key=lambda item: item["delta_f1_macro"], reverse=True)[:10],
        "top_10_regressions": sorted(results, key=lambda item: item["delta_f1_macro"])[:10],
    }
    with open(os.path.join(run_dir, "summary.json"), "w", encoding="utf-8") as output:
        json.dump(json_safe(summary_payload), output, indent=2)
    print_summary(len(selected), results, failures, summary)
    if results:
        print("TOP 10 IMPROVEMENTS")
        for result in summary_payload["top_10_improvements"]:
            print(f"  {result['ticker']}: {result['delta_f1_macro']:+.6f}")
        print("TOP 10 REGRESSIONS")
        for result in summary_payload["top_10_regressions"]:
            print(f"  {result['ticker']}: {result['delta_f1_macro']:+.6f}")
    print(f"Local experiment directory: {run_dir}")
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
