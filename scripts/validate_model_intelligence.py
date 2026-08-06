import json
import math
import sys
from pathlib import Path
from statistics import median


MODELS_DIR = Path("saved_models")

CLASS_NAMES = {"SELL", "HOLD", "BUY"}

REQUIRED_METADATA_FIELDS = {
    "trained_at",
    "model_version",
    "prediction_horizon",
    "feature_count",
    "model_type",
}


def is_valid_number(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def validate_metrics_file(path: Path):
    errors = []
    warnings = []

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        return None, [f"Could not read JSON: {exc}"], []

    ticker = data.get("ticker", path.stem.replace("_metrics", ""))

    # ---------------------------------------------------------
    # 1. Basic required fields
    # ---------------------------------------------------------

    if not data.get("ticker"):
        errors.append("Missing ticker")

    f1_macro = data.get("f1_macro")

    if not is_valid_number(f1_macro):
        errors.append("f1_macro missing or invalid")
    elif not 0 <= f1_macro <= 1:
        errors.append(f"f1_macro outside [0, 1]: {f1_macro}")

    test_size = data.get("test_size")

    if not isinstance(test_size, int) or test_size <= 0:
        errors.append(f"Invalid test_size: {test_size}")

    # ---------------------------------------------------------
    # 2. Model metadata
    # ---------------------------------------------------------

    metadata = data.get("model_metadata")

    if not isinstance(metadata, dict):
        errors.append("model_metadata missing")
        metadata = {}
    else:
        for field in REQUIRED_METADATA_FIELDS:
            if metadata.get(field) is None:
                errors.append(f"model_metadata.{field} missing")

    feature_count = metadata.get("feature_count")

    if not isinstance(feature_count, int) or feature_count <= 0:
        errors.append(
            f"Invalid model_metadata.feature_count: {feature_count}"
        )

    # ---------------------------------------------------------
    # 3. Test prediction distribution
    # ---------------------------------------------------------

    distribution = data.get("test_prediction_distribution")

    if not isinstance(distribution, dict):
        errors.append("test_prediction_distribution missing")
    else:
        missing_classes = CLASS_NAMES - set(distribution.keys())

        if missing_classes:
            errors.append(
                "Prediction distribution missing classes: "
                + ", ".join(sorted(missing_classes))
            )

        values = [
            distribution.get("SELL"),
            distribution.get("HOLD"),
            distribution.get("BUY"),
        ]

        if all(isinstance(v, int) and v >= 0 for v in values):
            distribution_total = sum(values)

            if isinstance(test_size, int):
                if distribution_total != test_size:
                    errors.append(
                        "Prediction distribution total "
                        f"{distribution_total} != test_size {test_size}"
                    )
        else:
            errors.append(
                "Prediction distribution contains invalid counts"
            )

    # ---------------------------------------------------------
    # 4. Per-class metrics support
    # ---------------------------------------------------------

    per_class = data.get("per_class_metrics")

    if not isinstance(per_class, dict):
        errors.append("per_class_metrics missing")
    else:
        missing_classes = CLASS_NAMES - set(per_class.keys())

        if missing_classes:
            errors.append(
                "per_class_metrics missing classes: "
                + ", ".join(sorted(missing_classes))
            )

        supports = []

        for class_name in ["SELL", "HOLD", "BUY"]:
            metrics = per_class.get(class_name)

            if not isinstance(metrics, dict):
                continue

            support = metrics.get("support")

            if not isinstance(support, int) or support < 0:
                errors.append(
                    f"{class_name} has invalid support: {support}"
                )
            else:
                supports.append(support)

            for metric_name in ["precision", "recall", "f1"]:
                value = metrics.get(metric_name)

                if not is_valid_number(value):
                    errors.append(
                        f"{class_name}.{metric_name} invalid"
                    )
                elif not 0 <= value <= 1:
                    errors.append(
                        f"{class_name}.{metric_name} outside [0,1]"
                    )

        if (
            len(supports) == 3
            and isinstance(test_size, int)
            and sum(supports) != test_size
        ):
            errors.append(
                f"Per-class support total {sum(supports)} "
                f"!= test_size {test_size}"
            )

    # ---------------------------------------------------------
    # 5. Feature importance
    # ---------------------------------------------------------

    importance = data.get("feature_importance")

    if not isinstance(importance, list):
        errors.append("feature_importance missing")
    else:
        if (
            isinstance(feature_count, int)
            and len(importance) != feature_count
        ):
            errors.append(
                f"feature_count={feature_count}, but "
                f"feature_importance contains {len(importance)} entries"
            )

        importance_values = []
        feature_names = []

        for index, item in enumerate(importance):
            if not isinstance(item, dict):
                errors.append(
                    f"feature_importance[{index}] is not an object"
                )
                continue

            feature = item.get("feature")
            value = item.get("importance")

            if not feature:
                errors.append(
                    f"feature_importance[{index}] missing feature name"
                )
            else:
                feature_names.append(feature)

            if not is_valid_number(value):
                errors.append(
                    f"feature_importance[{index}] invalid importance"
                )
            elif value < 0:
                errors.append(
                    f"feature_importance[{index}] negative importance"
                )
            else:
                importance_values.append(value)

        if len(feature_names) != len(set(feature_names)):
            errors.append("Duplicate feature names detected")

        if len(importance_values) == len(importance):
            if importance_values != sorted(
                importance_values,
                reverse=True,
            ):
                errors.append(
                    "feature_importance is not sorted descending"
                )

    # ---------------------------------------------------------
    # 6. Data fingerprint
    # ---------------------------------------------------------

    fingerprint = data.get("data_fingerprint")

    if not isinstance(fingerprint, dict):
        errors.append("data_fingerprint missing")
    else:
        if not fingerprint.get("feature_date_min"):
            errors.append(
                "data_fingerprint.feature_date_min missing"
            )

        if not fingerprint.get("feature_date_max"):
            errors.append(
                "data_fingerprint.feature_date_max missing"
            )

        if fingerprint.get("row_hash") is None:
            errors.append(
                "data_fingerprint.row_hash missing"
            )

    # ---------------------------------------------------------
    # 7. Confidence metadata
    # ---------------------------------------------------------

    confidence = data.get("confidence_stats")

    if not isinstance(confidence, dict):
        warnings.append("confidence_stats missing")
    else:
        for field in [
            "mean_max_proba",
            "mean_top2_margin",
            "f1_macro",
        ]:
            if not is_valid_number(confidence.get(field)):
                warnings.append(
                    f"confidence_stats.{field} missing/invalid"
                )

    # ---------------------------------------------------------
    # 8. Model quality warning
    # ---------------------------------------------------------

    if is_valid_number(f1_macro):
        if f1_macro < 0.30:
            warnings.append(
                f"Very weak macro F1: {f1_macro:.3f}"
            )

    if data.get("very_low_confidence") is True:
        warnings.append(
            "Model marked very_low_confidence"
        )

    return {
        "ticker": ticker,
        "f1_macro": f1_macro,
        "very_low_confidence": data.get(
            "very_low_confidence", False
        ),
        "feature_count": feature_count,
    }, errors, warnings


def main():
    files = sorted(MODELS_DIR.glob("*_metrics.json"))

    print("=" * 72)
    print("STOCKINTEL MODEL INTELLIGENCE VALIDATION")
    print("=" * 72)

    print(f"\nMetrics files found: {len(files)}")

    if not files:
        print("\nERROR: No metrics files found.")
        sys.exit(1)

    all_results = []
    failed_models = []

    for path in files:
        result, errors, warnings = validate_metrics_file(path)

        ticker = (
            result["ticker"]
            if result
            else path.stem.replace("_metrics", "")
        )

        if errors:
            failed_models.append(ticker)
            status = "FAIL"
        elif warnings:
            status = "PASS (warnings)"
        else:
            status = "PASS"

        f1 = (
            f'{result["f1_macro"]:.3f}'
            if result
            and is_valid_number(result["f1_macro"])
            else "N/A"
        )

        print(
            f"\n[{status}] {ticker:<18} "
            f"F1={f1}"
        )

        for error in errors:
            print(f"    ERROR: {error}")

        for warning in warnings:
            print(f"    WARN : {warning}")

        if result:
            all_results.append(result)

    # ---------------------------------------------------------
    # Fleet statistics
    # ---------------------------------------------------------

    valid_f1 = [
        result
        for result in all_results
        if is_valid_number(result["f1_macro"])
    ]

    if valid_f1:
        ranked = sorted(
            valid_f1,
            key=lambda x: x["f1_macro"],
            reverse=True,
        )

        scores = [
            item["f1_macro"]
            for item in ranked
        ]

        best = ranked[0]
        worst = ranked[-1]

        print("\n" + "=" * 72)
        print("MODEL FLEET QUALITY")
        print("=" * 72)

        print(
            f"\nBest model   : {best['ticker']} "
            f"(F1={best['f1_macro']:.3f})"
        )

        print(
            f"Median F1    : {median(scores):.3f}"
        )

        print(
            f"Worst model  : {worst['ticker']} "
            f"(F1={worst['f1_macro']:.3f})"
        )

        low_confidence = [
            r["ticker"]
            for r in all_results
            if r["very_low_confidence"]
        ]

        print(
            f"Low-confidence models: "
            f"{len(low_confidence)}"
        )

        if low_confidence:
            for ticker in low_confidence:
                print(f"  - {ticker}")

    # ---------------------------------------------------------
    # Final verdict
    # ---------------------------------------------------------

    print("\n" + "=" * 72)
    print("VALIDATION SUMMARY")
    print("=" * 72)

    print(f"\nTotal artifacts : {len(files)}")
    print(
        f"Passed          : "
        f"{len(files) - len(failed_models)}"
    )
    print(
        f"Failed          : "
        f"{len(failed_models)}"
    )

    if len(files) != 51:
        print(
            f"\nERROR: Expected 51 metrics artifacts, "
            f"found {len(files)}."
        )

        if "__fleet_count__" not in failed_models:
            failed_models.append("__fleet_count__")

    if failed_models:
        print("\nFAILED VALIDATION")

        for ticker in failed_models:
            if ticker != "__fleet_count__":
                print(f"  - {ticker}")

        sys.exit(1)

    print("\nALL MODEL INTELLIGENCE ARTIFACTS VALID.")
    sys.exit(0)


if __name__ == "__main__":
    main()