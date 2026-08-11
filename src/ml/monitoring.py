"""
Monitoring module for Model Performance, Evaluation & Drift Detection.
Calculates performance metrics safely from 'EVALUATED' predictions only.
Maintains strict separation between raw model and recommendation performance.
"""

from typing import List, Dict, Any, Optional
from collections import defaultdict
import datetime

def safe_divide(num: float, den: float, default: float = 0.0) -> float:
    return num / den if den > 0 else default

def calculate_metrics(predictions: List[Dict[str, Any]], field_pred: str, field_actual: str = "actual_class") -> Dict[str, Any]:
    """Calculates classification metrics (accuracy, precision, recall, f1, confusion matrix)"""
    if not predictions:
        return {"sample_size": 0}

    labels = ["BUY", "HOLD", "SELL"]
    y_true = [p.get(field_actual) for p in predictions]
    y_pred = [p.get(field_pred, p.get("recommendation", "HOLD")) for p in predictions]

    cm = {l: {l2: 0 for l2 in labels} for l in labels}
    for t, p in zip(y_true, y_pred):
        if t in labels and p in labels:
            cm[t][p] += 1

    precision = {}
    recall = {}
    f1 = {}
    support = {}

    for l in labels:
        tp = cm[l][l]
        fp = sum(cm[other][l] for other in labels if other != l)
        fn = sum(cm[l][other] for other in labels if other != l)

        support[l] = tp + fn

        p_val = safe_divide(tp, tp + fp)
        r_val = safe_divide(tp, tp + fn)

        precision[l] = round(p_val, 4)
        recall[l] = round(r_val, 4)
        f1[l] = round(safe_divide(2 * p_val * r_val, p_val + r_val), 4)

    correct = sum(cm[l][l] for l in labels)
    total = sum(support.values())
    accuracy = round(safe_divide(correct, total), 4)

    active_classes = [l for l in labels if support[l] > 0]
    macro_f1 = round(safe_divide(sum(f1[l] for l in active_classes), len(active_classes)), 4) if active_classes else 0.0

    weighted_f1 = round(safe_divide(sum(f1[l] * support[l] for l in labels), total), 4) if total > 0 else 0.0

    return {
        "sample_size": total,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "macro_f1": macro_f1,
        "weighted_f1": weighted_f1,
        "confusion_matrix": cm
    }

def calculate_financials(predictions: List[Dict[str, Any]], field_pred: str) -> Dict[str, Any]:
    """Calculates directional returns and hit rates."""
    results = {
        "BUY": {"returns": [], "hits": 0},
        "SELL": {"returns": [], "hits": 0},
        "HOLD": {"returns": [], "hits": 0}
    }

    for p in predictions:
        pred = p.get(field_pred, p.get("recommendation", "HOLD"))
        ret = p.get("actual_return")

        if ret is None or pred not in results:
            continue

        if pred == "BUY":
            results["BUY"]["returns"].append(ret)
            if ret > 0: results["BUY"]["hits"] += 1
        elif pred == "SELL":
            results["SELL"]["returns"].append(-ret)
            if ret < 0: results["SELL"]["hits"] += 1
        elif pred == "HOLD":
            results["HOLD"]["returns"].append(ret)

    def agg(arr):
        return round(sum(arr)/len(arr), 4) if arr else None

    return {
        "BUY": {
            "average_directional_return": agg(results["BUY"]["returns"]),
            "hit_rate": round(safe_divide(results["BUY"]["hits"], len(results["BUY"]["returns"])), 4) if results["BUY"]["returns"] else None,
            "sample_size": len(results["BUY"]["returns"])
        },
        "SELL": {
            "average_directional_return": agg(results["SELL"]["returns"]),
            "hit_rate": round(safe_divide(results["SELL"]["hits"], len(results["SELL"]["returns"])), 4) if results["SELL"]["returns"] else None,
            "sample_size": len(results["SELL"]["returns"])
        },
        "HOLD": {
            "average_actual_return": agg(results["HOLD"]["returns"]),
            "sample_size": len(results["HOLD"]["returns"])
        }
    }

def calculate_prediction_distribution(predictions: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not predictions:
        return {"sample_size": 0}

    raw_dist = defaultdict(int)
    rec_dist = defaultdict(int)
    conf_dist = defaultdict(int)

    for p in predictions:
        raw_dist[p.get("raw_prediction", "HOLD")] += 1
        rec_dist[p.get("recommendation", "HOLD")] += 1
        conf_dist[p.get("confidence_tier", "UNKNOWN")] += 1

    total = len(predictions)
    return {
        "sample_size": total,
        "raw_prediction_distribution": {k: round(v/total, 4) for k, v in raw_dist.items()},
        "recommendation_distribution": {k: round(v/total, 4) for k, v in rec_dist.items()},
        "confidence_tier_distribution": {k: round(v/total, 4) for k, v in conf_dist.items()}
    }

def evaluate_model_health(sample_size: int, rolling_accuracy: float, lifetime_accuracy: float, rec_dist: Dict[str, float]) -> Dict[str, Any]:
    """Determines model health status based on evaluated sample size."""
    if sample_size < 30:
        return {"state": "INSUFFICIENT_DATA", "reason": "Insufficient evaluated observations for performance assessment.", "sample_size": sample_size}

    return {"state": "HEALTHY", "reason": "Meaningful evaluated sample available; health thresholds are not statistically calibrated.", "sample_size": sample_size}

def fetch_evaluated_predictions(db, match_query: Dict[str, Any], limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Fetch valid EVALUATED predictions, ignoring LEGACY_UNSETTLEABLE."""
    query = {
        "status": "EVALUATED",
        "target_return_threshold": {"$ne": None},
        **match_query
    }

    cursor = db.prediction_history.find(query).sort("market_date", -1)
    if limit:
        cursor = cursor.limit(limit)

    valid_predictions = []
    from src.ml.model_utils import compute_settlement_hash, reconstruct_settlement_payload

    for record in cursor:
        if "settlement_hash" not in record:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Missing settlement_hash for EVALUATED record {record.get('_id')}")
            continue

        stored_hash = record["settlement_hash"]

        try:
            canonical = reconstruct_settlement_payload(record)
            computed_hash = compute_settlement_hash(canonical)
        except (TypeError, ValueError) as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Hash Computation Failure for {record.get('_id')}: {e}")
            continue

        if computed_hash != stored_hash:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Tampered Record Detected: settlement_hash mismatch for {record.get('_id')}")
            continue

        valid_predictions.append(record)

    return valid_predictions

def analyze_performance(predictions: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not predictions:
        return {"status": "INSUFFICIENT_DATA", "sample_size": 0}

    res = {
        "status": "MEANINGFUL_SAMPLE" if len(predictions) >= 30 else "INSUFFICIENT_DATA",
        "sample_size": len(predictions),
        "raw_model": {
            "classification": calculate_metrics(predictions, "raw_prediction"),
            "financials": calculate_financials(predictions, "raw_prediction")
        },
        "recommendation": {
            "classification": calculate_metrics(predictions, "recommendation"),
            "financials": calculate_financials(predictions, "recommendation")
        },
        "distribution": calculate_prediction_distribution(predictions)
    }

    # Confidence Tier Analysis
    tiers = ["VERY_LOW", "LOW", "MEDIUM", "HIGH", "VERY_HIGH"]
    conf_analysis = {}
    for tier in tiers:
        tier_preds = [p for p in predictions if p.get("confidence_tier") == tier]
        if tier_preds:
            conf_analysis[tier] = {
                "sample_size": len(tier_preds),
                "raw_accuracy": calculate_metrics(tier_preds, "raw_prediction")["accuracy"],
                "recommendation_accuracy": calculate_metrics(tier_preds, "recommendation")["accuracy"],
                "financials": calculate_financials(tier_preds, "recommendation")
            }
    res["confidence_analysis"] = conf_analysis
    return res

def get_ticker_performance(db, ticker: str, model_version: Optional[str] = None) -> Dict[str, Any]:
    query = {"symbol": ticker}
    if model_version:
        query["model_version"] = model_version

    # 1. Lifetime Performance
    lifetime_preds = fetch_evaluated_predictions(db, query)
    lifetime_perf = analyze_performance(lifetime_preds)

    # 2. Rolling Performance (Last 50)
    rolling_preds = fetch_evaluated_predictions(db, query, limit=50)
    rolling_perf = analyze_performance(rolling_preds)

    # 3. Model Health
    rolling_acc = rolling_perf.get("recommendation", {}).get("classification", {}).get("accuracy", 0.0)
    lifetime_acc = lifetime_perf.get("recommendation", {}).get("classification", {}).get("accuracy", 0.0)
    rec_dist = rolling_perf.get("distribution", {}).get("recommendation_distribution", {})
    health = evaluate_model_health(rolling_perf.get("sample_size", 0), rolling_acc, lifetime_acc, rec_dist)

    return {
        "ticker": ticker,
        "model_version": model_version or "ALL",
        "health": health,
        "lifetime_performance": lifetime_perf,
        "rolling_performance": rolling_perf
    }

def get_system_health(db) -> Dict[str, Any]:
    """Provides system-wide health and distribution summary."""
    preds = fetch_evaluated_predictions(db, {})
    total = len(preds)

    if total < 30:
        return {
            "health": {"state": "INSUFFICIENT_DATA", "reason": "Insufficient evaluated observations for performance assessment.", "sample_size": total},
            "overall_performance": {"status": "INSUFFICIENT_DATA", "sample_size": total}
        }

    overall_perf = analyze_performance(preds)
    acc = overall_perf["recommendation"]["classification"]["accuracy"]
    rec_dist = overall_perf["distribution"]["recommendation_distribution"]

    return {
        "health": evaluate_model_health(total, acc, acc, rec_dist),
        "overall_performance": overall_perf
    }
