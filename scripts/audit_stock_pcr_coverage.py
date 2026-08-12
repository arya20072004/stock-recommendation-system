"""
Read-only audit of historical stock-level PCR coverage.

This measures whether the source data could support a future controlled
retraining experiment. It does not evaluate predictive value and does not
change production features, models, manifests, or MongoDB records.

The feature-usability calculation mirrors _prepare_stock_pcr_data():
stock_pcr_oi = pcr_oi, stock_pcr_chg_5d = pcr_oi.diff(5), then both are
shifted by one row for leakage prevention. The five-row observation rule and
the one-row shift are therefore both reflected in usable_feature_dates.
"""

import argparse
import csv
import math
import os
import sys
from collections import Counter
from datetime import date, datetime, time, timedelta


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
from pymongo import MongoClient

from src.data.nifty50 import TICKERS


def parse_date(value):
    """Parse a YYYY-MM-DD command-line date."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'; expected YYYY-MM-DD."
        ) from exc


def as_date(value):
    """Return a MongoDB date value as a date, or None when invalid."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
        except ValueError:
            return None
    return None


def as_usable_number(value):
    """Return a finite PCR value, or None for null/non-numeric values."""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def weekday_sessions(start_date, end_date):
    """Count weekdays inclusively; this intentionally is not holiday-aware."""
    current = start_date
    sessions = 0
    while current <= end_date:
        if current.weekday() < 5:
            sessions += 1
        current += timedelta(days=1)
    return sessions


def coverage_classification(total_rows, coverage_pct):
    if total_rows == 0:
        return "NO_DATA"
    if coverage_pct < 20:
        return "VERY_LOW_COVERAGE"
    if coverage_pct < 50:
        return "LOW_COVERAGE"
    if coverage_pct < 80:
        return "MODERATE_COVERAGE"
    return "HIGH_COVERAGE"


def usability_classification(coverage_pct):
    if coverage_pct < 20:
        return "UNUSABLE"
    if coverage_pct < 80:
        return "LIMITED"
    return "USABLE"


def percentage(numerator, denominator):
    return (numerator / denominator * 100) if denominator else 0.0


def count_weekday_gaps(sorted_dates):
    """Count weekday gaps between PCR dates; NSE holidays can appear as gaps."""
    gaps = 0
    for previous, current in zip(sorted_dates, sorted_dates[1:]):
        expected_next = previous + timedelta(days=1)
        while expected_next.weekday() >= 5:
            expected_next += timedelta(days=1)
        if current > expected_next:
            gaps += 1
    return gaps


def audit_ticker(collection, ticker, start_date, end_date, expected_sessions):
    """Read and audit one ticker without modifying source data."""
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)
    documents = list(collection.find(
        {"ticker": ticker, "date": {"$gte": start_dt, "$lte": end_dt}},
        {"date": 1, "pcr_oi": 1, "_id": 0},
    ))

    raw_dates = [as_date(document.get("date")) for document in documents]
    valid_documents = [
        (document, document_date)
        for document, document_date in zip(documents, raw_dates)
        if document_date is not None
    ]

    # find() has no ordering guarantee. Retain this as a source-order
    # diagnostic only; calculations below deliberately sort exactly as the
    # production feature preparation does.
    raw_cursor_non_monotonic_date_pairs = sum(
        1
        for previous, current in zip(raw_dates, raw_dates[1:])
        if previous is not None and current is not None and current < previous
    )

    valid_documents.sort(key=lambda item: item[1])
    sorted_dates = [document_date for _, document_date in valid_documents]
    date_counts = Counter(sorted_dates)
    distinct_dates = sorted(date_counts)
    duplicate_records = sum(count - 1 for count in date_counts.values() if count > 1)

    pcr_oi_null = sum(
        1
        for document, _ in valid_documents
        if document.get("pcr_oi") is None
    )
    non_numeric_pcr_oi = sum(
        1
        for document, _ in valid_documents
        if document.get("pcr_oi") is not None
        and as_usable_number(document.get("pcr_oi")) is None
    )
    values = [as_usable_number(document.get("pcr_oi")) for document, _ in valid_documents]
    pcr_oi_non_null = sum(value is not None for value in values)

    # Preserve duplicate observations. This mirrors the source pipeline's
    # sorted DataFrame and does not silently deduplicate the audit input.
    chg5_usable_rows = sum(
        1
        for index in range(5, len(values))
        if values[index] is not None and values[index - 5] is not None
    )

    # result.shift(1) makes row i usable only when row i - 1 had both the
    # raw PCR value and its five-observation difference available.
    usable_feature_dates = set()
    for index in range(6, len(values)):
        if values[index - 1] is not None and values[index - 6] is not None:
            usable_feature_dates.add(sorted_dates[index])

    coverage_pct = percentage(len(distinct_dates), expected_sessions)
    usable_feature_coverage_pct = percentage(
        len(usable_feature_dates),
        expected_sessions,
    )

    return {
        "ticker": ticker,
        "total_pcr_rows": len(documents),
        "distinct_pcr_dates": len(distinct_dates),
        "first_pcr_date": str(distinct_dates[0]) if distinct_dates else "",
        "last_pcr_date": str(distinct_dates[-1]) if distinct_dates else "",
        "expected_trading_sessions": expected_sessions,
        "coverage_pct": coverage_pct,
        "pcr_oi_non_null": pcr_oi_non_null,
        "pcr_oi_null": pcr_oi_null,
        "non_numeric_pcr_oi": non_numeric_pcr_oi,
        "pcr_chg5_usable": chg5_usable_rows,
        "usable_feature_dates": len(usable_feature_dates),
        "usable_feature_coverage_pct": usable_feature_coverage_pct,
        "latest_usable_feature_date": (
            str(max(usable_feature_dates)) if usable_feature_dates else ""
        ),
        "duplicate_records": duplicate_records,
        "raw_cursor_non_monotonic_date_pairs": (
            raw_cursor_non_monotonic_date_pairs
        ),
        "weekday_gap_count": count_weekday_gaps(distinct_dates),
        "invalid_date_records": len(documents) - len(valid_documents),
        "coverage_classification": coverage_classification(
            len(documents),
            coverage_pct,
        ),
        "feature_usability": usability_classification(
            usable_feature_coverage_pct,
        ),
    }


def print_table(results):
    headers = (
        "TICKER", "PCR ROWS", "DISTINCT", "COVERAGE", "USABLE DATES",
        "USABLE COVERAGE", "FIRST DATE", "LAST DATE", "CLASSIFICATION",
    )
    widths = (15, 9, 8, 10, 12, 15, 12, 12, 22)
    row_format = " ".join(f"{{:{width}}}" for width in widths)
    print(row_format.format(*headers))
    print("-" * (sum(widths) + len(widths) - 1))
    for result in results:
        print(row_format.format(
            result["ticker"],
            result["total_pcr_rows"],
            result["distinct_pcr_dates"],
            f"{result['coverage_pct']:.2f}%",
            result["usable_feature_dates"],
            f"{result['usable_feature_coverage_pct']:.2f}%",
            result["first_pcr_date"] or "-",
            result["last_pcr_date"] or "-",
            result["coverage_classification"],
        ))


def print_summary(results):
    classification_counts = Counter(
        result["coverage_classification"] for result in results
    )
    usability_counts = Counter(result["feature_usability"] for result in results)
    print()
    print("AUDIT SUMMARY")
    print("-" * 80)
    print(f"Configured tickers                 : {len(results)}")
    print(f"Tickers with PCR data              : {sum(r['total_pcr_rows'] > 0 for r in results)}")
    print(f"Tickers without PCR data           : {classification_counts['NO_DATA']}")
    print(f"HIGH_COVERAGE                      : {classification_counts['HIGH_COVERAGE']}")
    print(f"MODERATE_COVERAGE                  : {classification_counts['MODERATE_COVERAGE']}")
    print(f"LOW_COVERAGE                       : {classification_counts['LOW_COVERAGE']}")
    print(f"VERY_LOW_COVERAGE                  : {classification_counts['VERY_LOW_COVERAGE']}")
    print(f"USABLE                             : {usability_counts['USABLE']}")
    print(f"LIMITED                            : {usability_counts['LIMITED']}")
    print(f"UNUSABLE                           : {usability_counts['UNUSABLE']}")
    print(f"Total duplicate records            : {sum(r['duplicate_records'] for r in results)}")
    print(f"Total null pcr_oi records          : {sum(r['pcr_oi_null'] for r in results)}")
    print(f"Total non-numeric pcr_oi records   : {sum(r['non_numeric_pcr_oi'] for r in results)}")
    print(f"Raw-cursor non-monotonic date pairs: {sum(r['raw_cursor_non_monotonic_date_pairs'] for r in results)}")
    print(f"Total weekday sequence gaps         : {sum(r['weekday_gap_count'] for r in results)}")
    print(f"Total invalid date records          : {sum(r['invalid_date_records'] for r in results)}")
    print()
    print("Coverage classifications describe availability only; they do not")
    print("establish whether stock-level PCR is predictive.")
    print("Raw-cursor ordering is not guaranteed by MongoDB; feature calculations")
    print("sort dates chronologically, matching the production preparation logic.")


def main():
    parser = argparse.ArgumentParser(
        description="Read-only audit of stock-level PCR historical coverage."
    )
    parser.add_argument(
        "--start-date",
        required=True,
        type=parse_date,
        help="Inclusive audit start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        type=parse_date,
        help="Inclusive audit end date (YYYY-MM-DD).",
    )
    args = parser.parse_args()

    if args.start_date > args.end_date:
        parser.error("--start-date must be on or before --end-date.")

    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise RuntimeError("MONGO_URI is not configured.")

    expected_sessions = weekday_sessions(args.start_date, args.end_date)
    client = MongoClient(mongo_uri, readPreference="primaryPreferred")
    try:
        collection = client["stock_market_db"]["pcr_data"]
        results = [
            audit_ticker(
                collection,
                ticker,
                args.start_date,
                args.end_date,
                expected_sessions,
            )
            for ticker in TICKERS
        ]
    finally:
        client.close()

    print("=" * 120)
    print("READ-ONLY STOCK-LEVEL PCR COVERAGE AUDIT")
    print("=" * 120)
    print(f"Date range                         : {args.start_date} through {args.end_date}")
    print(f"Expected trading sessions           : {expected_sessions} (weekday-based approximation; not NSE holiday-aware)")
    print("Feature usability                  : pcr_oi.diff(5), followed by a one-row leakage-prevention shift")
    print("MongoDB writes                     : NONE")
    print()
    print_table(results)
    print_summary(results)

    output_path = (
        f"stock_pcr_coverage_{args.start_date}_{args.end_date}.csv"
    )
    with open(output_path, "w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)

    print()
    print(f"Local audit CSV                    : {output_path}")
    print("READ-ONLY AUDIT COMPLETE")


if __name__ == "__main__":
    main()
