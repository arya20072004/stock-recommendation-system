"""
cleanup_stale_predictions.py

One-time cleanup utility for removing stale prediction snapshots created
during the Phase 6 prediction-history bug.

SAFE BEHAVIOUR:
- Dry-run by default.
- Shows exactly what would be deleted.
- Requires --execute to actually delete anything.
- Preserves the valid 2026-08-05 prediction batch.
"""

import argparse
import os
from collections import Counter

from dotenv import load_dotenv
from pymongo import MongoClient


VALID_MARKET_DATE = "2026-08-05"


def main():
    parser = argparse.ArgumentParser(
        description="Clean stale prediction_history records."
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete stale records. Without this flag, dry-run only.",
    )

    args = parser.parse_args()

    load_dotenv()

    mongo_uri = os.getenv("MONGO_URI")

    if not mongo_uri:
        raise RuntimeError("MONGO_URI is not configured.")

    client = MongoClient(mongo_uri)

    try:
        db = client["stock_market_db"]
        collection = db["prediction_history"]

        # --------------------------------------------------------------
        # Inspect collection
        # --------------------------------------------------------------

        all_records = list(
            collection.find(
                {},
                {
                    "_id": 1,
                    "symbol": 1,
                    "market_date": 1,
                    "prediction_timestamp": 1,
                    "recommendation": 1,
                    "raw_prediction": 1,
                    "confidence": 1,
                    "prediction_horizon": 1,
                    "status": 1,
                },
            )
        )

        print("\n" + "=" * 70)
        print("PREDICTION HISTORY CLEANUP")
        print("=" * 70)

        print(f"\nTotal records currently in collection: {len(all_records)}")

        # --------------------------------------------------------------
        # Date distribution
        # --------------------------------------------------------------

        date_counts = Counter(
            record.get("market_date")
            for record in all_records
        )

        print("\nRecords by market_date:")

        for market_date, count in sorted(
            date_counts.items(),
            key=lambda item: str(item[0]),
        ):
            print(f"  {market_date}: {count}")

        # --------------------------------------------------------------
        # Identify stale Phase 6 records
        # --------------------------------------------------------------
        #
        # At this stage of the project, prediction_history was introduced
        # during Phase 6 and the only legitimate generated prediction batch
        # should correspond to 2026-08-05.
        #
        # This script is intentionally a ONE-TIME migration utility.
        # Do not reuse this rule once genuine historical batches exist.
        # --------------------------------------------------------------

        stale_records = [
            record
            for record in all_records
            if record.get("market_date") != VALID_MARKET_DATE
        ]

        valid_records = [
            record
            for record in all_records
            if record.get("market_date") == VALID_MARKET_DATE
        ]

        print("\n" + "-" * 70)
        print("VALID RECORDS")
        print("-" * 70)

        print(
            f"Records for {VALID_MARKET_DATE}: "
            f"{len(valid_records)}"
        )

        print("\n" + "-" * 70)
        print("STALE RECORDS IDENTIFIED")
        print("-" * 70)

        print(
            f"Records NOT belonging to {VALID_MARKET_DATE}: "
            f"{len(stale_records)}"
        )

        if stale_records:

            print("\nRecords marked for deletion:\n")

            for record in sorted(
                stale_records,
                key=lambda r: (
                    str(r.get("market_date")),
                    str(r.get("symbol")),
                ),
            ):

                print(
                    f"  {record.get('symbol', 'UNKNOWN'):15} | "
                    f"market_date={record.get('market_date')} | "
                    f"timestamp={record.get('prediction_timestamp')} | "
                    f"signal={record.get('recommendation')} | "
                    f"status={record.get('status')} | "
                    f"id={record.get('_id')}"
                )

        else:
            print("\nNo stale records found.")

        # --------------------------------------------------------------
        # Safety checks
        # --------------------------------------------------------------

        print("\n" + "-" * 70)
        print("SAFETY CHECK")
        print("-" * 70)

        if len(valid_records) != 51:

            print(
                f"\nWARNING: Expected 51 valid predictions for "
                f"{VALID_MARKET_DATE}, but found {len(valid_records)}."
            )

            print(
                "Cleanup aborted. Investigate before deleting anything."
            )

            return

        print(
            f"\n✓ Found exactly 51 valid predictions for "
            f"{VALID_MARKET_DATE}."
        )

        valid_symbols = {
            record.get("symbol")
            for record in valid_records
        }

        if len(valid_symbols) != 51:

            print(
                f"\nWARNING: Found 51 records but only "
                f"{len(valid_symbols)} unique symbols."
            )

            print(
                "Cleanup aborted because duplicate/missing tickers "
                "may exist."
            )

            return

        print("✓ All 51 valid records belong to unique symbols.")

        if not stale_records:

            print("\nNothing needs to be deleted.")
            return

        # --------------------------------------------------------------
        # Dry run
        # --------------------------------------------------------------

        if not args.execute:

            print("\n" + "=" * 70)
            print("DRY RUN ONLY — NOTHING WAS DELETED")
            print("=" * 70)

            print(
                f"\nWould delete {len(stale_records)} stale record(s)."
            )

            print(
                "\nReview the records above carefully."
            )

            print(
                "\nIf they are the known broken April/May snapshots, run:"
            )

            print(
                "\npython -m scripts.cleanup_stale_predictions --execute"
            )

            return

        # --------------------------------------------------------------
        # Execute deletion
        # --------------------------------------------------------------

        stale_ids = [
            record["_id"]
            for record in stale_records
        ]

        result = collection.delete_many(
            {
                "_id": {
                    "$in": stale_ids
                }
            }
        )

        print("\n" + "=" * 70)
        print("CLEANUP EXECUTED")
        print("=" * 70)

        print(
            f"\nDeleted records: {result.deleted_count}"
        )

        # --------------------------------------------------------------
        # Verify collection after deletion
        # --------------------------------------------------------------

        remaining_total = collection.count_documents({})

        remaining_valid = collection.count_documents(
            {
                "market_date": VALID_MARKET_DATE
            }
        )

        remaining_other = collection.count_documents(
            {
                "market_date": {
                    "$ne": VALID_MARKET_DATE
                }
            }
        )

        print("\nPost-cleanup verification:")

        print(
            f"  Total records:             {remaining_total}"
        )

        print(
            f"  {VALID_MARKET_DATE} records:     {remaining_valid}"
        )

        print(
            f"  Other market-date records: {remaining_other}"
        )

        if (
            remaining_total == 51
            and remaining_valid == 51
            and remaining_other == 0
        ):

            print(
                "\n✓ Cleanup successful."
            )

            print(
                "✓ prediction_history contains exactly "
                "51 valid prediction snapshots."
            )

        else:

            print(
                "\nWARNING: Final collection state was not what "
                "we expected."
            )

            print(
                "Do not proceed to evaluation until investigated."
            )

    finally:

        client.close()


if __name__ == "__main__":
    main()