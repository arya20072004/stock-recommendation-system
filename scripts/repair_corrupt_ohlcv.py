import argparse
import os
import sys
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

# Add project root to PYTHONPATH for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data.collector import validate_ohlcv_record
from src.data.equity_bhavcopy import fetch_equity_ohlcv_for_date
from src.data.pcr_builder import TICKER_TO_FO_SYMBOL_OVERRIDES

def main():
    parser = argparse.ArgumentParser(description="Repair corrupt OHLCV market data.")
    parser.add_argument('--apply', action='store_true', help="Apply repairs to database (default is dry-run)")
    args = parser.parse_args()

    is_dry_run = not args.apply
    if is_dry_run:
        print("--- RUNNING IN DRY RUN MODE ---")
    else:
        print("--- RUNNING IN APPLY MODE ---")

    load_dotenv()
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    db = client['stock_market_db']
    collection = db['historical_data']

    print("Scanning database for invalid records...")
    invalid_records = []
    total_docs = 0

    for doc in collection.find():
        total_docs += 1
        is_valid, reason = validate_ohlcv_record(doc)
        if not is_valid:
            invalid_records.append({
                'ticker': doc['ticker'],
                'date': doc['date'],
                'reason': reason,
                'doc': doc
            })

    print(f"Total documents scanned: {total_docs}")
    print(f"Found {len(invalid_records)} invalid records.")

    if not invalid_records:
        print("INVALID TARGET RECORDS: 0")
        print("UPDATES REQUIRED: 0")
        return

    # Process and repair
    valid_replacements_found = 0
    
    for item in invalid_records:
        ticker = item['ticker']
        dt = item['date']
        doc = item['doc']
        
        print(f"\nTargeting corrupt record: {ticker} on {dt.date()}")
        print(f"  Reason: {item['reason']}")
        print(f"  Before: open={doc.get('open')}, high={doc.get('high')}, low={doc.get('low')}, close={doc.get('close')}, volume={doc.get('volume')}")

        # Fetch replacement
        print("  Fetching replacement from Bhavcopy...")
        records = fetch_equity_ohlcv_for_date(dt, fo_symbol_overrides=TICKER_TO_FO_SYMBOL_OVERRIDES, tickers=[ticker])
        
        if not records:
            print("  ERROR: No replacement data found.")
            continue
            
        replacement = records[0]
        
        # Validate replacement
        is_valid, reason = validate_ohlcv_record(replacement)
        if not is_valid:
            print(f"  ERROR: Replacement data is invalid: {reason}")
            continue
            
        valid_replacements_found += 1
        print(f"  After: open={replacement['open']}, high={replacement['high']}, low={replacement['low']}, close={replacement['close']}, volume={replacement['volume']}")
        
        if not is_dry_run:
            result = collection.update_one(
                {'ticker': ticker, 'date': dt},
                {'$set': {
                    'open': replacement['open'],
                    'high': replacement['high'],
                    'low': replacement['low'],
                    'close': replacement['close'],
                    'volume': replacement['volume']
                }}
            )
            print(f"  Update result - matched_count: {result.matched_count}, modified_count: {result.modified_count}")

    print(f"\nTotal invalid records targeted: {len(invalid_records)}")
    print(f"Total valid replacements found: {valid_replacements_found}")
    
    if is_dry_run:
        print("\nDRY RUN completed. Run with --apply to push updates to MongoDB.")
    else:
        print("\nAPPLY completed.")

if __name__ == '__main__':
    main()
