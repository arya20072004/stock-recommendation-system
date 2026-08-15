import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

def get_future_cutoffs():
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
    uri = os.environ["MONGO_URI"]
    client = MongoClient(uri)
    db = client["stock_market_db"]
    
    docs = list(db.historical_data.find({"ticker": "RELIANCE.NS"}, {"date": 1, "_id": 0}).sort("date", 1))
    trading_dates = [pd.to_datetime(d["date"]).tz_localize(None) for d in docs]
    
    if not trading_dates:
        print("No data found for RELIANCE.NS")
        return
        
    print(f"Total dates for RELIANCE.NS: {len(trading_dates)}")
    print(f"Min date: {trading_dates[0].date()}")
    print(f"Max date: {trading_dates[-1].date()}")
    
    candidate_months = ["2026-11", "2027-02", "2027-05", "2027-08", "2027-11", "2028-02", "2028-05"]
    
    for cm in candidate_months:
        target = pd.to_datetime(f"{cm}-10")
        valid_dates = [d for d in trading_dates if d >= target]
        if valid_dates:
            cutoff = valid_dates[0]
            idx = trading_dates.index(cutoff)
            if idx + 10 < len(trading_dates):
                future = trading_dates[idx + 10]
                print(f"Candidate {cm} -> Resolved Cutoff: {cutoff.date()} (Future Target max req: {future.date()})")
            else:
                print(f"Candidate {cm} -> Resolved Cutoff: {cutoff.date()} but INSUFFICIENT FUTURE HORIZON (need 10 days)")
        else:
            print(f"Candidate {cm} -> No dates found.")
            
if __name__ == "__main__":
    get_future_cutoffs()
