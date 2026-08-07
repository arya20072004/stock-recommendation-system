import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.pipeline.daily import DailyPipeline
from src.data.nifty50 import TICKERS
from datetime import datetime, timedelta, timezone

def test_resolve_trading_session():
    print("--- 1. TARGET MARKET DATE ---")
    p = DailyPipeline("mongodb://mock", dry_run=True)
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
    
    # 1. Normal weekday after cutoff (e.g., Wed 22:00)
    now1 = datetime(2026, 8, 5, 22, 0, tzinfo=IST)
    with __import__('unittest').mock.patch("src.pipeline.daily.datetime") as m_dt:
        m_dt.now.return_value = now1
        p.force = False
        p.resolve_trading_session()
        print(f"Normal weekday AFTER cutoff: {p.target_market_date} (expected Wed 2026-08-05)")

    # 2. Normal weekday before cutoff (e.g., Wed 10:00)
    now2 = datetime(2026, 8, 5, 10, 0, tzinfo=IST)
    with __import__('unittest').mock.patch("src.pipeline.daily.datetime") as m_dt:
        m_dt.now.return_value = now2
        p.force = False
        try:
            p.resolve_trading_session()
            print("FAILED! Should block before cutoff")
        except RuntimeError as e:
            print(f"Normal weekday BEFORE cutoff without force: BLOCKED -> {e}")
            
        p.force = True
        p.resolve_trading_session()
        print(f"Normal weekday BEFORE cutoff WITH force: {p.target_market_date} (expected Tue 2026-08-04)")

    # 3. Saturday (e.g., Sat 10:00)
    now3 = datetime(2026, 8, 8, 10, 0, tzinfo=IST)
    with __import__('unittest').mock.patch("src.pipeline.daily.datetime") as m_dt:
        m_dt.now.return_value = now3
        p.force = True
        p.resolve_trading_session()
        print(f"Saturday BEFORE cutoff: {p.target_market_date} (expected Fri 2026-08-07)")

    # 4. Sunday (e.g., Sun 22:00)
    now4 = datetime(2026, 8, 9, 22, 0, tzinfo=IST)
    with __import__('unittest').mock.patch("src.pipeline.daily.datetime") as m_dt:
        m_dt.now.return_value = now4
        p.force = False
        p.resolve_trading_session()
        print(f"Sunday AFTER cutoff: {p.target_market_date} (expected Fri 2026-08-07)")

if __name__ == "__main__":
    test_resolve_trading_session()
