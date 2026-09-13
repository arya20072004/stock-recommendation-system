import yfinance as yf
import pandas as pd
import numpy as np

def run_tests():
    # Fetch dates from early 2025 to Sept 2026
    start_date = "2025-01-01"
    end_date = "2026-09-10"
    
    ndx = yf.download("^NDX", start=start_date, end=end_date, progress=False)
    nse = yf.download("^NSEI", start=start_date, end=end_date, progress=False)
    
    if isinstance(ndx.columns, pd.MultiIndex):
        ndx.columns = ndx.columns.get_level_values(0)
    if isinstance(nse.columns, pd.MultiIndex):
        nse.columns = nse.columns.get_level_values(0)
        
    ndx_close = ndx["Close"].copy()
    nse_close = nse["Close"].copy()
    
    # Base macro index (NSE dates)
    macro_idx = nse_close.index
    
    # ----------------------------------------------------
    # CURRENT IMPLEMENTATION
    # ----------------------------------------------------
    ndx_ret_5d_current = ndx_close.pct_change(5)
    macro_current = pd.DataFrame(index=macro_idx)
    macro_current["nasdaq_ret_5d"] = ndx_ret_5d_current
    
    # ----------------------------------------------------
    # DESIGN A: Fill Raw Data
    # ----------------------------------------------------
    ndx_close_filled = ndx_close.reindex(macro_idx.union(ndx_close.index)).ffill().loc[macro_idx]
    ndx_ret_5d_A = ndx_close_filled.pct_change(5)
    
    # ----------------------------------------------------
    # DESIGN B: Calculate Returns Then Align (and ffill the returns)
    # ----------------------------------------------------
    ndx_ret_5d_B_raw = ndx_close.pct_change(5)
    ndx_ret_5d_B = ndx_ret_5d_B_raw.reindex(macro_idx).ffill()
    
    # ----------------------------------------------------
    # Comparison
    # ----------------------------------------------------
    df = pd.DataFrame({
        "current": macro_current["nasdaq_ret_5d"],
        "design_A": ndx_ret_5d_A,
        "design_B": ndx_ret_5d_B
    }, index=macro_idx)
    
    # Identify US holidays where NSE is open
    us_holidays_nse_open = df[df["current"].isna() & df["design_B"].notna()]
    print("US Holidays when NSE was open (Sample):")
    for dt in us_holidays_nse_open.index[:5]:
        print(f"Date: {dt.date()}")
        prev_valid = ndx_close[ndx_close.index < dt].index[-1]
        print(f"  Latest US Session: {prev_valid.date()}")
        print(f"  Current: {df.loc[dt, 'current']}")
        print(f"  Design A (fill raw then pct_change): {df.loc[dt, 'design_A']:.6f}")
        print(f"  Design B (pct_change then fill): {df.loc[dt, 'design_B']:.6f}")
        
    # How many times do Design A and Design B disagree on NORMAL days?
    # Normal day = both NSE and US open
    normal_days = df[df["current"].notna()]
    diff_A = (normal_days["current"] - normal_days["design_A"]).abs()
    diff_B = (normal_days["current"] - normal_days["design_B"]).abs()
    
    print("\nComparison on Normal Days (Yahoo Complete):")
    print(f"Total normal days compared: {len(normal_days)}")
    
    changed_A = (diff_A > 1e-8).sum()
    changed_B = (diff_B > 1e-8).sum()
    
    print(f"Design A changed rows: {changed_A}")
    if changed_A > 0:
        print(f"  Max absolute difference (Design A): {diff_A.max():.6e}")
        
    print(f"Design B changed rows: {changed_B}")
    if changed_B > 0:
        print(f"  Max absolute difference (Design B): {diff_B.max():.6e}")
        
    # Examine Sep 4, Sep 7, Sep 8
    print("\nReconstruction around Sep 7, 2026:")
    target_dates = [pd.Timestamp("2026-09-04"), pd.Timestamp("2026-09-07"), pd.Timestamp("2026-09-08")]
    # mock macro shift
    shifted_current = df["current"].copy()
    shifted_current.loc[pd.Timestamp("2026-09-08")] = np.nan
    shifted_current = shifted_current.shift(1)
    
    shifted_B = df["design_B"].copy()
    shifted_B.loc[pd.Timestamp("2026-09-08")] = shifted_B.iloc[-1] # forward fill manually to the new date
    shifted_B = shifted_B.shift(1)
    
    for dt in target_dates:
        print(f"\nTarget Date (Inference Time) = {dt.date()}")
        prev_dt = dt - pd.Timedelta(days=1) if dt.weekday() != 0 else dt - pd.Timedelta(days=3)
        # Just manually fetching the shifted values to display:
        if dt in shifted_current.index:
            print(f"  Shifted Current: {shifted_current.loc[dt]}")
            print(f"  Shifted Design B: {shifted_B.loc[dt]:.6f}")

if __name__ == "__main__":
    run_tests()
