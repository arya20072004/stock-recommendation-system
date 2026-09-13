import yfinance as yf
import pandas as pd
import numpy as np

def test_stale_limits():
    # Long period to catch all edge cases
    start_date = "2015-01-01"
    end_date = "2026-09-10"
    
    ndx = yf.download("^NDX", start=start_date, end=end_date, progress=False)
    nse = yf.download("^NSEI", start=start_date, end=end_date, progress=False)
    
    if isinstance(ndx.columns, pd.MultiIndex):
        ndx.columns = ndx.columns.get_level_values(0)
    if isinstance(nse.columns, pd.MultiIndex):
        nse.columns = nse.columns.get_level_values(0)
        
    ndx_close = ndx["Close"].copy()
    nse_close = nse["Close"].copy()
    
    macro_idx = nse_close.index
    ndx_ret_5d = ndx_close.pct_change(5)
    
    # Analyze gaps
    aligned = ndx_ret_5d.reindex(macro_idx)
    
    # We want to see how many consecutive NaNs exist in the aligned series
    # A NaN means NSE was open but US was closed.
    
    consecutive_nans = aligned.isna().astype(int).groupby(aligned.notna().astype(int).cumsum()).sum()
    
    print("Distribution of consecutive US-closed days on NSE-open days:")
    print(consecutive_nans.value_counts().sort_index())
    
    # Find max gap
    max_gap = consecutive_nans.max()
    print(f"\nMax consecutive NaNs: {max_gap}")
    
    if max_gap > 0:
        max_gap_group = consecutive_nans[consecutive_nans == max_gap].index[0]
        # the group index corresponds to the last valid observation before the gap
        valid_indices = aligned.notna().astype(int).cumsum()
        gap_dates = aligned[valid_indices == max_gap_group].index
        print(f"\nDates for max gap ({max_gap} consecutive days):")
        for d in gap_dates:
            print(d.date(), "NDX valid?", not pd.isna(aligned.loc[d]))

if __name__ == "__main__":
    test_stale_limits()
