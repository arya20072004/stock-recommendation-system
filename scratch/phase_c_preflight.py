import sys
import os
import copy
from datetime import datetime, timedelta

# Add parent directory to path to import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.features.v1.engineering import _fetch_cached_macro, _MACRO_CACHE
from src.features.router import get_feature_pipeline_hash
import pandas as pd

def run_checks():
    # Check 1: Old hash
    old_hash = get_feature_pipeline_hash("v1")
    print(f"Old Hash: {old_hash}")
    
    # Check 3: NSE India VIX support
    from src.data.nse_index_fallback import fetch_nse_index_close
    dt_sep4 = datetime(2026, 9, 4)
    vix_val = fetch_nse_index_close(dt_sep4, "India VIX")
    print(f"NSE India VIX for Sep 4, 2026: {vix_val}")
    
if __name__ == "__main__":
    run_checks()
