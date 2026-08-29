"""
nse_index_fallback.py

Fetches official NSE Indices Bhavcopy to extract specific index closing values.
Acts as a secure, authoritative fallback when upstream providers (e.g. Yahoo Finance)
are missing the completed trading session for a benchmark like ^NSEI (Nifty 50).
"""

import io
import logging
from datetime import datetime

import pandas as pd
import requests

logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/all-reports",
})

def _index_bhavcopy_url(dt: datetime) -> str:
    """Constructs the NSE URL for the daily indices file."""
    # Pattern: https://nsearchives.nseindia.com/content/indices/ind_close_all_DDMMYYYY.csv
    return (
        f"https://nsearchives.nseindia.com/content/indices/"
        f"ind_close_all_{dt.strftime('%d%m%Y')}.csv"
    )

def _fetch_index_bhavcopy_raw(dt: datetime) -> pd.DataFrame | None:
    """Downloads the raw Indices Bhavcopy CSV for a given date."""
    url = _index_bhavcopy_url(dt)
    try:
        resp = SESSION.get(url, timeout=15)
        if resp.status_code != 200 or len(resp.content) < 100:
            logger.debug(f"nse index fallback {dt.date()}: fetch failed (status {resp.status_code})")
            return None
            
        df = pd.read_csv(io.StringIO(resp.text))
        return df
    except Exception as ex:
        logger.debug(f"nse index fallback {dt.date()}: fetch failed — {ex}")
        return None

def fetch_nse_index_close(dt: datetime, index_name: str = "Nifty 50") -> float | None:
    """
    Fetches the authoritative Closing Index Value from NSE for a specific index.
    
    Args:
        dt: The trading session date to fetch.
        index_name: The exact name of the index to search for (case-insensitive).
    
    Returns:
        float closing value if found and strictly positive, else None.
    """
    raw = _fetch_index_bhavcopy_raw(dt)
    if raw is None or raw.empty:
        logger.warning(f"nse index fallback: no data returned for {dt.date()}")
        return None

    # Normalize column names to avoid trailing whitespace issues
    raw.columns = [str(c).strip() for c in raw.columns]
    
    # Required columns
    target_col = "Closing Index Value"
    name_col = "Index Name"
    
    if target_col not in raw.columns or name_col not in raw.columns:
        logger.warning(f"nse index fallback {dt.date()}: missing expected columns")
        return None
        
    # Search for exactly the index_name
    target_name = index_name.strip().upper()
    
    # Strip whitespace from names in the dataframe and convert to uppercase for robust matching
    matched_row = raw[raw[name_col].astype(str).str.strip().str.upper() == target_name]
    
    if matched_row.empty:
        logger.warning(f"nse index fallback {dt.date()}: row for '{index_name}' not found")
        return None
        
    try:
        # Extract the closing value
        close_val = float(matched_row.iloc[0][target_col])
        if close_val <= 0:
            logger.warning(f"nse index fallback {dt.date()}: invalid close value ({close_val}) for '{index_name}'")
            return None
        
        logger.info(f"nse index fallback {dt.date()}: successfully fetched '{index_name}' close: {close_val}")
        return close_val
    except (ValueError, TypeError) as ex:
        logger.warning(f"nse index fallback {dt.date()}: could not parse close value for '{index_name}' — {ex}")
        return None
