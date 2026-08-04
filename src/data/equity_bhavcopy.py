"""
equity_bhavcopy.py (or append to collector.py / a shared utils module —
your call on placement)

Fetches official NSE equity Bhavcopy (Capital Market segment) as a
fallback data source for dates where yfinance is missing OHLCV rows —
e.g. the 2026-08-03 CAS (Closing Auction Session) transition, where
Yahoo's backend has not yet adapted to the new auction-based closing
price mechanism for F&O-eligible stocks.

Mirrors the UDiFF fetch pattern already used in pcr_builder.py's
_fetch_bhavcopy/_normalize, but pointed at the /content/cm/ (Capital
Market / equity) endpoint instead of /content/fo/.
"""

import io
import logging
import zipfile
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


def _equity_udiff_url(dt: datetime) -> str:
    return (
        f"https://nsearchives.nseindia.com/content/cm/"
        f"BhavCopy_NSE_CM_0_0_0_{dt.strftime('%Y%m%d')}_F_0000.csv.zip"
    )


def _fetch_equity_bhavcopy_raw(dt: datetime) -> pd.DataFrame | None:
    """Downloads and unzips the raw CM Bhavcopy CSV for a given date.
    Returns None on failure (holiday, not-yet-published, network error)."""
    url = _equity_udiff_url(dt)
    try:
        resp = SESSION.get(url, timeout=15)
        if resp.status_code != 200 or len(resp.content) < 200:
            return None
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_name = zf.namelist()[0]
            with zf.open(csv_name) as f:
                df = pd.read_csv(f)
        return df
    except Exception as ex:
        logger.debug("equity bhavcopy %s: fetch failed — %s", dt.date(), ex)
        return None


def fetch_equity_ohlcv_for_date(dt: datetime, fo_symbol_overrides: dict[str, str] | None = None,
                                  tickers: list[str] | None = None) -> list[dict]:
    """
    Fetches CM Bhavcopy for one date and returns OHLCV records for the
    given tickers (matched against the same TICKER_TO_FO_SYMBOL_OVERRIDES
    convention pcr_builder.py already uses, since NSE's underlying
    SYMBOL naming is shared across CM and FO segments).

    tickers: list of ".NS"-suffixed tickers to extract, e.g. ["RELIANCE.NS", ...]
             If None, returns all STK/EQ rows.

    Returns a list of dicts matching collector.py's historical_data
    record schema: {ticker, date, open, high, low, close, volume}
    """
    overrides = fo_symbol_overrides or {}
    raw = _fetch_equity_bhavcopy_raw(dt)
    if raw is None or raw.empty:
        logger.warning("equity bhavcopy: no data returned for %s", dt.date())
        return []

    raw.columns = [c.strip() for c in raw.columns]

    # Filter to equity series only — STK instrument type, EQ series.
    # (Bhavcopy CM file includes other instrument/series types mixed in.)
    eq_rows = raw[
        (raw["FinInstrmTp"] == "STK")
        & (raw["SctySrs"] == "EQ")
    ].copy()

    eq_rows["TckrSymb"] = eq_rows["TckrSymb"].astype(str).str.strip().str.upper()

    records = []
    target_tickers = tickers or []
    for ticker in target_tickers:
        fo_symbol = overrides.get(ticker, ticker.replace(".NS", "")).upper()
        row = eq_rows[eq_rows["TckrSymb"] == fo_symbol]
        if row.empty:
            logger.debug("equity bhavcopy %s: no row found for %s (symbol=%s)",
                         dt.date(), ticker, fo_symbol)
            continue
        row = row.iloc[0]
        try:
            records.append({
                "ticker": ticker,
                "date": dt,
                "open":   float(row["OpnPric"]),
                "high":   float(row["HghPric"]),
                "low":    float(row["LwPric"]),
                "close":  float(row["ClsPric"]),
                "volume": int(row["TtlTradgVol"]),
            })
        except (ValueError, TypeError) as ex:
            logger.warning("equity bhavcopy %s: could not parse row for %s — %s",
                           dt.date(), ticker, ex)
            continue

    logger.info("equity bhavcopy %s: fetched %d/%d requested tickers",
               dt.date(), len(records), len(target_tickers))
    return records