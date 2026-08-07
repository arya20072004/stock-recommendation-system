"""
feature_engineering.py
Shared feature-engineering constants, data-preparation functions,
and the inference-time feature builder used by both ml_trainer.py
and app.py.  Moving these here guarantees training/inference parity.
"""

import calendar
import logging
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pandas_ta as ta
import yfinance as yf

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HISTORY_YEARS = 5

# --- SECTOR MAPPING (Nifty 50) ---
SECTOR_MAP = {
    # Information Technology
    "INFY.NS":       "InformationTechnology",
    "TCS.NS":        "InformationTechnology",
    "HCLTECH.NS":    "InformationTechnology",
    "WIPRO.NS":      "InformationTechnology",
    "TECHM.NS":      "InformationTechnology",

    # Banking / Financial Services
    "HDFCBANK.NS":   "FinancialServices",
    "ICICIBANK.NS":  "FinancialServices",
    "KOTAKBANK.NS":  "FinancialServices",
    "AXISBANK.NS":   "FinancialServices",
    "SBIN.NS":       "FinancialServices",

    # NBFC / Insurance / Financials
    "BAJFINANCE.NS": "FinancialServices",
    "BAJAJFINSV.NS": "FinancialServices",
    "HDFCLIFE.NS":   "FinancialServices",
    "SBILIFE.NS":    "FinancialServices",
    "SHRIRAMFIN.NS": "FinancialServices",
    "JIOFIN.NS":     "FinancialServices",

    # Healthcare
    "SUNPHARMA.NS":  "Healthcare",
    "DRREDDY.NS":    "Healthcare",
    "CIPLA.NS":      "Healthcare",
    "APOLLOHOSP.NS": "Healthcare",
    "MAXHEALTH.NS":  "Healthcare",

    # Automobile & Auto Components
    "MARUTI.NS":     "AutomobileAndAutoComponents",
    "BAJAJ-AUTO.NS": "AutomobileAndAutoComponents",
    "HEROMOTOCO.NS": "AutomobileAndAutoComponents",
    "EICHERMOT.NS":  "AutomobileAndAutoComponents",
    "M&M.NS":        "AutomobileAndAutoComponents",
    "TMPV.NS":       "AutomobileAndAutoComponents",

    # Fast Moving Consumer Goods
    "HINDUNILVR.NS": "FastMovingConsumerGoods",
    "ITC.NS":        "FastMovingConsumerGoods",
    "NESTLEIND.NS":  "FastMovingConsumerGoods",
    "BRITANNIA.NS":  "FastMovingConsumerGoods",
    "TATACONSUM.NS": "FastMovingConsumerGoods",

    # Oil Gas & Consumable Fuels
    "RELIANCE.NS":   "OilGasAndConsumableFuels",
    "ONGC.NS":       "OilGasAndConsumableFuels",

    # Metals & Mining
    "TATASTEEL.NS":  "MetalsAndMining",
    "JSWSTEEL.NS":   "MetalsAndMining",
    "HINDALCO.NS":   "MetalsAndMining",

    # Construction Materials / Infra
    "ULTRACEMCO.NS": "ConstructionMaterials",
    "GRASIM.NS":     "ConstructionMaterials",
    "LT.NS":         "Construction",

    # Power Utilities
    "NTPC.NS":       "PowerUtilities",
    "POWERGRID.NS":  "PowerUtilities",
    "COALINDIA.NS":  "PowerUtilities",
    "BEL.NS":        "CapitalGoods",

    # Telecom
    "BHARTIARTL.NS": "Telecommunication",

    # Services / Logistics
    "ADANIPORTS.NS": "Services",

    # Metals / Resources Proxy
    "ADANIENT.NS":   "MetalsAndMining",

    # Consumer Durables / Retail / Services
    "TITAN.NS":      "ConsumerDurables",
    "TRENT.NS":      "ConsumerServices",
    "ASIANPAINT.NS": "ConsumerDurables",
    "INDIGO.NS":     "Services",
    "ETERNAL.NS":    "ConsumerServices",
}

SECTOR_INDEX_NAME_MAP = {
    "IT":             "InformationTechnology",
    "OilGas":         "OilGasAndConsumableFuels",
    "Auto":           "AutomobileAndAutoComponents",
    "Metals":         "MetalsAndMining",
    "Banking":        "FinancialServices",
    "Pharma":         "Healthcare",
    "PowerUtilities": "PowerUtilities",
    "Telecom":        "Telecommunication",
    "FMCG":           "FastMovingConsumerGoods",
    "CementInfra":    "ConstructionMaterials",
    "Realty":         "Realty",
    "CapitalGoods":   "CapitalGoods",
    "MediaEnt":       "MediaEntertainmentAndPublication",
    "Construction":   "Construction",
    "Services":       "Services",
    "Diversified":    "Diversified",
    "ConsumerServices": "ConsumerServices",
}

TICKER_HISTORY_OVERRIDE = {
    "MARUTI.NS": 3,   # confirmed concept drift across 15 runs
    "HDFCBANK.NS": 3, # sector disable insufficient — regime mismatch
}

TICKER_HORIZON_OVERRIDE: dict[str, int] = {
    "NTPC.NS":      5,
    "POWERGRID.NS": 5,
    "BAJAJFINSV.NS":5,
    "INFY.NS":      5,   # ADD — 10d too noisy for IT earnings-driven stock
    "WIPRO.NS":     5,   # ADD — same reasoning
    "HCLTECH.NS":   5,   # ADD — same reasoning
    "SBIN.NS":      5,   # ADD — same reasoning, persistent train/test disconnect
}

# Explicit start-date floors — for tickers where a relative "N years
# back" window would still reach into contaminated/pre-corporate-action
# history. Checked BEFORE TICKER_HISTORY_OVERRIDE's relative window in
# create_dataset() and build_feature_row().
TICKER_START_DATE_OVERRIDE = {
    "TMPV.NS": datetime(2025, 10, 15),  # demerger date confirmed via
                                        # -40.15% single-day break on
                                        # 2025-10-14 (old combined-entity
                                        # Tata Motors price series before
                                        # this date is not representative
                                        # of TMPV standalone post-demerger)
}

TICKER_ATR_THRESHOLD_SCALE: dict[str, float] = {
    "NTPC.NS":       0.70,
    "POWERGRID.NS":  0.65,
    "COALINDIA.NS":  0.75,
    "SHRIRAMFIN.NS": 0.75,
}

def get_target_return_threshold(ticker: str, atr_pct) -> float:
    """
    Canonical definition of the return threshold used to create the BUY/HOLD/SELL
    target labels during training, and later used for outcome settlement.
    """
    atr_scale = TICKER_ATR_THRESHOLD_SCALE.get(ticker, 1.0)
    # np.maximum handles both python floats and pandas Series
    result = np.maximum(atr_scale * atr_pct, 0.01)
    if isinstance(result, np.ndarray) or hasattr(result, "iloc"):
        return result
    return float(result)


SECTOR_MIN_PEERS = 4

EVENT_DRIVEN_SECTORS_NO_INDEX = {"Healthcare", "InformationTechnology"}

SECTOR_INDEX_DISABLED_TICKERS = {
    "TITAN.NS",
    "HDFCBANK.NS",
    "NESTLEIND.NS",
}

# Defined but not gated in create_dataset — kept for completeness/future use
IT_MACRO_DISABLED_TICKERS = {"INFY.NS", "TCS.NS", "HCLTECH.NS", "WIPRO.NS", "TECHM.NS"}

TREND_FOLLOWING_SECTORS = {"Auto", "Metals", "FMCG", "CementInfra"}

ALL_MACRO_COLS = [
    "nifty_ret_1d", "nifty_ret_5d", "nifty_ret_10d", "nifty_ret_20d",
    "nifty_vol_10d", "usdinr_ret_1d", "usdinr_ret_5d", "usdinr_vol_10d",
    "nasdaq_ret_5d", "nasdaq_ret_20d",
    "crude_ret_1d", "crude_ret_5d", "crude_vol_10d",
    "gold_ret_1d", "gold_ret_5d", "gold_vol_10d",
    "copper_ret_1d", "copper_ret_5d", "copper_vol_10d",
    "vix_level", "vix_ret_1d", "vix_chg_5d", "vix_vol_10d",
    "nifty_pcr_oi", "nifty_pcr_chg_5d",
    "banknifty_pcr_oi", "banknifty_pcr_chg_5d",
    "nifty_futures_basis", "nifty_futures_basis_chg_5d",
    "fii_net_value", "fii_net_chg_5d",
    "dii_net_value", "dii_net_chg_5d",
    "fii_dii_divergence",
]

TICKER_CLASS_THRESHOLDS = {
    "ADANIPORTS.NS": {0: 0.33, 1: 0.33, 2: 0.20},
    "TATASTEEL.NS":  {0: 0.33, 1: 0.33, 2: 0.20},
    "SBIN.NS":       {0: 0.33, 1: 0.33, 2: 0.25},
    "CIPLA.NS":      {0: 0.28, 1: 0.33, 2: 0.33},
    "COALINDIA.NS":  {0: 0.30, 1: 0.30, 2: 0.30},
    "EICHERMOT.NS":  {0: 0.20, 1: 0.33, 2: 0.33},
    "INDIGO.NS":     {0: 0.25, 1: 0.33, 2: 0.33},
    "HEROMOTOCO.NS": {0: 0.33, 1: 0.33, 2: 0.27},
    "GRASIM.NS":     {0: 0.25, 1: 0.33, 2: 0.33},
    "M&M.NS":        {0: 0.33, 1: 0.28, 2: 0.33},
    "APOLLOHOSP.NS": {0: 0.25, 1: 0.33, 2: 0.33},
    "BAJAJFINSV.NS": {0: 0.30, 1: 0.30, 2: 0.30},
    "DRREDDY.NS":    {0: 0.33, 1: 0.25, 2: 0.33},
    "TATACONSUM.NS": {0: 0.28, 1: 0.38, 2: 0.28},
    "MAXHEALTH.NS":  {0: 0.28, 1: 0.38, 2: 0.28},
    "ONGC.NS":       {0: 0.28, 1: 0.33, 2: 0.33},
    "KOTAKBANK.NS":  {0: 0.33, 1: 0.38, 2: 0.28},
    "BRITANNIA.NS":  {0: 0.30, 1: 0.35, 2: 0.20},
}

def apply_threshold_calibration(proba, thresholds):
    """
    Apply per-class probability threshold calibration.
    proba: 1D array of shape (3,) — [SELL, HOLD, BUY] probabilities for one sample.
    thresholds: dict {class_idx: threshold} or None.
    Returns predicted class index (int).
    If thresholds is None, falls back to plain argmax.
    """
    if not thresholds:
        return int(np.argmax(proba))
    return max(
        thresholds.keys(),
        key=lambda c: proba[c] / thresholds[c]
    )


# ---------------------------------------------------------------------------
# Data preparation functions (moved verbatim from ml_trainer.py)
# ---------------------------------------------------------------------------

_MACRO_CACHE = {}

def _validate_macro_asset(df, asset_name, required_column="Close", min_valid_rows=10):
    """
    Validates externally downloaded macro data.
    """
    logger.info(f"MACRO DATA | {asset_name} | rows={len(df) if df is not None else 0}")
    
    if df is None or df.empty:
        logger.warning(f"MACRO DATA | {asset_name} | status=INSUFFICIENT_DATA (Empty/None)")
        return False, None
        
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
        
    if required_column not in df.columns:
        logger.warning(f"MACRO DATA | {asset_name} | status=INSUFFICIENT_DATA (Missing {required_column})")
        return False, df
        
    valid_close = df[required_column].dropna()
    valid_count = len(valid_close)
    
    if valid_count < min_valid_rows:
        logger.warning(f"MACRO DATA | {asset_name} | valid_close={valid_count} | status=INSUFFICIENT_DATA (Requires >= {min_valid_rows})")
        return False, df
        
    first_valid = valid_close.index.min()
    last_valid = valid_close.index.max()
    logger.info(f"MACRO DATA | {asset_name} | valid_close={valid_count} | first={first_valid.date() if first_valid else 'N/A'} | last={last_valid.date() if last_valid else 'N/A'} | status=VALID")
    
    return True, df

def _fetch_cached_macro(ticker, start_date, end_date):
    """Fetches macro data using a run-local memory cache to prevent rate-limiting."""
    cache_key = f"{ticker}_{start_date.date()}_{end_date.date()}"
    if cache_key in _MACRO_CACHE:
        return _MACRO_CACHE[cache_key].copy() if not _MACRO_CACHE[cache_key].empty else _MACRO_CACHE[cache_key]
        
    try:
        df = yf.download(
            ticker, 
            start=start_date, 
            end=end_date + timedelta(days=1), 
            progress=False, 
            auto_adjust=True,
            timeout=10
        )
        _MACRO_CACHE[cache_key] = df
        return df.copy() if not df.empty else df
    except Exception as ex:
        logger.warning(f"macro: {ticker} download failed — {ex}")
        _MACRO_CACHE[cache_key] = pd.DataFrame()
        return pd.DataFrame()

def _prepare_nifty_data(start_date, end_date):
    nifty_df = _fetch_cached_macro("^NSEI", start_date, end_date)
    is_valid, nifty_df = _validate_macro_asset(nifty_df, "^NSEI", min_valid_rows=200)
    
    if not is_valid:
        return pd.DataFrame()

    nifty_df = nifty_df.rename(columns={"Close": "nifty_close"})
    nifty_df = nifty_df[["nifty_close"]].copy()
    nifty_df["nifty_return"] = nifty_df["nifty_close"].pct_change()
    nifty_df["nifty_sma_200"] = nifty_df["nifty_close"].rolling(window=200).mean()
    # Shift by one day to avoid same-day lookahead leakage.
    nifty_df["market_regime"] = (nifty_df["nifty_close"] > nifty_df["nifty_sma_200"]).astype(int).shift(1)
    return nifty_df


def _prepare_macro_data(start_date, end_date, client):
    macro = pd.DataFrame()

    # NIFTY
    nifty = _fetch_cached_macro("^NSEI", start_date, end_date)
    is_valid, nifty = _validate_macro_asset(nifty, "^NSEI", min_valid_rows=20)
    if is_valid:
        c = nifty["Close"]
        nifty_ret_1d = c.pct_change(1)
        macro["nifty_ret_1d"]  = nifty_ret_1d
        macro["nifty_ret_5d"]  = c.pct_change(5)
        macro["nifty_ret_10d"] = c.pct_change(10)
        macro["nifty_ret_20d"] = c.pct_change(20)
        macro["nifty_vol_10d"] = nifty_ret_1d.rolling(10).std()

    # USDINR
    usdinr = _fetch_cached_macro("INR=X", start_date, end_date)
    is_valid, usdinr = _validate_macro_asset(usdinr, "INR=X", min_valid_rows=10)
    if is_valid:
        c = usdinr["Close"]
        usdinr_ret_1d = c.pct_change(1)
        macro["usdinr_ret_1d"]  = usdinr_ret_1d
        macro["usdinr_ret_5d"]  = c.pct_change(5)
        macro["usdinr_vol_10d"] = usdinr_ret_1d.rolling(10).std()
    else:
        logger.warning("WARNING: INR=X unavailable/insufficient. usdinr_ret_1d, usdinr_ret_5d and usdinr_vol_10d using neutral fallback 0.0.")
        macro["usdinr_ret_1d"]  = 0.0
        macro["usdinr_ret_5d"]  = 0.0
        macro["usdinr_vol_10d"] = 0.0

    # NASDAQ
    nasdaq = _fetch_cached_macro("^NDX", start_date, end_date)
    is_valid, nasdaq = _validate_macro_asset(nasdaq, "^NDX", min_valid_rows=20)
    if is_valid:
        c = nasdaq["Close"]
        macro["nasdaq_ret_5d"]  = c.pct_change(5)
        macro["nasdaq_ret_20d"] = c.pct_change(20)
    else:
        logger.warning("WARNING: ^NDX unavailable/insufficient. nasdaq_ret_5d, nasdaq_ret_20d using neutral fallback 0.0.")
        macro["nasdaq_ret_5d"]  = 0.0
        macro["nasdaq_ret_20d"] = 0.0

    # CRUDE
    crude = _fetch_cached_macro("BZ=F", start_date, end_date)
    is_valid, crude = _validate_macro_asset(crude, "BZ=F", min_valid_rows=10)
    if is_valid:
        c = crude["Close"]
        crude_ret_1d = c.pct_change(1)
        macro["crude_ret_1d"]  = crude_ret_1d
        macro["crude_ret_5d"]  = c.pct_change(5)
        macro["crude_vol_10d"] = crude_ret_1d.rolling(10).std()
    else:
        logger.warning("WARNING: BZ=F unavailable/insufficient. crude_ret_1d, crude_ret_5d and crude_vol_10d using neutral fallback 0.0.")
        macro["crude_ret_1d"]  = 0.0
        macro["crude_ret_5d"]  = 0.0
        macro["crude_vol_10d"] = 0.0

    # GOLD
    gold = _fetch_cached_macro("GC=F", start_date, end_date)
    is_valid, gold = _validate_macro_asset(gold, "GC=F", min_valid_rows=10)
    if is_valid:
        c = gold["Close"]
        gold_ret_1d = c.pct_change(1)
        macro["gold_ret_1d"]  = gold_ret_1d
        macro["gold_ret_5d"]  = c.pct_change(5)
        macro["gold_vol_10d"] = gold_ret_1d.rolling(10).std()
    else:
        logger.warning("WARNING: GC=F unavailable/insufficient. gold_ret_1d, gold_ret_5d and gold_vol_10d using neutral fallback 0.0.")
        macro["gold_ret_1d"]  = 0.0
        macro["gold_ret_5d"]  = 0.0
        macro["gold_vol_10d"] = 0.0

    # COPPER
    copper = _fetch_cached_macro("HG=F", start_date, end_date)
    is_valid, copper = _validate_macro_asset(copper, "HG=F", min_valid_rows=10)
    if is_valid:
        c = copper["Close"]
        copper_ret_1d = c.pct_change(1)
        macro["copper_ret_1d"]  = copper_ret_1d
        macro["copper_ret_5d"]  = c.pct_change(5)
        macro["copper_vol_10d"] = copper_ret_1d.rolling(10).std()
    else:
        logger.warning("WARNING: HG=F unavailable/insufficient. copper_ret_1d, copper_ret_5d and copper_vol_10d using neutral fallback 0.0.")
        macro["copper_ret_1d"]  = 0.0
        macro["copper_ret_5d"]  = 0.0
        macro["copper_vol_10d"] = 0.0

    # INDIA VIX
    india_vix = _fetch_cached_macro("^INDIAVIX", start_date, end_date)
    is_valid, india_vix = _validate_macro_asset(india_vix, "^INDIAVIX", min_valid_rows=10)
    if is_valid:
        c = india_vix["Close"]
        vix_ret_1d = c.pct_change(1)
        macro["vix_level"]   = c
        macro["vix_ret_1d"]  = vix_ret_1d
        macro["vix_chg_5d"]  = c.diff(5)
        macro["vix_vol_10d"] = vix_ret_1d.rolling(10).std()
    else:
        logger.warning("WARNING: ^INDIAVIX unavailable/insufficient. vix_level, vix_ret_1d, vix_chg_5d and vix_vol_10d using neutral fallback 0.0.")
        macro["vix_level"]   = 0.0
        macro["vix_ret_1d"]  = 0.0
        macro["vix_chg_5d"]  = 0.0
        macro["vix_vol_10d"] = 0.0

    try:
        db = client["stock_market_db"]
        pcr_docs = list(db.pcr_data.find(
            {"underlying": "NIFTY", "date": {"$gte": start_date, "$lte": end_date}},
            {"date": 1, "pcr_oi": 1, "_id": 0}
        ))
        if pcr_docs:
            pcr_df = pd.DataFrame(pcr_docs)
            pcr_df["date"] = pd.to_datetime(pcr_df["date"]).dt.tz_localize(None)
            pcr_df.set_index("date", inplace=True)
            pcr_df.sort_index(inplace=True)
            macro["nifty_pcr_oi"] = pcr_df["pcr_oi"]
            macro["nifty_pcr_chg_5d"] = pcr_df["pcr_oi"].diff(5)
        else:
            logger.warning("macro: no PCR data found in range — zeroing PCR features")
            macro["nifty_pcr_oi"] = 0.0
            macro["nifty_pcr_chg_5d"] = 0.0
    except Exception as ex:
        logger.warning("macro: PCR fetch failed — %s", ex)
        macro["nifty_pcr_oi"] = 0.0
        macro["nifty_pcr_chg_5d"] = 0.0

    try:
        db = client["stock_market_db"]
        banknifty_pcr_docs = list(db.pcr_data.find(
            {"underlying": "BANKNIFTY", "date": {"$gte": start_date, "$lte": end_date}},
            {"date": 1, "pcr_oi": 1, "_id": 0}
        ))
        if banknifty_pcr_docs:
            bn_pcr_df = pd.DataFrame(banknifty_pcr_docs)
            bn_pcr_df["date"] = pd.to_datetime(bn_pcr_df["date"]).dt.tz_localize(None)
            bn_pcr_df.set_index("date", inplace=True)
            bn_pcr_df.sort_index(inplace=True)
            macro["banknifty_pcr_oi"] = bn_pcr_df["pcr_oi"]
            macro["banknifty_pcr_chg_5d"] = bn_pcr_df["pcr_oi"].diff(5)
        else:
            logger.warning("macro: no BANKNIFTY PCR data found in range — zeroing features")
            macro["banknifty_pcr_oi"] = 0.0
            macro["banknifty_pcr_chg_5d"] = 0.0
    except Exception as ex:
        logger.warning("macro: BANKNIFTY PCR fetch failed — %s", ex)
        macro["banknifty_pcr_oi"] = 0.0
        macro["banknifty_pcr_chg_5d"] = 0.0

    try:
        db = client["stock_market_db"]
        fii_docs = list(db.fii_dii_data.find(
            {"investor_type": "FII", "date": {"$gte": start_date, "$lte": end_date}},
            {"date": 1, "net_value_cr": 1, "_id": 0}
        ))
        dii_docs = list(db.fii_dii_data.find(
            {"investor_type": "DII", "date": {"$gte": start_date, "$lte": end_date}},
            {"date": 1, "net_value_cr": 1, "_id": 0}
        ))
        if fii_docs and dii_docs:
            fii_df = pd.DataFrame(fii_docs)
            fii_df["date"] = pd.to_datetime(fii_df["date"]).dt.tz_localize(None)
            fii_df.set_index("date", inplace=True)
            fii_df.sort_index(inplace=True)

            dii_df = pd.DataFrame(dii_docs)
            dii_df["date"] = pd.to_datetime(dii_df["date"]).dt.tz_localize(None)
            dii_df.set_index("date", inplace=True)
            dii_df.sort_index(inplace=True)

            macro["fii_net_value"]  = fii_df["net_value_cr"]
            macro["fii_net_chg_5d"] = fii_df["net_value_cr"].diff(5)
            macro["dii_net_value"]  = dii_df["net_value_cr"]
            macro["dii_net_chg_5d"] = dii_df["net_value_cr"].diff(5)

            # Aligned on the union of both indices — divergence undefined
            # (NaN) on any date missing either side, which is intentional:
            # dropna() downstream will correctly exclude partial rows
            # rather than silently comparing a real FII value to a
            # stale/zero DII value.
            macro["fii_dii_divergence"] = macro["fii_net_value"] - macro["dii_net_value"]
        else:
            logger.warning(
                "macro: no FII/DII data found in range — zeroing features "
                "(expected until fii_dii_builder.py has accumulated enough "
                "daily history; this is not a fetch failure)"
            )
            macro["fii_net_value"]       = 0.0
            macro["fii_net_chg_5d"]      = 0.0
            macro["dii_net_value"]       = 0.0
            macro["dii_net_chg_5d"]      = 0.0
            macro["fii_dii_divergence"]  = 0.0
    except Exception as ex:
        logger.warning("macro: FII/DII fetch failed — %s", ex)
        macro["fii_net_value"]       = 0.0
        macro["fii_net_chg_5d"]      = 0.0
        macro["dii_net_value"]       = 0.0
        macro["dii_net_chg_5d"]      = 0.0
        macro["fii_dii_divergence"]  = 0.0

    try:
        db = client["stock_market_db"]
        fut_docs = list(db.pcr_data.find(
            {"underlying": "NIFTY", "date": {"$gte": start_date, "$lte": end_date},
             "nifty_fut_close": {"$exists": True}},
            {"date": 1, "nifty_fut_close": 1, "_id": 0}
        ))
        if fut_docs and "nifty_ret_1d" in macro.columns:
            fut_df = pd.DataFrame(fut_docs)
            fut_df["date"] = pd.to_datetime(fut_df["date"]).dt.tz_localize(None)
            fut_df.set_index("date", inplace=True)
            fut_df.sort_index(inplace=True)
            # nifty spot close computed earlier in this function as `c` (from the ^NSEI block)
            spot_aligned = nifty["Close"].reindex(fut_df.index) if "nifty" in dir() else None
            basis_df = fut_df.join(nifty["Close"].rename("spot_close"), how="left")
            basis = (basis_df["nifty_fut_close"] - basis_df["spot_close"]) / basis_df["spot_close"]
            macro["nifty_futures_basis"] = basis
            macro["nifty_futures_basis_chg_5d"] = basis.diff(5)
        else:
            logger.warning("macro: no NIFTY futures data found in range — zeroing futures basis features")
            macro["nifty_futures_basis"] = 0.0
            macro["nifty_futures_basis_chg_5d"] = 0.0
    except Exception as ex:
        logger.warning("macro: NIFTY futures basis fetch failed — %s", ex)
        macro["nifty_futures_basis"] = 0.0
        macro["nifty_futures_basis_chg_5d"] = 0.0

    if macro.empty:
        return pd.DataFrame()

    macro = macro.shift(1)
    return macro

def _prepare_stock_pcr_data(ticker, client, start_date, end_date):
    """
    Fetches per-ticker stock-level options PCR from pcr_data collection
    (populated by pcr_builder.py's _compute_daily_stock_pcr). Distinct
    from the index-level NIFTY/BANKNIFTY PCR in _prepare_macro_data —
    this is joined per-ticker, not market-wide, same architectural
    pattern as _prepare_sector_data.

    Returns empty DataFrame if the ticker has no F&O options data
    (not all Nifty 50 constituents are F&O-enabled, and some symbol
    mappings in TICKER_TO_FO_SYMBOL_OVERRIDES may be incomplete/wrong —
    zero-filled downstream like any other missing-data case).
    """
    db = client["stock_market_db"]
    docs = list(db.pcr_data.find(
        {"ticker": ticker, "date": {"$gte": start_date, "$lte": end_date}},
        {"date": 1, "pcr_oi": 1, "_id": 0}
    ))
    if not docs:
        logger.info("%s: no stock-level PCR data found — stock_pcr features will be zeroed", ticker)
        return pd.DataFrame()

    pcr_df = pd.DataFrame(docs)
    pcr_df["date"] = pd.to_datetime(pcr_df["date"]).dt.tz_localize(None)
    pcr_df.set_index("date", inplace=True)
    pcr_df.sort_index(inplace=True)

    result = pd.DataFrame(index=pcr_df.index)
    result["stock_pcr_oi"] = pcr_df["pcr_oi"]
    result["stock_pcr_chg_5d"] = pcr_df["pcr_oi"].diff(5)

    # Shift by 1 day — same leakage-safety convention as every other
    # macro/PCR column (day T's PCR is only known after market close T).
    result = result.shift(1)
    return result

def _prepare_sentiment_data(news_docs):
    news_df = pd.DataFrame(news_docs)
    if news_df.empty or "published_at" not in news_df.columns:
        return pd.DataFrame(columns=["sentiment"])

    news_df["date"] = pd.to_datetime(news_df["published_at"]).dt.normalize()

    if "compound" in news_df.columns:
        sentiment_df = news_df.groupby("date")["compound"].mean().to_frame(name="sentiment")
        return sentiment_df

    if "sentiment" in news_df.columns:
        if isinstance(news_df["sentiment"].iloc[0], dict):
            extracted = news_df["sentiment"].apply(
                lambda val: val.get("score", 0.0) if isinstance(val, dict) else 0.0
            )
            sentiment_df = extracted.groupby(news_df["date"]).mean().to_frame(name="sentiment")
            return sentiment_df
        sentiment_df = news_df.groupby("date")["sentiment"].mean().to_frame(name="sentiment")
        return sentiment_df

    return pd.DataFrame(columns=["sentiment"])


def _prepare_sector_data(ticker, client, history_years=None):
    """
    Fetches pre-built sector index from sector_indices collection,
    then subtracts this ticker's own contribution for self-exclusion.
    Falls back to Nifty 50 peer loop if sector_indices is unavailable.
    """
    if history_years is None:
        history_years = HISTORY_YEARS

    sector = SECTOR_MAP.get(ticker)
    if sector is None:
        return pd.Series(dtype=float, name="sector_return")

    if ticker in SECTOR_INDEX_DISABLED_TICKERS:
        logger.info("%s: sector index disabled at ticker level", ticker)
        return pd.Series(dtype=float, name="sector_return")

    if sector in EVENT_DRIVEN_SECTORS_NO_INDEX:
        return pd.Series(dtype=float, name="sector_return")

    db = client["stock_market_db"]
    cutoff_date = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=365 * history_years + 10)

    index_sector_name = SECTOR_INDEX_NAME_MAP.get(sector, sector)

    query = {"sector": index_sector_name, "date": {"$gte": cutoff_date}}
    import os
    env_cutoff = os.getenv("TRAINING_CUTOFF_DATE")
    if env_cutoff:
        query["date"]["$lte"] = datetime.fromisoformat(env_cutoff)

    index_docs = list(db.sector_indices.find(
        query,
        {"date": 1, "return": 1, "peer_count": 1, "_id": 0}
    ))
    logger.info("%s: sector index query '%s' returned %d docs", ticker, index_sector_name, len(index_docs))

    if len(index_docs) >= 50:
        index_df = pd.DataFrame(index_docs)
        index_df["date"] = pd.to_datetime(index_df["date"]).dt.tz_localize(None)
        index_df.set_index("date", inplace=True)
        index_df.sort_index(inplace=True)
        sector_index = index_df["return"].rename("sector_return")
        peer_count   = int(index_df["peer_count"].iloc[-1])

        # Self-exclusion: subtract this ticker's weighted contribution
        # sector_index = (N * sector_avg + ticker_return) / N
        # => sector_excl = (N * sector_avg - ticker_return) / (N - 1)
        ticker_docs = list(db.historical_data.find(
            {"ticker": ticker, "date": {"$gte": cutoff_date}},
            {"date": 1, "close": 1, "_id": 0}
        ))
        if ticker_docs and peer_count > 1:
            t_df = pd.DataFrame(ticker_docs)
            t_df["date"] = pd.to_datetime(t_df["date"]).dt.tz_localize(None)
            t_df.set_index("date", inplace=True)
            t_df.sort_index(inplace=True)
            t_df["close"] = pd.to_numeric(t_df["close"], errors="coerce")
            ticker_return = t_df["close"].pct_change().rename("ticker_return")

            aligned = sector_index.to_frame().join(ticker_return, how="left")
            aligned["ticker_return"] = aligned["ticker_return"].fillna(0.0)
            sector_excl = (
                (peer_count * aligned["sector_return"] - aligned["ticker_return"])
                / (peer_count - 1)
            )
            sector_excl.name = "sector_return"
            logger.info(
                "%s: sector '%s' index loaded (%d peers, self-excluded)",
                ticker, sector, peer_count - 1,
            )
            return sector_excl

        logger.info("%s: sector '%s' index loaded (%d peers, no self-exclusion)", ticker, sector, peer_count)
        return sector_index

    # --- Fallback: original Nifty 50 peer loop ---
    logger.info(
        "%s: sector_indices unavailable for '%s' — falling back to Nifty 50 peer loop",
        ticker, sector,
    )
    sector_peers = [t for t, s in SECTOR_MAP.items() if s == sector and t != ticker]
    if len(sector_peers) < SECTOR_MIN_PEERS:
        logger.info(
            "%s: sector '%s' has only %d peers after self-exclusion (min=%d) — sector momentum disabled",
            ticker, sector, len(sector_peers), SECTOR_MIN_PEERS,
        )
        return pd.Series(dtype=float, name="sector_return")

    peer_returns = []
    for peer in sector_peers:
        peer_df = pd.DataFrame(list(db.historical_data.find(
            {"ticker": peer, "date": {"$gte": cutoff_date}},
            {"date": 1, "close": 1, "_id": 0}
        )))
        if peer_df.empty:
            continue
        peer_df["date"] = pd.to_datetime(peer_df["date"]).dt.tz_localize(None)
        peer_df.set_index("date", inplace=True)
        peer_df.sort_index(inplace=True)
        peer_df["close"] = pd.to_numeric(peer_df["close"], errors="coerce")
        peer_returns.append(peer_df["close"].pct_change().rename(peer))

    if len(peer_returns) < SECTOR_MIN_PEERS:
        logger.info(
            "%s: only %d peers had data in MongoDB (min=%d) — sector momentum disabled",
            ticker, len(peer_returns), SECTOR_MIN_PEERS,
        )
        return pd.Series(dtype=float, name="sector_return")

    sector_df = pd.concat(peer_returns, axis=1)
    sector_return = sector_df.mean(axis=1)
    sector_return.name = "sector_return"
    return sector_return


def _find_col(df_in, *tokens):
    """Return first column whose uppercase name contains ALL tokens."""
    for col in df_in.columns:
        col_up = str(col).upper()
        if all(str(t).upper() in col_up for t in tokens):
            return col
    return None


def _is_expiry_week(idx):
    """NSE F&O expiry week: week containing last Thursday of the month."""
    result = np.zeros(len(idx), dtype=int)
    for i, dt in enumerate(idx):
        last_day = calendar.monthrange(dt.year, dt.month)[1]
        last_thu = max(
            d for d in range(1, last_day + 1)
            if pd.Timestamp(dt.year, dt.month, d).weekday() == 3
        )
        expiry = pd.Timestamp(dt.year, dt.month, last_thu)
        week_start = expiry - pd.Timedelta(days=4)
        if week_start <= dt <= expiry:
            result[i] = 1
    return result


def add_calendar_features(df):
    """
    Adds calendar features to a DataFrame with a DatetimeIndex.
    Identical to the calendar block in ml_trainer.py's create_dataset().
    """
    month = df.index.month
    df["month_sin"] = np.sin(2 * np.pi * month / 12)
    df["month_cos"] = np.cos(2 * np.pi * month / 12)

    # Month-end: last 3 trading days of each calendar month
    df["is_month_end"] = (
        df.index.to_series()
        .groupby(df.index.to_period("M"))
        .transform(lambda x: x.rank(ascending=False) <= 3)
        .astype(int)
        .values
    )

    # Month-start: first 3 trading days of each calendar month
    df["is_month_start"] = (
        df.index.to_series()
        .groupby(df.index.to_period("M"))
        .transform(lambda x: x.rank(ascending=True) <= 3)
        .astype(int)
        .values
    )

    # Quarter-end: last 5 trading days of March/June/September/December
    quarter_end_months = {3, 6, 9, 12}
    last_5_of_month = (
        df.index.to_series()
        .groupby(df.index.to_period("M"))
        .transform(lambda x: x.rank(ascending=False) <= 5)
        .astype(int)
        .values
    )
    in_quarter_end_month = df.index.month.isin(quarter_end_months).astype(int)
    df["quarter_end"] = last_5_of_month * in_quarter_end_month

    df["is_expiry_week"] = _is_expiry_week(df.index)

    # Earnings season: Q4=Apr-May, Q1=Jul-Aug, Q2=Oct-Nov, Q3=Jan-Feb
    earnings_months = {1, 2, 4, 5, 7, 8, 10, 11}
    df["in_earnings_season"] = df.index.month.isin(earnings_months).astype(int)

    return df


def add_technical_indicators(df, ticker):
    """
    Computes technical indicators (RSI/MACD/BBands/ATR/ADX), resolves
    pandas_ta column names via _find_col, shifts by 1 to prevent
    look-ahead bias, and adds derived features.

    Identical to the technical indicator block in ml_trainer.py's
    create_dataset().

    Returns (df, success) — success is False if required indicator
    columns could not be resolved.
    """
    df.ta.rsi(length=14, append=True)
    df.ta.macd(append=True)
    df.ta.bbands(append=True)
    df.ta.atr(length=14, append=True)
    df.ta.adx(length=14, append=True)

    ta_cols = [c for c in df.columns if any(x in c.upper() for x in ["RSI", "MACD", "BB", "ATR", "ADX"])]
    logger.debug("%s: pandas_ta columns detected = %s", ticker, ta_cols)

    rsi_col   = _find_col(df, "RSI")
    macdh_col = _find_col(df, "MACD", "H")
    bbl_col   = _find_col(df, "BBL")
    bbm_col   = _find_col(df, "BBM")
    bbu_col   = _find_col(df, "BBU")
    atr_col   = _find_col(df, "ATR")
    adx_col   = _find_col(df, "ADX")

    missing = {
        "rsi":   rsi_col,
        "macdh": macdh_col,
        "bbl":   bbl_col,
        "bbm":   bbm_col,
        "bbu":   bbu_col,
        "atr":   atr_col,
    }
    missing_keys = [k for k, v in missing.items() if v is None]
    if missing_keys:
        logger.warning(
            "%s: missing indicator columns %s. Available ta cols: %s",
            ticker, missing_keys, ta_cols,
        )
        return df, False

    # Shift resolved indicator columns to avoid look-ahead bias
    resolved_indicator_cols = [
        c for c in [rsi_col, macdh_col, bbl_col, bbm_col, bbu_col, atr_col, adx_col]
        if c is not None
    ]
    for col in resolved_indicator_cols:
        df[col] = df[col].shift(1)

    df["rsi"]       = df[rsi_col]
    df["macd_hist"] = df[macdh_col]
    df["bb_width"]  = (df[bbu_col] - df[bbl_col]) / df[bbm_col].replace(0, pd.NA)
    df["atr"]       = df[atr_col]
    df["atr_pct"]   = df["atr"] / df["close"].replace(0, pd.NA)

    # ADX — already shifted above via resolved_indicator_cols
    ticker_sector = SECTOR_MAP.get(ticker, "")
    if ticker_sector in TREND_FOLLOWING_SECTORS:
        if adx_col:
            df["adx"] = df[adx_col]
            df["adx_trending"] = (df["adx"] > 25).astype(int)
        else:
            df["adx"] = 25.0
            df["adx_trending"] = 0
    else:
        df["adx"] = 25.0
        df["adx_trending"] = 0
        logger.info(
            "%s: ADX disabled for sector '%s' — event-driven stock",
            ticker, ticker_sector,
        )

    return df, True


def add_derived_features(df, ticker, client):
    """
    Adds sentiment rolling averages, price changes, market correlation,
    OBV/VWAP deviations, relative volume, HL compression, and sector
    momentum.

    Identical to the derived-feature block in ml_trainer.py's
    create_dataset().
    """
    df["sentiment_7d_avg"]  = df["sentiment"].shift(1).rolling(window=7).mean()
    df["sentiment_30d_avg"] = df["sentiment"].shift(1).rolling(window=30).mean()
    df["price_change_1d"]   = df["close"].shift(1).pct_change(1)
    df["price_change_5d"]   = df["close"].shift(1).pct_change(5)
    df["market_correlation"] = (
        df["return"]
        .shift(1)
        .rolling(
            window=30,
            min_periods=20,
        )
        .corr(
            df["nifty_return"].shift(1)
        )
    )

    # --- DIAGNOSTIC: inspect market correlation inputs/output ---
    logger.info(
        "%s: MARKET CORRELATION DIAGNOSTIC | "
        "latest_date=%s | "
        "market_corr_latest=%s | market_corr_last_valid=%s | "
        "nifty_return_latest=%s | nifty_return_last_valid=%s | "
        "stock_return_latest=%s | stock_return_last_valid=%s",
        ticker,
        df.index[-1],
        df["market_correlation"].iloc[-1],
        df["market_correlation"].last_valid_index(),
        df["nifty_return"].iloc[-1],
        df["nifty_return"].last_valid_index(),
        df["return"].iloc[-1],
        df["return"].last_valid_index(),
    )

    # Count valid stock/Nifty return pairs in the latest 30-row window
    latest_30 = pd.DataFrame({
        "stock_return": df["return"].shift(1),
        "nifty_return": df["nifty_return"].shift(1),
    }).tail(30)

    valid_pairs = latest_30.dropna()

    logger.info(
        "%s: MARKET CORRELATION WINDOW | "
        "rows=30 | valid_pairs=%d | missing_stock=%d | missing_nifty=%d",
        ticker,
        len(valid_pairs),
        int(latest_30["stock_return"].isna().sum()),
        int(latest_30["nifty_return"].isna().sum()),
    )

    # --- OBV deviation ---
    obv_raw  = (np.sign(df["close"].diff()) * df["volume"]).fillna(0).cumsum()
    obv_mean = obv_raw.rolling(window=20).mean()
    df["obv_deviation"] = (
        (obv_raw - obv_mean) / obv_mean.abs().replace(0, pd.NA)
    ).shift(1)

    # --- VWAP deviation ---
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    vwap_20 = (
        (typical_price * df["volume"]).rolling(window=20).sum()
        / df["volume"].rolling(window=20).sum()
    )
    df["vwap_deviation"] = (
        (df["close"] - vwap_20) / vwap_20.replace(0, pd.NA)
    ).shift(1)

    # Relative volume — today's volume vs. 20-day rolling average volume.
    vol_sma_20 = df["volume"].rolling(window=20).mean()
    df["relative_volume"] = (
        df["volume"] / vol_sma_20.replace(0, pd.NA)
    ).shift(1)

    stock_pcr_df = _prepare_stock_pcr_data(ticker, client, df.index.min(), df.index.max())
    if not stock_pcr_df.empty:
        df = df.join(stock_pcr_df, how="left")
        logger.info("%s: stock-level PCR joined (%d rows with data)", ticker, stock_pcr_df["stock_pcr_oi"].notna().sum())
    else:
        df["stock_pcr_oi"] = 0.0
        df["stock_pcr_chg_5d"] = 0.0

    # HL compression — today's high-low range vs. 20-day average range.
    daily_range = df["high"] - df["low"]
    avg_range_20 = daily_range.rolling(window=20).mean()
    df["hl_compression"] = (
        daily_range / avg_range_20.replace(0, pd.NA)
    ).shift(1)

    # --- SECTOR MOMENTUM ---
    history_years = TICKER_HISTORY_OVERRIDE.get(ticker, HISTORY_YEARS)
    sector_return = _prepare_sector_data(ticker, client, history_years=history_years)
    if not sector_return.empty:
        df = df.join(sector_return.to_frame(), how="left")
        df["sector_return"] = df["sector_return"].fillna(0.0)
        df["sector_momentum"] = df["return"].shift(1) - df["sector_return"].shift(1)
        df["sector_momentum_5d"] = (
            df["close"].shift(1).pct_change(5)
            - df["sector_return"].shift(1).rolling(5).sum()
        )
    else:
        df["sector_momentum"]    = 0.0
        df["sector_momentum_5d"] = 0.0

    return df


# ---------------------------------------------------------------------------
# Inference-time feature builder
# ---------------------------------------------------------------------------

def build_feature_row(ticker, client, db):
    """
    Builds the full feature DataFrame for *inference* — identical
    pipeline to create_dataset() in ml_trainer.py, minus the target
    label columns (future_return, target).

    Fetches enough historical data (250+ trading days, or more for
    tickers in TICKER_HISTORY_OVERRIDE) to satisfy all rolling-window
    requirements (200-day Nifty SMA, 30-day correlation, 20-day
    OBV/VWAP/relative_volume, 5-day sector momentum).

    Returns the full DataFrame; the caller should select
    df[feature_names].iloc[-1] to get the latest feature row.
    """
    if ticker in TICKER_START_DATE_OVERRIDE:
        cutoff_date = TICKER_START_DATE_OVERRIDE[ticker]
    else:
        history_years = TICKER_HISTORY_OVERRIDE.get(ticker, HISTORY_YEARS)
        cutoff_date = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=365 * history_years + 10)

    prices_df = pd.DataFrame(
        list(db.historical_data.find({"ticker": ticker, "date": {"$gte": cutoff_date}}))
    )
    if prices_df.empty:
        raise FileNotFoundError(
            f"No historical data found for {ticker} in MongoDB for last {history_years} years"
        )

    prices_df["date"] = pd.to_datetime(prices_df["date"]).dt.tz_localize(None)
    prices_df.set_index("date", inplace=True)
    prices_df.sort_index(inplace=True)

    for col in ["open", "high", "low", "close", "volume"]:
        if col in prices_df.columns:
            prices_df[col] = pd.to_numeric(prices_df[col], errors="coerce")

    start_date = prices_df.index.min()
    end_date = prices_df.index.max()

    # --- 1. Nifty join ---
    nifty_df = _prepare_nifty_data(start_date, end_date)
    if nifty_df.empty:
        raise ValueError(f"{ticker}: failed to fetch Nifty data")

    df = prices_df.join(nifty_df[["nifty_return", "market_regime"]], how="left")

    # --- DIAGNOSTIC: verify Nifty alignment with stock trading dates ---
    missing_nifty = df["nifty_return"].isna()

    if missing_nifty.any():
        missing_dates = df.index[missing_nifty]

        logger.warning(
            "%s: Nifty return missing on %d/%d stock trading dates | "
            "first_missing=%s | last_missing=%s | nifty_last_valid=%s",
            ticker,
            int(missing_nifty.sum()),
            len(df),
            missing_dates.min(),
            missing_dates.max(),
            df["nifty_return"].last_valid_index(),
        )
    else:
        logger.info(
            "%s: Nifty return alignment OK | %d/%d dates populated | "
            "last_valid=%s",
            ticker,
            int(df["nifty_return"].notna().sum()),
            len(df),
            df["nifty_return"].last_valid_index(),
        )

    logger.info(
        "%s: stock_date_range=%s -> %s | nifty_date_range=%s -> %s",
        ticker,
        df.index.min(),
        df.index.max(),
        nifty_df.index.min(),
        nifty_df.index.max(),
    )

    # --- 2. Macro join (zero-fill on failure, matching ALL_MACRO_COLS) ---
    macro_df = _prepare_macro_data(start_date, end_date, client)
    if not macro_df.empty:
        df = df.join(macro_df, how="left")
        for col in ALL_MACRO_COLS:
            if col not in df.columns:
                logger.warning(
                    "%s: macro column '%s' missing after join — zeroing",
                    ticker, col,
                )
                df[col] = 0.0
        logger.info(
            "%s: macro features joined — nifty multi-tf, usdinr, nasdaq, crude/gold/copper (%d cols)",
            ticker, macro_df.shape[1],
        )
    else:
        logger.warning("%s: macro data empty — macro features will be zeroed", ticker)
        for col in ALL_MACRO_COLS:
            df[col] = 0.0

    # --- 3. Return & outperformance ---
    df["return"] = df["close"].pct_change()
    df["outperformance"] = df["return"].shift(1) - df["nifty_return"].shift(1)

    # --- 4. Sentiment join ---
    news_docs = list(
        db.news_articles.find(
            {"$or": [{"tickers": ticker}, {"ticker": ticker}], "published_at": {"$gte": cutoff_date}},
            {"published_at": 1, "compound": 1, "sentiment": 1},
        )
    )
    sentiment_df = _prepare_sentiment_data(news_docs)
    if sentiment_df.empty:
        df["sentiment"] = 0.0
    else:
        df = df.join(sentiment_df, how="left")
        df["sentiment"] = df["sentiment"].fillna(0.0)

    # --- 5. Technical indicators (shifted by 1) ---
    df, success = add_technical_indicators(df, ticker)
    if not success:
        raise ValueError(
            f"{ticker}: required technical indicator columns could not be resolved"
        )

    # --- 6. Derived features + sector momentum ---
    df = add_derived_features(df, ticker, client)

    # --- 7. Calendar features ---
    df = add_calendar_features(df)

    # --- 8. Clean up infinities ---
    df = df.replace([float("inf"), float("-inf")], pd.NA)

    logger.info(
        "%s: inference feature build complete — %d rows, %d columns",
        ticker, len(df), len(df.columns),
    )

    return df
