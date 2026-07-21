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

TICKER_ATR_THRESHOLD_SCALE: dict[str, float] = {
    "NTPC.NS":       0.70,
    "POWERGRID.NS":  0.65,
    "COALINDIA.NS":  0.75,
    "SHRIRAMFIN.NS": 0.75,
}

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
]


# ---------------------------------------------------------------------------
# Data preparation functions (moved verbatim from ml_trainer.py)
# ---------------------------------------------------------------------------

def _prepare_nifty_data(start_date, end_date):
    nifty_df = yf.download(
        "^NSEI",
        start=start_date,
        end=end_date + timedelta(days=1),
        progress=False,
        auto_adjust=True,
    )
    if nifty_df.empty:
        return pd.DataFrame()

    if isinstance(nifty_df.columns, pd.MultiIndex):
        nifty_df.columns = nifty_df.columns.get_level_values(0)

    nifty_df = nifty_df.rename(columns={"Close": "nifty_close"})
    nifty_df = nifty_df[["nifty_close"]].copy()
    nifty_df["nifty_return"] = nifty_df["nifty_close"].pct_change()
    nifty_df["nifty_sma_200"] = nifty_df["nifty_close"].rolling(window=200).mean()
    # Shift by one day to avoid same-day lookahead leakage.
    nifty_df["market_regime"] = (nifty_df["nifty_close"] > nifty_df["nifty_sma_200"]).astype(int).shift(1)
    return nifty_df


def _prepare_macro_data(start_date, end_date, client):
    macro = pd.DataFrame()

    try:
        nifty = yf.download(
            "^NSEI", start=start_date,
            end=end_date + timedelta(days=1),
            progress=False, auto_adjust=True,
        )
        if not nifty.empty:
            if isinstance(nifty.columns, pd.MultiIndex):
                nifty.columns = nifty.columns.get_level_values(0)
            c = nifty["Close"]
            nifty_ret_1d = c.pct_change(1)
            macro["nifty_ret_1d"]  = nifty_ret_1d
            macro["nifty_ret_5d"]  = c.pct_change(5)
            macro["nifty_ret_10d"] = c.pct_change(10)
            macro["nifty_ret_20d"] = c.pct_change(20)
            macro["nifty_vol_10d"] = nifty_ret_1d.rolling(10).std()
    except Exception as ex:
        logger.warning("macro: Nifty download failed — %s", ex)

    try:
        usdinr = yf.download(
            "INR=X", start=start_date,
            end=end_date + timedelta(days=1),
            progress=False, auto_adjust=True,
        )
        if not usdinr.empty:
            if isinstance(usdinr.columns, pd.MultiIndex):
                usdinr.columns = usdinr.columns.get_level_values(0)
            c = usdinr["Close"]
            usdinr_ret_1d = c.pct_change(1)
            macro["usdinr_ret_1d"]  = usdinr_ret_1d
            macro["usdinr_ret_5d"]  = c.pct_change(5)
            macro["usdinr_vol_10d"] = usdinr_ret_1d.rolling(10).std()
    except Exception as ex:
        logger.warning("macro: USD/INR download failed — %s", ex)

    try:
        nasdaq = yf.download(
            "^NDX", start=start_date,
            end=end_date + timedelta(days=1),
            progress=False, auto_adjust=True,
        )
        if not nasdaq.empty:
            if isinstance(nasdaq.columns, pd.MultiIndex):
                nasdaq.columns = nasdaq.columns.get_level_values(0)
            c = nasdaq["Close"]
            macro["nasdaq_ret_5d"]  = c.pct_change(5)
            macro["nasdaq_ret_20d"] = c.pct_change(20)
        else:
            # Download succeeded but returned empty — zero-fill
            logger.warning("macro: Nasdaq returned empty data — zeroing nasdaq features")
            macro["nasdaq_ret_5d"]  = 0.0
            macro["nasdaq_ret_20d"] = 0.0
    except Exception as ex:
        logger.warning("macro: Nasdaq download failed — %s", ex)
        # Zero-fill so downstream code always sees these columns
        macro["nasdaq_ret_5d"]  = 0.0
        macro["nasdaq_ret_20d"] = 0.0

    try:
        crude = yf.download(
            "BZ=F", start=start_date,
            end=end_date + timedelta(days=1),
            progress=False, auto_adjust=True,
        )
        if not crude.empty:
            if isinstance(crude.columns, pd.MultiIndex):
                crude.columns = crude.columns.get_level_values(0)
            c = crude["Close"]
            crude_ret_1d = c.pct_change(1)
            macro["crude_ret_1d"]  = crude_ret_1d
            macro["crude_ret_5d"]  = c.pct_change(5)
            macro["crude_vol_10d"] = crude_ret_1d.rolling(10).std()
        else:
            logger.warning("macro: Crude returned empty data — zeroing crude features")
            macro["crude_ret_1d"]  = 0.0
            macro["crude_ret_5d"]  = 0.0
            macro["crude_vol_10d"] = 0.0
    except Exception as ex:
        logger.warning("macro: Crude download failed — %s", ex)
        macro["crude_ret_1d"]  = 0.0
        macro["crude_ret_5d"]  = 0.0
        macro["crude_vol_10d"] = 0.0

    try:
        gold = yf.download(
            "GC=F", start=start_date,
            end=end_date + timedelta(days=1),
            progress=False, auto_adjust=True,
        )
        if not gold.empty:
            if isinstance(gold.columns, pd.MultiIndex):
                gold.columns = gold.columns.get_level_values(0)
            c = gold["Close"]
            gold_ret_1d = c.pct_change(1)
            macro["gold_ret_1d"]  = gold_ret_1d
            macro["gold_ret_5d"]  = c.pct_change(5)
            macro["gold_vol_10d"] = gold_ret_1d.rolling(10).std()
        else:
            logger.warning("macro: Gold returned empty data — zeroing gold features")
            macro["gold_ret_1d"]  = 0.0
            macro["gold_ret_5d"]  = 0.0
            macro["gold_vol_10d"] = 0.0
    except Exception as ex:
        logger.warning("macro: Gold download failed — %s", ex)
        macro["gold_ret_1d"]  = 0.0
        macro["gold_ret_5d"]  = 0.0
        macro["gold_vol_10d"] = 0.0

    try:
        copper = yf.download(
            "HG=F", start=start_date,
            end=end_date + timedelta(days=1),
            progress=False, auto_adjust=True,
        )
        if not copper.empty:
            if isinstance(copper.columns, pd.MultiIndex):
                copper.columns = copper.columns.get_level_values(0)
            c = copper["Close"]
            copper_ret_1d = c.pct_change(1)
            macro["copper_ret_1d"]  = copper_ret_1d
            macro["copper_ret_5d"]  = c.pct_change(5)
            macro["copper_vol_10d"] = copper_ret_1d.rolling(10).std()
        else:
            logger.warning("macro: Copper returned empty data — zeroing copper features")
            macro["copper_ret_1d"]  = 0.0
            macro["copper_ret_5d"]  = 0.0
            macro["copper_vol_10d"] = 0.0
    except Exception as ex:
        logger.warning("macro: Copper download failed — %s", ex)
        macro["copper_ret_1d"]  = 0.0
        macro["copper_ret_5d"]  = 0.0
        macro["copper_vol_10d"] = 0.0

    try:
        india_vix = yf.download(
            "^INDIAVIX", start=start_date,
            end=end_date + timedelta(days=1),
            progress=False, auto_adjust=True,
        )
        if not india_vix.empty:
            if isinstance(india_vix.columns, pd.MultiIndex):
                india_vix.columns = india_vix.columns.get_level_values(0)
            c = india_vix["Close"]
            vix_ret_1d = c.pct_change(1)
            macro["vix_level"]   = c
            macro["vix_ret_1d"]  = vix_ret_1d
            macro["vix_chg_5d"]  = c.diff(5)
            macro["vix_vol_10d"] = vix_ret_1d.rolling(10).std()
        else:
            logger.warning("macro: India VIX returned empty data — zeroing vix features")
            macro["vix_level"]   = 0.0
            macro["vix_ret_1d"]  = 0.0
            macro["vix_chg_5d"]  = 0.0
            macro["vix_vol_10d"] = 0.0
    except Exception as ex:
        logger.warning("macro: India VIX download failed — %s", ex)
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

    if macro.empty:
        return pd.DataFrame()

    macro = macro.shift(1)
    return macro


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

    index_docs = list(db.sector_indices.find(
        {"sector": index_sector_name, "date": {"$gte": cutoff_date}},
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
        df["return"].shift(1)
        .rolling(window=30)
        .corr(df["nifty_return"].shift(1))
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
            {"ticker": ticker, "published_at": {"$gte": cutoff_date}},
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
