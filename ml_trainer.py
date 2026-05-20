import json
import logging
import os
from datetime import datetime, timedelta, timezone

import joblib
import optuna
import pandas as pd
import numpy as np
import pandas_ta as ta
import yfinance as yf
from dotenv import load_dotenv
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline
from pymongo import MongoClient
from sklearn.metrics import (
    classification_report,
    f1_score,
    precision_recall_fscore_support,
)
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBClassifier


# --- SETUP ---
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MODELS_DIR = "models"
FEATURES_DIR = "features"
RANDOM_STATE = 42
HISTORY_YEARS = 5
MIN_ROWS_AFTER_FEATURES = 200
N_SPLITS = 5
N_OPTUNA_TRIALS = 75

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)

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
    "COALINDIA.NS":  "OilGasAndConsumableFuels",
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

# Add near the top of ml_trainer.py, after SECTOR_MAP definition
SECTOR_INDEX_NAME_MAP = {
    "IT":           "InformationTechnology",
    "OilGas":       "OilGasAndConsumableFuels",
    "Auto":         "AutomobileAndAutoComponents",
    "Metals":       "MetalsAndMining",
    "Banking":      "FinancialServices",
    "Pharma":       "Healthcare",
    "PowerUtilities": "PowerUtilities",
    "Telecom":      "Telecommunication",
    "FMCG":         "FastMovingConsumerGoods",
    "CementInfra":  "ConstructionMaterials",
    "Realty":       "Realty",          # already long-form
    "CapitalGoods": "CapitalGoods",    # already long-form
    "MediaEnt":     "MediaEntertainmentAndPublication",
    "Construction": "Construction",
    "Services": "Services",
    "Diversified": "Diversified",
    "ConsumerServices": "ConsumerServices",
}

TICKER_HISTORY_OVERRIDE = {
    "MARUTI.NS": 3,  # confirmed concept drift across 15 runs
}

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

SECTOR_MIN_PEERS = 4  # if fewer valid peers, disable sector momentum

def _prepare_sector_data(ticker, client):
    """
    Fetches pre-built sector index from sector_indices collection,
    then subtracts this ticker's own contribution for self-exclusion.
    Falls back to Nifty 50 peer loop if sector_indices is unavailable.
    """
    sector = SECTOR_MAP.get(ticker)
    if sector is None:
        return pd.Series(dtype=float, name="sector_return")
    
    EVENT_DRIVEN_SECTORS_NO_INDEX = {"Healthcare", "InformationTechnology"}
    if sector in EVENT_DRIVEN_SECTORS_NO_INDEX:
        return pd.Series(dtype=float, name="sector_return")

    db = client["stock_market_db"]
    cutoff_date = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=365 * HISTORY_YEARS + 10)

    index_sector_name = SECTOR_INDEX_NAME_MAP.get(sector, sector)

    index_docs = list(db.sector_indices.find(
        {"sector": index_sector_name, "date": {"$gte": cutoff_date}},
        {"date": 1, "return": 1, "peer_count": 1, "_id": 0}
    ))
    logger.info("%s: sector index query '%s' returned %d docs", ticker, index_sector_name, len(index_docs))

    # Then use index_sector_name in the find() query:
    index_docs = list(db.sector_indices.find(
        {"sector": index_sector_name, "date": {"$gte": cutoff_date}},
        {"date": 1, "return": 1, "peer_count": 1, "_id": 0}
    ))

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

TREND_FOLLOWING_SECTORS = {"Auto", "Metals", "FMCG", "CementInfra"}

def create_dataset(ticker, client):
    """
    Pulls 5 years of data from MongoDB and engineers leakage-safe features.
    """
    db = client["stock_market_db"]
    history_years = TICKER_HISTORY_OVERRIDE.get(ticker, HISTORY_YEARS)
    cutoff_date = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=365 * history_years + 10)

    prices_df = pd.DataFrame(
        list(db.historical_data.find({"ticker": ticker, "date": {"$gte": cutoff_date}}))
    )
    if prices_df.empty:
        logger.warning("%s: no historical data found in MongoDB for last %s years", ticker, HISTORY_YEARS)
        return pd.DataFrame()

    prices_df["date"] = pd.to_datetime(prices_df["date"]).dt.tz_localize(None)
    prices_df.set_index("date", inplace=True)
    prices_df.sort_index(inplace=True)

    for col in ["open", "high", "low", "close", "volume"]:
        if col in prices_df.columns:
            prices_df[col] = pd.to_numeric(prices_df[col], errors="coerce")

    start_date = prices_df.index.min()
    end_date = prices_df.index.max()
    nifty_df = _prepare_nifty_data(start_date, end_date)
    if nifty_df.empty:
        logger.warning("%s: failed to fetch Nifty data, skipping", ticker)
        return pd.DataFrame()

    df = prices_df.join(nifty_df[["nifty_return", "market_regime"]], how="left")
    df["return"] = df["close"].pct_change()
    df["outperformance"] = df["return"].shift(1) - df["nifty_return"].shift(1)

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

    df.ta.rsi(length=14, append=True)
    df.ta.macd(append=True)
    df.ta.bbands(append=True)
    df.ta.atr(length=14, append=True)
    df.ta.adx(length=14, append=True)

    ta_cols = [c for c in df.columns if any(x in c.upper() for x in ["RSI", "MACD", "BB", "ATR", "ADX"])]
    logger.debug("%s: pandas_ta columns detected = %s", ticker, ta_cols)

    def _find_col(df_in, *tokens):
        """Return first column whose uppercase name contains ALL tokens."""
        for col in df_in.columns:
            col_up = str(col).upper()
            if all(str(t).upper() in col_up for t in tokens):
                return col
        return None

    rsi_col   = _find_col(df, "RSI")
    macdh_col = _find_col(df, "MACD", "H")
    bbl_col   = _find_col(df, "BBL")
    bbm_col   = _find_col(df, "BBM")
    bbu_col   = _find_col(df, "BBU")
    atr_col   = _find_col(df, "ATR")
    adx_col   = _find_col(df, "ADX")

    missing = {
        "rsi": rsi_col,
        "macdh": macdh_col,
        "bbl": bbl_col,
        "bbm": bbm_col,
        "bbu": bbu_col,
        "atr": atr_col,
    }
    missing_keys = [k for k, v in missing.items() if v is None]
    if missing_keys:
        logger.warning(
            "%s: missing indicator columns %s. Available ta cols: %s",
            ticker,
            missing_keys,
            ta_cols,
        )
        return pd.DataFrame()

    # Shift resolved indicator columns to avoid look-ahead bias
    resolved_indicator_cols = [
        c for c in [rsi_col, macdh_col, bbl_col, bbm_col, bbu_col, atr_col, adx_col] if c is not None
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
        # Banking, IT, FinancialServices, Telecom, OilGas — ADX adds noise
        df["adx"] = 25.0          # neutral, won't influence model
        df["adx_trending"] = 0
        logger.info(
            "%s: ADX disabled for sector '%s' — event-driven stock",
            ticker, ticker_sector
        )

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
    obv_raw = (np.sign(df["close"].diff()) * df["volume"]).fillna(0).cumsum()
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

    # --- SECTOR MOMENTUM ---
    sector_return = _prepare_sector_data(ticker, client)
    if not sector_return.empty:
        df = df.join(sector_return.to_frame(), how="left")
        df["sector_return"] = df["sector_return"].fillna(0.0)
        df["sector_momentum"] = df["return"].shift(1) - df["sector_return"].shift(1)
        df["sector_momentum_5d"] = (
            df["close"].shift(1).pct_change(5)
            - df["sector_return"].shift(1).rolling(5).sum()
        )
    else:
        df["sector_momentum"] = 0.0
        df["sector_momentum_5d"] = 0.0

    # --- TARGET LABEL ---
    df["future_10d_return"] = df["close"].shift(-10) / df["close"] - 1
    threshold = np.maximum(1.0 * df["atr_pct"], 0.01)
    df["target"] = 1
    df.loc[df["future_10d_return"] > threshold, "target"] = 2
    df.loc[df["future_10d_return"] < -threshold, "target"] = 0

    required_columns = [
        "rsi",
        "macd_hist",
        "bb_width",
        "atr",
        "atr_pct",
        "sentiment_7d_avg",
        "price_change_1d",
        "price_change_5d",
        "market_correlation",
        "outperformance",
        "market_regime",
        "obv_deviation",
        "vwap_deviation",
        "sector_momentum",
        "adx",
        "target",
    ]
    df = df.replace([float("inf"), float("-inf")], pd.NA)
    df = df.dropna(subset=required_columns)

    return df


def _make_feature_list(df):
    candidate_features = [
        "rsi",
        "macd_hist",
        "bb_width",
        "atr",
        "atr_pct",
        "sentiment_7d_avg",
        "sentiment_30d_avg",
        "price_change_1d",
        "price_change_5d",
        "market_correlation",
        "outperformance",
        "market_regime",
        "obv_deviation",
        "vwap_deviation",
        "sector_momentum",
        "sector_momentum_5d",
        "adx",
        "adx_trending",
    ]
    return [feature for feature in candidate_features if feature in df.columns]


# Exponential fold weights: later folds penalize overfitting to early regimes
FOLD_WEIGHTS = {1: 1.0, 2: 1.2, 3: 1.5, 4: 2.0, 5: 2.5}

def _optuna_objective(trial, X_train, y_train):
    params = {
        "n_estimators":      trial.suggest_int("n_estimators", 100, 700),
        "max_depth":         trial.suggest_int("max_depth", 3, 7),
        "learning_rate":     trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "subsample":         trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree":  trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "min_child_weight":  trial.suggest_int("min_child_weight", 5, 25),
        "reg_lambda":        trial.suggest_float("reg_lambda", 2.0, 15.0),
        "reg_alpha":         trial.suggest_float("reg_alpha", 0.5, 5.0),
        "gamma":             trial.suggest_float("gamma", 0.5, 5.0),
    }

    # Optuna-tunable SMOTE ratios — prevent BUY/SELL inflation per trial
    smote_minority_ratio = trial.suggest_float("smote_minority_ratio", 0.40, 0.70)

    cv = TimeSeriesSplit(n_splits=N_SPLITS)
    weighted_score = 0.0
    total_weight   = 0.0

    for fold_idx, (train_idx, valid_idx) in enumerate(cv.split(X_train), start=1):
        X_fold_train = X_train.iloc[train_idx]
        y_fold_train = y_train.iloc[train_idx]
        X_fold_valid = X_train.iloc[valid_idx]
        y_fold_valid = y_train.iloc[valid_idx]

        if y_fold_train.nunique() < 2:
            logger.debug("Skipping fold %s — single class in training subset", fold_idx)
            continue

        # Build controlled SMOTE strategy for this fold
        label_counts  = y_fold_train.value_counts()
        majority_count = label_counts.max()
        sampling_target = {}
        for label, count in label_counts.items():
            target = min(majority_count, max(count, int(majority_count * smote_minority_ratio)))
            sampling_target[label] = target

        try:
            fold_smote = SMOTE(
                random_state=RANDOM_STATE,
                sampling_strategy=sampling_target,
                k_neighbors=min(5, label_counts.min() - 1),
            )
        except ValueError:
            # k_neighbors too large for this fold — skip
            continue

        model = Pipeline(
            steps=[
                ("smote", fold_smote),
                (
                    "xgb",
                    XGBClassifier(
                        objective="multi:softprob",
                        num_class=3,
                        eval_metric="mlogloss",
                        random_state=RANDOM_STATE,
                        n_jobs=1,
                        **params,
                    ),
                ),
            ]
        )

        fold_weight = FOLD_WEIGHTS.get(fold_idx, 1.0)
        try:
            model.fit(X_fold_train, y_fold_train)
            preds        = model.predict(X_fold_valid)
            fold_f1      = f1_score(y_fold_valid, preds, average="macro", zero_division=0)
            weighted_score += fold_weight * fold_f1
            total_weight   += fold_weight
        except ValueError as ex:
            logger.debug("Fold %s failed for trial %s: %s", fold_idx, trial.number, ex)
            continue

    if total_weight == 0.0:
        return 0.0

    return float(weighted_score / total_weight)


def train_model(df, ticker):
    """
    Tunes with Optuna + TimeSeriesSplit (SMOTE inside fold), trains final model,
    and saves model, feature list, and metrics.
    """
    logger.info("Training model for %s", ticker)
    os.makedirs(MODELS_DIR, exist_ok=True)
    os.makedirs(FEATURES_DIR, exist_ok=True)

    if len(df) < MIN_ROWS_AFTER_FEATURES:
        logger.warning("%s: insufficient rows after feature engineering (%s), skipping", ticker, len(df))
        return

    features = _make_feature_list(df)
    if not features:
        logger.warning("%s: no valid features available, skipping", ticker)
        return

    X = df[features].copy()
    y = df["target"].astype(int).copy()

    label_distribution = {str(int(k)): int(v) for k, v in y.value_counts().sort_index().to_dict().items()}
    if y.nunique() < 2:
        logger.warning("%s: all labels are the same (%s), skipping", ticker, label_distribution)
        return

    split_index = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]

    if X_train.empty or X_test.empty:
        logger.warning("%s: train/test split failed due to insufficient rows", ticker)
        return
    if y_train.nunique() < 2:
        logger.warning("%s: training split has single class, skipping", ticker)
        return
    
    MIN_TRAIN_ROWS_FOR_CV = N_SPLITS * 30
    if len(X_train) < MIN_TRAIN_ROWS_FOR_CV:
        logger.warning(
            "%s: training set too small for %d-fold CV (%d rows < %d required) — skipping",
            ticker, N_SPLITS, len(X_train), MIN_TRAIN_ROWS_FOR_CV,
        )
        return

    try:
        study = optuna.create_study(direction="maximize")
        study.optimize(
            lambda trial: _optuna_objective(trial, X_train, y_train),
            n_trials=N_OPTUNA_TRIALS,
            show_progress_bar=False,
        )
    except Exception as ex:
        logger.exception("%s: Optuna optimization failed: %s", ticker, ex)
        return

    if not study.best_trials:
        logger.warning("%s: no successful Optuna trials, skipping", ticker)
        return

    # Extract best params — exclude SMOTE ratio (not an XGB param)
    best_params = {
        k: v for k, v in study.best_params.items()
        if k != "smote_minority_ratio"
    }
    logger.info("%s: best CV f1_macro = %.4f", ticker, study.best_value)

    # --- Controlled SMOTE — cap minority oversampling to prevent BUY/SELL inflation ---
    # Fixes: ITC BUY recall 0.74/precision 0.24, NTPC BUY recall 0.82, SBIN collapse
    label_counts   = y_train.value_counts()
    majority_count = label_counts.max()
    minority_count  = label_counts.min()
    SMOTE_FLOORS = {0: 0.50, 1: 0.50, 2: 0.50}

    sampling_target = {}
    for label, count in label_counts.items():
        floor = SMOTE_FLOORS.get(int(label), 0.50)
        target = max(count, int(majority_count * floor))
        target = min(target, majority_count)
        sampling_target[label] = target

    smote = SMOTE(
        random_state=RANDOM_STATE,
        sampling_strategy=sampling_target,
        k_neighbors=min(5, int(minority_count) - 1),
    )
    try:
        X_train_resampled, y_train_resampled = smote.fit_resample(X_train, y_train)
    except ValueError as ex:
        logger.warning("%s: SMOTE failed (%s) — using original training data", ticker, ex)
        X_train_resampled, y_train_resampled = X_train, y_train

    best_model = XGBClassifier(
        objective="multi:softprob",
        num_class=3,
        eval_metric="mlogloss",
        random_state=RANDOM_STATE,
        n_jobs=-1,
        **best_params,
    )
    resampled_counts = pd.Series(y_train_resampled).value_counts()
    sell_count = resampled_counts.get(0, 1)
    hold_count = resampled_counts.get(1, 1)
    buy_count  = resampled_counts.get(2, 1)
    raw_boost = (hold_count + buy_count) / max(sell_count, 1)
    sell_boost = min(raw_boost, 1.5)
    hold_boost = 1.2 if sell_boost > 1.2 else 1.0

    sw = np.where(
        y_train_resampled == 0, sell_boost,
        np.where(y_train_resampled == 1, hold_boost, 1.0)
    )

    logger.info(
        "%s: SELL sample_weight boost = %.2f | HOLD boost = %.2f "
        "(sell=%d, hold=%d, buy=%d after SMOTE)",
        ticker, sell_boost, hold_boost, sell_count, hold_count, buy_count,
    )

    best_model.fit(X_train_resampled, y_train_resampled, sample_weight=sw)

    y_pred = best_model.predict(X_test)
    f1_macro = f1_score(y_test, y_pred, average="macro", zero_division=0)

    precision, recall, f1_per_class, support = precision_recall_fscore_support(
        y_test, y_pred, labels=[0, 1, 2], zero_division=0
    )
    class_names = {0: "SELL", 1: "HOLD", 2: "BUY"}
    per_class_metrics = {
        class_names[idx]: {
            "precision": float(precision[i]),
            "recall": float(recall[i]),
            "f1": float(f1_per_class[i]),
            "support": int(support[i]),
        }
        for i, idx in enumerate([0, 1, 2])
    }

    logger.info(
        "%s: test f1_macro=%.4f | SELL(f1=%.4f) HOLD(f1=%.4f) BUY(f1=%.4f)",
        ticker,
        f1_macro,
        per_class_metrics["SELL"]["f1"],
        per_class_metrics["HOLD"]["f1"],
        per_class_metrics["BUY"]["f1"],
    )
    logger.info("\n%s", classification_report(y_test, y_pred, target_names=["SELL", "HOLD", "BUY"], zero_division=0))

    model_filename    = os.path.join(MODELS_DIR, f"model_{ticker}.joblib")
    features_filename = os.path.join(FEATURES_DIR, f"features_{ticker}.json")
    metrics_filename  = os.path.join(MODELS_DIR, f"{ticker}_metrics.json")

    joblib.dump(best_model, model_filename)
    with open(features_filename, "w", encoding="utf-8") as feat_file:
        json.dump(features, feat_file)

    y_proba = best_model.predict_proba(X_test)
    max_probas = y_proba.max(axis=1)
    sorted_probas = np.sort(y_proba, axis=1)[:, ::-1]
    top2_margins = sorted_probas[:, 0] - sorted_probas[:, 1]

    metrics_payload = {
        "ticker": ticker,
        "f1_macro": float(f1_macro),
        "per_class_metrics": per_class_metrics,
        "train_size": int(len(X_train)),
        "test_size": int(len(X_test)),
        "total_rows_after_features": int(len(df)),
        "label_distribution": label_distribution,
        "confidence_stats": {
            # Used by app.py to compute per-stock tier thresholds
            "mean_max_proba": float(np.mean(max_probas)),
            "mean_top2_margin": float(np.mean(top2_margins)),
            "f1_macro": float(f1_macro),       # redundant but explicit for tier lookup
        },
        "optuna": {
            "best_value": float(study.best_value),
            "best_params": best_params,
            "n_trials": N_OPTUNA_TRIALS,
            "cv_splits": N_SPLITS,
            "scoring": "f1_macro (recency-weighted)",
        },
    }
    with open(metrics_filename, "w", encoding="utf-8") as metrics_file:
        json.dump(metrics_payload, metrics_file, indent=2)

    logger.info("%s: model saved to %s", ticker, model_filename)
    logger.info("%s: features saved to %s", ticker, features_filename)
    logger.info("%s: metrics saved to %s", ticker, metrics_filename)


def run(tickers_to_process):
    """
    Main function to run the training pipeline for a given list of tickers.
    """
    client = MongoClient(MONGO_URI)
    logger.info("Starting training run for %s tickers", len(tickers_to_process))
    try:
        for ticker in tickers_to_process:
            logger.info("Processing %s", ticker)
            dataset = create_dataset(ticker, client)
            if dataset.empty:
                logger.warning("%s: dataset creation failed or returned empty; skipping", ticker)
                continue
            train_model(dataset, ticker)
    finally:
        client.close()
        logger.info("Training run complete")


if __name__ == "__main__":
    from nifty50 import TICKERS

    run(TICKERS)

