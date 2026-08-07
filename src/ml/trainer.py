import json
import logging
import os
import hashlib
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

from src.features.engineering import (
    HISTORY_YEARS,
    SECTOR_MAP,
    SECTOR_INDEX_NAME_MAP,
    TICKER_HISTORY_OVERRIDE,
    TICKER_ATR_THRESHOLD_SCALE,
    EVENT_DRIVEN_SECTORS_NO_INDEX,
    SECTOR_INDEX_DISABLED_TICKERS,
    TICKER_START_DATE_OVERRIDE,
    TICKER_HORIZON_OVERRIDE,
    TREND_FOLLOWING_SECTORS,
    ALL_MACRO_COLS,
    SECTOR_MIN_PEERS,
    IT_MACRO_DISABLED_TICKERS,
    _prepare_nifty_data,
    _prepare_macro_data,
    _prepare_sentiment_data,
    _prepare_sector_data,
    _find_col,
    add_technical_indicators,
    add_derived_features,
    add_calendar_features,
    TICKER_CLASS_THRESHOLDS,
    apply_threshold_calibration,
    get_target_return_threshold,
)
from src.ml.model_utils import get_model_version


# --- SETUP ---
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MODELS_DIR = os.getenv("MODELS_DIR", "saved_models")
FEATURES_DIR = os.getenv("FEATURES_DIR", "saved_features")
RANDOM_STATE = 42
if os.getenv("ENFORCE_SEEDS") == "1":
    import random
    random.seed(RANDOM_STATE)
    np.random.seed(RANDOM_STATE)

MIN_ROWS_AFTER_FEATURES = 200
N_SPLITS = 5
N_OPTUNA_TRIALS = 75

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)

# --- Constants and data-preparation functions imported from feature_engineering ---
# SECTOR_MAP, SECTOR_INDEX_NAME_MAP, TICKER_HISTORY_OVERRIDE,
# TICKER_ATR_THRESHOLD_SCALE, EVENT_DRIVEN_SECTORS_NO_INDEX,
# SECTOR_INDEX_DISABLED_TICKERS, TREND_FOLLOWING_SECTORS,
# ALL_MACRO_COLS, SECTOR_MIN_PEERS, IT_MACRO_DISABLED_TICKERS,
# _prepare_nifty_data, _prepare_macro_data, _prepare_sentiment_data,
# _prepare_sector_data, _find_col, add_technical_indicators,
# add_derived_features, add_calendar_features


def _to_json_safe(obj):
    """Recursively convert numpy/pandas-native values to JSON-safe Python types."""
    if isinstance(obj, dict):
        return {str(k): _to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_json_safe(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return [_to_json_safe(v) for v in obj.tolist()]
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if isinstance(obj, pd.Timedelta):
        return str(obj)
    return obj


def create_dataset(ticker, client):
    """
    Pulls 5 years of data from MongoDB and engineers leakage-safe features.
    """

    db = client["stock_market_db"]
    if ticker in TICKER_START_DATE_OVERRIDE:
        cutoff_date = TICKER_START_DATE_OVERRIDE[ticker]
        logger.info("%s: using explicit start-date override — cutoff=%s", ticker, cutoff_date)
    else:
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
    
    cutoff_str = os.getenv("TRAINING_CUTOFF_DATE")
    if cutoff_str:
        dt_cutoff = pd.to_datetime(cutoff_str)
        if dt_cutoff < end_date:
            end_date = dt_cutoff
            
    nifty_df = _prepare_nifty_data(start_date, end_date)
    if nifty_df.empty:
        logger.warning("%s: failed to fetch Nifty data, skipping", ticker)
        return pd.DataFrame()

    df = prices_df.join(nifty_df[["nifty_return", "market_regime"]], how="left")

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

    df["return"] = df["close"].pct_change()
    df["outperformance"] = df["return"].shift(1) - df["nifty_return"].shift(1)

    news_query = {"$or": [{"tickers": ticker}, {"ticker": ticker}], "published_at": {"$gte": cutoff_date}}
    cutoff_str = os.getenv("TRAINING_CUTOFF_DATE")
    if cutoff_str:
        dt_cutoff = datetime.fromisoformat(cutoff_str)
        news_query["published_at"] = {"$gte": cutoff_date, "$lte": dt_cutoff.replace(hour=23, minute=59, second=59)}
    
    news_docs = list(db.news_articles.find(news_query))
    sentiment_df = _prepare_sentiment_data(news_docs)
    if sentiment_df.empty:
        df["sentiment"] = 0.0
    else:
        df = df.join(sentiment_df, how="left")
        df["sentiment"] = df["sentiment"].fillna(0.0)

    # --- Technical indicators (shifted by 1) ---
    df, ta_success = add_technical_indicators(df, ticker)
    if not ta_success:
        return pd.DataFrame()

    # --- Derived features + sector momentum ---
    df = add_derived_features(df, ticker, client)

    # --- Calendar features ---
    df = add_calendar_features(df)
    logger.info(
        "%s: calendar features added — month_sin/cos, is_month_end, is_month_start, "
        "quarter_end, is_expiry_week, in_earnings_season",
        ticker,
    )

    horizon = TICKER_HORIZON_OVERRIDE.get(ticker, 10)
    df["future_return"] = df["close"].shift(-horizon) / df["close"] - 1
    
    threshold = get_target_return_threshold(ticker, df["atr_pct"])
    
    df["target"] = 1
    df.loc[df["future_return"] > threshold, "target"] = 2
    df.loc[df["future_return"] < -threshold, "target"] = 0
    logger.info(
        "%s: label threshold scale = %.2f× ATR (BUY=%d, HOLD=%d, SELL=%d)",
        ticker,
        atr_scale,
        (df["target"] == 2).sum(),
        (df["target"] == 1).sum(),
        (df["target"] == 0).sum(),
    )

    buy_label_count = (df["target"] == 2).sum()
    total_labels    = len(df)
    buy_pct         = buy_label_count / total_labels if total_labels > 0 else 0.0

    if buy_pct < 0.15:
        logger.warning(
            "%s: BUY label frequency = %.1f%% (%d/%d) — below 15%% floor. "
            "Consider adding to TICKER_ATR_THRESHOLD_SCALE.",
            ticker, buy_pct * 100, buy_label_count, total_labels,
        )

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
        "relative_volume",
        "adx",
        "target",
        "nifty_ret_1d",
        "nifty_ret_5d",
        "nifty_ret_10d",
        "nifty_ret_20d",
        "nifty_vol_10d",
        "usdinr_ret_1d",
        "usdinr_ret_5d",
        "usdinr_vol_10d",
        "nasdaq_ret_5d",
        "nasdaq_ret_20d",
        "crude_ret_1d",
        "crude_ret_5d",
        "crude_vol_10d",
        "gold_ret_1d",
        "gold_ret_5d",
        "gold_vol_10d",
        "copper_ret_1d",
        "copper_ret_5d",
        "copper_vol_10d",
        "vix_level",
        "vix_ret_1d",
        "vix_chg_5d",
        "vix_vol_10d",
        "nifty_pcr_oi",
        "nifty_pcr_chg_5d",
        "banknifty_pcr_oi",
        "banknifty_pcr_chg_5d",
        "nifty_futures_basis",
        "nifty_futures_basis_chg_5d",
        # "fii_net_value",
        # "fii_net_chg_5d",
        # "dii_net_value",
        # "dii_net_chg_5d",
        # "fii_dii_divergence",
        # "stock_pcr_oi",
        # "stock_pcr_chg_5d",
    ]

    # Verify all required columns exist before dropna to give a clear error
    missing_required = [c for c in required_columns if c not in df.columns]
    if missing_required:
        logger.error(
            "%s: required columns still missing after all fallbacks — %s. "
            "Skipping ticker.",
            ticker, missing_required,
        )
        return pd.DataFrame()

    df = df.replace([float("inf"), float("-inf")], pd.NA)
    df = df.dropna(subset=required_columns)

    # Strictly truncate dataset to experiment cutoff (if provided)
    # This ensures no future knowledge sneaks into the training matrix,
    # and matches the temporal boundary of the historical baseline.
    cutoff_date_str = os.getenv("TRAINING_CUTOFF_DATE")
    if cutoff_date_str:
        dt_cutoff = pd.to_datetime(cutoff_date_str)
        df = df[df.index <= dt_cutoff]
        logger.info("%s: truncated dataset to <= %s. Rows remaining: %d", ticker, dt_cutoff, len(df))

    if not df.empty:
        row_hash = pd.util.hash_pandas_object(df[required_columns], index=True).sum()
        logger.info(
            "%s: dataset fingerprint — rows=%d, date_range=[%s, %s], row_hash=%s",
            ticker, len(df), df.index.min(), df.index.max(), row_hash,
        )

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
        "relative_volume",
        "sector_momentum",
        "sector_momentum_5d",
        "adx",
        "adx_trending",
        "nifty_ret_1d",
        "nifty_ret_5d",
        "nifty_ret_10d",
        "nifty_ret_20d",
        "nifty_vol_10d",
        "usdinr_ret_1d",
        "usdinr_ret_5d",
        "usdinr_vol_10d",
        "nasdaq_ret_5d",
        "nasdaq_ret_20d",
        "crude_ret_1d",
        "crude_ret_5d",
        "crude_vol_10d",
        "gold_ret_1d",
        "gold_ret_5d",
        "gold_vol_10d",
        "copper_ret_1d",
        "copper_ret_5d",
        "copper_vol_10d",
        "vix_level",
        "vix_ret_1d",
        "vix_chg_5d",
        "vix_vol_10d",
        "month_sin",
        "month_cos",
        "is_month_end",
        "is_month_start",
        "quarter_end",
        "is_expiry_week",
        "in_earnings_season",
        "nifty_pcr_oi",
        "nifty_pcr_chg_5d",
        "banknifty_pcr_oi",
        "banknifty_pcr_chg_5d",
        "nifty_futures_basis",
        "nifty_futures_basis_chg_5d",
        # "fii_net_value",
        # "fii_net_chg_5d",
        # "dii_net_value",
        # "dii_net_chg_5d",
        # "fii_dii_divergence",
        # "stock_pcr_oi",
        # "stock_pcr_chg_5d",
    ]
    return [feature for feature in candidate_features if feature in df.columns]


# Exponential fold weights: later folds penalize overfitting to early regimes
FOLD_WEIGHTS = {1: 1.0, 2: 1.2, 3: 1.5, 4: 2.0, 5: 2.5}

def _optuna_objective(trial, X_train, y_train, ticker=""):
    _mcw_floor = TICKER_MIN_CHILD_WEIGHT_FLOOR.get(ticker, 3)

    params = {
        "n_estimators":      trial.suggest_int("n_estimators", 100, 700),
        "max_depth":         trial.suggest_int("max_depth", 3, 7),
        "learning_rate":     trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "subsample":         trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree":  trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "min_child_weight":  trial.suggest_int("min_child_weight", _mcw_floor, 15),
        "reg_lambda":        trial.suggest_float("reg_lambda", 2.0, 15.0),
        "reg_alpha":         trial.suggest_float("reg_alpha", 0.5, 5.0),
        "gamma":             trial.suggest_float("gamma", 0.1, 3.0),
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
            preds   = model.predict(X_fold_valid)
            f1_per  = f1_score(y_fold_valid, preds, average=None,
                               labels=[0, 1, 2], zero_division=0)
            hold_f1 = f1_per[1]  # class index 1 = HOLD
            fold_f1 = f1_score(y_fold_valid, preds, average="macro", zero_division=0)
            # Penalise runs where HOLD is completely ignored
            hold_penalty = max(0.0, 0.10 - hold_f1) * 0.5
            adjusted_f1  = fold_f1 - hold_penalty
            weighted_score += fold_weight * adjusted_f1
            total_weight   += fold_weight
        except ValueError as ex:
            logger.debug("Fold %s failed for trial %s: %s", fold_idx, trial.number, ex)
            continue

    if total_weight == 0.0:
        return 0.0

    return float(weighted_score / total_weight)

SMOTE_FLOORS: dict[int, float] = {0: 0.50, 1: 0.50, 2: 0.50}

TICKER_SMOTE_FLOOR_OVERRIDES: dict[str, dict[int, float]] = {
    "NESTLEIND.NS":  {0: 0.50, 1: 0.40, 2: 0.65},  # BUY f1=0.000 for 3 consecutive runs
    "BAJFINANCE.NS": {0: 0.50, 1: 0.50, 2: 0.65},  # BUY f1≈0.05 two consecutive runs
    "SBIN.NS":       {0: 0.60, 1: 0.55, 2: 0.65},
    "BAJAJFINSV.NS":  {0: 0.65, 1: 0.45, 2: 0.65},
    "TRENT.NS": {0: 0.75, 1: 0.40, 2: 0.50},  # force massive SELL oversampling
    "SHRIRAMFIN.NS": {0: 0.70, 1: 0.45, 2: 0.50},
    "TATACONSUM.NS": {0: 0.65, 1: 0.45, 2: 0.65},  # NEW — force SELL/BUY
    "MAXHEALTH.NS":  {0: 0.65, 1: 0.45, 2: 0.65},  # NEW — force SELL/BUY
    #"KOTAKBANK.NS": {0: 0.50, 1: 0.50, 2: 0.65},  # NEW — BUY f1=0.00 two consecutive runs
}

TICKER_HOLD_WEIGHT_OVERRIDE: dict[str, float] = {
    #
}



TICKER_MIN_CHILD_WEIGHT_FLOOR: dict[str, int] = {
    "TRENT.NS":      8,   # SELL at 0.12, all classes weak — same pathology
    "RELIANCE.NS":   8,   # HOLD/BUY suppressed across runs
    "POWERGRID.NS":  8,   # SELL refusal + BUY inflation across multiple runs
    "BAJAJFINSV.NS": 6,
}

# Add near TICKER_HISTORY_OVERRIDE at module level:
VERY_LOW_CONFIDENCE_TICKERS = {
    #"TITAN.NS",      # HOLD structurally broken, gold/wedding cycle unlearnable at 10d horizon
    #"TECHM.NS",      # BUY structurally broken across 4+ runs, all interventions failed
    "MARUTI.NS",     # 3yr lookback — thin test set (134 rows), BUY recall persistently near 0
    "NESTLEIND.NS",  # BUY f1=0.00 across 6 consecutive runs — unfixable with current features
    #"BAJFINANCE.NS", # BUY structural collapse; SMOTE BUY floor override applied
    "ETERNAL.NS",    # All three classes weak across runs; no recoverable signal at 10d
    "ITC.NS",        # SELL recall=10%, model non-functional for SELL/BUY signals
    "SBILIFE.NS",    # BUY near-zero two consecutive runs; reverted after Run 3 recovery
    #"BAJAJFINSV.NS", # Recovered to 0.2988 with reduced trials — monitoring
    #"HDFCLIFE.NS",   # BUY near-zero two consecutive runs, structurally weak
    "HINDALCO.NS",   # SELL f1=0.08 two consecutive runs; near-random on SELL
    "JSWSTEEL.NS",   # Sub-0.27 two consecutive runs; no recoverable pattern
    "SHRIRAMFIN.NS", # Sub-0.26 three consecutive runs; all classes weak
    #"BEL.NS",        # HOLD/BUY seesawing across 4 consecutive runs — no stable configuration
    #"ADANIENT.NS",    # BUY f1=0.00 across 3 consecutive runs — unfixable with current features
    "RELIANCE.NS",    # HOLD/BUY persistently weak across 4+ runs, no recoverable pattern
    "TRENT.NS",       # SELL f1=0.12 across 3 consecutive runs; all classes weak
    #"POWERGRID.NS",    # SELL refusal + BUY inflation across 4+ runs; SMOTE override applied, monitoring for improvement
    #"MAXHEALTH.NS",   # HOLD/BUY persistently weak across 4+ runs, no recoverable pattern
    "INDIGO.NS",      # BUY precision=0.50 recall=0.07 across 3 consecutive runs — forcing BUY samples with SMOTE override, monitoring for improvement
    #"NTPC.NS",      # 6+ threshold/SMOTE iterations, no structural convergence
    #"BAJAJ-AUTO.NS",  # 3 consecutive sub-0.33 runs, declining CV scores
    #"LT.NS",        # train/test disconnect confirmed, HOLD structural failure
    #"ASIANPAINT.NS",  # 3 consecutive sub-0.30 runs, no recoverable pattern
    "JIOFIN.NS",  # 3 consecutive sub-0.30 runs, no recoverable pattern
    #'SBIN.NS',  # 3 consecutive sub-0.30 runs, no recoverable pattern
    #'INFY.NS',  # 3 consecutive sub-0.30 runs, no recoverable pattern
    'TATACONSUM.NS',  # 3 consecutive sub-0.30 runs, no recoverable pattern
    'COALINDIA.NS',  # 3 consecutive sub-0.30 runs, no recoverable pattern
    "HCLTECH.NS",  # 3 consecutive sub-0.30 runs, no recoverable pattern
    #"BRITANNIA.NS",  # 3 consecutive sub-0.30 runs, no recoverable pattern
    "TCS.NS",  # 3 consecutive sub-0.30 runs, no recoverable pattern
}

def train_model(df, ticker):
    """
    Tunes with Optuna + TimeSeriesSplit (SMOTE inside fold), trains final model,
    and saves model, feature list, and metrics.
    """
    TICKER_SELL_BOOST_OVERRIDE: dict[str, float] = {
        "TRENT.NS":     2.0,   # current cap is 1.5, need more
        #"SHRIRAMFIN.NS":2.0,
    }
    logger.info("Training model for %s", ticker)
    os.makedirs(MODELS_DIR, exist_ok=True)
    os.makedirs(FEATURES_DIR, exist_ok=True)

    if ticker in VERY_LOW_CONFIDENCE_TICKERS:
        logger.warning(
            "%s: ticker is in VERY_LOW_CONFIDENCE_TICKERS — "
            "model will train but signals should not be acted on",
            ticker,
        )

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

    effective_trials = 30 if ticker in VERY_LOW_CONFIDENCE_TICKERS else N_OPTUNA_TRIALS

    try:
        study = optuna.create_study(
            direction="maximize",
            sampler=optuna.samplers.TPESampler(seed=RANDOM_STATE),  # ADD THIS
        )
        study.optimize(
            lambda trial: _optuna_objective(trial, X_train, y_train, ticker),
            n_trials=effective_trials,
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

    # --- Controlled SMOTE — ticker-aware floor overrides ---
    # Global floor: {0: 0.50, 1: 0.50, 2: 0.50}
    # NESTLEIND override: BUY floor raised to 0.65 (BUY f1=0.000 for 3 consecutive runs)
    label_counts   = y_train.value_counts()
    majority_count = label_counts.max()
    minority_count = label_counts.min()

    smote_floors = TICKER_SMOTE_FLOOR_OVERRIDES.get(ticker, SMOTE_FLOORS)
    logger.info(
        "%s: SMOTE floors = %s%s",
        ticker,
        smote_floors,
        " [TICKER OVERRIDE]" if ticker in TICKER_SMOTE_FLOOR_OVERRIDES else "",
    )

    sampling_target = {}
    for label, count in label_counts.items():
        floor = smote_floors.get(int(label), 0.50)
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
    raw_boost  = (hold_count + buy_count) / max(sell_count, 1)
    max_boost  = TICKER_SELL_BOOST_OVERRIDE.get(ticker, 1.5)
    sell_boost = min(raw_boost, max_boost)
    hold_boost = 1.2 if sell_boost > 1.2 else 1.0
    # Apply ticker-level HOLD weight override if configured
    hold_boost = max(hold_boost, TICKER_HOLD_WEIGHT_OVERRIDE.get(ticker, 0.0))

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

    # Apply per-class threshold calibration if configured for this ticker
    _thresholds = TICKER_CLASS_THRESHOLDS.get(ticker)
    _y_proba = best_model.predict_proba(X_test)
    y_pred = np.array([
        apply_threshold_calibration(_y_proba[i], _thresholds)
        for i in range(len(_y_proba))
    ])
    if _thresholds:
        logger.info("%s: threshold calibration applied %s", ticker, _thresholds)

    f1_macro = f1_score(y_test, y_pred, average="macro", zero_division=0)

    precision, recall, f1_per_class, support = precision_recall_fscore_support(
        y_test, y_pred, labels=[0, 1, 2], zero_division=0
    )
    class_names = {0: "SELL", 1: "HOLD", 2: "BUY"}
    per_class_metrics = {
        class_names[idx]: {
            "precision": float(precision[i]),
            "recall":    float(recall[i]),
            "f1":        float(f1_per_class[i]),
            "support":   int(support[i]),
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

    y_proba        = best_model.predict_proba(X_test)
    max_probas     = y_proba.max(axis=1)
    sorted_probas  = np.sort(y_proba, axis=1)[:, ::-1]
    top2_margins   = sorted_probas[:, 0] - sorted_probas[:, 1]

    # Model Intelligence: test prediction distribution using the exact final y_pred
    pred_counts = pd.Series(y_pred).value_counts()
    test_prediction_distribution = {
        "SELL": int(pred_counts.get(0, 0)),
        "HOLD": int(pred_counts.get(1, 0)),
        "BUY":  int(pred_counts.get(2, 0))
    }

    # Model Intelligence: feature importance
    try:
        importances = best_model.feature_importances_
        if len(importances) == len(features):
            feature_importance = [
                {"feature": f, "importance": float(imp)} 
                for f, imp in zip(features, importances)
            ]
            feature_importance.sort(key=lambda x: x["importance"], reverse=True)
        else:
            logger.error("%s: feature_importances length (%d) != features length (%d)", ticker, len(importances), len(features))
            feature_importance = []
    except Exception as e:
        logger.warning("%s: could not extract feature importance: %s", ticker, e)
        feature_importance = []

    # Model Intelligence: metadata
    trained_at = datetime.now(timezone.utc).isoformat()
    model_version = get_model_version(ticker)
    prediction_horizon = TICKER_HORIZON_OVERRIDE.get(ticker, 10)

    metrics_payload = {
        "ticker": ticker,
        "model_metadata": {
            "trained_at": trained_at,
            "model_version": model_version,
            "prediction_horizon": prediction_horizon,
            "feature_count": len(features),
            "model_type": "XGBClassifier"
        },
        "f1_macro": float(f1_macro),
        "per_class_metrics": per_class_metrics,
        "train_size": int(len(X_train)),
        "test_size":  int(len(X_test)),
        "total_rows_after_features": int(len(df)),
        "label_distribution": label_distribution,
        "test_prediction_distribution": test_prediction_distribution,
        "feature_importance": feature_importance,
        "confidence_stats": {
            "mean_max_proba":   float(np.mean(max_probas)),
            "mean_top2_margin": float(np.mean(top2_margins)),
            "f1_macro":         float(f1_macro),
        },
        "optuna": {
            "best_value": float(study.best_value),
            "best_params": best_params,
            "n_trials":    effective_trials,  # reflects reduced trials for VLC tickers
            "cv_splits":   N_SPLITS,
            "scoring":     "f1_macro (recency-weighted, HOLD-penalty)",
        },
        "smote_floors_used": smote_floors,
        "threshold_calibration": TICKER_CLASS_THRESHOLDS.get(ticker),  # None if not applied
        "very_low_confidence": ticker in VERY_LOW_CONFIDENCE_TICKERS,
        "data_fingerprint": {
            "feature_date_min": str(df.index.min()),
            "feature_date_max": str(df.index.max()),
            "train_date_min": str(X_train.index.min()),
            "train_date_max": str(X_train.index.max()),
            "test_date_min": str(X_test.index.min()),
            "test_date_max": str(X_test.index.max()),
            "feature_matrix_hash": hashlib.sha256(X.values.tobytes()).hexdigest(),
            "train_matrix_hash": hashlib.sha256(X_train.values.tobytes()).hexdigest(),
            "test_matrix_hash": hashlib.sha256(X_test.values.tobytes()).hexdigest(),
            "train_labels_hash": hashlib.sha256(y_train.values.tobytes()).hexdigest(),
            "test_labels_hash": hashlib.sha256(y_test.values.tobytes()).hexdigest(),
            "row_identity_hash": hashlib.sha256(''.join(X.index.to_series().dt.strftime('%Y-%m-%d')).encode('utf-8')).hexdigest(),
            "train_row_identity_hash": hashlib.sha256(''.join(X_train.index.to_series().dt.strftime('%Y-%m-%d')).encode('utf-8')).hexdigest(),
            "test_row_identity_hash": hashlib.sha256(''.join(X_test.index.to_series().dt.strftime('%Y-%m-%d')).encode('utf-8')).hexdigest(),
            "row_hash": pd.util.hash_pandas_object(X, index=True).sum(),
        },
    }
    safe_metrics_payload = _to_json_safe(metrics_payload)
    with open(metrics_filename, "w", encoding="utf-8") as metrics_file:
        json.dump(safe_metrics_payload, metrics_file, indent=2)

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
    from src.data.nifty50 import TICKERS

    run(TICKERS)

