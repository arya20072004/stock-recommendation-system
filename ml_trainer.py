import json
import logging
import os
from datetime import datetime, timedelta, timezone

import joblib
import optuna
import pandas as pd
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
N_OPTUNA_TRIALS = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)


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


def create_dataset(ticker, client):
    """
    Pulls 5 years of data from MongoDB and engineers leakage-safe features.
    """
    db = client["stock_market_db"]
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=365 * HISTORY_YEARS + 10)

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

    ta_cols = [c for c in df.columns if any(x in c.upper() for x in ["RSI", "MACD", "BB", "ATR"])]
    logger.debug("%s: pandas_ta columns detected = %s", ticker, ta_cols)

    def _find_col(df_in, *tokens):
        """Return first column whose uppercase name contains ALL tokens."""
        for col in df_in.columns:
            col_up = str(col).upper()
            if all(str(t).upper() in col_up for t in tokens):
                return col
        return None

    rsi_col = _find_col(df, "RSI")
    macdh_col = _find_col(df, "MACD", "H")   # histogram has 'H' suffix
    bbl_col = _find_col(df, "BBL")
    bbm_col = _find_col(df, "BBM")
    bbu_col = _find_col(df, "BBU")
    atr_col = _find_col(df, "ATR")

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

    # Shift only the resolved indicator columns to avoid look-ahead bias
    resolved_indicator_cols = [
        c for c in [rsi_col, macdh_col, bbl_col, bbm_col, bbu_col, atr_col] if c is not None
    ]
    for col in resolved_indicator_cols:
        df[col] = df[col].shift(1)

    df["rsi"] = df[rsi_col]
    df["macd_hist"] = df[macdh_col]
    df["bb_width"] = (df[bbu_col] - df[bbl_col]) / df[bbm_col].replace(0, pd.NA)
    df["atr"] = df[atr_col]
    df["atr_pct"] = df["atr"] / df["close"].replace(0, pd.NA)

    df["sentiment_7d_avg"] = df["sentiment"].shift(1).rolling(window=7).mean()
    df["sentiment_30d_avg"] = df["sentiment"].shift(1).rolling(window=30).mean()
    df["price_change_1d"] = df["close"].shift(1).pct_change(1)
    df["price_change_5d"] = df["close"].shift(1).pct_change(5)
    df["market_correlation"] = (
        df["return"].shift(1)
        .rolling(window=30)
        .corr(df["nifty_return"].shift(1))
    )
    df["future_5d_return"] = df["close"].shift(-5) / df["close"] - 1
    threshold = 0.5 * df["atr_pct"]
    df["target"] = 1
    df.loc[df["future_5d_return"] > threshold, "target"] = 2
    df.loc[df["future_5d_return"] < -threshold, "target"] = 0

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
        "sentiment_30d_avg",   # ADD THIS
        "price_change_1d",
        "price_change_5d",
        "market_correlation",
        "outperformance",
        "market_regime",
    ]
    return [feature for feature in candidate_features if feature in df.columns]


def _optuna_objective(trial, X_train, y_train):
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 100, 700),
        "max_depth": trial.suggest_int("max_depth", 2, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
    }

    cv = TimeSeriesSplit(n_splits=N_SPLITS)
    fold_scores = []

    for fold_idx, (train_idx, valid_idx) in enumerate(cv.split(X_train), start=1):
        X_fold_train = X_train.iloc[train_idx]
        y_fold_train = y_train.iloc[train_idx]
        X_fold_valid = X_train.iloc[valid_idx]
        y_fold_valid = y_train.iloc[valid_idx]

        if y_fold_train.nunique() < 2:
            logger.debug("Skipping fold %s due to single class in training subset", fold_idx)
            continue

        model = Pipeline(
            steps=[
                ("smote", SMOTE(random_state=RANDOM_STATE)),
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

        try:
            model.fit(X_fold_train, y_fold_train)
            preds = model.predict(X_fold_valid)
            fold_scores.append(f1_score(y_fold_valid, preds, average="macro", zero_division=0))
        except ValueError as ex:
            logger.debug("Fold failed for trial %s due to %s", trial.number, ex)
            continue

    if not fold_scores:
        return 0.0

    return float(sum(fold_scores) / len(fold_scores))


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

    best_params = study.best_params
    logger.info("%s: best CV f1_macro = %.4f", ticker, study.best_value)

    smote = SMOTE(random_state=RANDOM_STATE)
    try:
        X_train_resampled, y_train_resampled = smote.fit_resample(X_train, y_train)
    except ValueError as ex:
        logger.warning("%s: SMOTE failed on final train split (%s), skipping", ticker, ex)
        return

    best_model = XGBClassifier(
        objective="multi:softprob",
        num_class=3,
        eval_metric="mlogloss",
        random_state=RANDOM_STATE,
        n_jobs=-1,
        **best_params,
    )
    best_model.fit(X_train_resampled, y_train_resampled)

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

    model_filename = os.path.join(MODELS_DIR, f"model_{ticker}.joblib")
    features_filename = os.path.join(FEATURES_DIR, f"features_{ticker}.json")
    metrics_filename = os.path.join(MODELS_DIR, f"{ticker}_metrics.json")

    joblib.dump(best_model, model_filename)
    with open(features_filename, "w", encoding="utf-8") as feat_file:
        json.dump(features, feat_file)

    metrics_payload = {
        "ticker": ticker,
        "f1_macro": float(f1_macro),
        "per_class_metrics": per_class_metrics,
        "train_size": int(len(X_train)),
        "test_size": int(len(X_test)),
        "total_rows_after_features": int(len(df)),
        "label_distribution": label_distribution,
        "optuna": {
            "best_value": float(study.best_value),
            "best_params": best_params,
            "n_trials": N_OPTUNA_TRIALS,
            "cv_splits": N_SPLITS,
            "scoring": "f1_macro",
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

