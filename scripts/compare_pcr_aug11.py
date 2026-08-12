"""
compare_pcr_aug11.py

READ-ONLY PCR ON/OFF inference experiment.

Purpose
-------
Compare frozen active-model predictions for 2026-08-11 under:

    PCR ON  -> real leakage-safe PCR features
    PCR OFF -> the four PCR features forced to 0.0

IMPORTANT
---------
This script:
- DOES NOT train models.
- DOES NOT modify MongoDB.
- DOES NOT create prediction_history records.
- DOES NOT create prediction_provenance records.
- DOES NOT modify active manifests.
- DOES NOT modify saved model/feature artifacts.

It only reads existing data and performs inference.

The Aug-11 PCR features are already leakage-safe because engineering.py
shifts the PCR series by one trading row/day before joining it to the
stock feature frame.

Usage
-----
python scripts/compare_pcr_aug11.py

Optional:
python scripts/compare_pcr_aug11.py --date 2026-08-11
"""

import argparse
import csv
import os
import sys

# Ensure project root is importable when this file is executed directly.
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient

from src.data.nifty50 import TICKERS
from src.ml.history import (
    load_active_bundle,
    get_latest_valid_feature_row,
)

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

DEFAULT_DATE = "2026-08-11"

# Frozen-model sensitivity to the four PCR features already present in
# the model contract. This experiment does not test stock-level PCR.
PCR_FEATURES = [
    "nifty_pcr_oi",
    "nifty_pcr_chg_5d",
    "banknifty_pcr_oi",
    "banknifty_pcr_chg_5d",
]

CLASS_NAMES = [
    "SELL",
    "HOLD",
    "BUY",
]


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def get_calibrated_prediction(
    proba,
    thresholds,
    apply_threshold_calibration,
):
    """
    Apply the exact same threshold calibration used by production
    inference.
    """

    class_idx = apply_threshold_calibration(
        proba,
        thresholds,
    )

    return CLASS_NAMES[class_idx]


def probability_dict(proba):
    """
    Convert model probability vector into explicit class values.
    """

    return {
        "SELL": float(proba[0]),
        "HOLD": float(proba[1]),
        "BUY": float(proba[2]),
    }


def get_argmax_class(proba):
    return CLASS_NAMES[int(proba.argmax())]


# ---------------------------------------------------------------------
# Main experiment
# ---------------------------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description="Read-only PCR ON/OFF inference comparison."
    )

    parser.add_argument(
        "--date",
        default=DEFAULT_DATE,
        help="Target market date YYYY-MM-DD "
             "(default: 2026-08-11)",
    )

    args = parser.parse_args()

    target_date = datetime.strptime(
        args.date,
        "%Y-%m-%d",
    ).date()

    load_dotenv()

    mongo_uri = os.getenv("MONGO_URI")

    if not mongo_uri:
        raise RuntimeError(
            "MONGO_URI is not configured."
        )

    client = MongoClient(
        mongo_uri,
        readPreference="primaryPreferred",
    )

    db = client["stock_market_db"]

    print("=" * 110)
    print("PCR ON/OFF READ-ONLY INFERENCE EXPERIMENT")
    print("=" * 110)
    print()
    print(f"Target market date : {target_date}")
    print("PCR ON             : Real leakage-safe PCR features")
    print("PCR OFF            : Four PCR features forced to 0.0")
    print("Training           : FROZEN / NO RETRAINING")
    print("MongoDB writes     : NONE")
    print()

    results = []

    errors = []

    unavailable = []

    active_model_tickers = []

    # -----------------------------------------------------------------
    # Process every ticker
    # -----------------------------------------------------------------

    for ticker in TICKERS:

        print(f"[{ticker}]")

        # A missing active manifest is expected for tickers that are not
        # deployed. Treat it as unavailable, not as an experiment failure.
        manifest_path = os.path.join(
            "saved_models",
            f"{ticker}_active.json",
        )

        if not os.path.exists(manifest_path):
            unavailable.append(ticker)
            print("  --- UNAVAILABLE: Active manifest missing")
            print()
            continue

        try:

            # ---------------------------------------------------------
            # Load exact active model bundle
            # ---------------------------------------------------------

            bundle = load_active_bundle(
                ticker
            )

            if not bundle:
                raise RuntimeError(
                    f"No active bundle found for {ticker}"
                )

            active_model_tickers.append(ticker)

            (
                model,
                feature_names,
                model_version,
                engineering_module,
                pipeline_version,
                pipeline_hash,
                f1_macro,
            ) = bundle

            # ---------------------------------------------------------
            # Resolve exact feature engineering implementation
            # ---------------------------------------------------------

            build_feature_row = (
                engineering_module.build_feature_row
            )

            thresholds = (
                engineering_module.TICKER_CLASS_THRESHOLDS.get(
                    ticker
                )
            )

            apply_threshold_calibration = (
                engineering_module.apply_threshold_calibration
            )

            # ---------------------------------------------------------
            # Rebuild latest feature frame
            # ---------------------------------------------------------

            computed_df = build_feature_row(
                ticker,
                client,
                db,
            )

            if computed_df.empty:
                raise ValueError(
                    "Feature engineering returned empty DataFrame."
                )

            # ---------------------------------------------------------
            # Validate latest row exactly like production history.py
            # ---------------------------------------------------------

            (
                market_date,
                latest_row,
            ) = get_latest_valid_feature_row(
                ticker,
                computed_df,
                feature_names,
            )

            market_date_obj = (
                market_date.date()
                if hasattr(market_date, "date")
                else market_date
            )

            if market_date_obj != target_date:

                raise ValueError(
                    f"Latest valid market date is "
                    f"{market_date_obj}, expected "
                    f"{target_date}."
                )

            # ---------------------------------------------------------
            # Verify PCR features exist
            # ---------------------------------------------------------

            missing_pcr = [
                col
                for col in PCR_FEATURES
                if col not in computed_df.columns
            ]

            if missing_pcr:
                raise ValueError(
                    f"Missing PCR columns: {missing_pcr}"
                )

            missing_from_contract = [
                col
                for col in PCR_FEATURES
                if col not in feature_names
            ]

            if missing_from_contract:
                raise ValueError(
                    "PCR columns missing from model feature "
                    f"contract: {missing_from_contract}"
                )

            # ---------------------------------------------------------
            # IMPORTANT:
            # Work on independent copies.
            #
            # We never mutate computed_df.
            # ---------------------------------------------------------

            latest_on = latest_row.copy()
            latest_off = latest_row.copy()

            # Capture actual PCR values BEFORE modifying anything.

            actual_pcr = {
                col: float(latest_on[col])
                for col in PCR_FEATURES
            }

            # PCR OFF experiment.

            for col in PCR_FEATURES:
                latest_off[col] = 0.0

            # ---------------------------------------------------------
            # Make sure feature order is EXACTLY the saved contract.
            # ---------------------------------------------------------

            features_on = (
                latest_on[feature_names]
                .values
                .reshape(1, -1)
            )

            features_off = (
                latest_off[feature_names]
                .values
                .reshape(1, -1)
            )

            # ---------------------------------------------------------
            # Frozen-model inference
            # ---------------------------------------------------------

            proba_on = model.predict_proba(
                features_on
            )[0]

            proba_off = model.predict_proba(
                features_off
            )[0]

            # ---------------------------------------------------------
            # Predictions
            # ---------------------------------------------------------

            argmax_on = get_argmax_class(
                proba_on
            )

            argmax_off = get_argmax_class(
                proba_off
            )

            calibrated_on = get_calibrated_prediction(
                proba_on,
                thresholds,
                apply_threshold_calibration,
            )

            calibrated_off = get_calibrated_prediction(
                proba_off,
                thresholds,
                apply_threshold_calibration,
            )

            # ---------------------------------------------------------
            # Probability deltas
            # ---------------------------------------------------------

            delta_sell = (
                float(proba_on[0])
                - float(proba_off[0])
            )

            delta_hold = (
                float(proba_on[1])
                - float(proba_off[1])
            )

            delta_buy = (
                float(proba_on[2])
                - float(proba_off[2])
            )

            max_probability_on = float(
                proba_on.max()
            )

            max_probability_off = float(
                proba_off.max()
            )

            # ---------------------------------------------------------
            # Store result
            # ---------------------------------------------------------

            result = {
                "ticker": ticker,
                "market_date": str(target_date),
                "model_version": model_version,
                "pipeline_version": pipeline_version,
                "f1_macro": float(f1_macro),

                "nifty_pcr_oi": actual_pcr[
                    "nifty_pcr_oi"
                ],

                "nifty_pcr_chg_5d": actual_pcr[
                    "nifty_pcr_chg_5d"
                ],

                "banknifty_pcr_oi": actual_pcr[
                    "banknifty_pcr_oi"
                ],

                "banknifty_pcr_chg_5d": actual_pcr[
                    "banknifty_pcr_chg_5d"
                ],

                "on_sell_prob": float(proba_on[0]),
                "on_hold_prob": float(proba_on[1]),
                "on_buy_prob": float(proba_on[2]),

                "off_sell_prob": float(proba_off[0]),
                "off_hold_prob": float(proba_off[1]),
                "off_buy_prob": float(proba_off[2]),

                "delta_sell": delta_sell,
                "delta_hold": delta_hold,
                "delta_buy": delta_buy,

                "on_argmax": argmax_on,
                "off_argmax": argmax_off,

                "on_signal": calibrated_on,
                "off_signal": calibrated_off,

                "on_max_probability": max_probability_on,
                "off_max_probability": max_probability_off,

                "argmax_changed": (
                    argmax_on != argmax_off
                ),

                "signal_changed": (
                    calibrated_on != calibrated_off
                ),
            }

            results.append(result)

            print(
                f"  ON  = {calibrated_on:<5} "
                f"[{proba_on[0]:.3f}, "
                f"{proba_on[1]:.3f}, "
                f"{proba_on[2]:.3f}]"
            )

            print(
                f"  OFF = {calibrated_off:<5} "
                f"[{proba_off[0]:.3f}, "
                f"{proba_off[1]:.3f}, "
                f"{proba_off[2]:.3f}]"
            )

            if calibrated_on != calibrated_off:
                print(
                    "  >>> SIGNAL CHANGED"
                )

            print()

        except Exception as exc:

            errors.append(
                {
                    "ticker": ticker,
                    "error": str(exc),
                }
            )

            print(
                f"  !!! ERROR: {exc}"
            )
            print()

    # -----------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------

    total = len(TICKERS)
    successful = len(results)
    failed = len(errors)
    active_models = len(active_model_tickers)
    unavailable_count = len(unavailable)

    signal_changes = sum(
        1
        for r in results
        if r["signal_changed"]
    )

    argmax_changes = sum(
        1
        for r in results
        if r["argmax_changed"]
    )

    print("=" * 110)
    print("EXPERIMENT SUMMARY")
    print("=" * 110)

    print(
        f"Tickers configured       : {total}"
    )

    print(
        f"Tickers with active model: {active_models}"
    )

    print(
        f"Successful comparisons   : {successful}"
    )

    print(
        f"Unavailable tickers     : {unavailable_count}"
    )

    print(
        f"Experiment failures     : {failed}"
    )

    print(
        f"Calibrated signal changes: "
        f"{signal_changes}"
    )

    print(
        f"Argmax class changes    : "
        f"{argmax_changes}"
    )

    if successful:

        print(
            f"Signal change rate      : "
            f"{signal_changes / successful:.2%}"
        )

        print(
            f"Argmax change rate      : "
            f"{argmax_changes / successful:.2%}"
        )

    # -----------------------------------------------------------------
    # Signal-change details
    # -----------------------------------------------------------------

    changed = [
        r
        for r in results
        if r["signal_changed"]
    ]

    if changed:

        print()
        print("=" * 110)
        print("TICKERS WHERE PCR CHANGED THE CALIBRATED SIGNAL")
        print("=" * 110)

        print(
            f"{'TICKER':15} "
            f"{'PCR ON':8} "
            f"{'PCR OFF':8} "
            f"{'ΔSELL':>8} "
            f"{'ΔHOLD':>8} "
            f"{'ΔBUY':>8}"
        )

        print("-" * 110)

        for r in changed:

            print(
                f"{r['ticker']:15} "
                f"{r['on_signal']:8} "
                f"{r['off_signal']:8} "
                f"{r['delta_sell']:+8.4f} "
                f"{r['delta_hold']:+8.4f} "
                f"{r['delta_buy']:+8.4f}"
            )

    else:

        print()
        print(
            "PCR did not change the calibrated signal "
            "for any successfully evaluated ticker."
        )

    # -----------------------------------------------------------------
    # Write LOCAL experiment CSV only.
    #
    # This is not a MongoDB write and does not affect production state.
    # -----------------------------------------------------------------

    output_path = (
        f"pcr_aug11_comparison_{target_date}.csv"
    )

    if results:

        fieldnames = list(
            results[0].keys()
        )

        with open(
            output_path,
            "w",
            newline="",
            encoding="utf-8",
        ) as f:

            writer = csv.DictWriter(
                f,
                fieldnames=fieldnames,
            )

            writer.writeheader()
            writer.writerows(results)

        print()
        print(
            f"Local comparison saved to: "
            f"{output_path}"
        )

    # -----------------------------------------------------------------
    # Unavailable tickers and experiment failures
    # -----------------------------------------------------------------

    if unavailable:

        print()
        print("=" * 110)
        print("UNAVAILABLE TICKERS")
        print("=" * 110)

        for ticker in unavailable:
            print(
                f"{ticker}: Active manifest missing"
            )

    if errors:

        print()
        print("=" * 110)
        print("EXPERIMENT FAILURES")
        print("=" * 110)

        for item in errors:

            print(
                f"{item['ticker']}: "
                f"{item['error']}"
            )

    print()
    print("=" * 110)
    print("READ-ONLY EXPERIMENT COMPLETE")
    print("=" * 110)

    # Fail the command if any ticker failed.
    # This prevents us from accidentally treating a partial experiment
    # as complete.

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
