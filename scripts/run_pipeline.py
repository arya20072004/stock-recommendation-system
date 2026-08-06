"""
run_pipeline.py

Wrapper that runs the full daily StockIntel pipeline in the correct order,
with safeguards against accidental intraday/pre-Bhavcopy execution.

trainer.py output is logged to:
- A timestamped log for each training run
- trainer_latest.log containing the most recent training run

Pipeline order:

    collector
        ↓
    sector_index_builder
        ↓
    pcr_builder
        ↓
    max_dates
        ↓
    trainer
        ↓
    prediction history generation
        ↓
    pending prediction evaluation

Usage:

    python -m scripts.run_pipeline

        Normal guarded run.

    python -m scripts.run_pipeline --force

        Skip the time-of-day guard.
        Collection still runs.

    python -m scripts.run_pipeline --skip-collect

        Skip collector, sector index builder and PCR builder.
        Verify existing data, train models, generate prediction
        history and evaluate eligible predictions.

    python -m scripts.run_pipeline --unattended

        Skip the manual confirmation after freshness validation.
        Intended for future Task Scheduler usage.

IMPORTANT:

The freshness validator currently understands:
- Before the safe EOD cutoff → previous weekday is expected
- After the safe EOD cutoff → current weekday is expected
- Saturday/Sunday → previous Friday is expected

It does NOT yet understand NSE exchange holidays.
A proper NSE trading calendar should be added before fully
unattended production scheduling.
"""

import argparse
import subprocess
import sys

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


# ======================================================================
# Configuration
# ======================================================================

IST = ZoneInfo("Asia/Kolkata")

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Earliest IST hour at which each data source is considered safe.
#
# Collector:
#   yfinance EOD prices should normally be synchronized by 18:00.
#
# PCR:
#   NSE Bhavcopy should normally be available by 20:00.
#
# Since PCR is the later dependency, the complete pipeline uses
# MIN_HOUR_PCR as its guarded cutoff.
MIN_HOUR_COLLECTOR = 18
MIN_HOUR_PCR = 20


# ======================================================================
# Trading-session helpers
# ======================================================================

def get_expected_data_date(now_ist: datetime):
    """
    Determine the latest completed market-data date that should be
    available to the pipeline.

    Rules
    -----
    1. Before MIN_HOUR_PCR:
       Today's complete EOD/PCR dataset is not expected yet, so start
       from the previous calendar day.

    2. At/after MIN_HOUR_PCR:
       Today's dataset is expected when today is a weekday.

    3. Saturday/Sunday:
       Roll backward until Friday.

    Examples
    --------
    Thursday 08:00
        -> Wednesday

    Thursday 21:00
        -> Thursday

    Saturday
        -> Friday

    Sunday
        -> Friday

    Monday 08:00
        -> Friday

    NOTE:
    This helper does not yet understand NSE exchange holidays.
    """

    candidate = now_ist.date()

    # Before today's safe EOD/PCR cutoff, today's completed market
    # dataset should not yet be expected.
    if now_ist.hour < MIN_HOUR_PCR:
        candidate -= timedelta(days=1)

    # Roll weekends backward.
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)

    return candidate


# ======================================================================
# Generic subprocess runner
# ======================================================================

def run_step(cmd: list[str], label: str) -> bool:
    """
    Execute a pipeline subprocess.

    Returns True when the process succeeds.
    Returns False when the process exits with a non-zero status.
    """

    print(f"\n{'=' * 70}")
    print(f">>> {label}")
    print(f"{'=' * 70}")

    result = subprocess.run(
        cmd,
        capture_output=False,
    )

    if result.returncode != 0:
        print(
            f"\n!!! {label} FAILED "
            f"(exit code {result.returncode}) — stopping pipeline."
        )
        return False

    return True


# ======================================================================
# Trainer
# ======================================================================

def run_trainer_with_logging() -> bool:
    """
    Run trainer.py while:

    - displaying its output live
    - writing a timestamped training log
    - replacing trainer_latest.log with the newest run
    """

    now_ist = datetime.now(IST)

    timestamped_log = (
        LOG_DIR
        / f"trainer_{now_ist.strftime('%Y%m%d_%H%M%S')}.log"
    )

    latest_log = LOG_DIR / "trainer_latest.log"

    print(f"\n{'=' * 70}")
    print(
        f">>> Running trainer.py "
        f"(logging to {timestamped_log})"
    )
    print(f"{'=' * 70}")

    with open(
        timestamped_log,
        "w",
        encoding="utf-8",
    ) as f:

        f.write(
            f"=== Training run started "
            f"{now_ist.isoformat()} ===\n\n"
        )

        f.flush()

        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "src.ml.trainer",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        if process.stdout is not None:
            for line in process.stdout:

                # Display output live.
                print(line, end="")

                # Persist same output.
                f.write(line)

        process.wait()

        f.write(
            f"\n=== Training run finished "
            f"{datetime.now(IST).isoformat()} "
            f"(exit code {process.returncode}) ===\n"
        )

    # Keep one convenient rolling copy of the latest training run.
    latest_log.write_text(
        timestamped_log.read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    print(f"\nLog saved to: {timestamped_log}")
    print(f"Latest run also copied to: {latest_log}")

    return process.returncode == 0


# ======================================================================
# Freshness validation
# ======================================================================

def validate_data_freshness(now_ist: datetime) -> bool:
    """
    Run scripts.max_dates and verify that every reported
    market-data max date matches the expected latest completed session.

    This prevents training against stale or inconsistent input data.
    """

    print(f"\n{'=' * 70}")
    print(">>> Verifying collected data freshness")
    print(f"{'=' * 70}")

    check_result = subprocess.run(
        [
            sys.executable,
            "-m",
            "max_dates",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    # Normalize captured output once.
    check_output = check_result.stdout or ""

    print(check_output)

    # --------------------------------------------------------------
    # Checker execution failure
    # --------------------------------------------------------------

    if check_result.returncode != 0:

        print(
            "\n!!! max_dates.py FAILED "
            f"(exit code {check_result.returncode}) "
            "— stopping pipeline."
        )

        return False

    # --------------------------------------------------------------
    # Determine expected latest completed session
    # --------------------------------------------------------------

    expected_date = get_expected_data_date(now_ist)

    expected_date_str = expected_date.strftime(
        "%Y-%m-%d"
    )

    print(
        f">>> Expected latest completed market-data session: "
        f"{expected_date_str}"
    )

    # --------------------------------------------------------------
    # Extract actual max-date records
    # --------------------------------------------------------------
    #
    # We intentionally require:
    #
    #     "max date ="
    #
    # rather than merely:
    #
    #     "max date"
    #
    # because max_dates.py also prints headings/explanatory
    # sentences containing the words "max date".
    # --------------------------------------------------------------

    date_lines = [
        line.strip()
        for line in check_output.splitlines()
        if "max date =" in line.lower()
    ]

    print(
        f">>> Freshness check found "
        f"{len(date_lines)} max-date entries."
    )

    # --------------------------------------------------------------
    # No parseable date records
    # --------------------------------------------------------------

    if not date_lines:

        msg = (
            "\n!!! FRESHNESS CHECK FAILED\n"
            "\n"
            "max_dates.py completed successfully, but no "
            "actual 'max date =' entries were detected.\n"
            "\n"
            "Training has been aborted because input-data "
            "freshness cannot be verified safely."
        )

        print(msg)

        aborted_log = (
            LOG_DIR
            / (
                "ABORTED_freshness_check_"
                f"{now_ist.strftime('%Y%m%d_%H%M%S')}.log"
            )
        )

        aborted_log.write_text(
            (
                f"{msg}\n\n"
                f"Expected session: {expected_date_str}\n\n"
                f"Raw checker output:\n\n"
                f"{check_output}"
            ),
            encoding="utf-8",
        )

        return False

    # --------------------------------------------------------------
    # Detect stale/inconsistent records
    # --------------------------------------------------------------

    stale_lines = [
        line
        for line in date_lines
        if expected_date_str not in line
    ]

    if stale_lines:

        stale_details = "\n".join(
            f"  - {line}"
            for line in stale_lines
        )

        msg = (
            f"\n!!! STALE DATA DETECTED\n"
            f"\n"
            f"Expected latest completed market-data date: "
            f"{expected_date_str}\n"
            f"\n"
            f"The following max-date entries are not current:\n"
            f"{stale_details}\n"
            f"\n"
            f"Aborting before training to avoid training on "
            f"stale or inconsistent data."
        )

        print(msg)

        aborted_log = (
            LOG_DIR
            / (
                "ABORTED_stale_data_"
                f"{now_ist.strftime('%Y%m%d_%H%M%S')}.log"
            )
        )

        aborted_log.write_text(
            (
                f"{msg}\n\n"
                f"Raw checker output:\n\n"
                f"{check_output}"
            ),
            encoding="utf-8",
        )

        return False

    # --------------------------------------------------------------
    # Success
    # --------------------------------------------------------------

    print(
        f">>> Freshness check PASSED: "
        f"all {len(date_lines)} reported max-date entries "
        f"match expected completed session "
        f"{expected_date_str}."
    )

    return True


# ======================================================================
# Main pipeline
# ======================================================================

def main():

    parser = argparse.ArgumentParser(
        description="Run the StockIntel daily ML pipeline."
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Skip the time-of-day collection guard. "
            "Collection itself still runs."
        ),
    )

    parser.add_argument(
        "--skip-collect",
        action="store_true",
        help=(
            "Skip collector/sector/PCR steps and continue "
            "with freshness verification, training, prediction "
            "history generation and evaluation."
        ),
    )

    parser.add_argument(
        "--unattended",
        action="store_true",
        help=(
            "Skip the manual confirmation pause after freshness "
            "validation. Intended for future Task Scheduler usage."
        ),
    )

    args = parser.parse_args()

    now_ist = datetime.now(IST)

    print(
        f"Current IST time: "
        f"{now_ist.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    # ==================================================================
    # Steps 1-3: Daily data collection
    # ==================================================================

    if not args.skip_collect:

        # --------------------------------------------------------------
        # Time guard
        # --------------------------------------------------------------
        #
        # --force bypasses ONLY this guard.
        #
        # It must NOT accidentally bypass collector/sector/PCR.
        # --------------------------------------------------------------

        if not args.force:

            if now_ist.hour < MIN_HOUR_PCR:

                print(
                    f"\nBLOCKED: it's before "
                    f"{MIN_HOUR_PCR}:00 IST.\n"
                    f"\n"
                    f"NSE Bhavcopy for today may not yet be "
                    f"published, and running collector.py too early "
                    f"risks capturing an intraday/partial market "
                    f"price.\n"
                    f"\n"
                    f"Run again after {MIN_HOUR_PCR}:00 IST, "
                    f"or pass --force to override the time guard."
                )

                sys.exit(1)

        # --------------------------------------------------------------
        # Collector
        # --------------------------------------------------------------

        if not run_step(
            [
                sys.executable,
                "-m",
                "src.data.collector",
            ],
            "collector.py (price history)",
        ):
            sys.exit(1)

        # --------------------------------------------------------------
        # Sector indices
        # --------------------------------------------------------------

        if not run_step(
            [
                sys.executable,
                "-m",
                "src.data.sector_index_builder",
            ],
            "sector_index_builder.py",
        ):
            sys.exit(1)

        # --------------------------------------------------------------
        # PCR / futures basis
        # --------------------------------------------------------------

        if not run_step(
            [
                sys.executable,
                "-m",
                "src.data.pcr_builder",
            ],
            "pcr_builder.py (PCR + futures basis)",
        ):
            sys.exit(1)

    else:

        print(
            "\n>>> --skip-collect enabled: "
            "collector, sector index builder and PCR builder skipped."
        )

    # ==================================================================
    # Step 4: Freshness validation
    # ==================================================================

    if not validate_data_freshness(now_ist):
        sys.exit(1)

    # ==================================================================
    # Manual safety confirmation
    # ==================================================================

    if args.unattended:

        print(
            "\n>>> Unattended mode: freshness validation passed. "
            "Proceeding to trainer.py automatically."
        )

    else:

        print(
            "\n>>> Freshness validation passed.\n"
            "\n"
            ">>> Review the max-date output above before training.\n"
            ">>> Press Enter to proceed with trainer.py, "
            "or Ctrl+C to abort."
        )

        input()

    # ==================================================================
    # Step 5: Model training
    # ==================================================================

    success = run_trainer_with_logging()

    if not success:

        print(
            "\n!!! trainer.py FAILED — "
            "prediction history will NOT be generated."
        )

        sys.exit(1)

    # ==================================================================
    # Step 6: Prediction-history generation
    # ==================================================================

    print(
        "\n>>> Generating and persisting "
        "historical prediction snapshots..."
    )

    history_success = run_step(
        [
            sys.executable,
            "-m",
            "src.ml.history",
        ],
        "history.py (Prediction History)",
    )

    if not history_success:

        print(
            "\n!!! Prediction history generation failed."
        )

        sys.exit(1)

    # ==================================================================
    # Step 7: Evaluate eligible historical predictions
    # ==================================================================

    print(
        "\n>>> Evaluating eligible pending predictions..."
    )

    evaluation_success = run_step(
        [
            sys.executable,
            "-m",
            "src.ml.evaluation",
        ],
        "evaluation.py (Evaluate Predictions)",
    )

    if not evaluation_success:

        print(
            "\n!!! Prediction evaluation failed."
        )

        sys.exit(1)

    # ==================================================================
    # Pipeline complete
    # ==================================================================

    print(f"\n{'=' * 70}")
    print(">>> STOCKINTEL PIPELINE COMPLETED SUCCESSFULLY")
    print(f"{'=' * 70}")

    print(
        "\nCompleted:"
        "\n  ✓ Data freshness verified"
        "\n  ✓ Models trained"
        "\n  ✓ Prediction snapshots persisted"
        "\n  ✓ Eligible historical predictions evaluated"
    )

    sys.exit(0)


if __name__ == "__main__":
    main()