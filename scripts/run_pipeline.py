"""
run_pipeline.py

Wrapper that runs the full daily StockIntel pipeline in the correct order,
with a time guard to prevent accidental intraday/pre-Bhavcopy runs.

trainer.py's raw output is logged to disk:
- A timestamped log for every training run
- A rolling trainer_latest.log containing the most recent run

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

        Skip collector, sector index builder, and PCR builder.
        Run freshness verification, training, history generation,
        and evaluation.

    python -m scripts.run_pipeline --unattended

        Skip the manual confirmation after freshness validation.
        Intended for future Task Scheduler usage.
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

IST = ZoneInfo("Asia/Kolkata")

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Earliest IST hour at which the complete daily pipeline is considered safe.
#
# Collector:
#   yfinance EOD close should already be synchronized.
#
# PCR:
#   NSE Bhavcopy should normally be available.
#
# Since PCR is the later dependency, the full guarded pipeline waits until
# MIN_HOUR_PCR.
MIN_HOUR_COLLECTOR = 18  # 6:00 PM IST
MIN_HOUR_PCR = 20        # 8:00 PM IST


# ----------------------------------------------------------------------
# Generic subprocess runner
# ----------------------------------------------------------------------

def run_step(cmd: list[str], label: str) -> bool:
    """
    Run one pipeline subprocess.

    Returns True when the subprocess exits successfully,
    otherwise prints an error and returns False.
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


# ----------------------------------------------------------------------
# Trainer
# ----------------------------------------------------------------------

def run_trainer_with_logging() -> bool:
    """
    Run trainer.py while streaming output to the terminal and saving
    the complete output to timestamped and rolling log files.
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
                # Display trainer output live.
                print(line, end="")

                # Also persist it to the log.
                f.write(line)

        process.wait()

        f.write(
            f"\n=== Training run finished "
            f"{datetime.now(IST).isoformat()} "
            f"(exit code {process.returncode}) ===\n"
        )

    # Replace rolling log with this run.
    latest_log.write_text(
        timestamped_log.read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    print(f"\nLog saved to: {timestamped_log}")
    print(f"Latest run also copied to: {latest_log}")

    return process.returncode == 0


# ----------------------------------------------------------------------
# Freshness validation
# ----------------------------------------------------------------------

def validate_data_freshness(now_ist: datetime) -> bool:
    """
    Run max_dates.py and verify that all reported max-date entries
    contain today's IST date.

    NOTE:
    This is intentionally a strict daily check for now.

    Before fully unattended scheduling is enabled, this should eventually
    become trading-calendar aware so weekends and exchange holidays are
    handled correctly.
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

    check_output = check_result.stdout or ""

    print(check_output)

    if check_result.returncode != 0:
        print(
            "\n!!! max_dates.py FAILED "
            f"(exit code {check_result.returncode}) — stopping pipeline."
        )
        return False

    today_str = now_ist.strftime("%Y-%m-%d")

    # Only capture actual data rows such as:
    #
    # RELIANCE.NS  max date = 2026-08-05 00:00:00
    #
    # Do NOT match headings or explanatory text containing "max date".
    date_lines = [
        line.strip()
        for line in check_output.splitlines()
        if "max date =" in line.lower()
    ]

    print(
        f">>> Freshness check found "
        f"{len(date_lines)} max-date entries."
    )

    if not date_lines:
        msg = (
            "\n!!! FRESHNESS CHECK FAILED\n"
            "\n"
            "max_dates.py completed successfully, but no "
            "actual 'max date =' entries were detected.\n"
            "\n"
            "Training has been aborted because data freshness "
            "cannot be verified safely."
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
            f"{msg}\n\nRaw checker output:\n\n{check_output}",
            encoding="utf-8",
        )

        return False

    stale_lines = [
        line
        for line in date_lines
        if today_str not in line
    ]

    if stale_lines:
        stale_details = "\n".join(
            f"  - {line}"
            for line in stale_lines
        )

        msg = (
            f"\n!!! STALE DATA DETECTED\n"
            f"\n"
            f"Expected latest date: {today_str}\n"
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
            f"{msg}\n\nRaw checker output:\n\n{check_output}",
            encoding="utf-8",
        )

        return False

    print(
        f">>> Freshness check PASSED: "
        f"all {len(date_lines)} reported max-date entries "
        f"match {today_str}."
    )

    return True


# ----------------------------------------------------------------------
# Main pipeline
# ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Run the StockIntel daily ML pipeline."
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip the time-of-day guard.",
    )

    parser.add_argument(
        "--skip-collect",
        action="store_true",
        help=(
            "Skip collector/sector/PCR steps and continue "
            "with freshness verification and training."
        ),
    )

    parser.add_argument(
        "--unattended",
        action="store_true",
        help=(
            "Skip the manual confirmation pause. "
            "Intended for future Task Scheduler usage."
        ),
    )

    args = parser.parse_args()

    now_ist = datetime.now(IST)

    print(
        f"Current IST time: "
        f"{now_ist.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    # ------------------------------------------------------------------
    # Step 1-3: Data collection
    # ------------------------------------------------------------------

    if not args.skip_collect:

        # --force bypasses ONLY the time guard.
        # It must NOT bypass collection itself.
        if not args.force:
            if now_ist.hour < MIN_HOUR_PCR:
                print(
                    f"\nBLOCKED: it's before {MIN_HOUR_PCR}:00 IST.\n"
                    f"\n"
                    f"NSE Bhavcopy for today may not yet be published, "
                    f"and running collector.py too early risks capturing "
                    f"an intraday/partial market price.\n"
                    f"\n"
                    f"Run again after {MIN_HOUR_PCR}:00 IST, "
                    f"or pass --force to override the time guard."
                )

                sys.exit(1)

        if not run_step(
            [
                sys.executable,
                "-m",
                "src.data.collector",
            ],
            "collector.py (price history)",
        ):
            sys.exit(1)

        if not run_step(
            [
                sys.executable,
                "-m",
                "src.data.sector_index_builder",
            ],
            "sector_index_builder.py",
        ):
            sys.exit(1)

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

    # ------------------------------------------------------------------
    # Step 4: Verify freshness
    # ------------------------------------------------------------------

    if not validate_data_freshness(now_ist):
        sys.exit(1)

    # ------------------------------------------------------------------
    # Manual safety confirmation
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Step 5: Train models
    # ------------------------------------------------------------------

    success = run_trainer_with_logging()

    if not success:
        print(
            "\n!!! trainer.py FAILED — "
            "prediction history will NOT be generated."
        )

        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 6: Generate immutable prediction history snapshots
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Step 7: Evaluate eligible pending predictions
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Complete
    # ------------------------------------------------------------------

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