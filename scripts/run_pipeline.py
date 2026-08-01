"""
run_pipeline.py

Wrapper that runs the full daily pipeline in the correct order,
with a time guard to prevent accidental intraday/pre-Bhavcopy runs,
and logs trainer.py's raw output to disk (both a fresh timestamped
file per run, and a single rolling "latest.log" that always holds
the most recent run for quick reference).

Order: collector.py -> sector_index_builder.py -> pcr_builder.py
       -> check_max_dates.py (verify) -> trainer.py (logged)

Usage:
    python run_pipeline.py                # normal guarded run
    python run_pipeline.py --force         # skip the time guard (use with care)
    python run_pipeline.py --skip-collect  # skip steps 1-3, just verify + train
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Earliest IST hour it's safe to run each step (24h clock)
MIN_HOUR_COLLECTOR = 18   # 6:00 PM — yfinance EOD close reliably synced
MIN_HOUR_PCR       = 20   # 8:00 PM — NSE Bhavcopy reliably published


def run_step(cmd: list[str], label: str) -> bool:
    print(f"\n{'='*70}\n>>> {label}\n{'='*70}")
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        print(f"\n!!! {label} FAILED (exit code {result.returncode}) — stopping pipeline.")
        return False
    return True


def run_trainer_with_logging() -> bool:
    now_ist = datetime.now(IST)
    timestamped_log = LOG_DIR / f"trainer_{now_ist.strftime('%Y%m%d_%H%M%S')}.log"
    latest_log = LOG_DIR / "trainer_latest.log"

    print(f"\n{'='*70}\n>>> Running trainer.py (logging to {timestamped_log})\n{'='*70}")

    with open(timestamped_log, "w", encoding="utf-8") as f:
        f.write(f"=== Training run started {now_ist.isoformat()} ===\n\n")
        f.flush()
        process = subprocess.Popen(
            [sys.executable, "-m", "src.ml.trainer"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        for line in process.stdout:
            print(line, end="")   # still shows live in your terminal
            f.write(line)
        process.wait()
        f.write(f"\n=== Training run finished {datetime.now(IST).isoformat()} "
                f"(exit code {process.returncode}) ===\n")

    # Overwrite the rolling "latest" copy with this run's content
    latest_log.write_text(timestamped_log.read_text(encoding="utf-8"), encoding="utf-8")

    print(f"\nLog saved to: {timestamped_log}")
    print(f"Latest run also copied to: {latest_log}")

    return process.returncode == 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Skip the time-of-day guard")
    parser.add_argument("--skip-collect", action="store_true",
                         help="Skip collector/sector/pcr steps, just verify + train")
    parser.add_argument("--unattended", action="store_true",
                         help="Skip the manual confirmation pause (for Task Scheduler)")
    args = parser.parse_args()

    now_ist = datetime.now(IST)
    print(f"Current IST time: {now_ist.strftime('%Y-%m-%d %H:%M:%S')}")

    if not args.skip_collect:
        if not args.force:
            if now_ist.hour < MIN_HOUR_PCR:
                print(
                    f"\nBLOCKED: it's before {MIN_HOUR_PCR}:00 IST — NSE Bhavcopy "
                    f"for today likely isn't published yet, and running collector.py "
                    f"before market close risks capturing an intraday partial price.\n"
                    f"Run again after {MIN_HOUR_PCR}:00 IST, or pass --force to override."
                )
                sys.exit(1)

        if not run_step([sys.executable, "collector.py"], "collector.py (price history)"):
            sys.exit(1)
        if not run_step([sys.executable, "sector_index_builder.py"], "sector_index_builder.py"):
            sys.exit(1)
        if not run_step([sys.executable, "pcr_builder.py"], "pcr_builder.py (PCR + futures basis)"):
            sys.exit(1)

    check_result = subprocess.run(
        [sys.executable, "check_max_dates.py"],
        capture_output=True, text=True,
    )
    print(check_result.stdout)
    if check_result.returncode != 0:
        print("!!! check_max_dates.py FAILED — stopping pipeline.")
        sys.exit(1)

    today_str = now_ist.strftime("%Y-%m-%d")
    date_lines = [
        line for line in check_result.stdout.splitlines()
        if "max date" in line
    ]
    stale_lines = [line for line in date_lines if today_str not in line]
    stale = len(date_lines) == 0 or len(stale_lines) > 0

    if stale:
        msg = (
            f"\n!!! STALE DATA DETECTED: today's date ({today_str}) does not appear "
            f"in check_max_dates.py output. Aborting before training to avoid a "
            f"contaminated/duplicate run.\n\n{check_result.stdout}"
        )
        print(msg)
        # Leave a clear marker in the logs directory so you notice on your next check-in
        (LOG_DIR / f"ABORTED_stale_data_{now_ist.strftime('%Y%m%d_%H%M%S')}.log").write_text(
            msg, encoding="utf-8"
        )
        sys.exit(1)

    if args.unattended:
        print(">>> Unattended mode: max dates confirmed current, proceeding to trainer.py automatically.")
    else:
        print(
            "\n>>> Review the max-date output above. If any collection is NOT showing "
            "today's date, stop here and investigate before training.\n"
            ">>> Press Enter to proceed with trainer.py, or Ctrl+C to abort."
        )
        input()

    success = run_trainer_with_logging()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()