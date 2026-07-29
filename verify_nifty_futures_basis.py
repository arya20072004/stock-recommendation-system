"""
verify_nifty_futures_basis.py

Standalone, read-only check: fetches ONE recent trading day's F&O Bhavcopy,
extracts the near-month NIFTY futures close, pulls NIFTY spot close for the
same day via yfinance, and prints the computed basis.

Does NOT write to MongoDB. Run this before touching pcr_builder.py / trainer.py.

Usage:
    python verify_nifty_futures_basis.py                # uses most recent business day
    python verify_nifty_futures_basis.py 2026-07-28      # specific date (YYYY-MM-DD)
"""

import io
import sys
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import requests
import yfinance as yf

CUTOVER_DATE = datetime(2024, 7, 8)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/all-reports-derivatives",
})


def _legacy_url(dt: datetime) -> str:
    mon = dt.strftime("%b").upper()
    return (
        f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/"
        f"{dt.year}/{mon}/fo{dt.strftime('%d')}{mon}{dt.year}bhav.csv.zip"
    )


def _udiff_url(dt: datetime) -> str:
    return (
        f"https://nsearchives.nseindia.com/content/fo/"
        f"BhavCopy_NSE_FO_0_0_0_{dt.strftime('%Y%m%d')}_F_0000.csv.zip"
    )


def fetch_bhavcopy(dt: datetime) -> pd.DataFrame | None:
    url = _udiff_url(dt) if dt >= CUTOVER_DATE else _legacy_url(dt)
    print(f"Fetching: {url}")
    resp = SESSION.get(url, timeout=15)
    print(f"  status={resp.status_code}, bytes={len(resp.content)}")
    if resp.status_code != 200 or len(resp.content) < 200:
        return None
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_name = zf.namelist()[0]
        print(f"  zip contains: {csv_name}")
        with zf.open(csv_name) as f:
            df = pd.read_csv(f)
    return df


def inspect_and_extract(df: pd.DataFrame, dt: datetime):
    df.columns = [c.strip() for c in df.columns]
    print("\n--- RAW COLUMNS ---")
    print(list(df.columns))

    is_udiff = dt >= CUTOVER_DATE
    if is_udiff:
        df = df.rename(columns={
            "TckrSymb": "SYMBOL",
            "FinInstrmTp": "INSTRUMENT",
            "OptnTp": "OPTION_TYP",
            "OpnIntrst": "OPEN_INT",
            "XpryDt": "EXPIRY_DT",
            "ClsPric": "CLOSE",
        })

    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()

    print("\n--- UNIQUE INSTRUMENT/FinInstrmTp VALUES FOR SYMBOL=NIFTY ---")
    nifty_rows = df[df["SYMBOL"] == "NIFTY"]
    if nifty_rows.empty:
        print("  No rows found where SYMBOL == 'NIFTY'. Check symbol column mapping.")
        return
    print(sorted(nifty_rows["INSTRUMENT"].astype(str).str.upper().unique().tolist()))

    print("\n--- SAMPLE NIFTY ROWS (first 5) ---")
    print(nifty_rows.head(5).to_string())

    # Try both known futures codes — legacy and a guessed UDiFF code
    candidate_codes = ["FUTIDX", "IDF"]
    fut_rows = nifty_rows[
        nifty_rows["INSTRUMENT"].astype(str).str.upper().isin(candidate_codes)
    ]

    print(f"\n--- FUTURES ROWS MATCHED (codes tried: {candidate_codes}) ---")
    if fut_rows.empty:
        print("  NO MATCH. Inspect the unique INSTRUMENT values above and update "
              "candidate_codes / the mapping in pcr_builder.py accordingly.")
        return

    print(fut_rows[["SYMBOL", "INSTRUMENT", "EXPIRY_DT", "CLOSE"]].to_string())

    fut_rows = fut_rows.copy()
    fut_rows["EXPIRY_DT"] = pd.to_datetime(fut_rows["EXPIRY_DT"], errors="coerce")
    near_month = fut_rows.sort_values("EXPIRY_DT").iloc[0]
    fut_close = pd.to_numeric(near_month["CLOSE"], errors="coerce")

    print(f"\n--- NEAR-MONTH CONTRACT SELECTED ---")
    print(f"  expiry: {near_month['EXPIRY_DT']}")
    print(f"  futures close: {fut_close}")

    # Fetch NIFTY spot close for same day
    spot = yf.download("^NSEI", start=dt, end=dt + timedelta(days=3), progress=False, auto_adjust=True)
    if isinstance(spot.columns, pd.MultiIndex):
        spot.columns = spot.columns.get_level_values(0)
    if spot.empty:
        print("\n  Could not fetch NIFTY spot close from yfinance for this date.")
        return
    spot_close = float(spot["Close"].iloc[0])
    basis = (fut_close - spot_close) / spot_close

    print(f"\n--- BASIS COMPUTATION ---")
    print(f"  spot close:    {spot_close}")
    print(f"  futures close: {fut_close}")
    print(f"  basis:         {basis:.6f}  ({basis*100:.3f}%)")


def main():
    if len(sys.argv) > 1:
        dt = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    else:
        # walk back from today to find a business day likely to have data
        dt = datetime.now() - timedelta(days=1)
        while dt.weekday() >= 5:
            dt -= timedelta(days=1)

    print(f"Target date: {dt.date()} (cutover comparison: {'UDiFF' if dt >= CUTOVER_DATE else 'legacy'})")

    df = fetch_bhavcopy(dt)
    if df is None:
        print("Fetch failed — try a different (earlier) date, e.g. a recent Tuesday/Wednesday.")
        return

    inspect_and_extract(df, dt)


if __name__ == "__main__":
    main()