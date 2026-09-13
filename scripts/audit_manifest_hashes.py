import sys
import os
import json
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from src.ml.history import TICKERS
from src.features.router import get_feature_pipeline_hash


expected = get_feature_pipeline_hash("v1")
root = PROJECT_ROOT / "saved_models"

print("EXPECTED HASH:", expected)
print("--- MANIFEST HASH AUDIT ---")

bad = []

for ticker in TICKERS:
    path = root / f"{ticker}_active.json"

    if not path.exists():
        bad.append((ticker, "MISSING"))
        continue

    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        bad.append((ticker, f"INVALID_JSON: {exc}"))
        continue

    actual = manifest.get("feature_pipeline_hash")

    if actual != expected:
        bad.append((ticker, actual))

print("EXPECTED TICKER COUNT:", len(TICKERS))
print("BAD COUNT:", len(bad))

for ticker, reason in bad:
    print(f"{ticker}: {reason}")

print("MANIFEST HASH AUDIT COMPLETE")
