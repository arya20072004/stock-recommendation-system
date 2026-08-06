import hashlib
import logging
import os

logger = logging.getLogger(__name__)

MODELS_DIR = "saved_models"

def get_model_version(ticker: str) -> str:
    """
    Generate a deterministic model version using SHA-256.

    Preferred artifact:
        saved_models/model_<ticker>.ubj

    Fallback:
        saved_models/model_<ticker>.joblib

    The first 12 hexadecimal characters are stored as the version.
    """
    ubj_path = os.path.join(MODELS_DIR, f"model_{ticker}.ubj")
    joblib_path = os.path.join(MODELS_DIR, f"model_{ticker}.joblib")

    if os.path.exists(ubj_path):
        model_path = ubj_path
    elif os.path.exists(joblib_path):
        model_path = joblib_path
    else:
        logger.error("No model artifact found for %s", ticker)
        return "unknown"

    sha256 = hashlib.sha256()

    try:
        with open(model_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()[:12]
    except Exception as exc:
        logger.exception("Error hashing model for %s: %s", ticker, exc)
        return "error"
