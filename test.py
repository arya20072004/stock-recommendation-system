"""
train_single_ticker.py
Run this to train (or re-train) a single ticker using the existing
ml_trainer.py pipeline — useful for resuming after an interrupted batch run.

Usage:
    python train_single_ticker.py
"""

import logging
from pymongo import MongoClient

from ml_trainer import create_dataset, train_model, MONGO_URI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

TICKER = "HDFCLIFE.NS"

def run_single(ticker):
    client = MongoClient(MONGO_URI)
    try:
        logger.info("Processing %s", ticker)
        dataset = create_dataset(ticker, client)
        if dataset.empty:
            logger.warning("%s: dataset creation failed or returned empty; aborting", ticker)
            return
        train_model(dataset, ticker)
    finally:
        client.close()
        logger.info("Single-ticker training run complete for %s", ticker)

if __name__ == "__main__":
    run_single(TICKER)