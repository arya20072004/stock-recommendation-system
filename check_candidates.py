from dotenv import load_dotenv
import os
from pymongo import MongoClient

load_dotenv()

db = MongoClient(os.getenv("MONGO_URI"))["stock_market_db"]

print("TICKER | MACRO | SELL | HOLD | BUY | TEST DISTRIBUTION")
print("-" * 120)

records = db.model_registry.find(
    {"status": "CANDIDATE"}
).sort("ticker", 1)

for r in records:
    metrics = r.get("metrics", {})
    per_class = metrics.get("per_class_metrics", {})
    distribution = metrics.get("test_prediction_distribution", {})

    print(
        f"{r['ticker']:15} | "
        f"{metrics.get('f1_macro', 0):.4f} | "
        f"{per_class.get('SELL', {}).get('f1', 0):.4f} | "
        f"{per_class.get('HOLD', {}).get('f1', 0):.4f} | "
        f"{per_class.get('BUY', {}).get('f1', 0):.4f} | "
        f"{distribution}"
    )
