import json
import joblib

from ml_trainer import create_dataset, MONGO_URI
from pymongo import MongoClient

client = MongoClient(MONGO_URI)

ticker = "RELIANCE.NS"

df = create_dataset(ticker, client)

with open(f"features/features_{ticker}.json") as f:
    features = json.load(f)

model = joblib.load(f"models/model_{ticker}.joblib")

X = df[features]

importance = model.feature_importances_

for feature, score in sorted(
    zip(features, importance),
    key=lambda x: x[1],
    reverse=True,
):
    print(f"{feature:30} {score:.6f}")