import sys
from pymongo import MongoClient

client = MongoClient("mongodb+srv://stockuser:Stockml2024@cluster0.qlhakda.mongodb.net/?appName=Cluster0")
db = client["stock_market_db"]

# Print registry samples
active_models = list(db.model_registry.find({"status": "ACTIVE"}))
print("Total ACTIVE models:", len(active_models))
if len(active_models) > 0:
    print("Example version:", active_models[0].get("version"))
    print("Example feature_pipeline_version:", active_models[0].get("feature_pipeline_version"))
    
# Let's check max date of historical_data for ticker
h_date = db.historical_data.find_one({"ticker": "WIPRO.NS"}, sort=[("date", -1)])
print("Max historical_data date for WIPRO.NS:", h_date.get("date") if h_date else "None")

# And news
n_date = db.news_articles.find_one({"related_tickers": "WIPRO.NS"}, sort=[("published_at", -1)])
print("Max news date for WIPRO.NS:", n_date.get("published_at") if n_date else "None")
