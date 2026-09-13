import pymongo
from dotenv import load_dotenv
import os

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")
db = pymongo.MongoClient(mongo_uri)["stock_market_db"]

# Historical data schema
doc = db.historical_data.find_one()
print("Historical Data Keys:", doc.keys() if doc else "Empty")
if doc:
    print("Sample:", {k: v for k, v in doc.items() if k != "_id"})

# Search for VIX by any means
# Let's search for any document where ticker or symbol has VIX
vix1 = list(db.historical_data.find({"ticker": {"$regex": "VIX", "$options": "i"}}).limit(1))
vix2 = list(db.historical_data.find({"symbol": {"$regex": "VIX", "$options": "i"}}).limit(1))
print("Found by ticker:", len(vix1))
print("Found by symbol:", len(vix2))

# Let's see if the NSE India VIX is stored in `market_data` instead? Wait, what collections?
# ['sector_indices', 'model_locks', 'pipeline_runs', 'fii_dii_data', 'model_registry', 'historical_data', 'pcr_data', 'prediction_provenance', 'news_articles', 'prediction_history', 'pipeline_locks']
# No market_data.

# Let's dump all manifests to verify the hashes
import glob, json
manifests = glob.glob("saved_models/*_active.json")
mismatches = 0
EXPECTED = "26cc670ca5d434821c0485bde3310599e2c077dbee5504c8b266d4a37333388c"
for m in manifests:
    with open(m) as f:
        data = json.load(f)
        if data.get("feature_pipeline_hash") != EXPECTED:
            mismatches += 1
            print(f"Mismatch in {m}: {data.get('feature_pipeline_hash')}")

print(f"Manifests verified: {len(manifests)}, Mismatches: {mismatches}")

