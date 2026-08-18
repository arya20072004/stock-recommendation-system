import os
import sys
import hashlib
from pymongo import MongoClient

sys.path.append('C:/Users/aryab/Coding/stock_recommendations')
from src.features.router import get_feature_pipeline_hash
from src.data.nifty50 import TICKERS
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
client = MongoClient(mongo_uri, tlsAllowInvalidCertificates=True)
db = client['stock_market_db']

canonical = get_feature_pipeline_hash('v1')
print('CANONICAL=' + canonical)

expected = 'f4891c1b2172b8e024b176cd221cf33c8b5e006acd567d95f2768cb7bf72384e'
print('EXPECTED=' + expected)
print('MATCH=' + str(canonical == expected))

with open('src/features/v1/engineering.py', 'r') as f:
    content = f.read()

# Check for the exact corrected string
has_correction = 'df["atr_pct"]   = df["atr"] / df["close"].shift(1).replace(0, pd.NA)' in content
print('CORRECTION_PRESENT=' + str(has_correction))

docs = list(db.model_registry.find({'status': 'ACTIVE'}))
hashes = [d.get('feature_pipeline_hash') for d in docs]
print('ACTIVE=' + str(len(docs)))
print('OLD=' + str(hashes.count('685cb3dbe63d7923126e44c597914c93a7bcebc83c6f6e42017dd1101f7d2c68')))
print('16E7=' + str(hashes.count('16e7f2049d88e62f915e57d043fe6d6baa5e4937459b56ab90d410664cf9c746')))
print('CORRECTED=' + str(hashes.count(expected)))

unexp = len(docs) - hashes.count('685cb3dbe63d7923126e44c597914c93a7bcebc83c6f6e42017dd1101f7d2c68') - hashes.count('16e7f2049d88e62f915e57d043fe6d6baa5e4937459b56ab90d410664cf9c746') - hashes.count(expected)
print('UNEXPECTED=' + str(unexp))

print('TICKERS=' + str(len(TICKERS)))
print('DUPES=' + str(len(TICKERS) - len(set(TICKERS))))
