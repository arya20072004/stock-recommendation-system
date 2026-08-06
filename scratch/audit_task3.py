import pymongo
import os
import json
import collections
from dotenv import load_dotenv
from transformers import pipeline

load_dotenv()
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')

client = pymongo.MongoClient(MONGO_URI)
db = client['stock_market_db']
collection = db['news_articles']

print("Loading FinBERT...")
finbert = pipeline('sentiment-analysis', model='ProsusAI/finbert', tokenizer='ProsusAI/finbert')
print("Loaded FinBERT.")

def build_sentiment_text(title, clean_description):
    title = (title or "").strip()
    desc = (clean_description or "").strip()
    if desc:
        return f"{title}. {desc[:500]}".strip()
    return title

cursor = list(collection.find({"article_id": {"$exists": True}}))

print("Total Canonical:", len(cursor))
missing = 0
has_sentiment = 0

labels_old = collections.Counter()
labels_new = collections.Counter()
transitions = collections.Counter()
shifts = []
examples = []

print("Running Inference...")
for i, doc in enumerate(cursor):
    if 'label' not in doc:
        missing += 1
        continue
    has_sentiment += 1
    
    old_label = doc['label']
    old_score = doc['score']
    old_comp = doc['compound']
    
    title = doc.get('title', '')
    desc = doc.get('description', '')
    
    input_text = build_sentiment_text(title, desc)
    if not input_text:
        continue
        
    res = finbert(input_text)[0]
    new_label = res['label'].lower()
    new_score = float(res['score'])
    new_comp = new_score if new_label == 'positive' else (-new_score if new_label == 'negative' else 0.0)
    
    labels_old[old_label] += 1
    labels_new[new_label] += 1
    transitions[(old_label, new_label)] += 1
    
    shift = abs(new_comp - old_comp)
    shifts.append(shift)
    
    if old_label != new_label and len(examples) < 10:
        examples.append({
            'tickers': doc.get('tickers', []),
            'published_at': str(doc.get('published_at')),
            'old_label': old_label,
            'new_label': new_label,
            'old_comp': old_comp,
            'new_comp': new_comp,
            'title': title,
            'desc': desc
        })

shifts.sort()
changed = sum(1 for v in shifts if v > 1e-4)

print("="*40)
print(f"Audited: {len(cursor)}")
print(f"Has Sentiment: {has_sentiment}, Missing: {missing}")
print(f"Labels Unchanged: {has_sentiment - sum(1 for (o, n), c in transitions.items() if o != n)}")
print(f"Labels Changed: {sum(1 for (o, n), c in transitions.items() if o != n)}")
if has_sentiment > 0:
    print(f"Change %: {sum(1 for (o, n), c in transitions.items() if o != n) / has_sentiment * 100:.2f}%")
print(f"Mean Shift: {sum(shifts) / len(shifts) if shifts else 0:.4f}")
print(f"Median Shift: {shifts[len(shifts)//2] if shifts else 0:.4f}")
print(f"P90 Shift: {shifts[int(len(shifts)*0.9)] if shifts else 0:.4f}")
print(f"P95 Shift: {shifts[int(len(shifts)*0.95)] if shifts else 0:.4f}")
print(f"Max Shift: {max(shifts) if shifts else 0:.4f}")

print("\nTRANSITIONS:")
for (o, n), c in transitions.items():
    print(f"{o} -> {n}: {c}")

print("\nOLD LABELS:")
print(labels_old)
print("\nNEW LABELS:")
print(labels_new)

print("\nEXAMPLES:")
for ex in examples:
    print(json.dumps(ex, indent=2))
