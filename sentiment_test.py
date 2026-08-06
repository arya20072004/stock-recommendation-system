import pymongo, os, re, html
from dotenv import load_dotenv
from transformers import pipeline

load_dotenv()
client = pymongo.MongoClient(os.getenv('MONGO_URI'))
db = client['stock_market_db']
collection = db['news_articles']

finbert = pipeline(
    'sentiment-analysis',
    model='ProsusAI/finbert',
    tokenizer='ProsusAI/finbert'
)

def clean_html(raw_html):
    if not raw_html: return ""
    text = html.unescape(raw_html)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

dirty_docs = list(collection.find({'fetched_source': 'Google News', 'article_id': {'$exists': True}}).limit(20))

diffs = []
for doc in dirty_docs:
    title = doc.get('title', '')
    desc_raw = doc.get('description', '')
    desc_clean = clean_html(desc_raw)
    
    input_old = f"{title}. {desc_raw[:500]}".strip()
    input_new = f"{title}. {desc_clean[:500]}".strip()
    
    res_old = finbert(input_old)[0]
    res_new = finbert(input_new)[0]
    
    old_label = res_old['label'].lower()
    old_score = float(res_old['score'])
    new_label = res_new['label'].lower()
    new_score = float(res_new['score'])
    
    old_comp = old_score if old_label == 'positive' else (-old_score if old_label == 'negative' else 0.0)
    new_comp = new_score if new_label == 'positive' else (-new_score if new_label == 'negative' else 0.0)
    
    diffs.append({
        'title': title,
        'old_label': old_label,
        'new_label': new_label,
        'old_comp': old_comp,
        'new_comp': new_comp,
        'diff': abs(new_comp - old_comp)
    })

changed_labels = sum(1 for d in diffs if d['old_label'] != d['new_label'])
avg_diff = sum(d['diff'] for d in diffs) / len(diffs)

print(f"Tested 20 articles.")
print(f"Labels changed: {changed_labels} ({changed_labels/20*100}%)")
print(f"Average absolute compound difference: {avg_diff:.4f}")

for d in diffs:
    if d['old_label'] != d['new_label'] or d['diff'] > 0.1:
        print(f"[{d['old_label']} -> {d['new_label']}] diff={d['diff']:.4f} | {d['title']}")
