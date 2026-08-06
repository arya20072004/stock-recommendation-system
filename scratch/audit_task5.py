from transformers import pipeline

finbert = pipeline('sentiment-analysis', model='ProsusAI/finbert', tokenizer='ProsusAI/finbert')

texts = [
    "RELIANCE sets new record highs with stellar earnings. Growth continues across all sectors.", # POSITIVE
    "Reliance Industries Q3 Results.", # NEUTRAL
    "Market collapses as Reliance reports terrible Q3 losses.", # NEGATIVE
]

for t in texts:
    print("Testing text:", t)
    for i in range(5):
        res = finbert(t)[0]
        print(f"  {i}: {res['label']} ({res['score']})")
    print()
