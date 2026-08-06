import requests

def test_api():
    print("="*60)
    print("API REGRESSION VALIDATION")
    print("="*60)
    
    endpoints = [
        "http://localhost:5000/api/news",
        "http://localhost:5000/api/news?ticker=RELIANCE.NS",
        "http://localhost:5000/api/news?sentiment=positive",
        "http://localhost:5000/api/news?ticker=TCS.NS"
    ]
    
    for url in endpoints:
        try:
            res = requests.get(url).json()
            meta = res.get('meta', {})
            counts = meta.get('sentiment_counts', {})
            total = meta.get('total', 0)
            
            calc_total = counts.get('positive', 0) + counts.get('neutral', 0) + counts.get('negative', 0) + counts.get('unscored', 0)
            
            print(f"URL: {url}")
            print(f"Total: {total}, Calculated: {calc_total}")
            if total == calc_total:
                print("PASS - Counts match.")
            else:
                print("FAIL - Counts mismatch!")
        except Exception as e:
            print(f"Failed hitting {url}: {e}")

if __name__ == '__main__':
    test_api()
