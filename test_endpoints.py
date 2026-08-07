import json
from app import app, db

def run_tests():
    app.config['TESTING'] = True
    client = app.test_client()

    print("--- 1. Testing GET /api/predictions/history ---")
    res1 = client.get('/api/predictions/history')
    print(f"Status: {res1.status_code}")
    data1 = json.loads(res1.data)
    print(f"Total returned: {data1.get('total')}")
    print(f"Sample records length: {len(data1.get('data', []))}")
    if data1.get('data'):
        first_record = data1['data'][0]
        print("Sample keys:", list(first_record.keys()))

    print("\n--- 2. Testing GET /api/stocks/summary ---")
    res2 = client.get('/api/stocks/summary')
    print(f"Status: {res2.status_code}")
    data2 = json.loads(res2.data)
    print(f"Total returned: {data2.get('total')}")
    print(f"Sample length: {len(data2.get('data', []))}")
    if data2.get('data'):
        first_record = data2['data'][0]
        print("Sample keys:", list(first_record.keys()))
        print("Screener/Stocks data specific keys include sector:", 'sector' in first_record)

    print("\n--- 3. Testing GET /api/recommendations ---")
    res3 = client.get('/api/recommendations')
    print(f"Status: {res3.status_code}")
    data3 = json.loads(res3.data)
    print(f"Total returned: {data3.get('total')}")

    print("\n--- 4. Testing GET /api/stocks/RELIANCE.NS/details?range=1Y ---")
    res4 = client.get('/api/stocks/RELIANCE.NS/details?range=1Y')
    print(f"Status: {res4.status_code}")
    data4 = json.loads(res4.data)
    print("Keys returned:", list(data4.keys()))
    if 'company' in data4:
        print("Company info:", data4['company'])

    print("\n--- 5. Cross-page Consistency Check ---")
    tickers_to_check = ['RELIANCE.NS', 'TCS.NS', 'SBIN.NS', 'INFY.NS', 'TATASTEEL.NS']
    
    # from summary
    summary_dict = {row['ticker']: row for row in data2.get('data', [])}
    # from recommendations
    rec_dict = {row['ticker']: row for row in data3.get('data', [])}
    
    for ticker in tickers_to_check:
        print(f"\nChecking {ticker}:")
        sum_data = summary_dict.get(ticker, {})
        rec_data = rec_dict.get(ticker, {})
        
        print(f"  Summary Rec: {sum_data.get('recommendation')} | Conf: {sum_data.get('confidence')} | Tier: {sum_data.get('confidence_tier')}")
        print(f"  Recs Rec: {rec_data.get('recommendation')} | Conf: {rec_data.get('confidence')} | Tier: {rec_data.get('confidence_tier')}")
        
        if sum_data.get('recommendation') != rec_data.get('recommendation') or sum_data.get('confidence') != rec_data.get('confidence'):
            print(f"  --> DISCREPANCY DETECTED for {ticker}")
        else:
            print(f"  --> CONSISTENT")

if __name__ == '__main__':
    run_tests()
