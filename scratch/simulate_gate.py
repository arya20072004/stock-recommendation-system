import os
import sys

# We'll just define the mock logic here to avoid actual DB mutation
def test_states():
    TICKERS = ["ADANIENT.NS", "RELIANCE.NS"] # Mock just 2
    current_hash = "f4891c1b2172b8e024b176cd221cf33c8b5e006acd567d95f2768cb7bf72384e"
    legacy_hash = "16e7f2049d88e62f915e57d043fe6d6baa5e4937459b56ab90d410664cf9c746"
    
    def simulate_gate(active_records):
        for rec in active_records:
            ticker = rec["ticker"]
            if rec.get("feature_pipeline_hash") != current_hash:
                return f"FAIL: {ticker} has wrong pipeline hash"
        return "PASS"
        
    print("STATE A (all legacy):", simulate_gate([{"ticker": t, "feature_pipeline_hash": legacy_hash} for t in TICKERS]))
    print("STATE B (50 corrected, 1 legacy):", simulate_gate([{"ticker": "ADANIENT.NS", "feature_pipeline_hash": current_hash}, {"ticker": "SOME_TICKER", "feature_pipeline_hash": legacy_hash}]))
    print("STATE C (50 corrected, RELIANCE blocked):", simulate_gate([{"ticker": "ADANIENT.NS", "feature_pipeline_hash": current_hash}, {"ticker": "RELIANCE.NS", "feature_pipeline_hash": legacy_hash}]))
    print("STATE D (51 corrected):", simulate_gate([{"ticker": t, "feature_pipeline_hash": current_hash} for t in TICKERS]))

test_states()
