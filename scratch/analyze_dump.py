import json

with open("scratch/audit_dump_v2.json", "r") as f:
    data = json.load(f)

# 1. RUN
run = data["runs"][0]
print("=== RUN INFO ===")
print("Run ID:", run["run_id"])
print("Status:", run["status"])
print("Market Date:", run["market_date"])
start = run["stages"]["START"]["timestamp"]["$date"]
end = run["stages"].get("API_HEALTH", {}).get("timestamp", {}).get("$date", "N/A")
print("Start time:", start)
print("End time:", end)

print("\n=== STAGES ===")
for stage, details in run["stages"].items():
    print(f"{stage}: {details['status']}")

print("\n=== VIX ===")
vix_data = data.get("vix", [])
if not vix_data:
    print("VIX exact match empty. Symbols:", data.get("vix_symbols", []))
else:
    for v in vix_data:
        print(f"VIX Date: {v['date']} Close: {v['close']}")

print("\n=== PREDICTIONS ===")
preds = data["predictions"]
print("Total predictions for the run date:", len(preds))
if preds:
    print("First prediction date:", preds[0].get("market_date"))
    symbols = [p["symbol"] for p in preds]
    print("Unique symbols:", len(set(symbols)))
    # Check hashes in features if present
    # Check for any lookahead in feature_snapshot
    lookahead = False
    for p in preds:
        fs = p.get("feature_snapshot", {})
        # check if any feature value or key implies Sep 5 or later
    
    print("Lookahead found:", lookahead)
    
print("\n=== REGISTRY ===")
registry = data["registry"]
print("Registry count:", len(registry))
expected_hash = "26cc670ca5d434821c0485bde3310599e2c077dbee5504c8b266d4a37333388c"
mismatches = [m for m in registry if m.get("feature_pipeline_hash") != expected_hash]
print("Registry mismatches:", len(mismatches))

print("\n=== PREDICTION HASHES ===")
# Is the pipeline hash stored in the prediction?
if preds:
    p0 = preds[0]
    hash_fields = [k for k in p0.keys() if "hash" in k.lower() or "version" in k.lower()]
    print("Hash/Version fields in prediction:", hash_fields)
    print("Model Version:", p0.get("model_version"))
