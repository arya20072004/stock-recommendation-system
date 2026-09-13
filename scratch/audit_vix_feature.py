import json

with open("scratch/audit_dump_v2.json", "r") as f:
    data = json.load(f)

preds = data.get("predictions", [])
print(f"Total predictions: {len(preds)}")

vix_levels = set()
for p in preds:
    fs = p.get("feature_snapshot", {})
    vix = fs.get("vix_level")
    if vix is not None:
        vix_levels.add(vix)

print("VIX levels found in feature_snapshots:")
print(vix_levels)

