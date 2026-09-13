import json
import glob

manifests = glob.glob("saved_models/*_active.json")
with open(manifests[0], "r") as f:
    data = json.load(f)

print(data)
