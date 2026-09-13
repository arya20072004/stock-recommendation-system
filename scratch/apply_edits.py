import json

with open('scratch/engineering_edits.json', 'r') as f:
    edits = json.load(f)

for edit in edits:
    print(edit)
