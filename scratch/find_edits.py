import json

edits = []
with open(r"C:\Users\aryab\.gemini\antigravity-ide\brain\a85ca318-94e9-4432-93bb-a2509cecd2dd\.system_generated\logs\transcript_full.jsonl", 'r', encoding='utf-8') as f:
    for line in f:
        data = json.loads(line)
        if data.get("tool_calls"):
            for call in data["tool_calls"]:
                if call.get("name") in ["multi_replace_file_content", "replace_file_content"]:
                    args = call.get("args", {})
                    if "engineering.py" in args.get("TargetFile", ""):
                        edits.append(args)

if edits:
    print(f"Found {len(edits)} edit(s) for engineering.py.")
    with open('scratch/engineering_edits.json', 'w') as f:
        json.dump(edits, f, indent=2)
    print("Saved to scratch/engineering_edits.json")
else:
    print("No edits found.")
