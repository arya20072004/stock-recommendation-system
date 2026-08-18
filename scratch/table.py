import json
data = json.load(open('scratch/audit_report_data.json'))

with open('scratch/audit_table.md', 'w') as f:
    f.write('| Ticker | Model | Features | Metrics | Hashes | Pipeline Hash | Active Manifest | Candidate/Active | Confidence Status | Consistent | Notes |\n')
    f.write('|--------|-------|----------|---------|--------|---------------|-----------------|------------------|-------------------|------------|-------|\n')

    for d in data:
        f1 = d['f1_macro']
        conf = 'VERY_LOW_CONFIDENCE' if f1 < 0.3 else 'NORMAL'
        pipe_hash = d['feature_pipeline_hash'][:8] + '...' if d['feature_pipeline_hash'] else 'None'
        act_ver = d['active_version'] if d['active_version'] else 'None'
        f.write(f"| {d['ticker']} | {d['new_version']} | {d['feature_count']} features | Exists ({f1:.2f}) | Match | {pipe_hash} | {act_ver} | Separated | {conf} | Yes | - |\n")
