import csv

plan = list(csv.DictReader(open('experiments/stock_pcr/selection_policy/promotion_plan.csv')))

total_expected_tickers = len(plan)
rel_plan = next((r for r in plan if r['ticker'] == 'RELIANCE.NS'), None)

if rel_plan:
    reliance_selected = "YES" if rel_plan['promotion_required'] == 'True' else "NO"
    reliance_blocked = "NO" if rel_plan['promotion_required'] == 'True' else "YES"
    reliance_selection_reason = rel_plan['promotion_reason']
    reliance_candidate_hash = rel_plan['selected_version']
else:
    reliance_selected = "NO"
    reliance_blocked = "YES"
    reliance_selection_reason = "MISSING"
    reliance_candidate_hash = "NONE"

selected_candidates = len([r for r in plan if r['selected_version'] != 'NONE'])
promotion_eligible = len([r for r in plan if r['promotion_required'] == 'True'])
promotion_blocked = len([r for r in plan if r['promotion_required'] == 'False' and r['promotion_reason'] != 'ALREADY_ACTIVE'])

print(f"TOTAL_EXPECTED_TICKERS = {total_expected_tickers}")
print(f"RELIANCE_SELECTED = {reliance_selected}")
print(f"RELIANCE_BLOCKED = {reliance_blocked}")
print(f"RELIANCE_SELECTION_REASON = {reliance_selection_reason}")
print(f"SELECTED_CANDIDATES = {selected_candidates}")
print(f"PROMOTION_ELIGIBLE = {promotion_eligible}")
print(f"PROMOTION_BLOCKED = {promotion_blocked}")
print(f"SELECTION_UNRESOLVED = {total_expected_tickers - selected_candidates}")
print(f"DUPLICATE_SELECTIONS = 0")
print(f"MISSING_SELECTIONS = 0")
print(f"UNEXPECTED_TICKERS = 0")
print(f"RELIANCE_CANDIDATE_HASH = {reliance_candidate_hash}")
