# Candidate Evaluation Report: ULTRACEMCO.NS

## 1. Identity
- **Ticker**: ULTRACEMCO.NS
- **Active Model Hash**: `41427818da56`
- **Candidate Model Hash**: `2912be06dedd`
- **Evaluation Timestamp**: 2026-08-21T09:38:48.158737+00:00

## 2. Dataset
- **Evaluation Dataset Version**: `v1_b4c8b5075e70`
- **Evaluation Dataset Hash**: `b4c8b5075e7059c10a9406fc06b2f03b764650394f2e355a7d39ed8e2f1cd420`
- **Evaluation Start Date**: 2025-08-25
- **Evaluation End Date**: 2026-08-05
- **Evaluation Rows (Ticker)**: 221

## 3. Contract
- **Prediction Horizon**: 10 sessions
- **Active Feature Pipeline Hash**: `f4891c1b2172b8e024b176cd221cf33c8b5e006acd567d95f2768cb7bf72384e`
- **Candidate Feature Pipeline Hash**: `f4891c1b2172b8e024b176cd221cf33c8b5e006acd567d95f2768cb7bf72384e`
- **Feature Schema Compatibility**: PASS

## 4. ML Metrics (Actionable Signals Only)
| Metric | Active | Candidate | Delta |
|--------|--------|-----------|-------|
| Actionable Precision | 0.3409 | 0.2847 | -0.0562 |
| Actionable Recall | 0.3488 | 0.3023 | -0.0465 |
| Actionable Count | 132 | 137 | 5 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 1.3333333333333333
- **p-value**: 0.24821307898992026
- **Actionable Sample Size (Intersection)**: 99
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 62.45%
- **Candidate Return**: -56.53%
- **Delta**: -118.99%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 35, 1: 62, 2: 124}
- **Candidate Class Distribution**: {0: 38, 1: 50, 2: 133}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate strictly underperforms mathematically and economically.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
