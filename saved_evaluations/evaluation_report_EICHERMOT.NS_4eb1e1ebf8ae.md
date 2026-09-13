# Candidate Evaluation Report: EICHERMOT.NS

## 1. Identity
- **Ticker**: EICHERMOT.NS
- **Active Model Hash**: `3892bb6bc643`
- **Candidate Model Hash**: `4eb1e1ebf8ae`
- **Evaluation Timestamp**: 2026-08-21T09:38:43.323920+00:00

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
| Actionable Precision | 0.4327 | 0.2967 | -0.1360 |
| Actionable Recall | 0.3191 | 0.1915 | -0.1277 |
| Actionable Count | 104 | 91 | -13 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.125
- **p-value**: 0.7236736098317629
- **Actionable Sample Size (Intersection)**: 61
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 175.94%
- **Candidate Return**: 71.62%
- **Delta**: -104.32%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 78, 1: 77, 2: 66}
- **Candidate Class Distribution**: {0: 112, 1: 80, 2: 29}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate strictly underperforms mathematically and economically.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
