# Candidate Evaluation Report: TECHM.NS

## 1. Identity
- **Ticker**: TECHM.NS
- **Active Model Hash**: `5b607da2d633`
- **Candidate Model Hash**: `772dbce93d30`
- **Evaluation Timestamp**: 2026-08-21T09:38:47.825167+00:00

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
| Actionable Precision | 0.3835 | 0.3690 | -0.0144 |
| Actionable Recall | 0.3493 | 0.2123 | -0.1370 |
| Actionable Count | 133 | 84 | -49 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.5
- **p-value**: 0.47950012218695337
- **Actionable Sample Size (Intersection)**: 60
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 171.19%
- **Candidate Return**: 115.29%
- **Delta**: -55.90%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 145, 1: 29, 2: 47}
- **Candidate Class Distribution**: {0: 89, 1: 54, 2: 78}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate strictly underperforms mathematically and economically.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
