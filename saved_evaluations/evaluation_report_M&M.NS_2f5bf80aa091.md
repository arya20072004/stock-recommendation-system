# Candidate Evaluation Report: M&M.NS

## 1. Identity
- **Ticker**: M&M.NS
- **Active Model Hash**: `58003cbc6ace`
- **Candidate Model Hash**: `2f5bf80aa091`
- **Evaluation Timestamp**: 2026-08-21T09:38:46.665515+00:00

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
| Actionable Precision | 0.4756 | 0.3908 | -0.0848 |
| Actionable Recall | 0.2746 | 0.2394 | -0.0352 |
| Actionable Count | 82 | 87 | 5 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.5
- **p-value**: 0.47950012218695337
- **Actionable Sample Size (Intersection)**: 45
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 120.94%
- **Candidate Return**: 66.98%
- **Delta**: -53.96%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 24, 1: 116, 2: 81}
- **Candidate Class Distribution**: {0: 51, 1: 87, 2: 83}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate strictly underperforms mathematically and economically.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
