# Candidate Evaluation Report: LT.NS

## 1. Identity
- **Ticker**: LT.NS
- **Active Model Hash**: `094c1649e4e1`
- **Candidate Model Hash**: `f3fa8587f680`
- **Evaluation Timestamp**: 2026-08-21T09:38:46.572883+00:00

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
| Actionable Precision | 0.3875 | 0.4390 | 0.0515 |
| Actionable Recall | 0.2039 | 0.3553 | 0.1513 |
| Actionable Count | 80 | 123 | 43 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 4.166666666666667
- **p-value**: 0.041226833337163815
- **Actionable Sample Size (Intersection)**: 64
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 82.81%
- **Candidate Return**: 147.79%
- **Delta**: 64.97%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 34, 1: 108, 2: 79}
- **Candidate Class Distribution**: {0: 54, 1: 69, 2: 98}

## 8. Final Decision
### **Verdict**: PASS
**Explanation**: Candidate statistically and economically outperforms Active model on actionable precision.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
