# Candidate Evaluation Report: APOLLOHOSP.NS

## 1. Identity
- **Ticker**: APOLLOHOSP.NS
- **Active Model Hash**: `51c339635e1b`
- **Candidate Model Hash**: `3402a2836e61`
- **Evaluation Timestamp**: 2026-08-21T09:38:42.271371+00:00

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
| Actionable Precision | 0.6957 | 0.0000 | -0.6957 |
| Actionable Recall | 0.1088 | 0.0000 | -0.1088 |
| Actionable Count | 23 | 0 | -23 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: NOT APPLICABLE
- **Test Statistic**: None
- **p-value**: None
- **Actionable Sample Size (Intersection)**: 0
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 44.11%
- **Candidate Return**: 0.00%
- **Delta**: -44.11%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 43, 1: 149, 2: 29}
- **Candidate Class Distribution**: {0: 56, 1: 140, 2: 25}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate produced ZERO actionable predictions.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
