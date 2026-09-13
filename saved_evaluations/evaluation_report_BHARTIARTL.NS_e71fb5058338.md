# Candidate Evaluation Report: BHARTIARTL.NS

## 1. Identity
- **Ticker**: BHARTIARTL.NS
- **Active Model Hash**: `b80cbc8d95e4`
- **Candidate Model Hash**: `e71fb5058338`
- **Evaluation Timestamp**: 2026-08-21T09:38:42.874794+00:00

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
| Actionable Precision | 0.3108 | 0.0000 | -0.3108 |
| Actionable Recall | 0.1608 | 0.0000 | -0.1608 |
| Actionable Count | 74 | 0 | -74 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: NOT APPLICABLE
- **Test Statistic**: None
- **p-value**: None
- **Actionable Sample Size (Intersection)**: 0
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 8.37%
- **Candidate Return**: 0.00%
- **Delta**: -8.37%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 30, 1: 108, 2: 83}
- **Candidate Class Distribution**: {0: 36, 1: 68, 2: 117}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate produced ZERO actionable predictions.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
