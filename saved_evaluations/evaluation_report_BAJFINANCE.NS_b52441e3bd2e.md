# Candidate Evaluation Report: BAJFINANCE.NS

## 1. Identity
- **Ticker**: BAJFINANCE.NS
- **Active Model Hash**: `ad44ce6a4b40`
- **Candidate Model Hash**: `b52441e3bd2e`
- **Evaluation Timestamp**: 2026-08-21T09:38:42.640885+00:00

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
| Actionable Precision | 0.5185 | 0.0000 | -0.5185 |
| Actionable Recall | 0.1739 | 0.0000 | -0.1739 |
| Actionable Count | 54 | 0 | -54 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: NOT APPLICABLE
- **Test Statistic**: None
- **p-value**: None
- **Actionable Sample Size (Intersection)**: 0
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 78.47%
- **Candidate Return**: 0.00%
- **Delta**: -78.47%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 52, 1: 141, 2: 28}
- **Candidate Class Distribution**: {0: 125, 1: 68, 2: 28}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate produced ZERO actionable predictions.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
