# Candidate Evaluation Report: NESTLEIND.NS

## 1. Identity
- **Ticker**: NESTLEIND.NS
- **Active Model Hash**: `d5fea4af8e8c`
- **Candidate Model Hash**: `50b5503bc292`
- **Evaluation Timestamp**: 2026-08-21T09:38:46.816832+00:00

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
| Actionable Precision | 0.2703 | 0.0000 | -0.2703 |
| Actionable Recall | 0.0787 | 0.0000 | -0.0787 |
| Actionable Count | 37 | 0 | -37 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: NOT APPLICABLE
- **Test Statistic**: None
- **p-value**: None
- **Actionable Sample Size (Intersection)**: 0
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 22.34%
- **Candidate Return**: 0.00%
- **Delta**: -22.34%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 39, 1: 138, 2: 44}
- **Candidate Class Distribution**: {0: 29, 1: 177, 2: 15}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate produced ZERO actionable predictions.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
