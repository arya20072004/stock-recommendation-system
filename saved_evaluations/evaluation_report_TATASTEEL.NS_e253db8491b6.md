# Candidate Evaluation Report: TATASTEEL.NS

## 1. Identity
- **Ticker**: TATASTEEL.NS
- **Active Model Hash**: `f94e09e9cba3`
- **Candidate Model Hash**: `e253db8491b6`
- **Evaluation Timestamp**: 2026-08-21T09:38:47.607791+00:00

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
| Actionable Precision | 0.4352 | 0.4684 | 0.0332 |
| Actionable Recall | 0.3241 | 0.2552 | -0.0690 |
| Actionable Count | 108 | 79 | -29 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.0
- **p-value**: 1.0
- **Actionable Sample Size (Intersection)**: 60
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 109.87%
- **Candidate Return**: 91.62%
- **Delta**: -18.24%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 93, 1: 58, 2: 70}
- **Candidate Class Distribution**: {0: 87, 1: 83, 2: 51}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Mixed results between precision and economic returns.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
