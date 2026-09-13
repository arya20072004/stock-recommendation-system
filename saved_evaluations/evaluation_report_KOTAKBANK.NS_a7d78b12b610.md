# Candidate Evaluation Report: KOTAKBANK.NS

## 1. Identity
- **Ticker**: KOTAKBANK.NS
- **Active Model Hash**: `f3f93241aa7f`
- **Candidate Model Hash**: `a7d78b12b610`
- **Evaluation Timestamp**: 2026-08-21T09:38:46.480655+00:00

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
| Actionable Precision | 0.2021 | 0.1724 | -0.0297 |
| Actionable Recall | 0.1473 | 0.0775 | -0.0698 |
| Actionable Count | 94 | 58 | -36 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.25
- **p-value**: 0.6170750774519739
- **Actionable Sample Size (Intersection)**: 37
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: -159.56%
- **Candidate Return**: -116.33%
- **Delta**: 43.22%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 55, 1: 68, 2: 98}
- **Candidate Class Distribution**: {0: 65, 1: 78, 2: 78}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Mixed results between precision and economic returns.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
