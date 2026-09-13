# Candidate Evaluation Report: HINDALCO.NS

## 1. Identity
- **Ticker**: HINDALCO.NS
- **Active Model Hash**: `d7629d379cc2`
- **Candidate Model Hash**: `5739b74ff87f`
- **Evaluation Timestamp**: 2026-08-21T09:38:45.870749+00:00

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
| Actionable Precision | 0.3521 | 0.4265 | 0.0744 |
| Actionable Recall | 0.1701 | 0.1973 | 0.0272 |
| Actionable Count | 71 | 68 | -3 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 1.3333333333333333
- **p-value**: 0.24821307898992026
- **Actionable Sample Size (Intersection)**: 31
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 43.88%
- **Candidate Return**: 162.36%
- **Delta**: 118.47%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 44, 1: 110, 2: 67}
- **Candidate Class Distribution**: {0: 51, 1: 115, 2: 55}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Candidate economically outperforms, but statistical significance (McNemar) is weak or not applicable.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
