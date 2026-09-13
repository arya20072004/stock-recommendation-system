# Candidate Evaluation Report: EICHERMOT.NS

## 1. Identity
- **Ticker**: EICHERMOT.NS
- **Active Model Hash**: `3892bb6bc643`
- **Candidate Model Hash**: `fcf690c5c6da`
- **Evaluation Timestamp**: 2026-08-20T13:09:39.037625+00:00

## 2. Dataset
- **Evaluation Dataset Version**: `v1_a3262716b006`
- **Evaluation Dataset Hash**: `a3262716b006e3097e592cd2855d91fdaed759bb52107fd164ffc70932a4f520`
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
| Actionable Precision | 0.4327 | 0.5000 | 0.0673 |
| Actionable Recall | 0.3191 | 0.3262 | 0.0071 |
| Actionable Count | 104 | 92 | -12 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.5
- **p-value**: 0.47950012218695337
- **Actionable Sample Size (Intersection)**: 73
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 175.94%
- **Candidate Return**: 171.32%
- **Delta**: -4.61%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 78, 1: 77, 2: 66}
- **Candidate Class Distribution**: {0: 79, 1: 72, 2: 70}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Mixed results between precision and economic returns.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
