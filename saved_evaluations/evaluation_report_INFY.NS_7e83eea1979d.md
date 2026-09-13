# Candidate Evaluation Report: INFY.NS

## 1. Identity
- **Ticker**: INFY.NS
- **Active Model Hash**: `cfaba47c9bb5`
- **Candidate Model Hash**: `7e83eea1979d`
- **Evaluation Timestamp**: 2026-08-20T13:09:39.705691+00:00

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
| Actionable Precision | 0.4286 | 0.4167 | -0.0119 |
| Actionable Recall | 0.0744 | 0.0826 | 0.0083 |
| Actionable Count | 21 | 24 | 3 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: NOT APPLICABLE
- **Test Statistic**: None
- **p-value**: None
- **Actionable Sample Size (Intersection)**: 10
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 21.67%
- **Candidate Return**: 35.17%
- **Delta**: 13.50%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 36, 1: 171, 2: 14}
- **Candidate Class Distribution**: {0: 29, 1: 168, 2: 24}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Mixed results between precision and economic returns.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
