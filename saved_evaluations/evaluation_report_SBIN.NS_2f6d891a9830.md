# Candidate Evaluation Report: SBIN.NS

## 1. Identity
- **Ticker**: SBIN.NS
- **Active Model Hash**: `edf7eb6f74c2`
- **Candidate Model Hash**: `2f6d891a9830`
- **Evaluation Timestamp**: 2026-08-20T13:09:40.583060+00:00

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
| Actionable Precision | 0.0000 | 0.0943 | 0.0943 |
| Actionable Recall | 0.0000 | 0.0427 | 0.0427 |
| Actionable Count | 0 | 53 | 53 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: NOT APPLICABLE
- **Test Statistic**: None
- **p-value**: None
- **Actionable Sample Size (Intersection)**: 0
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 0.00%
- **Candidate Return**: -65.10%
- **Delta**: -65.10%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 82, 1: 125, 2: 14}
- **Candidate Class Distribution**: {0: 69, 1: 135, 2: 17}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Mixed results between precision and economic returns.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
