# Candidate Evaluation Report: ULTRACEMCO.NS

## 1. Identity
- **Ticker**: ULTRACEMCO.NS
- **Active Model Hash**: `41427818da56`
- **Candidate Model Hash**: `12bd954e097b`
- **Evaluation Timestamp**: 2026-08-20T13:09:41.228049+00:00

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
| Actionable Precision | 0.3409 | 0.3308 | -0.0101 |
| Actionable Recall | 0.3488 | 0.3411 | -0.0078 |
| Actionable Count | 132 | 133 | 1 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.0
- **p-value**: 1.0
- **Actionable Sample Size (Intersection)**: 109
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 62.45%
- **Candidate Return**: 14.65%
- **Delta**: -47.80%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 35, 1: 62, 2: 124}
- **Candidate Class Distribution**: {0: 35, 1: 64, 2: 122}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate strictly underperforms mathematically and economically.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
