# Candidate Evaluation Report: M&M.NS

## 1. Identity
- **Ticker**: M&M.NS
- **Active Model Hash**: `58003cbc6ace`
- **Candidate Model Hash**: `ce2c3b092fa6`
- **Evaluation Timestamp**: 2026-08-20T13:09:40.082931+00:00

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
| Actionable Precision | 0.4756 | 0.4595 | -0.0162 |
| Actionable Recall | 0.2746 | 0.2394 | -0.0352 |
| Actionable Count | 82 | 74 | -8 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 1.3333333333333333
- **p-value**: 0.24821307898992026
- **Actionable Sample Size (Intersection)**: 48
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 120.94%
- **Candidate Return**: 101.41%
- **Delta**: -19.53%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 24, 1: 116, 2: 81}
- **Candidate Class Distribution**: {0: 20, 1: 113, 2: 88}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate strictly underperforms mathematically and economically.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
