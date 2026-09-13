# Candidate Evaluation Report: HEROMOTOCO.NS

## 1. Identity
- **Ticker**: HEROMOTOCO.NS
- **Active Model Hash**: `d4c009de83c6`
- **Candidate Model Hash**: `0a507b9750d3`
- **Evaluation Timestamp**: 2026-08-20T13:09:39.432422+00:00

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
| Actionable Precision | 0.0000 | 0.3125 | 0.3125 |
| Actionable Recall | 0.0000 | 0.2222 | 0.2222 |
| Actionable Count | 0 | 96 | 96 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: NOT APPLICABLE
- **Test Statistic**: None
- **p-value**: None
- **Actionable Sample Size (Intersection)**: 0
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 0.00%
- **Candidate Return**: 30.93%
- **Delta**: 30.93%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 68, 1: 70, 2: 83}
- **Candidate Class Distribution**: {0: 87, 1: 52, 2: 82}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Candidate economically outperforms, but statistical significance (McNemar) is weak or not applicable.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
