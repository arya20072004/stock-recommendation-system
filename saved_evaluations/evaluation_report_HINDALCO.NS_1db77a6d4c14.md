# Candidate Evaluation Report: HINDALCO.NS

## 1. Identity
- **Ticker**: HINDALCO.NS
- **Active Model Hash**: `d7629d379cc2`
- **Candidate Model Hash**: `1db77a6d4c14`
- **Evaluation Timestamp**: 2026-08-20T13:09:39.488646+00:00

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
| Actionable Precision | 0.3521 | 0.4655 | 0.1134 |
| Actionable Recall | 0.1701 | 0.1837 | 0.0136 |
| Actionable Count | 71 | 58 | -13 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.0
- **p-value**: 1.0
- **Actionable Sample Size (Intersection)**: 42
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 43.88%
- **Candidate Return**: 160.26%
- **Delta**: 116.38%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 44, 1: 110, 2: 67}
- **Candidate Class Distribution**: {0: 31, 1: 115, 2: 75}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Candidate economically outperforms, but statistical significance (McNemar) is weak or not applicable.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
