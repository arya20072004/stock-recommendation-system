# Candidate Evaluation Report: INDIGO.NS

## 1. Identity
- **Ticker**: INDIGO.NS
- **Active Model Hash**: `d12253eafe81`
- **Candidate Model Hash**: `1cf4ac17df8c`
- **Evaluation Timestamp**: 2026-08-20T13:09:39.649571+00:00

## 2. Dataset
- **Evaluation Dataset Version**: `v1_a3262716b006`
- **Evaluation Dataset Hash**: `a3262716b006e3097e592cd2855d91fdaed759bb52107fd164ffc70932a4f520`
- **Evaluation Start Date**: 2025-08-25
- **Evaluation End Date**: 2026-08-05
- **Evaluation Rows (Ticker)**: 222

## 3. Contract
- **Prediction Horizon**: 10 sessions
- **Active Feature Pipeline Hash**: `f4891c1b2172b8e024b176cd221cf33c8b5e006acd567d95f2768cb7bf72384e`
- **Candidate Feature Pipeline Hash**: `f4891c1b2172b8e024b176cd221cf33c8b5e006acd567d95f2768cb7bf72384e`
- **Feature Schema Compatibility**: PASS

## 4. ML Metrics (Actionable Signals Only)
| Metric | Active | Candidate | Delta |
|--------|--------|-----------|-------|
| Actionable Precision | 0.2892 | 0.3294 | 0.0403 |
| Actionable Recall | 0.1832 | 0.2137 | 0.0305 |
| Actionable Count | 83 | 85 | 2 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 1.3333333333333333
- **p-value**: 0.24821307898992026
- **Actionable Sample Size (Intersection)**: 64
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: -26.63%
- **Candidate Return**: 5.97%
- **Delta**: 32.59%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 25, 1: 97, 2: 100}
- **Candidate Class Distribution**: {0: 36, 1: 98, 2: 88}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Candidate economically outperforms, but statistical significance (McNemar) is weak or not applicable.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
