# Candidate Evaluation Report: DRREDDY.NS

## 1. Identity
- **Ticker**: DRREDDY.NS
- **Active Model Hash**: `67463cc7852f`
- **Candidate Model Hash**: `90fe2f479276`
- **Evaluation Timestamp**: 2026-08-20T13:09:38.961187+00:00

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
| Actionable Precision | 0.4048 | 0.4214 | 0.0167 |
| Actionable Recall | 0.3446 | 0.3986 | 0.0541 |
| Actionable Count | 126 | 140 | 14 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.25
- **p-value**: 0.6170750774519739
- **Actionable Sample Size (Intersection)**: 116
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 22.49%
- **Candidate Return**: 82.05%
- **Delta**: 59.56%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 58, 1: 77, 2: 86}
- **Candidate Class Distribution**: {0: 48, 1: 72, 2: 101}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Candidate economically outperforms, but statistical significance (McNemar) is weak or not applicable.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
