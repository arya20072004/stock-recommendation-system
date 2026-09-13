# Candidate Evaluation Report: CIPLA.NS

## 1. Identity
- **Ticker**: CIPLA.NS
- **Active Model Hash**: `5d2ac87ac446`
- **Candidate Model Hash**: `6acd8ddc7044`
- **Evaluation Timestamp**: 2026-08-20T13:09:38.807176+00:00

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
| Actionable Precision | 0.1818 | 0.1000 | -0.0818 |
| Actionable Recall | 0.0164 | 0.0082 | -0.0082 |
| Actionable Count | 11 | 10 | -1 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: NOT APPLICABLE
- **Test Statistic**: None
- **p-value**: None
- **Actionable Sample Size (Intersection)**: 6
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 12.39%
- **Candidate Return**: 8.35%
- **Delta**: -4.04%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 40, 1: 152, 2: 29}
- **Candidate Class Distribution**: {0: 37, 1: 169, 2: 15}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate strictly underperforms mathematically and economically.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
