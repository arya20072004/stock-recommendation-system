# Candidate Evaluation Report: SUNPHARMA.NS

## 1. Identity
- **Ticker**: SUNPHARMA.NS
- **Active Model Hash**: `28b648b70b23`
- **Candidate Model Hash**: `7cc81d14ebf0`
- **Evaluation Timestamp**: 2026-08-20T13:09:40.748815+00:00

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
| Actionable Precision | 0.3721 | 0.0000 | -0.3721 |
| Actionable Recall | 0.1103 | 0.0000 | -0.1103 |
| Actionable Count | 43 | 0 | -43 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: NOT APPLICABLE
- **Test Statistic**: None
- **p-value**: None
- **Actionable Sample Size (Intersection)**: 0
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: -6.45%
- **Candidate Return**: 0.00%
- **Delta**: 6.45%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 40, 1: 133, 2: 48}
- **Candidate Class Distribution**: {0: 57, 1: 125, 2: 39}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate produced ZERO actionable predictions.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
