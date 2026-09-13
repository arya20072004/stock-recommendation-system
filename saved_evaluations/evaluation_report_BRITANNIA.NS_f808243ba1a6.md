# Candidate Evaluation Report: BRITANNIA.NS

## 1. Identity
- **Ticker**: BRITANNIA.NS
- **Active Model Hash**: `a33a7dc36c06`
- **Candidate Model Hash**: `f808243ba1a6`
- **Evaluation Timestamp**: 2026-08-21T09:38:42.963081+00:00

## 2. Dataset
- **Evaluation Dataset Version**: `v1_b4c8b5075e70`
- **Evaluation Dataset Hash**: `b4c8b5075e7059c10a9406fc06b2f03b764650394f2e355a7d39ed8e2f1cd420`
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
| Actionable Precision | 0.2687 | 0.2857 | 0.0171 |
| Actionable Recall | 0.1452 | 0.0968 | -0.0484 |
| Actionable Count | 67 | 42 | -25 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: NOT APPLICABLE
- **Test Statistic**: None
- **p-value**: None
- **Actionable Sample Size (Intersection)**: 20
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: -20.30%
- **Candidate Return**: -12.53%
- **Delta**: 7.77%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 30, 1: 103, 2: 88}
- **Candidate Class Distribution**: {0: 68, 1: 53, 2: 100}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Candidate economically outperforms, but statistical significance (McNemar) is weak or not applicable.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
