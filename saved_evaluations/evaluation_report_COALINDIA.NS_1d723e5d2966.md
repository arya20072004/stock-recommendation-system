# Candidate Evaluation Report: COALINDIA.NS

## 1. Identity
- **Ticker**: COALINDIA.NS
- **Active Model Hash**: `5915d86aa47e`
- **Candidate Model Hash**: `1d723e5d2966`
- **Evaluation Timestamp**: 2026-08-21T09:38:43.164024+00:00

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
| Actionable Precision | 0.3810 | 0.4106 | 0.0296 |
| Actionable Recall | 0.2963 | 0.3827 | 0.0864 |
| Actionable Count | 126 | 151 | 25 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 1.7777777777777777
- **p-value**: 0.18242243945173198
- **Actionable Sample Size (Intersection)**: 97
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: -5.06%
- **Candidate Return**: 86.67%
- **Delta**: 91.72%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 82, 1: 53, 2: 86}
- **Candidate Class Distribution**: {0: 73, 1: 28, 2: 120}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Candidate economically outperforms, but statistical significance (McNemar) is weak or not applicable.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
