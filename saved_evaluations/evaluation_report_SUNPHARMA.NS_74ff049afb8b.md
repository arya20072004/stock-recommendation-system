# Candidate Evaluation Report: SUNPHARMA.NS

## 1. Identity
- **Ticker**: SUNPHARMA.NS
- **Active Model Hash**: `28b648b70b23`
- **Candidate Model Hash**: `74ff049afb8b`
- **Evaluation Timestamp**: 2026-08-21T09:38:47.420525+00:00

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
| Actionable Precision | 0.3721 | 0.3797 | 0.0077 |
| Actionable Recall | 0.1103 | 0.2069 | 0.0966 |
| Actionable Count | 43 | 79 | 36 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.5
- **p-value**: 0.47950012218695337
- **Actionable Sample Size (Intersection)**: 31
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: -6.45%
- **Candidate Return**: 23.57%
- **Delta**: 30.02%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 40, 1: 133, 2: 48}
- **Candidate Class Distribution**: {0: 38, 1: 93, 2: 90}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Candidate economically outperforms, but statistical significance (McNemar) is weak or not applicable.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
