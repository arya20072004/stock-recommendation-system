# Candidate Evaluation Report: HEROMOTOCO.NS

## 1. Identity
- **Ticker**: HEROMOTOCO.NS
- **Active Model Hash**: `d4c009de83c6`
- **Candidate Model Hash**: `ded3e7478ea1`
- **Evaluation Timestamp**: 2026-08-21T09:38:45.730372+00:00

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
| Actionable Precision | 0.0000 | 0.2911 | 0.2911 |
| Actionable Recall | 0.0000 | 0.1704 | 0.1704 |
| Actionable Count | 0 | 79 | 79 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: NOT APPLICABLE
- **Test Statistic**: None
- **p-value**: None
- **Actionable Sample Size (Intersection)**: 0
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 0.00%
- **Candidate Return**: 53.60%
- **Delta**: 53.60%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 68, 1: 70, 2: 83}
- **Candidate Class Distribution**: {0: 42, 1: 83, 2: 96}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Candidate economically outperforms, but statistical significance (McNemar) is weak or not applicable.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
