# Candidate Evaluation Report: GRASIM.NS

## 1. Identity
- **Ticker**: GRASIM.NS
- **Active Model Hash**: `6888c529893d`
- **Candidate Model Hash**: `8cd00b21b7c2`
- **Evaluation Timestamp**: 2026-08-21T09:38:44.349627+00:00

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
| Actionable Precision | 0.2788 | 0.2100 | -0.0688 |
| Actionable Recall | 0.2180 | 0.1579 | -0.0602 |
| Actionable Count | 104 | 100 | -4 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 2.25
- **p-value**: 0.13361440253771584
- **Actionable Sample Size (Intersection)**: 68
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 1.98%
- **Candidate Return**: -72.34%
- **Delta**: -74.32%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 68, 1: 74, 2: 79}
- **Candidate Class Distribution**: {0: 76, 1: 72, 2: 73}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate strictly underperforms mathematically and economically.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
