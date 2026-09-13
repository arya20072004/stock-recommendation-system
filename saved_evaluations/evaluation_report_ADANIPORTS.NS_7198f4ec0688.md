# Candidate Evaluation Report: ADANIPORTS.NS

## 1. Identity
- **Ticker**: ADANIPORTS.NS
- **Active Model Hash**: `7e4d3751fab0`
- **Candidate Model Hash**: `7198f4ec0688`
- **Evaluation Timestamp**: 2026-08-21T09:38:42.185111+00:00

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
| Actionable Precision | 0.3619 | 0.4286 | 0.0667 |
| Actionable Recall | 0.2969 | 0.2812 | -0.0156 |
| Actionable Count | 105 | 84 | -21 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.25
- **p-value**: 0.6170750774519739
- **Actionable Sample Size (Intersection)**: 49
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 160.52%
- **Candidate Return**: 155.41%
- **Delta**: -5.11%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 99, 1: 58, 2: 64}
- **Candidate Class Distribution**: {0: 105, 1: 83, 2: 33}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Mixed results between precision and economic returns.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
