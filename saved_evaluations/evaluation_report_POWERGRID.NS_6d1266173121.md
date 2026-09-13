# Candidate Evaluation Report: POWERGRID.NS

## 1. Identity
- **Ticker**: POWERGRID.NS
- **Active Model Hash**: `a4338ed0ae4a`
- **Candidate Model Hash**: `6d1266173121`
- **Evaluation Timestamp**: 2026-08-20T13:09:40.423698+00:00

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
| Actionable Precision | 0.2909 | 0.3000 | 0.0091 |
| Actionable Recall | 0.1250 | 0.1641 | 0.0391 |
| Actionable Count | 55 | 70 | 15 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.0
- **p-value**: 1.0
- **Actionable Sample Size (Intersection)**: 38
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: -19.97%
- **Candidate Return**: -26.00%
- **Delta**: -6.03%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 85, 1: 113, 2: 23}
- **Candidate Class Distribution**: {0: 73, 1: 109, 2: 39}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Mixed results between precision and economic returns.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
