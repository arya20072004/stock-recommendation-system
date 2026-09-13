# Candidate Evaluation Report: ETERNAL.NS

## 1. Identity
- **Ticker**: ETERNAL.NS
- **Active Model Hash**: `34153fbfa3c3`
- **Candidate Model Hash**: `15db26672eee`
- **Evaluation Timestamp**: 2026-08-20T18:33:46.173716+00:00

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
| Actionable Precision | 0.9516 | 0.8966 | -0.0551 |
| Actionable Recall | 0.8741 | 0.5778 | -0.2963 |
| Actionable Count | 124 | 87 | -37 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 1.3333333333333333
- **p-value**: 0.24821307898992026
- **Actionable Sample Size (Intersection)**: 85
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 870.26%
- **Candidate Return**: 621.13%
- **Delta**: -249.13%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 66, 1: 88, 2: 67}
- **Candidate Class Distribution**: {0: 71, 1: 99, 2: 51}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate strictly underperforms mathematically and economically.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
