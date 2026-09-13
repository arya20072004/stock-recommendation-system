# Candidate Evaluation Report: ASIANPAINT.NS

## 1. Identity
- **Ticker**: ASIANPAINT.NS
- **Active Model Hash**: `ab9595fb8f6e`
- **Candidate Model Hash**: `58911f543d75`
- **Evaluation Timestamp**: 2026-08-20T13:09:38.245211+00:00

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
| Actionable Precision | 0.4286 | 0.4098 | -0.0187 |
| Actionable Recall | 0.2374 | 0.1799 | -0.0576 |
| Actionable Count | 77 | 61 | -16 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.0
- **p-value**: 1.0
- **Actionable Sample Size (Intersection)**: 52
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 82.85%
- **Candidate Return**: 61.37%
- **Delta**: -21.49%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 65, 1: 115, 2: 41}
- **Candidate Class Distribution**: {0: 73, 1: 113, 2: 35}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate strictly underperforms mathematically and economically.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
