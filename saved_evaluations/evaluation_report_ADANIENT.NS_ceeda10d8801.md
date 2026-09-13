# Candidate Evaluation Report: ADANIENT.NS

## 1. Identity
- **Ticker**: ADANIENT.NS
- **Active Model Hash**: `1be7345ec2e4`
- **Candidate Model Hash**: `ceeda10d8801`
- **Evaluation Timestamp**: 2026-08-20T13:09:38.060644+00:00

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
| Actionable Precision | 0.4388 | 0.4528 | 0.0141 |
| Actionable Recall | 0.2925 | 0.3265 | 0.0340 |
| Actionable Count | 98 | 106 | 8 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.5
- **p-value**: 0.47950012218695337
- **Actionable Sample Size (Intersection)**: 72
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 110.26%
- **Candidate Return**: 203.51%
- **Delta**: 93.26%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 73, 1: 83, 2: 65}
- **Candidate Class Distribution**: {0: 66, 1: 90, 2: 65}

## 8. Final Decision
### **Verdict**: INCONCLUSIVE
**Explanation**: Candidate economically outperforms, but statistical significance (McNemar) is weak or not applicable.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
