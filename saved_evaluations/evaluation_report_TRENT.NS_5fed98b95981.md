# Candidate Evaluation Report: TRENT.NS

## 1. Identity
- **Ticker**: TRENT.NS
- **Active Model Hash**: `72bb31086173`
- **Candidate Model Hash**: `5fed98b95981`
- **Evaluation Timestamp**: 2026-08-21T09:38:48.043061+00:00

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
| Actionable Precision | 0.3261 | 0.3077 | -0.0184 |
| Actionable Recall | 0.0915 | 0.2195 | 0.1280 |
| Actionable Count | 46 | 117 | 71 |

## 5. Statistical Evidence (McNemar's Test)
- **Applicability**: APPLICABLE
- **Test Statistic**: 0.16666666666666666
- **p-value**: 0.6830913983096086
- **Actionable Sample Size (Intersection)**: 34
*(Note: Temporal dependence in observations limits strict p-value interpretation)*

## 6. Economic Evidence (Simulated Actionable Cumulative Return)
- **Active Return**: 45.65%
- **Candidate Return**: -204.61%
- **Delta**: -250.26%
- **Transaction Costs**: NOT MODELED

## 7. Robustness
- **Active Class Distribution**: {0: 27, 1: 98, 2: 96}
- **Candidate Class Distribution**: {0: 7, 1: 59, 2: 155}

## 8. Final Decision
### **Verdict**: FAIL
**Explanation**: Candidate strictly underperforms mathematically and economically.

> [!WARNING]
> This is an evidence report. It does NOT automatically promote the candidate.
