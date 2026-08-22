# Evaluation Gate Policy Decision

## Status

**FROZEN**

## Decision Date

2026-08-22

## Governing Commit

`2484d81397971752c53ea6c40bd1a90f68ee0251`

## Decision Scope

This document formally defines the pass/fail/inconclusive policy for candidate model evaluation.

It governs the evaluation gate only.

It does **not** authorize candidate promotion, active-model replacement, candidate retirement, or production deployment.

---

# 1. Decision

The evaluation gate SHALL use **Policy B**.

A candidate SHALL receive:

```text
FAIL
```

when:

```text
candidate_actionable == 0
```

OR:

```text
candidate_precision < active_precision
```

OR:

```text
candidate_return < active_return
```

Otherwise, the candidate may proceed to the remaining evaluation criteria and evidence review.

---

# 2. Formal Gate Definition

Let:

- `A_c` = candidate actionable prediction count
- `P_c` = candidate actionable precision
- `P_a` = active actionable precision
- `R_c` = candidate simulated actionable cumulative return
- `R_a` = active simulated actionable cumulative return

The mandatory failure condition is:

```text
(A_c == 0)
OR
(P_c < P_a)
OR
(R_c < R_a)
```

Therefore:

```text
FAIL ⇔
    (A_c == 0)
    OR
    (P_c < P_a)
    OR
    (R_c < R_a)
```

A candidate MUST NOT be classified as `PASS` merely because it improves one mandatory metric.

---

# 3. Interpretation

The evaluation gate requires preservation of **both mandatory performance dimensions**:

1. Actionable precision
2. Economic return

Both dimensions are mandatory because the candidate is being evaluated as a production model rather than solely as a classification model.

Consequently:

| Candidate Precision | Candidate Return | Gate Result |
|---|---|---|
| Improves | Improves | Eligible for further PASS evaluation |
| Improves | Degrades | **FAIL** |
| Degrades | Improves | **FAIL** |
| Degrades | Degrades | **FAIL** |
| Equal | Improves | Eligible for further PASS evaluation |
| Improves | Equal | Eligible for further PASS evaluation |
| Equal | Equal | Eligible for further evaluation |
| Any | Any | **FAIL** if actionable count = 0 |

Equality does not constitute degradation.

Only strict `<` comparisons trigger metric-based failure.

---

# 4. Why Policy B Was Selected

The project specification describes the evaluation gate as requiring both precision and return.

Under that requirement, degradation of either mandatory metric represents failure of a required dimension.

Policy A would instead permit the following situations:

```text
Precision ↓
Return ↑
```

or:

```text
Precision ↑
Return ↓
```

to become `INCONCLUSIVE`.

That interpretation does not enforce both mandatory dimensions.

Policy B therefore provides the stricter and more direct implementation of the stated requirement:

```text
Both precision and return are mandatory.
Degradation of either mandatory metric is a failure.
```

This decision is now project policy and MUST NOT be inferred from implementation behavior.

---

# 5. Zero-Actionable Rule

A candidate producing zero actionable predictions SHALL receive:

```text
FAIL
```

regardless of its calculated precision or economic return.

The rationale is that a model producing no actionable signals cannot satisfy the operational purpose of the recommendation system.

This rule takes precedence over metric comparison.

---

# 6. INCONCLUSIVE Classification

`INCONCLUSIVE` SHALL NOT be used as an alternative to a mandatory metric failure.

Under this policy, `INCONCLUSIVE` is reserved for situations where the gate cannot establish a definitive PASS/FAIL conclusion because of insufficient or non-applicable evidence, provided that no mandatory failure condition has already been triggered.

Examples may include:

- insufficient statistical evidence where statistical evidence is required for a later decision;
- insufficient applicable paired observations;
- evidence that cannot be evaluated under a defined criterion;
- other explicitly documented evidence limitations.

An `INCONCLUSIVE` result MUST NOT override:

```text
candidate_actionable == 0
```

or:

```text
candidate_precision < active_precision
```

or:

```text
candidate_return < active_return
```

---

# 7. PASS Semantics

Satisfying the mandatory gate does **not** by itself constitute automatic promotion.

A candidate that does not trigger a mandatory failure condition is only **eligible for further evaluation**.

The evaluator remains an evidence generator.

Promotion remains a separate controlled decision.

Therefore:

```text
Evaluation Gate
      ↓
Evidence
      ↓
Human / governed promotion decision
```

and NOT:

```text
Evaluation Gate
      ↓
Automatic Promotion
```

---

# 8. Promotion Boundary

This policy does not authorize:

- `promote_model()`
- active-model replacement
- candidate deletion
- candidate retirement
- registry status mutation
- production deployment

The evaluator MUST remain read-only with respect to model lifecycle state.

---

# 9. Specification Precedence

This document is the authoritative policy specification for the evaluation gate.

Implementation code MUST conform to this policy.

If implementation behavior conflicts with this document:

```text
Specification > Existing Implementation
```

The correct response is to audit and minimally modify the implementation.

The existing implementation MUST NOT be treated as evidence of the intended policy.

---

# 10. Implementation Constraint

Following this decision, implementation work SHALL proceed in the following order:

```text
1. Freeze this policy
2. Audit evaluate_candidate.py against this policy
3. Identify all specification/implementation gaps
4. Change the minimum necessary code
5. Add/update regression tests
6. Run the dedicated evaluation-gate test suite
7. Run production preflight
8. Perform the clean 48-candidate evaluation
9. Reconcile results
10. Analyze evidence
11. Make promotion decisions separately
```

No unrelated evaluator refactoring is authorized as part of this policy correction.

---

# 11. Baseline Before Implementation Changes

At the time this policy was frozen:

```text
Governing commit:
2484d813

Dedicated evaluation-gate regression suite:
27 passed

Command:
python -m pytest -q tests/test_candidate_selection.py tests/test_dataset_selection.py tests/test_evaluation_gate.py
```

The baseline establishes that the deterministic candidate selection, frozen evaluation dataset selection, and existing evaluation-gate regression tests pass before the policy-driven implementation correction.

The full repository test suite is not considered green at this checkpoint because unrelated repository-level collection failures remain separately identified.

---

# 12. Policy Change Rule

Any future change to the evaluation-gate policy MUST be made through an explicit policy decision record or an amended version of this document.

A code change alone MUST NOT redefine the evaluation policy.

Any future policy amendment MUST document:

- previous policy;
- proposed policy;
- reason for change;
- affected implementation;
- affected tests;
- approval/decision date;
- governing commit.

---

# 13. Final Frozen Rule

The evaluation gate SHALL treat either mandatory metric degradation as failure:

```text
FAIL if:

    candidate_actionable == 0

OR

    candidate_precision < active_precision

OR

    candidate_return < active_return
```

This is **Policy B**.

This policy is frozen as of **2026-08-22**.
