PHASE = 2B-MB

TASK = CROSS_STORE_SPLIT_BRAIN_RECOVERY_CORRECTIVE_IMPLEMENTATION

IMPLEMENTATION_STATUS = PASS

FILES_MODIFIED = src/ml/model_registry.py, src/ml/history.py

RECOVERY_MECHANISM = STARTUP_RECONCILIATION

AUTHORITATIVE_STATE = MONGODB_ACTIVE

STATE_A_HANDLED = PASS
STATE_B_HANDLED = PASS
STATE_C_HANDLED = PASS
STATE_D_HANDLING = IMPOSSIBLE_STATE_NO_OP

AUTOMATIC_RECOVERY = YES

RECOVERY_IDEMPOTENT = YES

STALE_REPAIR_PROTECTION = PASS

ACTIVE_IDENTITY_VALIDATION = PASS

CANONICAL_PIPELINE_VALIDATION = PASS

ARTIFACT_VALIDATION = PASS

FILESYSTEM_ATOMIC_REPLACEMENT = PASS

POST_REPAIR_VERIFICATION = PASS

RECOVERY_FAILURE_FAILS_CLOSED = PASS

PRODUCTION_GATE_MODIFIED = NO

PRODUCTION_GATE_REMAINS_STRICT = YES

PROMOTION_LOGIC_MODIFIED = NO

MONGODB_ACTIVE_STATE_MODIFIED = NO

CANDIDATE_STATE_MODIFIED = NO

MODEL_ARTIFACTS_MODIFIED = NO

FEATURE_ARTIFACTS_MODIFIED = NO

NEGATIVE_TESTS_PASSED = 17
POSITIVE_TESTS_PASSED = 1

MONGODB_WRITES = 0
PRODUCTION_MANIFEST_WRITES = 0
MODEL_ARTIFACT_WRITES = 0
FEATURE_ARTIFACT_WRITES = 0
PROMOTIONS_EXECUTED = 0
ROLLBACKS_EXECUTED = 0
RETRAINING_EXECUTED = 0
FEATURE_REGENERATION_EXECUTED = 0
PREDICTIONS_EXECUTED = 0
PRODUCTION_PIPELINE_EXECUTED = 0

FINAL_RESULT = PASS

============================================================
1. Exact files modified.
============================================================
- `src/ml/model_registry.py`: Hardened `sync_manifest()` to include lock management, exact State A/C NO-OP checks, pipeline canonical identity validation, and artifact hash validation. Added `reconcile_all_manifests(db)` to iterate over all 51 tickers.
- `src/ml/history.py`: Added the `reconcile_all_manifests(db)` call at the pre-inference boundary, immediately before `_verify_production_readiness(db)` is executed.
- `scratch/test_cross_store_recovery.py`: Contains 17 negative/positive idempotency test scenarios using isolated mocks.
- `scratch/test_production_gate_regression.py`: Proves `_verify_production_readiness` blocks on STATE B, `reconcile_all_manifests` repairs it, and the gate subsequently passes.

============================================================
2. Exact startup/reconciliation boundary selected.
============================================================
Reconciliation is invoked inside `generate_and_persist_predictions()` immediately before `_verify_production_readiness(db)`. If it fails to reconcile any manifest, it strictly raises a `RuntimeError` to block inference, leaving the production gate unchanged.

============================================================
3. Exact current sync_manifest() behavior before and after.
============================================================
BEFORE:
Read ACTIVE record -> blindly overwrite manifest via `update_manifest_atomically()`. Did not check if it was already correct. Did not validate canonical pipeline hash. Lacked intrinsic `os.replace` lock protection when called externally.

AFTER:
Read ACTIVE record -> read current filesystem manifest -> if they exactly match, NO-OP return `True`. If mismatch -> acquire model lock (or reuse parent lock) -> re-read ACTIVE record -> validate canonical pipeline version/hash -> validate bundle artifacts -> rewrite temporary manifest -> `os.replace()` -> verify replaced manifest matches MongoDB exactly.

============================================================
4. How STATE B is detected.
============================================================
STATE B is detected inherently by comparing the identity of the current filesystem manifest with the authoritative MongoDB ACTIVE record. A mismatch triggers a repair.

============================================================
5. How STATE B is repaired.
============================================================
Once a mismatch is detected, the full MongoDB ACTIVE identity is extracted, subjected to artifact and pipeline validation, written to a `.tmp` file, and `os.replace()` is used to atomically overwrite the active filesystem manifest. 

============================================================
6. Why MongoDB ACTIVE is authoritative.
============================================================
MongoDB implements true single-document transactions. The `promote_model()` function successfully wrapped candidate activation inside `session.start_transaction()`, cementing MongoDB as the source of truth that successfully committed prior to the crash/failure.

============================================================
7. How stale repair is prevented.
============================================================
We strictly validate `feature_pipeline_version` and `feature_pipeline_hash` against the canonical repository resolver `get_feature_pipeline_hash("v1")`. The repair is aborted if the ACTIVE record itself attempts to reference a legacy state. Also, post-repair verification confirms the written manifest perfectly mirrors the MongoDB ACTIVE record.

============================================================
8. Exact artifact/hash validation performed.
============================================================
`validate_bundle(ticker, version, model_hash, feature_hash)` is invoked to hash both the joblib model artifact and the features JSON artifact on disk, ensuring they actually exist and match the hashes prescribed by the MongoDB record.

============================================================
9. How os.replace() failure behaves.
============================================================
Caught by a try/except block. Logs an error and returns `False`. The reconciliation failure bubbles up to `history.py` and throws a `RuntimeError`, blocking inference. No state is compromised, keeping the system fail-closed.

============================================================
10. How repeated reconciliation behaves.
============================================================
Idempotent. The first call detects a mismatch and overwrites the manifest (State B -> C). Subsequent calls verify the new manifest matches the MongoDB ACTIVE record precisely and return `True` immediately (NO-OP).

============================================================
11. How concurrency is protected.
============================================================
`sync_manifest` accepts an optional `owner_id`. If omitted, it automatically negotiates a short-lived expiration lock via `db.model_locks`. This perfectly interoperates with `promote_model()` which passes its own `owner_id`.

============================================================
12. Test results for all 17 scenarios.
============================================================
Passed flawlessly using `unittest`. Included successful tests for STATE A NO-OP, STATE B repair, missing manifests, incorrect pipeline versions, lock acquisition, etc.

============================================================
13. Production gate regression results.
============================================================
Regression test confirmed `_verify_production_readiness` successfully blocks on mixed state prior to reconciliation, and clears the block strictly post-reconciliation.

============================================================
14. Confirmation that promotion logic was untouched.
============================================================
`scripts/select_canonical_candidates.py` and `scripts/promote_canonical_candidates.py` were entirely untouched.

============================================================
15. Confirmation that production state was untouched.
============================================================
0 MongoDB Writes. 0 Filesystem changes against live production artifacts. All changes isolated in mock test suites or code adjustments.

============================================================
16. Any remaining architectural limitations.
============================================================
Because filesystem lock resolution relies on `db.model_locks`, a process crash during `sync_manifest` while a lock is held may delay manual/automatic retries up to 120 seconds until the lock expires. This is standard and acceptable.
