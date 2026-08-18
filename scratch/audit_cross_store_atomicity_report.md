PHASE = 2B-MB

AUDIT = CROSS_STORE_PROMOTION_ATOMICITY_SPLIT_BRAIN_RECOVERY_AUDIT

EXPECTED_TICKERS = 51

CROSS_STORE_ATOMICITY = PARTIAL
SPLIT_BRAIN_STATE_B_REACHABLE = YES
SPLIT_BRAIN_STATE_D_REACHABLE = NO

MONGODB_TRANSACTION = PASS
FILESYSTEM_ATOMIC_REPLACEMENT = PASS

AUTOMATIC_RECOVERY = NO
MANUAL_RECOVERY_REQUIRED = YES
RECOVERY_IDEMPOTENT = YES

IDEMPOTENCY = PASS
PARTIAL_RESUME = PASS
CRASH_SAFE = FAIL
CONCURRENT_PROMOTION_SAFE = PASS

STATE_A_REACHABLE = YES
STATE_B_REACHABLE = YES
STATE_C_REACHABLE = YES
STATE_D_REACHABLE = NO

STATE_B_DETECTION = DETECTED_BY_PRODUCTION_GATE
STATE_B_RECOVERY = IDEMPOTENT_RETRY_OR_SYNC_MANIFEST

STATE_D_DETECTION = N/A
STATE_D_RECOVERY = N/A

PRODUCTION_GATE_PROTECTS_MIXED_STATE = YES
PRODUCTION_LOADER_PROTECTS_MIXED_STATE = YES

MONGODB_FILESYSTEM_CONSISTENCY = FAIL

SPLIT_BRAIN_RISK = HIGH

CRITICAL_FINDINGS = [
    "A crash after MongoDB commit but before os.replace() leaves the system in Split-Brain State B.",
    "State B causes the Production Gate to raise a RuntimeError, completely blocking all batch inferences until resolved."
]
HIGH_FINDINGS = [
    "No automatic background reconciliation exists; recovery requires an orchestrator retry or manual invocation of sync_manifest().",
    "Cross-store operations inherently lack true atomicity; they operate in a two-phase manner without an automatic rollback on phase two failure."
]
MEDIUM_FINDINGS = [
    "Idempotency is technically sound (rerunning the same promotion plan repairs the filesystem), but relies on the executor knowing to retry."
]

IMPLEMENTATION_REQUIRED = YES

MONGODB_WRITES = 0
FILESYSTEM_WRITES = 0
MANIFEST_WRITES = 0
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
1. Exact promotion state machine.
============================================================
- STATE A (Initial): MongoDB = OLD ACTIVE, Filesystem = OLD ACTIVE.
- STATE B (Split-Brain): MongoDB = NEW ACTIVE, Filesystem = OLD ACTIVE.
- STATE C (Final Success): MongoDB = NEW ACTIVE, Filesystem = NEW ACTIVE.
- STATE D (Impossible): MongoDB = OLD ACTIVE, Filesystem = NEW ACTIVE.

============================================================
2. Exact MongoDB -> filesystem ordering.
============================================================
1. Lock is acquired via db.model_locks.
2. Temporary filesystem manifest .tmp is written.
3. MongoDB transaction starts.
4. MongoDB updates OLD ACTIVE to RETIRED.
5. MongoDB updates NEW CANDIDATE to ACTIVE.
6. MongoDB transaction commits.
7. Lock ownership is verified.
8. os.replace replaces the filesystem manifest.
9. Post-write verification confirms sync.
10. Lock is deleted.

============================================================
3. Exact failure window where split-brain occurs.
============================================================
The split-brain failure window opens precisely after step 6 (MongoDB transaction commits) and closes exactly after step 8 (os.replace succeeds). If the process is terminated, OOM killed, or encounters a filesystem IO error during this window, the system is left in STATE B.

============================================================
4. Exact code paths responsible.
============================================================
In src/ml/model_registry.py -> promote_model():
Lines 179-191: The session.start_transaction() block commits the MongoDB changes.
Lines 209-213: The os.replace(temp_path, manifest_path) executes the filesystem change.
Between these two blocks, the system is permanently advanced in MongoDB but not yet on the filesystem.

============================================================
5. Whether STATE B is reachable.
============================================================
YES. If the process is killed after the MongoDB transaction commits but before os.replace() runs, or if os.replace() fails due to permissions or lock errors on Windows.

============================================================
6. Whether STATE D is reachable.
============================================================
NO. The filesystem manifest is never replaced unless the MongoDB transaction has already successfully committed. An exception during the MongoDB transaction aborts it and returns False before os.replace is ever called.

============================================================
7. Exact behavior after os.replace() failure.
============================================================
An exception is caught, logged ("Manifest replacement failed"), and the function returns False. The MongoDB state is already NEW ACTIVE. The filesystem is left as OLD ACTIVE (State B).

============================================================
8. Exact behavior after process crash.
============================================================
If the process crashes after MongoDB commit but before os.replace(), the script dies. No catch blocks run. The database is NEW ACTIVE, the filesystem is OLD ACTIVE (State B).

============================================================
9. Exact behavior on retry.
============================================================
If the exact same promotion is retried, promote_model() fetches the ACTIVE record, sees that the desired version is already ACTIVE, logs a message, calls sync_manifest(), and returns True. This perfectly resolves STATE B and completes the promotion.

============================================================
10. Exact behavior of sync_manifest().
============================================================
sync_manifest() reads the ACTIVE record from MongoDB. If none exists, it deletes the filesystem manifest. If it exists, it validates the artifact hashes, builds the manifest data exactly matching the MongoDB record, writes it to a .tmp file, and uses os.replace() to atomically update the active manifest.

============================================================
11. Whether recovery is automatic.
============================================================
NO. Recovery requires external invocation. Either the orchestrator must retry the promotion script, or an operator must run sync_manifest() manually. The production pipeline itself does not automatically self-heal upon detecting the split-brain.

============================================================
12. Whether recovery is idempotent.
============================================================
YES. sync_manifest() can be run safely any number of times. It always unconditionally overwrites the filesystem manifest with the authoritative MongoDB ACTIVE state.

============================================================
13. Whether production gate blocks mixed state.
============================================================
YES. _verify_production_readiness() in src/ml/history.py loads both the MongoDB ACTIVE records and the filesystem _active.json manifests for all 51 tickers. If there is any mismatch in model_version, model_hash, or feature_hash, it strictly raises a RuntimeError and aborts prediction generation for all tickers.

============================================================
14. Whether production loader blocks mixed state.
============================================================
YES. The loader relies entirely on the gate to block mixed states. If the gate were somehow bypassed, load_active_bundle strictly enforces hashes against the filesystem manifest, but the mismatch with MongoDB would be a severe architectural violation.

============================================================
15. Concurrency risks.
============================================================
Minimal to none. model_locks uses a 120-second expiration with an owner_id. A unique index on ticker with partialFilterExpression={"status": "ACTIVE"} physically prevents two ACTIVE records in MongoDB. The script verifies lock ownership immediately before os.replace(), eliminating race conditions between two concurrent executors.

============================================================
16. Crash-safety findings.
============================================================
The system fails-closed safely but destructively (it blocks downstream pipelines). It is not inherently crash-safe because it lacks a journaling or automatic reconciliation loop upon startup to resolve incomplete transactions.

============================================================
17. Required corrective architecture.
============================================================
Because true cross-store distributed transactions are natively unsupported between MongoDB and the OS filesystem, STATE B is fundamentally unavoidable during failure. 
The recommended corrective architecture is **Startup Reconciliation**.
The system should automatically detect and repair STATE B during the startup phase of downstream consumers (or via a dedicated reconciliation task) by unconditionally running sync_manifest() for any ticker where the filesystem manifest does not strictly match the MongoDB ACTIVE candidate. This avoids blocking production inference for a recoverable failure.
