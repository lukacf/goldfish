# State Machine Implementation Plan (TDD + Ralph Loop)

This plan is a **phase-divided checkbox checklist** for implementing the Stage Execution State Machine spec (`docs/state-machine-spec.md`) using **TDD**. It assumes development is performed by AI agents, and at the end of **each phase** a **Ralph loop** runs 5 sub-agents that must all approve before advancing.

Guiding rules:
- **TDD everywhere**: Tests are written before implementation (unit → integration → e2e).
- **No shortcuts**: The final state must implement the full spec, including all guards, timeouts, and edge cases.
- **Ralph loop gate**: A phase completes only when all 5 sub-agents approve. **If any sub-agent rejects, the entire 5-agent review must be re-run** after fixes, and this repeats until all approve.
- **Incremental migration**: Old code continues working until new code is verified.

Reference: `scripts/validate_state_machine.py` contains the formal state machine definition (39 transitions, 10 states, 18 events) that implementation must match.

---

## Phase 1 — Foundation & Test Harness

Goal: Establish the core types, test infrastructure, and formal validation required for TDD implementation.

- [ ] Create `src/goldfish/state_machine/` package with `__init__.py`.
- [ ] Define `StageState` enum matching spec (PREPARING, BUILDING, LAUNCHING, RUNNING, FINALIZING, COMPLETED, FAILED, TERMINATED, CANCELED, UNKNOWN).
- [ ] Define `StageEvent` enum matching spec (18 events: BUILD_START, BUILD_OK, BUILD_FAIL, etc.).
- [ ] Define `TerminationCause` enum (PREEMPTED, CRASHED, ORPHANED, TIMEOUT, AI_STOPPED, MANUAL).
- [ ] Define `ProgressPhase` enum for all sub-phases (gcs_check, versioning, docker_build, etc.).
- [ ] Define `EventContext` dataclass with all fields from spec (exit_code, exit_code_exists, termination_cause, instance_confirmed_dead, critical, critical_phases_done, gcs_error, gcs_outage_started, phase, error_message, timestamp, source).
- [ ] Define `TransitionResult` dataclass (success, new_state, reason, details).
- [ ] Define `TransitionDef` dataclass (from_state, event, to_state, guard).
- [ ] Create `TRANSITIONS` list with all 39 transitions from spec (matching `scripts/validate_state_machine.py`).
- [ ] Create test directory `tests/unit/state_machine/` with `conftest.py`.
- [ ] Add test fixtures for creating runs in specific states.
- [ ] Verify `scripts/validate_state_machine.py` passes (validates transition table correctness).
- [ ] **Ralph loop review**: 5 subagents validate type definitions match spec, test harness is ready, and transition table is complete.

---

## Phase 2 — State Machine Core Logic

Goal: Implement `find_transition()` and `transition()` with CAS semantics, guards, and idempotency.

### 2.1 — Unit Tests (Write First)

- [ ] `test_find_transition.py`: Test `find_transition()` returns correct TransitionDef for all 39 transitions.
- [ ] `test_find_transition.py`: Test guard iteration (FINALIZE_FAIL with critical=True vs critical=False).
- [ ] `test_find_transition.py`: Test returns None for invalid (state, event) pairs.
- [ ] `test_guards.py`: Test all 5 guards with explicit `is True`/`is False` context values.
- [ ] `test_guards.py`: Test guards reject `None` context values (don't treat None as False).
- [ ] `test_transition.py`: Test happy path transitions (PREPARING→BUILDING→LAUNCHING→RUNNING→FINALIZING→COMPLETED).
- [ ] `test_transition.py`: Test all failure paths reach terminal states.
- [ ] `test_transition.py`: Test CAS semantics - concurrent transitions, only one wins.
- [ ] `test_transition.py`: Test idempotency - already in target state returns success.
- [ ] `test_transition.py`: Test guard-aware idempotency (FINALIZE_FAIL critical=True not idempotent in COMPLETED).
- [ ] `test_transition.py`: Test invalid transitions return failure with reason.
- [ ] `test_audit.py`: Test every transition creates audit record in `stage_state_transitions`.
- [ ] `test_audit.py`: Test audit record contains all EventContext fields.

### 2.2 — Implementation

- [ ] Implement `find_transition(from_state, event, context)` that iterates ALL matches until guard passes.
- [ ] Implement `transition(db, run_id, event, context)` with:
  - [ ] State read INSIDE transaction (TOCTOU fix).
  - [ ] CAS update (`WHERE id = ? AND state = ?`).
  - [ ] Guard-aware idempotency check.
  - [ ] Audit insert in same transaction.
  - [ ] `completed_with_warnings` flag for FINALIZE_FAIL(critical=False) and TIMEOUT(critical_phases_done=True).
  - [ ] `termination_cause` defaulting for TIMEOUT→TERMINATED.
  - [ ] Phase update using `STATE_ENTRY_PHASES` for new states.
- [ ] Implement `update_phase(db, run_id, expected_state, new_phase, timestamp)` with CAS guard.
- [ ] Run all unit tests - must pass before proceeding.
- [ ] **Ralph loop review**: 5 subagents validate CAS semantics, guard logic, idempotency, and audit trail completeness.

---

## Phase 3 — Database Schema & Migration

Goal: Add new columns, create audit table, and migrate existing data without breaking running system.

### 3.1 — Schema Changes (Non-Breaking)

- [ ] Write migration SQL for `stage_runs` new columns:
  - [ ] `state TEXT` with CHECK constraint for valid states.
  - [ ] `phase TEXT` for progress phases.
  - [ ] `termination_cause TEXT` with CHECK constraint.
  - [ ] `state_entered_at TEXT` for timeout calculations.
  - [ ] `phase_updated_at TEXT` for observability.
  - [ ] `completed_with_warnings INTEGER DEFAULT 0`.
  - [ ] `error TEXT` for error messages.
  - [ ] `output_sync_done INTEGER DEFAULT 0` for finalization tracking.
  - [ ] `output_recording_done INTEGER DEFAULT 0` for finalization tracking.
  - [ ] `gcs_outage_started TEXT` for GCS outage escalation.
- [ ] Write migration SQL for `stage_state_transitions` table (audit trail).
- [ ] Write migration SQL for partial index `idx_stage_runs_state` on active states.
- [ ] Add migration to `db/migrations/` following existing pattern.
- [ ] Test migration applies cleanly on empty database.
- [ ] Test migration applies cleanly on database with existing runs.

### 3.2 — Data Migration Script

- [ ] `test_migration.py`: Test `determine_migration_state()` for all legacy status values.
- [ ] `test_migration.py`: Test preemption detection from error messages.
- [ ] `test_migration.py`: Test timeout detection from error messages.
- [ ] `test_migration.py`: Test orphan detection via backend API.
- [ ] `test_migration.py`: Test UNKNOWN assignment for ambiguous cases.
- [ ] `test_migration.py`: Test migration rollback restores original data.
- [ ] `test_migration.py`: Test batch migration with failures continues and reports.
- [ ] Implement `migrate_stage_runs()` Python script:
  - [ ] Create backup table before migration.
  - [ ] Track progress in `migration_progress` table.
  - [ ] Migrate in batches with `BEGIN IMMEDIATE`.
  - [ ] Check backend (GCE/Docker) for orphan detection.
  - [ ] Log all migration decisions for audit.
- [ ] Implement `rollback_migration()` using table recreation (SQLite <3.35 compatible).
- [ ] Implement `check_orphan_status()` that queries GCE/Docker APIs.
- [ ] Implement `safe_migration()` with drain mode for active runs.
- [ ] **Ralph loop review**: 5 subagents validate schema correctness, migration safety, rollback capability, and orphan detection logic.

---

## Phase 4 — Exit Code Bug Fix & Event Emission Layer

Goal: Fix the critical exit code bug and create the event emission layer that replaces direct status updates.

### 4.1 — Exit Code Bug Fix

- [ ] `test_exit_code.py`: Test `ExitCodeResult` distinguishes "file missing" from "exit code 1".
- [ ] `test_exit_code.py`: Test GCS unavailable returns `gcs_error=True`, not exit code 1.
- [ ] `test_exit_code.py`: Test exit code 0 file returns success.
- [ ] `test_exit_code.py`: Test exit code non-zero returns failure with code.
- [ ] Define `ExitCodeResult` dataclass (exists: bool, code: int | None, gcs_error: bool, error: str | None).
- [ ] Fix `daemon.py:_get_exit_code()` to return `ExitCodeResult` instead of defaulting to 1.
- [ ] Fix `cloud/adapters/gcp/gce_launcher.py:_get_exit_code()` to return `ExitCodeResult`.
- [ ] Update all callers to handle `ExitCodeResult` properly.

### 4.2 — Event Emission Functions

- [ ] `test_event_emission.py`: Test `determine_exit_event()` for all exit code scenarios.
- [ ] `test_event_emission.py`: Test GCS outage tracking and >1h escalation.
- [ ] `test_event_emission.py`: Test `verify_instance_stopped()` for GCE and local backends.
- [ ] `test_event_emission.py`: Test `detect_termination_cause()` returns PREEMPTED, CRASHED, or ORPHANED.
- [ ] Implement `determine_exit_event(run, exit_result)` that returns (StageEvent, EventContext) or None.
- [ ] Implement `verify_instance_stopped(run_id, backend)` for GCE and Docker.
- [ ] Implement `detect_termination_cause(run_id, backend)` that checks preemption via GCE operations API.
- [ ] Implement GCS outage tracking (`get_gcs_outage_started`, `set_gcs_outage_started`, `clear_gcs_outage_started`).
- [ ] **Ralph loop review**: 5 subagents validate exit code bug is fixed, event emission logic matches spec, and GCS outage handling is correct.

---

## Phase 5 — Daemon Rewrite

Goal: Replace the if/then/else mess in daemon.py with event-driven architecture using the state machine.

### 5.1 — Leader Election

- [ ] `test_leader_election.py`: Test `try_acquire_lease()` grants lease to first caller.
- [ ] `test_leader_election.py`: Test concurrent lease attempts - only one wins.
- [ ] `test_leader_election.py`: Test expired lease can be acquired by new holder.
- [ ] `test_leader_election.py`: Test lease renewal by same holder.
- [ ] `test_leader_election.py`: Test `release_lease()` allows immediate acquisition.
- [ ] Create `daemon_leases` table for leader election.
- [ ] Implement `DaemonLeaderElection` class with `BEGIN IMMEDIATE` for race prevention.
- [ ] Implement `try_acquire_lease(holder_id)` with UPSERT pattern.
- [ ] Implement `release_lease(holder_id)` for graceful shutdown.

### 5.2 — New Daemon Structure

- [ ] `test_daemon.py`: Test `_determine_event()` returns correct event for each state/backend combination.
- [ ] `test_daemon.py`: Test timeout detection for each state (PREPARING, BUILDING, LAUNCHING, RUNNING, FINALIZING).
- [ ] `test_daemon.py`: Test UNKNOWN auto-cleanup after 24h.
- [ ] `test_daemon.py`: Test poll loop acquires lease before processing.
- [ ] `test_daemon.py`: Test poll loop emits events to state machine.
- [ ] Implement `StageDaemon` class with state machine integration:
  - [ ] `poll_active_runs()` - main loop that determines and emits events.
  - [ ] `_determine_event(run)` - pure logic, no state mutations.
  - [ ] `_determine_gce_event(run)` - GCE-specific event detection.
  - [ ] `_determine_local_event(run)` - Docker-specific event detection.
  - [ ] `_check_timeout(run)` - state-specific timeout checking.
- [ ] Implement `get_active_runs()` query using partial index.
- [ ] Integrate leader election into daemon run loop.
- [ ] Remove old if/then/else code paths from daemon.py (or create new daemon_v2.py).
- [ ] **Ralph loop review**: 5 subagents validate daemon emits correct events, leader election prevents duplicates, and all timeout scenarios are handled.

---

## Phase 6 — Cancel Flow & Finalization

Goal: Update cancel to use state machine and integrate finalization tracking.

### 6.1 — Cancel Flow

- [ ] `test_cancel.py`: Test `cancel()` uses state machine transition.
- [ ] `test_cancel.py`: Test `cancel()` from all active states goes to CANCELED.
- [ ] `test_cancel.py`: Test `cancel()` triggers backend cleanup (best-effort).
- [ ] Rewrite `cancel()` MCP tool to use `state_machine.transition(run_id, USER_CANCEL, ctx)`.
- [ ] Implement backend cleanup helper `_cleanup_backend(run_id)`.

### 6.2 — Finalization Tracking

- [ ] `test_finalization.py`: Test `FinalizationTracker` persists progress to database.
- [ ] `test_finalization.py`: Test `critical_phases_done` derivation from output_sync_done and output_recording_done.
- [ ] `test_finalization.py`: Test TIMEOUT in FINALIZING uses critical_phases_done for outcome.
- [ ] Implement `FinalizationTracker` class with `mark_output_sync_done()` and `mark_output_recording_done()`.
- [ ] Integrate tracker into `_finalize_stage_run()`.

### 6.3 — TypedDict Updates & Review

- [ ] Update `StageRunRow` TypedDict to include new columns.
- [ ] **Ralph loop review**: 5 subagents validate cancel flow uses state machine and finalization tracking prevents data loss.

---

## Phase 7 — Integration & Stage Executor Updates

Goal: Update stage executor to emit events instead of direct status updates, integrate with state machine.

### 7.1 — Stage Executor Event Emission

- [ ] `test_stage_executor.py`: Test `run_stage()` emits BUILD_START when starting build.
- [ ] `test_stage_executor.py`: Test `run_stage()` emits BUILD_OK/BUILD_FAIL based on build result.
- [ ] `test_stage_executor.py`: Test `run_stage()` emits LAUNCH_OK/LAUNCH_FAIL based on launch result.
- [ ] `test_stage_executor.py`: Test `refresh_status_once()` returns (event, context) instead of mutating state.
- [ ] `test_stage_executor.py`: Test phase updates during execution use `update_phase()`.
- [ ] Update `run_stage()` to emit events at each stage transition.
- [ ] Rewrite `refresh_status_once()` to return event + context (caller handles transition).
- [ ] Update `_finalize_stage_run()` to emit FINALIZE_OK/FINALIZE_FAIL events.
- [ ] Add phase updates throughout execution (gcs_check, versioning, pipeline_load, etc.).

### 7.2 — Create Stage Run Updates

- [ ] `test_create_stage_run.py`: Test new runs start in PREPARING state with gcs_check phase.
- [ ] `test_create_stage_run.py`: Test initial transition audit record with from_state='none'.
- [ ] Update `create_stage_run()` to set initial state/phase and record audit entry.

### 7.3 — SVS Integration

- [ ] `test_svs.py`: Test SVS_BLOCK event from PREPARING goes to FAILED.
- [ ] `test_svs.py`: Test AI_STOPPED via metadata bus triggers graceful shutdown.
- [ ] Integrate SVS pre-run review with PREPARE_FAIL/SVS_BLOCK event emission.
- [ ] Implement AI_STOPPED command handler in container metadata syncer.
- [ ] **Ralph loop review**: 5 subagents validate stage executor emits correct events, phase updates work, and SVS integration is complete.

---

## Phase 8 — End-to-End Testing & Verification

Goal: Comprehensive end-to-end tests, performance validation, and final cleanup.

### 8.1 — Integration Tests

- [ ] `tests/integration/test_state_machine_e2e.py`: Full run lifecycle (PREPARING→...→COMPLETED).
- [ ] `tests/integration/test_state_machine_e2e.py`: Preemption scenario (RUNNING→EXIT_MISSING→TERMINATED).
- [ ] `tests/integration/test_state_machine_e2e.py`: Cancel during each active state.
- [ ] `tests/integration/test_state_machine_e2e.py`: Timeout in each active state.
- [ ] `tests/integration/test_state_machine_e2e.py`: Finalization failure (critical vs non-critical).
- [ ] `tests/integration/test_state_machine_e2e.py`: Concurrent transitions (CAS validation).
- [ ] `tests/integration/test_state_machine_e2e.py`: Migration script on production-like data.

### 8.2 — Deluxe (GCE) Tests

- [ ] `tests/e2e/deluxe/test_gce_state_machine.py`: Real GCE run with state machine.
- [ ] `tests/e2e/deluxe/test_gce_state_machine.py`: Spot preemption detection.
- [ ] `tests/e2e/deluxe/test_gce_state_machine.py`: Exit code retrieval from GCS.
- [ ] `tests/e2e/deluxe/test_gce_state_machine.py`: Daemon polling with real instances.

### 8.3 — Performance & Monitoring

- [ ] Verify `get_active_runs()` query uses partial index (EXPLAIN QUERY PLAN).
- [ ] Verify audit table growth is acceptable (retention policy if needed).
- [ ] Add metrics for transition latency, CAS failures, event emission rate.
- [ ] Add dashboard for state distribution and stuck run detection.

### 8.4 — Documentation & Cleanup

- [ ] Update CLAUDE.md with state machine patterns and event emission guidelines.
- [ ] Remove old `status` and `progress` columns from schema.
- [ ] Archive old daemon.py code (or delete if fully replaced).
- [ ] Update MCP tool documentation for new states.
- [ ] **Ralph loop review**: 5 subagents validate all tests pass, performance is acceptable, documentation is complete, and migration is safe for production.

---

## Verification Checklist (Final Gate)

Before declaring implementation complete, verify:

- [ ] `scripts/validate_state_machine.py` passes (all 11 checks).
- [ ] All unit tests pass (`make test`).
- [ ] All integration tests pass (`make test-integration`).
- [ ] Lint passes (`make lint`).
- [ ] Migration tested on backup of production database.
- [ ] Rollback procedure documented and tested.
- [ ] No ghost/zombie runs created in testing.
- [ ] Audit trail captures all state changes.
- [ ] Admin tools can recover from any stuck state.
- [ ] Exit code bug verified fixed (no false failures).
- [ ] CAS semantics prevent race conditions.
- [ ] Leader election prevents duplicate event emission.

---

## Appendix: Key Files to Modify/Create

| File | Action | Purpose |
|------|--------|---------|
| `src/goldfish/state_machine/__init__.py` | Create | State machine package |
| `src/goldfish/state_machine/types.py` | Create | StageState, StageEvent, EventContext, etc. |
| `src/goldfish/state_machine/transitions.py` | Create | TRANSITIONS table, find_transition() |
| `src/goldfish/state_machine/core.py` | Create | transition(), update_phase() |
| `src/goldfish/state_machine/migration.py` | Create | Migration script |
| `src/goldfish/daemon.py` | Rewrite | Event-driven daemon |
| `src/goldfish/jobs/stage_executor.py` | Modify | Emit events instead of direct updates |
| `src/goldfish/cloud/adapters/gcp/gce_launcher.py` | Modify | Fix _get_exit_code(), add preemption detection |
| `src/goldfish/db/schema.sql` | Modify | Add new columns and tables |
| `tests/unit/state_machine/` | Create | Unit test directory |
| `tests/integration/test_state_machine_e2e.py` | Create | Integration tests |

---

## Appendix: Transition Table Reference

```
Total: 39 transitions across 10 states

PREPARING (7 transitions):
  BUILD_START → BUILDING
  PREPARE_FAIL → FAILED
  SVS_BLOCK → FAILED
  INSTANCE_LOST → TERMINATED
  TIMEOUT → TERMINATED
  USER_CANCEL → CANCELED
  FORCE_TERMINATE → TERMINATED

BUILDING (6 transitions):
  BUILD_OK → LAUNCHING
  BUILD_FAIL → FAILED
  INSTANCE_LOST → TERMINATED
  TIMEOUT → TERMINATED
  USER_CANCEL → CANCELED
  FORCE_TERMINATE → TERMINATED

LAUNCHING (6 transitions):
  LAUNCH_OK → RUNNING
  LAUNCH_FAIL → FAILED
  INSTANCE_LOST → TERMINATED
  TIMEOUT → TERMINATED
  USER_CANCEL → CANCELED
  FORCE_TERMINATE → TERMINATED

RUNNING (7 transitions):
  EXIT_SUCCESS → FINALIZING
  EXIT_FAILURE → FAILED
  EXIT_MISSING → TERMINATED [guard: instance_confirmed_dead]
  INSTANCE_LOST → TERMINATED
  TIMEOUT → TERMINATED
  USER_CANCEL → CANCELED
  FORCE_TERMINATE → TERMINATED

FINALIZING (9 transitions):
  FINALIZE_OK → COMPLETED
  FINALIZE_FAIL → FAILED [guard: critical=True]
  FINALIZE_FAIL → COMPLETED [guard: critical=False]
  INSTANCE_LOST → TERMINATED
  TIMEOUT → COMPLETED [guard: critical_phases_done=True]
  TIMEOUT → FAILED [guard: critical_phases_done=False]
  USER_CANCEL → CANCELED
  FORCE_COMPLETE → COMPLETED
  FORCE_TERMINATE → TERMINATED

UNKNOWN (4 transitions):
  TIMEOUT → TERMINATED
  FORCE_TERMINATE → TERMINATED
  FORCE_COMPLETE → COMPLETED
  FORCE_FAIL → FAILED

TERMINAL STATES (0 transitions each):
  COMPLETED, FAILED, TERMINATED, CANCELED
```
