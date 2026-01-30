# Stage Execution State Machine Formal Specification

**Status**: Formal Spec v1.3 (Finalization Gate Refinement)
**Author**: Claude Agent
**Date**: January 17, 2026

### Implementation Notes (v1.3)
- **CAS**: Uses state-value comparison (`WHERE state = ?`), not version counters
- **Audit columns**: Normalized (`exit_code`, `termination_cause`, etc.) not JSON blob
- **Lease table**: `daemon_leases.lease_name` (not `key`)
- **v1.3 Changes**:
  - Finalization gate excludes `canceled` runs (reason already captured in cancel event)
  - `finalize_run` now emits `USER_FINALIZE` state machine event
  - `completed_with_warnings` flag preserved when transitioning to `completed`
- **v1.2 Changes**:
  - Renamed `finalizing` → `post_run` (infrastructure output collection)
  - Added `awaiting_user_finalization` state (user must call `finalize_run`)
  - Added `USER_FINALIZE` event for user-invoked finalization
  - `completed` now requires explicit user finalization

### Clean Slate Design (NO Backwards Compatibility)

**IMPORTANT**: This state machine is a clean-slate redesign with ZERO backwards compatibility requirements.

**Rationale**: Goldfish is an MCP server. The AI agent consuming it:
- Only sees MCP tool names, parameters, and responses
- Never accesses the database directly
- Has no dependency on legacy column names or values

**Implications**:
1. **Legacy `status` column**: Deprecated. All code MUST use `state` column exclusively.
2. **No dual-write**: `transition()` does NOT maintain legacy columns.
3. **Migration-only**: Existing runs are migrated ONCE to the new schema.
4. **MCP tools**: Updated to return `state` in responses (not `status`).
5. **STATE.md**: Updated to read from `state` column.

**Migration Path**: Run `migrate_stage_runs()` once to convert existing data. After migration, the `status` column is ignored and can be dropped in a future schema cleanup.

## 1. Overview
This document defines the formal operational semantics of the Goldfish Stage Execution State Machine. It serves as the authoritative reference for state management, event processing, and audit persistence.

### 1.1 Objectives
- Establish a single source of truth for execution states.
- Eliminate race conditions through Compare-And-Swap (CAS) semantics.
- Provide a rigorous audit trail of all state transitions.
- Ensure deterministic handling of infrastructure failures and timeouts.

---

## 2. State Model

### 2.1 Top-Level States
The execution lifecycle is governed by the following mutually exclusive states:

| Category | State | Description |
| :--- | :--- | :--- |
| **Active** | `preparing` | Pre-execution: versioning, validation, input resolution. |
| | `building` | Docker image construction (local or remote). |
| | `launching` | Container or VM instance provisioning. |
| | `running` | Execution of user-defined stage code. |
| | `post_run` | Infrastructure wrap-up: output sync, metrics, logs, cleanup. |
| | `awaiting_user_finalization` | Infrastructure done; awaiting user to call `finalize_run`. |
| **Terminal**| `completed` | User has finalized the run with ML outcome judgment. |
| | `failed` | Failure in build, launch, execution, or critical post-run phases. |
| | `terminated`| Infrastructure failure (preemption, crash, timeout). |
| | `canceled` | User-initiated termination. |
| | `unknown` | Fallback for ambiguous legacy or migration cases. |

### 2.1.1 State Lifecycle Summary
```
PREPARING → BUILDING → LAUNCHING → RUNNING → POST_RUN → AWAITING_USER_FINALIZATION → COMPLETED
                                      ↓           ↓                ↑
                                   FAILED      FAILED      (user: finalize_run)
```

**Key distinction**:
- `post_run`: Automatic infrastructure phase (output sync, logs, metrics, SVS review)
- `awaiting_user_finalization`: Requires user action to record ML outcome
- `completed`: User has committed their judgment (success/partial/miss)

### 2.2 Termination Causes
For runs entering the `terminated` state, the specific cause is captured in the `termination_cause` attribute:
- `preempted`: Instance reclaimed by cloud provider.
- `crashed`: Unexpected instance death or OOM.
- `orphaned`: Loss of instance tracking (no evidence of existence).
- `timeout`: Exceeded state-specific or total runtime thresholds.
- `ai_stopped`: Stop requested by autonomous agent/SVS.
- `manual`: Direct user intervention via MCP tool.

### 2.3 Progress Phases
Phases are observability metadata nested within states. They do not drive transition logic but provide granular visibility.

- **`preparing`**: `gcs_check`, `versioning`, `pipeline_load`, `svs_preflight`, `config_load`, `input_resolve`, `pre_run_review`.
- **`building`**: `image_check`, `docker_build`.
- **`launching`**: `instance_create`, `instance_provisioning`, `instance_staging`.
- **`running`**: `container_init`, `code_execution`.
- **`post_run`**: `output_sync`, `output_recording`, `log_fetch`, `metrics_collection`, `post_run_review`, `cleanup`.
- **`awaiting_user_finalization`**: No phases (waiting for user action).

### 2.4 Phase Updates (Within-State)
Phase updates are **within-state** observability changes and MUST NOT be treated as state transitions.

**Requirements:**
- Phase updates MUST use the same CAS guard as state transitions (`WHERE id = ? AND state = ?`).
- Phase updates MUST update `phase` and `phase_updated_at`, and MUST NOT update `state_entered_at`.
- Phase updates MUST be recorded in `stage_state_transitions` using the pseudo-event `phase_update` with `from_state == to_state == expected_state`.
- If the CAS guard fails (state changed), the phase update MUST be rejected (no partial updates).

---

## 3. Event Model

### 3.1 Transition Events
Events trigger state changes. Every transition must be associated with exactly one event.

- **Lifecycle**: `BUILD_START`, `BUILD_OK`, `BUILD_FAIL`, `LAUNCH_OK`, `LAUNCH_FAIL`, `POST_RUN_OK`, `POST_RUN_FAIL`.
- **Exit Signals**:
    - `EXIT_SUCCESS`: Process returned exit code 0.
    - `EXIT_FAILURE`: Process returned non-zero exit code.
    - `EXIT_MISSING`: Process terminated without an exit code (requires instance death verification).
- **User Actions**:
    - `USER_FINALIZE`: User called `finalize_run` to record ML outcome.
    - `USER_CANCEL`: External cancellation request.
- **Control & Faults**:
    - `PREPARE_FAIL`: Logic error during preparation.
    - `SVS_BLOCK`: Security/Safety review blocked execution.
    - `AI_STOP`: During-run SVS requested stop (via `stop_requested` file).
    - `INSTANCE_LOST`: Unexpected backend instance disappearance.
    - `TIMEOUT`: State duration limit exceeded.

### 3.2 Audit-Only Pseudo-Events
These pseudo-events are recorded in the audit trail but do not trigger state machine transitions:
- `run_start`: Initial entry into the state machine.
- `phase_update`: Within-state change of the `phase` metadata.

### 3.3 Event Context
Every event is accompanied by an `EventContext` containing:
- **Canonical Timestamp**: Used for audit and `state_entered_at`.
- **Source**: Identifier for the emitter (`mcp_tool`, `executor`, `daemon`, `container`, `migration`).
- **Exit Context**: `exit_code`, `exit_code_exists` (Boolean).
- **Termination Context**: `termination_cause`, `instance_confirmed_dead` (Boolean).
- **Error Context**: `error_message`.
- **GCS Context**: `gcs_error` (Boolean), `gcs_outage_started` (Timestamp).
- **Post-Run Context**: `critical` (Boolean), `critical_phases_done` (Boolean).
- **SVS Context**: `svs_review_id` (FK to `svs_reviews.id`) for `SVS_BLOCK` and `AI_STOP` events.

**Note**: User finalization data (`ml_outcome`, notes) is NOT part of `EventContext`. It is passed directly to `finalize_run()` and stored in `run_results`. The `USER_FINALIZE` event context only requires `timestamp` and `source`.

**Context field totality for guarded transitions:**
- Guards MUST be evaluated using explicit boolean checks (e.g., `is True` / `is False`), not truthiness.
- For events whose transitions require a guarded field, the emitter MUST set that field explicitly:
    - `EXIT_MISSING` requires `instance_confirmed_dead is True`.
    - `POST_RUN_FAIL` requires `critical is True` or `critical is False` (one must apply).
    - `TIMEOUT` in `post_run` requires `critical_phases_done is True` or `critical_phases_done is False` (one must apply).
- If a required guarded field is missing/unknown, the transition MUST be rejected (treated as guard failure).

---

## 4. Transition Logic

### 4.1 Transition Table
Transitions are defined by a `(FromState, Event, Guard) -> ToState` mapping. Guards are Boolean predicates evaluated against the `EventContext`.

| From State | Event | To State | Guard Condition |
| :--- | :--- | :--- | :--- |
| `preparing` | `BUILD_START` | `building` | None |
| `preparing` | `PREPARE_FAIL` | `failed` | None |
| `preparing` | `SVS_BLOCK` | `failed` | None |
| `preparing` | `INSTANCE_LOST`| `terminated` | None |
| `preparing` | `TIMEOUT` | `terminated` | None (Cause: `timeout`) |
| `preparing` | `USER_CANCEL` | `canceled` | None |
| `building` | `BUILD_OK` | `launching` | None |
| `building` | `BUILD_FAIL` | `failed` | None |
| `building` | `INSTANCE_LOST`| `terminated` | None |
| `building` | `TIMEOUT` | `terminated` | None (Cause: `timeout`) |
| `building` | `USER_CANCEL` | `canceled` | None |
| `launching` | `LAUNCH_OK` | `running` | None |
| `launching` | `LAUNCH_FAIL` | `failed` | None |
| `launching` | `INSTANCE_LOST`| `terminated` | None |
| `launching` | `TIMEOUT` | `terminated` | None (Cause: `timeout`) |
| `launching` | `USER_CANCEL` | `canceled` | None |
| `running` | `EXIT_SUCCESS` | `post_run` | None |
| `running` | `EXIT_FAILURE` | `failed` | None |
| `running` | `EXIT_MISSING` | `terminated` | `instance_confirmed_dead` is True |
| `running` | `INSTANCE_LOST`| `terminated` | None |
| `running` | `TIMEOUT` | `terminated` | None (Cause: `timeout`) |
| `running` | `USER_CANCEL` | `canceled` | None |
| `running` | `AI_STOP` | `terminated` | None (Cause: `ai_stopped`) |
| `post_run` | `POST_RUN_OK` | `awaiting_user_finalization` | None |
| `post_run` | `POST_RUN_FAIL`| `failed` | `critical` is True |
| `post_run` | `POST_RUN_FAIL`| `awaiting_user_finalization` | `critical` is False |
| `post_run` | `INSTANCE_LOST`| `terminated` | None |
| `post_run` | `TIMEOUT` | `awaiting_user_finalization` | `critical_phases_done` is True |
| `post_run` | `TIMEOUT` | `failed` | `critical_phases_done` is False |
| `post_run` | `USER_CANCEL` | `canceled` | None |
| `post_run` | `AI_STOP` | `terminated` | None (Cause: `ai_stopped`) |
| `awaiting_user_finalization` | `USER_FINALIZE` | `completed` | None |
| `awaiting_user_finalization` | `USER_CANCEL` | `canceled` | None |
| `unknown` | `TIMEOUT` | `terminated` | None |

### 4.2 State Entry Phases
When a state is entered without an explicit phase in the context, a default phase is assigned:
- `preparing` -> `gcs_check`
- `building` -> `image_check`
- `launching` -> `instance_create`
- `running` -> `container_init`
- `post_run` -> `output_sync`
- `awaiting_user_finalization` -> None (no phases)

### 4.3 Awaiting User Finalization with Warnings
The `completed_with_warnings` flag is set to True when entering `awaiting_user_finalization` in the following scenarios:
1. `POST_RUN_FAIL` event in `post_run` state with `critical` context as False.
2. `TIMEOUT` event in `post_run` state with `critical_phases_done` context as True.

The flag is preserved when transitioning from `awaiting_user_finalization` to `completed` via `USER_FINALIZE`.

### 4.4 Initial State and Audit Entry
New stage runs MUST enter the state machine in `preparing` with phase `gcs_check`.

On row creation:
- `stage_runs.state` MUST be set to `preparing`.
- `stage_runs.phase` MUST be set to `gcs_check`.
- `stage_runs.state_entered_at` and `stage_runs.phase_updated_at` MUST be set to the creation timestamp.
- An audit pseudo-event `run_start` MUST be inserted into `stage_state_transitions` with `from_state='none'` and `to_state='preparing'`.

---

## 5. Consistency and Atomicity

### 5.1 Compare-And-Swap (CAS) Semantics
Every state update MUST use a CAS mechanism:
1. Current `state` is read within a transaction.
2. The transition is validated against the Transition Table.
3. The update is performed ONLY if the `state` in the database matches the state read in Step 1.

### 5.2 Transaction Integrity
To ensure consistency, the following operations MUST happen within a single atomic database transaction:
- Validation of the transition.
- Update of the `stage_runs` table (state, phase, timestamps, etc.).
- Insertion of a record into the `stage_state_transitions` audit table.

### 5.3 Idempotency
Transitions are idempotent if the current state already matches the target state and the guard conditions for the specific event context are satisfied.

### 5.4 Leader Election
To prevent duplicate event emission and CAS contention, daemons MUST use a leader election mechanism based on SQLite row-level leases (`daemon_leases` table). Only one daemon may hold the active lease at any time.

---

## 6. Operational Specifications

### 6.1 Exit Code Detection
To prevent false failures, exit code detection must distinguish between:
- **Success/Failure**: File exists, code retrieved.
- **GCS Unavailability**: Infrastructure error, status is unknown (Retry).
- **Genuine Absence**: File is missing (requires instance death verification to emit `EXIT_MISSING`).

**Prolonged GCS outage escalation (anti-stuck-run rule):**
- If exit code retrieval fails due to GCS unavailability, the daemon MUST track outage duration using `stage_runs.gcs_outage_started`:
    - First observed GCS error: set `gcs_outage_started` if not already set.
    - On recovery (exit code readable): clear `gcs_outage_started`.
- If GCS remains unavailable for >1 hour AND the backend instance is confirmed dead, the daemon SHOULD emit `EXIT_MISSING` with:
    - `termination_cause=orphaned`
    - `gcs_error=True`
    - `gcs_outage_started` preserved in context for audit
This prevents runs from being stuck indefinitely due to prolonged storage outages.

### 6.2 Timeout Management
- **`state_entered_at`**: Updated only on state transitions; used for timeout calculation.
- **Investigation Period**: Runs in `unknown` state are auto-terminated after a 24-hour investigation period.
- **Post-Run Timeout**: Default duration is 30 minutes.
- **User Finalization Timeout**: None (runs can await user finalization indefinitely).

### 6.3 Post-Run Criticality
- **Critical Phases**: `output_sync`, `output_recording`. Failure results in `failed` state.
- **Non-Critical Phases**: `log_fetch`, `metrics_collection`, `post_run_review`, `cleanup`. Failure results in `awaiting_user_finalization` with warnings.

### 6.4 Finalization Gate
New runs in a workspace are blocked if there are existing runs with terminal `infra_outcome` that have not been finalized.

**Blocked outcomes** (require user finalization):
- `completed` - Successful infrastructure completion
- `preempted` - Cloud instance preemption
- `crashed` - Infrastructure crash or timeout

**NOT blocked** (finalization not required):
- `canceled` - User already provided reason via `cancel()` call

**Rationale**: Forces users to review and commit to outcomes before starting new experiments, ensuring a clean audit trail and preventing experiment sprawl. The `canceled` state is excluded because the `cancel()` operation already captures the user's reason, making additional finalization redundant.

**Implementation**: Check `run_results.infra_outcome IN ('completed', 'preempted', 'crashed') AND results_status != 'finalized'`.

### 6.5 finalize_run State Machine Integration
The `finalize_run` MCP tool emits the `USER_FINALIZE` event to the state machine when the run is in `awaiting_user_finalization` state. This ensures:
- The state transitions atomically from `awaiting_user_finalization` to `completed`
- The `completed_with_warnings` flag is preserved through the transition
- The audit trail captures the finalization event

---

## 7. Data Model

### 7.1 Database Schema: `stage_runs` (Modified)
- `state`: Enum (preparing, building, launching, running, post_run, awaiting_user_finalization, completed, failed, terminated, canceled, unknown).
- `phase`: Current sub-phase metadata.
- `termination_cause`: Enum (preempted, crashed, orphaned, timeout, ai_stopped, manual).
- `state_entered_at`: ISO timestamp of state entry.
- `phase_updated_at`: ISO timestamp of last phase change.
- `completed_with_warnings`: Integer (Boolean flag).
- `error`: Latest error message for failure/termination visibility.
- `output_sync_done`, `output_recording_done`: Post-run progress flags.
- `gcs_outage_started`: ISO timestamp for tracking prolonged GCS downtime.

**Note**: ML outcomes (`ml_outcome`, `finalization_notes`) are stored in `run_results`, not `stage_runs`. This separation ensures `stage_runs` tracks infrastructure state while `run_results` tracks experiment outcomes.

### 7.2 Database Schema: `stage_state_transitions` (Audit)
- `id`: Primary key.
- `stage_run_id`: Foreign key to `stage_runs`.
- `from_state`, `event`, `to_state`: Transition details.
- `phase`, `termination_cause`, `exit_code`, `exit_code_exists`, `error_message`: Context snapshots.
- `svs_review_id`: Foreign key to `svs_reviews.id` (for `SVS_BLOCK` and `AI_STOP` events).
- `source`: Emitter identifier.
- `created_at`: Audit timestamp.

**Note**: The audit trail captures state transitions. ML outcomes are persisted in `run_results.ml_outcome` and `run_results.results_final` (JSON with notes) when `finalize_run` is called.

### 7.3 Database Schema: `run_results` (ML Outcomes)
- `stage_run_id`: Foreign key to `stage_runs`.
- `record_id`: Foreign key to `experiment_records`.
- `results_status`: Enum (missing, auto, finalized).
- `infra_outcome`: Enum (completed, preempted, crashed, canceled, unknown).
- `ml_outcome`: Enum (success, partial, miss, unknown). Set by `finalize_run`.
- `results_auto`: JSON (immutable, auto-extracted from metrics).
- `results_final`: JSON (authoritative, set by `finalize_run`, includes notes).
- `results_spec`: JSON (expected results criteria).
- `comparison`: JSON (vs_previous, vs_best analysis).
- `finalized_by`, `finalized_at`: Audit fields for finalization.

### 7.4 Audit Retention Policy
- Retain all transitions from the last 90 days.
- For runs older than 90 days, retain at most the 100 most recent transitions per `stage_run_id`.

---

## 8. Migration Heuristics
Existing runs are mapped to the new state model based on:
1. **Status Mapping**: Terminal statuses map directly.
2. **Infrastructure Failure Detection**: Scan `error` text for preemption/OOM keywords to set `terminated` and `termination_cause`.
3. **Orphan Detection**: Verify instance existence for runs stuck in "running".
4. **Investigation Queue**: Unverifiable runs are placed in `unknown`.
5. **Drain Mode**: Migration must be performed after enabling drain mode to prevent new run starts.

## 9. Outcome Derivation

### 9.1 Infrastructure Outcome (`infra_outcome`)
The `infra_outcome` in `run_results` is derived from the terminal state machine state:
- `completed` -> `completed` (Infrastructure ran successfully, user finalized).
- `awaiting_user_finalization` -> `completed` (Infrastructure ran successfully, awaiting user).
- `failed` -> `crashed` (Infrastructure failure - build, launch, execution, or critical post-run).
- `canceled` -> `canceled`.
- `terminated` -> Map `termination_cause` as follows:
    - `preempted` -> `preempted`
    - `crashed` -> `crashed`
    - `timeout` -> `crashed`
    - `orphaned` -> `unknown`
    - `ai_stopped` -> `canceled`
    - `manual` -> `canceled`
- `unknown` -> `unknown`.

### 9.2 ML Outcome (`ml_outcome`)
The `ml_outcome` is set by the user when calling `finalize_run`:
- `success`: Experiment achieved its goals.
- `partial`: Experiment showed promise but didn't fully meet goals.
- `miss`: Experiment failed to produce useful results (bad loss, divergence, etc.).
- `unknown`: Outcome cannot be determined.

**Key distinction**: `infra_outcome` tracks whether infrastructure executed correctly; `ml_outcome` tracks whether the ML experiment produced good results. A run can have `infra_outcome=completed` but `ml_outcome=miss` (code ran fine, but model didn't learn).
