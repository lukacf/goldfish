# Stage Execution State Machine Specification

> **Status**: Draft v3 (Post-Critical-Review)
> **Author**: Claude + Luka
> **Created**: 2025-01-15
> **Updated**: 2025-01-15

## Overview

This document specifies a state machine to replace the ad-hoc state management in Goldfish stage execution. The design incorporates feedback from critical review identifying race conditions, migration risks, and over-engineering concerns.

### Problems Being Solved

1. **Ghost/Zombie Runs**: ~70+ runs stuck in `status=running` for weeks with no cleanup mechanism
2. **False Exit Code Detection**: `_get_exit_code()` returns `1` when file doesn't exist, causing false failures
3. **Lost State Tracking**: Runs complete successfully but database state never updates
4. **State Desync**: `status` and `progress` columns managed independently, can get out of sync
5. **Scattered Updates**: 70+ places in code that read/write status with subtly different logic
6. **No Audit Trail**: No record of state transitions for debugging
7. **Race Conditions**: Daemon and MCP tools can finalize simultaneously with no CAS protection

### Design Principles

1. **Single Source of Truth**: State machine owns the state. No scattered updates.
2. **Event-Driven**: External systems emit events. State machine processes them.
3. **Explicit Transitions**: Every valid state change documented in transition table.
4. **CAS Semantics**: All transitions use compare-and-swap to prevent races.
5. **Fail Loudly**: Invalid transitions raise errors during development.
6. **Full Audit**: Every transition recorded with timestamp, event, and context.
7. **TDD**: Comprehensive test suite written before implementation.
8. **Incremental Migration**: Backwards compatibility layer during transition.

### Non-Goals (Deferred)

- **Auto-recovery from PREEMPTED/ORPHANED** - Future roadmap item
- **Intelligent timeout detection** - Separate problem; use sensible defaults for now
- **Daemon implementation** - Spec the interface; implement after state machine is solid

---

## State Model

### Top-Level States (Simplified)

Based on review feedback, we reduce terminal states from 7 to 5. The distinction between CRASHED/PREEMPTED/ORPHANED doesn't change user action (all require manual retry), so we capture specifics in `termination_cause` field instead.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           ACTIVE STATES                                 │
├─────────────────────────────────────────────────────────────────────────┤
│  PREPARING    │ Pre-execution: versioning, validation, input resolution │
│  BUILDING     │ Docker image build                                      │
│  LAUNCHING    │ Container/instance provisioning                         │
│  RUNNING      │ User code execution                                     │
│  FINALIZING   │ Output recording, metrics collection, SVS reviews       │
├─────────────────────────────────────────────────────────────────────────┤
│                          TERMINAL STATES                                │
├─────────────────────────────────────────────────────────────────────────┤
│  COMPLETED    │ Successful completion                                   │
│  FAILED       │ Explicit failure (build, code, validation, SVS block)   │
│  TERMINATED   │ Infrastructure death (preemption, crash, orphan, timeout)│
│  CANCELED     │ User-initiated cancellation                             │
│  UNKNOWN      │ Fallback for migration edge cases                       │
└─────────────────────────────────────────────────────────────────────────┘
```

### Termination Cause (for TERMINATED state)

```python
class TerminationCause(str, Enum):
    PREEMPTED = "preempted"       # Spot instance preempted (detected via GCE API)
    CRASHED = "crashed"           # Instance died without exit code
    ORPHANED = "orphaned"         # Lost track of instance (timeout, no evidence of run)
    TIMEOUT = "timeout"           # Exceeded configured timeout threshold
    AI_STOPPED = "ai_stopped"     # AI/SVS requested stop (trigger mechanism TBD)
    MANUAL = "manual"             # Admin force_terminate_run via MCP
```

### Progress Phase (Observability, Not State)

Sub-phases are **metadata for observability**, not true states with their own transition tables. They indicate "what's happening within this state" but don't affect transition logic.

```python
class ProgressPhase(str, Enum):
    # PREPARING phases
    GCS_CHECK = "gcs_check"
    VERSIONING = "versioning"
    PIPELINE_LOAD = "pipeline_load"
    SVS_PREFLIGHT = "svs_preflight"
    CONFIG_LOAD = "config_load"
    INPUT_RESOLVE = "input_resolve"
    PRE_RUN_REVIEW = "pre_run_review"

    # BUILDING phases
    IMAGE_CHECK = "image_check"
    DOCKER_BUILD = "docker_build"

    # LAUNCHING phases
    INSTANCE_CREATE = "instance_create"
    INSTANCE_PROVISIONING = "instance_provisioning"
    INSTANCE_STAGING = "instance_staging"

    # RUNNING phases
    CONTAINER_INIT = "container_init"
    CODE_EXECUTION = "code_execution"

    # FINALIZING phases
    OUTPUT_SYNC = "output_sync"
    OUTPUT_RECORDING = "output_recording"
    LOG_FETCH = "log_fetch"
    METRICS_COLLECTION = "metrics_collection"
    POST_RUN_REVIEW = "post_run_review"
    CLEANUP = "cleanup"
```

### Phase Updates (Within-State Progress)

Phase is observability metadata that changes **within** a state without triggering state transitions. To avoid the status/progress desync problem, phase updates use the same CAS pattern:

```python
def update_phase(run_id: str, expected_state: str, new_phase: ProgressPhase) -> bool:
    """Update phase within a state using CAS.

    Returns True if update succeeded, False if state changed underneath us.
    """
    with db._conn() as conn:
        result = conn.execute(
            """UPDATE stage_runs
               SET phase = ?, state_updated_at = ?
               WHERE id = ? AND state = ?""",
            (new_phase.value, now_iso(), run_id, expected_state)
        )
        if result.rowcount == 0:
            return False  # State changed, phase update rejected

        # Record phase change in audit (same transaction)
        conn.execute(
            """INSERT INTO stage_state_transitions
               (stage_run_id, from_state, event, to_state, phase, source, created_at)
               VALUES (?, ?, 'phase_update', ?, ?, ?, ?)""",
            (run_id, expected_state, expected_state, new_phase.value, 'internal', now_iso())
        )
        return True
```

**Key principle**: Phase updates are rejected if the state has changed. This prevents stale phase writes from overwriting meaningful state transitions.

---

## Event Model

### Events

```python
class StageEvent(str, Enum):
    # State transition events
    BUILD_START = "build_start"              # Preparation done, starting build
    BUILD_OK = "build_ok"                    # Build succeeded
    BUILD_FAIL = "build_fail"                # Build failed
    LAUNCH_OK = "launch_ok"                  # Instance/container confirmed running
    LAUNCH_FAIL = "launch_fail"              # Launch failed (quota, capacity, config)
    FINALIZE_OK = "finalize_ok"              # Finalization complete
    FINALIZE_FAIL = "finalize_fail"          # Finalization failed (context.critical determines target)

    # Exit events (CRITICAL: must distinguish these!)
    EXIT_SUCCESS = "exit_success"            # exit_code.txt exists, value=0
    EXIT_FAILURE = "exit_failure"            # exit_code.txt exists, value!=0
    EXIT_MISSING = "exit_missing"            # exit_code.txt does NOT exist AND instance confirmed dead

    # Failure events (explicit failures, not infrastructure death)
    PREPARE_FAIL = "prepare_fail"            # Pipeline parse, versioning, input resolution failed
    SVS_BLOCK = "svs_block"                  # SVS preflight or pre-run review blocked

    # Infrastructure death events
    INSTANCE_LOST = "instance_lost"          # Instance gone, termination_cause in context
    TIMEOUT = "timeout"                      # Exceeded configured timeout for current state

    # User events
    USER_CANCEL = "user_cancel"

    # Admin events
    FORCE_TERMINATE = "force_terminate"      # Admin override for stuck runs
    FORCE_COMPLETE = "force_complete"        # Admin override to mark complete
    FORCE_FAIL = "force_fail"                # Admin override to mark failed (from UNKNOWN)
```

**Removed events**: `PREPARE_COMPLETE` and `FINALIZE_START` were defined but never used in transitions. The state machine infers these from the transition itself (PREPARING→BUILDING means preparation complete).

### Event Context

```python
@dataclass
class EventContext:
    """Context attached to each event for audit and decision-making."""
    timestamp: datetime                      # Canonical time for this event (used in audit)
    source: str                              # 'mcp_tool', 'daemon', 'container', 'admin'

    # Exit context
    exit_code: int | None = None
    exit_code_exists: bool = False           # CRITICAL: distinguishes missing from failure

    # Termination context
    termination_cause: TerminationCause | None = None
    instance_confirmed_dead: bool = False    # For EXIT_MISSING: verified instance is not running

    # Error context
    error_message: str | None = None

    # Progress context
    phase: ProgressPhase | None = None

    # Finalization context
    critical: bool = False                   # For FINALIZE_FAIL: True → FAILED, False → COMPLETED

    # SVS context
    svs_decision: str | None = None          # 'approved', 'blocked', 'warning'
    svs_findings: list[dict] | None = None
```

**Timestamp usage**: `EventContext.timestamp` is the canonical time source. The `state_updated_at` column uses `context.timestamp.isoformat()`, not `now()`. This ensures audit trail ordering matches actual event sequence.

---

## Transition Table

### Core Transitions

```
From State    | Event              | To State    | Guard                    | Notes
--------------|--------------------| ------------|--------------------------|------------------
PREPARING     | BUILD_START        | BUILDING    |                          | Preparation complete
PREPARING     | PREPARE_FAIL       | FAILED      |                          | Pipeline/version/input error
PREPARING     | SVS_BLOCK          | FAILED      |                          | Preflight or pre-run blocked
PREPARING     | TIMEOUT            | TERMINATED  | cause=TIMEOUT            | Stuck in preparation
PREPARING     | USER_CANCEL        | CANCELED    |                          |
PREPARING     | FORCE_TERMINATE    | TERMINATED  |                          | Admin override
              |                    |             |                          |
BUILDING      | BUILD_OK           | LAUNCHING   |                          |
BUILDING      | BUILD_FAIL         | FAILED      |                          |
BUILDING      | TIMEOUT            | TERMINATED  | cause=TIMEOUT            | Build took too long
BUILDING      | USER_CANCEL        | CANCELED    |                          |
BUILDING      | FORCE_TERMINATE    | TERMINATED  |                          | Admin override
              |                    |             |                          |
LAUNCHING     | LAUNCH_OK          | RUNNING     |                          | Instance confirmed running
LAUNCHING     | LAUNCH_FAIL        | FAILED      |                          | Quota, capacity, config error
LAUNCHING     | INSTANCE_LOST      | TERMINATED  |                          | Preemption during startup
LAUNCHING     | TIMEOUT            | TERMINATED  | cause=TIMEOUT            | No capacity after timeout
LAUNCHING     | USER_CANCEL        | CANCELED    |                          |
LAUNCHING     | FORCE_TERMINATE    | TERMINATED  |                          | Admin override
              |                    |             |                          |
RUNNING       | EXIT_SUCCESS       | FINALIZING  |                          | exit_code.txt=0
RUNNING       | EXIT_FAILURE       | FAILED      |                          | exit_code.txt!=0
RUNNING       | EXIT_MISSING       | TERMINATED  | instance_confirmed_dead  | No exit code + instance dead
RUNNING       | INSTANCE_LOST      | TERMINATED  |                          | Preemption, crash, etc.
RUNNING       | TIMEOUT            | TERMINATED  | cause=TIMEOUT            | Exceeded max runtime
RUNNING       | USER_CANCEL        | CANCELED    |                          |
RUNNING       | FORCE_TERMINATE    | TERMINATED  |                          | Admin override
              |                    |             |                          |
FINALIZING    | FINALIZE_OK        | COMPLETED   |                          | All finalization done
FINALIZING    | FINALIZE_FAIL      | FAILED      | ctx.critical=True        | Critical step failed
FINALIZING    | FINALIZE_FAIL      | COMPLETED   | ctx.critical=False       | Non-critical, still complete
FINALIZING    | TIMEOUT            | COMPLETED   | critical_phases_done     | Timeout but outputs saved
FINALIZING    | TIMEOUT            | FAILED      | !critical_phases_done    | Timeout, outputs lost
FINALIZING    | USER_CANCEL        | CANCELED    |                          | Partial outputs possible
FINALIZING    | FORCE_COMPLETE     | COMPLETED   |                          | Admin override
FINALIZING    | FORCE_TERMINATE    | TERMINATED  |                          | Admin override
              |                    |             |                          |
UNKNOWN       | FORCE_TERMINATE    | TERMINATED  |                          | Admin cleanup
UNKNOWN       | FORCE_COMPLETE     | COMPLETED   |                          | Admin override (verified ok)
UNKNOWN       | FORCE_FAIL         | FAILED      |                          | Admin override (verified bad)
```

**UNKNOWN state**: Migration fallback only. Admin tools provide escape hatches. No automatic transitions into or out of UNKNOWN.

### Finalization Criticality

Not all finalization failures should cause FAILED:

| Phase | Critical? | On Failure |
|-------|-----------|------------|
| OUTPUT_SYNC | Yes | FAILED - user's work is lost |
| OUTPUT_RECORDING | Yes | FAILED - provenance broken |
| LOG_FETCH | No | COMPLETED with warning |
| METRICS_COLLECTION | No | COMPLETED with warning |
| POST_RUN_REVIEW | No | COMPLETED with warning |
| CLEANUP | No | COMPLETED with warning |

### FINALIZING Timeout

Add maximum FINALIZING duration (default: 30 minutes). If exceeded:
- If OUTPUT_SYNC and OUTPUT_RECORDING completed: COMPLETED with warning
- Otherwise: FAILED with "finalization timeout"

---

## CAS (Compare-And-Swap) Semantics

**All state transitions MUST use CAS to prevent race conditions.**

### The Problem

Without CAS:
1. User calls `cancel()`, state becomes CANCELED
2. Daemon poll (1 second later) finds exit_code=0
3. Daemon overwrites state to COMPLETED
4. User's cancel is silently undone

### The Solution

```python
def transition(run_id: str, event: StageEvent, context: EventContext) -> TransitionResult:
    """Atomically transition state using compare-and-swap.

    CRITICAL: State update AND audit insert happen in the SAME transaction.
    This ensures we never have state changes without audit or vice versa.
    """

    # 1. Read current state
    current = db.get_stage_run(run_id)
    if not current:
        return TransitionResult(success=False, reason="not_found")

    # 2. Find valid transition
    transition_def = find_transition(current["state"], event)
    if not transition_def:
        return TransitionResult(success=False, reason="invalid_transition",
            details=f"No transition from {current['state']} on {event}")

    # 3. Check guard
    if transition_def.guard and not transition_def.guard(context):
        return TransitionResult(success=False, reason="guard_failed")

    # 4. ATOMIC: CAS update + audit insert in same transaction
    with db._conn() as conn:
        # CAS update - only succeeds if state hasn't changed
        result = conn.execute(
            """UPDATE stage_runs
               SET state = ?, phase = ?, state_updated_at = ?,
                   termination_cause = ?, error = ?
               WHERE id = ? AND state = ?""",
            (transition_def.to_state, context.phase,
             context.timestamp.isoformat(),  # Use event timestamp, not now()
             context.termination_cause, context.error_message,
             run_id, current["state"])
        )
        if result.rowcount == 0:
            return TransitionResult(success=False, reason="state_changed")

        # Audit insert - SAME transaction, guaranteed atomic
        conn.execute(
            """INSERT INTO stage_state_transitions
               (stage_run_id, from_state, event, to_state, phase,
                termination_cause, exit_code, exit_code_exists,
                error_message, source, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, current["state"], event.value, transition_def.to_state,
             context.phase, context.termination_cause,
             context.exit_code, 1 if context.exit_code_exists else 0,
             context.error_message, context.source,
             context.timestamp.isoformat())
        )
        # Transaction commits here - both succeed or both fail

    return TransitionResult(success=True, new_state=transition_def.to_state)
```

**Atomicity guarantee**: If the process crashes between the UPDATE and INSERT, the transaction rolls back and neither change persists. This eliminates the "audit without state change" and "state change without audit" failure modes.

### Idempotency

If a transition fails due to `state_changed`, the caller should:
1. Re-read current state
2. If already in expected terminal state, treat as success
3. Otherwise, log warning and don't retry

---

## Exit Code Detection (Critical Fix)

### Current Bug

```python
# gce_launcher.py - BROKEN
def _get_exit_code(self, instance_name: str) -> int:
    # ... retry logic ...
    return 1  # Returns 1 when file doesn't exist!
```

### Fixed Implementation

```python
@dataclass
class ExitCodeResult:
    exists: bool
    code: int | None
    error: str | None = None

def get_exit_code(run_id: str) -> ExitCodeResult:
    """Get exit code, distinguishing 'file missing' from 'file contains 1'."""
    gcs_path = f"gs://{bucket}/runs/{run_id}/logs/exit_code.txt"

    for attempt in range(max_attempts):
        try:
            content = gsutil_cat(gcs_path)
            return ExitCodeResult(exists=True, code=int(content.strip()))
        except NotFoundError:
            if attempt < max_attempts - 1:
                time.sleep(retry_delay)  # GCS eventual consistency
                continue
            return ExitCodeResult(exists=False, code=None)
        except Exception as e:
            return ExitCodeResult(exists=False, code=None, error=str(e))

    return ExitCodeResult(exists=False, code=None)
```

### Usage in Event Emission

```python
def determine_exit_event(run_id: str, backend: str) -> tuple[StageEvent, EventContext] | None:
    """Determine exit event based on exit code AND instance status.

    CRITICAL: EXIT_MISSING requires verification that instance is actually dead.
    Otherwise we risk marking running jobs as terminated due to GCS eventual
    consistency or delayed log sync.
    """
    result = get_exit_code(run_id)

    if result.exists:
        if result.code == 0:
            return StageEvent.EXIT_SUCCESS, EventContext(
                exit_code=0, exit_code_exists=True,
                timestamp=datetime.now(UTC), source="monitor"
            )
        else:
            return StageEvent.EXIT_FAILURE, EventContext(
                exit_code=result.code, exit_code_exists=True,
                timestamp=datetime.now(UTC), source="monitor"
            )
    else:
        # No exit code - but is the instance actually dead?
        instance_dead = verify_instance_stopped(run_id, backend)

        if not instance_dead:
            # Instance still running, exit code just not written yet
            # Do NOT emit EXIT_MISSING - wait for next poll
            return None

        # Instance confirmed dead + no exit code = crash
        return StageEvent.EXIT_MISSING, EventContext(
            exit_code_exists=False,
            instance_confirmed_dead=True,  # Guard requirement satisfied
            termination_cause=TerminationCause.CRASHED,
            timestamp=datetime.now(UTC), source="monitor"
        )


def verify_instance_stopped(run_id: str, backend: str) -> bool:
    """Verify instance/container is actually stopped before emitting EXIT_MISSING.

    For GCE: Check instance status via API (TERMINATED, STOPPED, or not found)
    For local: Check container status (exited or not found)
    """
    if backend == "gce":
        status = gce_launcher.get_instance_status(run_id)
        return status in (None, "TERMINATED", "STOPPED", "DELETED")
    else:
        status = docker_client.get_container_status(run_id)
        return status in (None, "exited", "dead", "not_found")
```

**Guard rationale**: The `instance_confirmed_dead` guard prevents false TERMINATED states. GCS eventual consistency means exit_code.txt might not be visible for seconds after write. Without this guard, a poll during that window would incorrectly mark a successful run as crashed.

---

## Admin Tools

### force_terminate_run

For stuck runs that can't be cleaned up normally:

```python
@mcp.tool()
def force_terminate_run(
    run_id: str,
    reason: str,
    termination_cause: str = "orphaned"
) -> dict:
    """Force a stuck run to TERMINATED state.

    Use when normal cleanup fails. Records admin action in audit trail.

    Args:
        run_id: Stage run ID
        reason: Why forcing termination (required for audit)
        termination_cause: One of 'orphaned', 'crashed', 'timeout', 'manual'
    """
    context = EventContext(
        source="admin",
        termination_cause=TerminationCause(termination_cause),
        error_message=f"Admin force terminate: {reason}"
    )
    result = state_machine.transition(run_id, Event.FORCE_TERMINATE, context)
    return {"success": result.success, "new_state": result.new_state}
```

### force_complete_run

For runs stuck in FINALIZING or UNKNOWN where outputs are safe:

```python
@mcp.tool()
def force_complete_run(run_id: str, reason: str) -> dict:
    """Force a run to COMPLETED state.

    Valid from: FINALIZING (outputs recorded), UNKNOWN (verified ok)
    Use when finalization hangs but outputs are recorded.

    Args:
        run_id: Stage run ID
        reason: Why forcing completion (required for audit)
    """
    context = EventContext(
        timestamp=datetime.now(UTC),
        source="admin",
        error_message=f"Admin force complete: {reason}"
    )
    result = state_machine.transition(run_id, StageEvent.FORCE_COMPLETE, context)
    return {"success": result.success, "new_state": result.new_state}
```

### force_fail_run

For UNKNOWN runs that need to be marked as failed after investigation:

```python
@mcp.tool()
def force_fail_run(run_id: str, reason: str, error_message: str | None = None) -> dict:
    """Force an UNKNOWN run to FAILED state.

    Valid from: UNKNOWN only (use force_terminate for others)
    Use after investigating an UNKNOWN run and determining it failed.

    Args:
        run_id: Stage run ID
        reason: Why marking as failed (required for audit)
        error_message: Optional error message to record
    """
    context = EventContext(
        timestamp=datetime.now(UTC),
        source="admin",
        error_message=error_message or f"Admin force fail: {reason}"
    )
    result = state_machine.transition(run_id, StageEvent.FORCE_FAIL, context)
    return {"success": result.success, "new_state": result.new_state}
```

---

## Database Schema

### Modified Table: `stage_runs`

```sql
-- Add new columns
ALTER TABLE stage_runs ADD COLUMN state TEXT;
ALTER TABLE stage_runs ADD COLUMN phase TEXT;
ALTER TABLE stage_runs ADD COLUMN termination_cause TEXT;
ALTER TABLE stage_runs ADD COLUMN state_updated_at TEXT;

-- Keep old columns during migration (remove after verified)
-- status TEXT (keep for backwards compat)
-- progress TEXT (keep for backwards compat)
```

### New Table: `stage_state_transitions`

```sql
CREATE TABLE stage_state_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage_run_id TEXT NOT NULL,

    from_state TEXT NOT NULL,
    event TEXT NOT NULL,
    to_state TEXT NOT NULL,

    -- Context
    phase TEXT,
    termination_cause TEXT,
    exit_code INTEGER,
    exit_code_exists INTEGER,  -- 0 or 1
    error_message TEXT,
    source TEXT,               -- 'mcp_tool', 'daemon', 'admin'

    created_at TEXT NOT NULL DEFAULT (datetime('now')),

    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id)
);

CREATE INDEX idx_transitions_run ON stage_state_transitions(stage_run_id, created_at);
CREATE INDEX idx_transitions_state ON stage_state_transitions(to_state, created_at);
```

### Initial State for New Runs

When creating a new stage run, the initial state is set atomically with row creation:

```python
def create_stage_run(
    workspace_name: str,
    version: str,
    stage_name: str,
    backend_type: str,
    ...
) -> str:
    """Create a new stage run with initial state.

    Returns the stage_run_id.
    """
    run_id = f"stage-{uuid.uuid4().hex[:8]}"
    now = datetime.now(UTC).isoformat()

    with db._conn() as conn:
        # Insert with initial state
        conn.execute(
            """INSERT INTO stage_runs
               (id, workspace_name, version, stage_name, backend_type,
                state, phase, state_updated_at, started_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, workspace_name, version, stage_name, backend_type,
             'preparing',        # Initial state
             'gcs_check',        # Initial phase
             now,                # state_updated_at
             now)                # started_at
        )

        # Record initial transition (state machine entry point)
        conn.execute(
            """INSERT INTO stage_state_transitions
               (stage_run_id, from_state, event, to_state, phase, source, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (run_id, 'none', 'run_start', 'preparing', 'gcs_check', 'mcp_tool', now)
        )

    return run_id
```

**Invariants for new runs**:
- `state` = 'preparing' (always start here)
- `phase` = 'gcs_check' (first phase of PREPARING)
- `state_updated_at` = creation timestamp
- Initial transition recorded in audit trail with `from_state='none'`

---

## Migration Strategy

### Phase 1: Add New Columns (Non-Breaking)

```sql
-- Migration 001: Add state machine columns
ALTER TABLE stage_runs ADD COLUMN state TEXT;
ALTER TABLE stage_runs ADD COLUMN phase TEXT;
ALTER TABLE stage_runs ADD COLUMN termination_cause TEXT;
ALTER TABLE stage_runs ADD COLUMN state_updated_at TEXT;
```

### Phase 1b: Migrate Existing Data (Python Script)

SQL-only migration is too brittle. Use a Python script that can:
1. Query additional context (GCS, GCE API) for ambiguous cases
2. Log decisions for audit
3. Handle edge cases gracefully

```python
def migrate_stage_runs():
    """Migrate existing stage_runs to new state model.

    Run ONCE during migration. Logs all decisions for audit.
    """
    with db._conn() as conn:
        rows = conn.execute(
            "SELECT id, status, progress, error, started_at, completed_at "
            "FROM stage_runs WHERE state IS NULL"
        ).fetchall()

    for row in rows:
        state, phase, cause = determine_migration_state(row)
        log_migration(row["id"], row["status"], state, cause)

        with db._conn() as conn:
            conn.execute(
                """UPDATE stage_runs
                   SET state = ?, phase = ?, termination_cause = ?,
                       state_updated_at = COALESCE(completed_at, ?)
                   WHERE id = ?""",
                (state, phase, cause, datetime.now(UTC).isoformat(), row["id"])
            )


def determine_migration_state(row: dict) -> tuple[str, str | None, str | None]:
    """Determine state from legacy columns with careful heuristics."""
    status = row["status"]
    progress = row["progress"]
    error = row["error"] or ""
    started_at = row["started_at"]
    completed_at = row["completed_at"]

    # Terminal states - straightforward
    if status == "completed":
        return ("completed", None, None)
    if status == "canceled":
        return ("canceled", None, None)

    # Failed - check for infrastructure vs code failure
    if status == "failed":
        # Check for preemption indicators (case-insensitive, multiple patterns)
        preemption_patterns = ["preempted", "spot instance", "preemption", "PREEMPTED"]
        if any(p.lower() in error.lower() for p in preemption_patterns):
            return ("terminated", None, "preempted")

        # Check for timeout indicators
        if "timeout" in error.lower() or "timed out" in error.lower():
            return ("terminated", None, "timeout")

        # Check for crash indicators (OOM, signal, etc.)
        crash_patterns = ["killed", "oom", "signal", "segfault", "core dump"]
        if any(p.lower() in error.lower() for p in crash_patterns):
            return ("terminated", None, "crashed")

        # Default: assume code failure (not infra)
        return ("failed", None, None)

    # Running - need to determine if actually running or orphaned
    if status == "running":
        # Map progress to state
        if progress == "building":
            state, phase = "building", "docker_build"
        elif progress == "launching":
            state, phase = "launching", "instance_create"
        elif progress == "finalizing":
            state, phase = "finalizing", "output_sync"
        else:
            state, phase = "running", "code_execution"

        # Check if this is an orphaned run
        # DON'T use arbitrary time threshold - check if instance actually exists
        if is_likely_orphaned(row["id"], started_at):
            return ("terminated", phase, "orphaned")

        # Still potentially active
        return (state, phase, None)

    # Pending
    if status == "pending":
        return ("preparing", "gcs_check", None)

    # Unknown status - flag for manual review
    return ("unknown", None, None)


def is_likely_orphaned(run_id: str, started_at: str) -> bool:
    """Check if a 'running' run is actually orphaned.

    More reliable than time-based heuristics.
    """
    # 1. Check if instance exists in GCE
    try:
        instance_status = gce_launcher.get_instance_status(run_id)
        if instance_status in (None, "TERMINATED", "STOPPED", "DELETED"):
            return True  # Instance gone but status=running = orphaned
    except Exception:
        pass  # Can't check GCE, fall back to time heuristic

    # 2. Fallback: Use conservative time threshold (30 days, not 7)
    # Only if we couldn't check GCE
    if started_at:
        started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        age = datetime.now(UTC) - started
        if age > timedelta(days=30):
            logger.warning(f"Run {run_id} is 30+ days old with status=running, marking orphaned")
            return True

    return False
```

**Migration improvements over SQL-only**:
1. **No arbitrary thresholds**: Checks actual instance status instead of 7-day cutoff
2. **Multiple pattern matching**: Catches "preempted", "PREEMPTED", "Spot instance terminated"
3. **Audit logging**: Every decision logged for review
4. **Conservative fallback**: 30 days instead of 7, and only if GCE check unavailable
5. **UNKNOWN for ambiguous**: Doesn't guess - flags for manual review

### Phase 2: Backwards Compatibility Layer

```python
# In models.py or database.py

def get_legacy_status(state: str, termination_cause: str | None) -> str:
    """Map new state to old status for backwards compatibility."""
    if state in ('preparing', 'building', 'launching', 'running', 'finalizing'):
        return 'running'
    if state == 'completed':
        return 'completed'
    if state in ('failed', 'terminated', 'unknown'):
        return 'failed'
    if state == 'canceled':
        return 'canceled'
    return 'failed'

def get_legacy_progress(state: str, phase: str | None) -> str | None:
    """Map new state/phase to old progress."""
    if state == 'building':
        return 'building'
    if state == 'launching':
        return 'launching'
    if state == 'running':
        return 'running'
    if state == 'finalizing':
        return 'finalizing'
    return None
```

### Phase 3: Update All Readers

Update 70+ call sites to use new columns. Track with:
```python
# Temporary: log when legacy mapping is used
if using_legacy_status:
    logger.warning(f"Legacy status access at {caller}", stack_info=True)
```

### Phase 4: Remove Old Columns

After verification:
```sql
-- Migration 002: Remove legacy columns (AFTER all code updated)
ALTER TABLE stage_runs DROP COLUMN status;
ALTER TABLE stage_runs DROP COLUMN progress;
```

---

## Daemon Interface (Spec Only)

The daemon is **not implemented in Phase 1**. This section specs the interface for future implementation.

### Responsibilities

1. Poll active runs for infrastructure state changes
2. Emit events to state machine (via `transition()`)
3. Detect orphaned runs (no progress for extended period)
4. Coordinate with MCP tools (don't double-finalize)

### Coordination with MCP Tools

When daemon is implemented:
- Daemon is the **primary** status checker for active runs
- MCP tools read from database (daemon keeps it fresh)
- `refresh_status_once()` becomes a no-op or removes entirely
- CAS semantics prevent race conditions

### Timeout Configuration (Sensible Defaults)

Timeouts are a **state detection problem**, not a state machine problem. For now, use sensible defaults:

```python
DEFAULT_TIMEOUTS = {
    "build": 1800,      # 30 min (covers GPU image with cuda)
    "launch": 900,      # 15 min (covers H100 provisioning)
    "not_found": 300,   # 5 min grace period
    "finalizing": 1800, # 30 min (covers slow GCS sync)
}
```

Future: Intelligent timeout detection via SVS-like agent that monitors build/startup progress.

---

## Container Boundary (Known Limitation)

### The Problem

SVS events that happen **inside** the container cannot directly emit events to the state machine running **outside**:

- Schema validation in `save_output()` - runs inside container
- During-run SVS findings - written to `.goldfish/` files
- Stats computation - runs inside container

### Current Approach

Container-side SVS writes to files:
- `.goldfish/svs_stats.json`
- `.goldfish/svs_findings.json`
- `.goldfish/svs_findings_during.json`

These are read during FINALIZING phase (post-execution).

### Future Enhancement

Container-side event bus (`.goldfish/events.jsonl`) that daemon polls for real-time event emission. Not in scope for Phase 1.

---

## Relationship to `run_results.infra_outcome`

### The Duplication

The `run_results` table already has:
```sql
infra_outcome TEXT CHECK(infra_outcome IN ('completed', 'preempted', 'crashed', 'canceled', 'unknown'))
```

This overlaps with `stage_runs.state` + `termination_cause`.

### Resolution

- `stage_runs.state` is the **live** state (updated during execution)
- `run_results.infra_outcome` is the **finalized** outcome (set once at end)
- `run_results.infra_outcome` should be derived from `stage_runs.state` at finalization

**Key insight**: `infra_outcome` tracks *infrastructure* outcomes, not code outcomes. FAILED state (code/validation errors) means infra worked fine - it's `completed` from infra perspective.

```python
def derive_infra_outcome(state: str, termination_cause: str | None) -> str:
    """Derive infra_outcome from state machine state.

    infra_outcome answers: "Did the infrastructure successfully run the code?"
    NOT: "Did the code succeed?"
    """
    if state == 'completed':
        return 'completed'
    if state == 'failed':
        # FAILED = code/validation error, but infra worked correctly
        # The container ran, the code executed, it just returned non-zero
        return 'completed'  # Infra did its job
    if state == 'canceled':
        return 'canceled'
    if state == 'terminated':
        # Terminated = infra died (preemption, crash, timeout, etc.)
        return termination_cause or 'unknown'
    if state == 'unknown':
        return 'unknown'
    return 'unknown'
```

**ML outcome tracking**: The *code* success/failure is tracked separately in `run_results.ml_outcome` (success, partial, miss, unknown). This separation is intentional:
- `infra_outcome=completed, ml_outcome=miss` → Infra worked, model underperformed
- `infra_outcome=preempted, ml_outcome=unknown` → Infra died, can't assess model

---

## TDD Approach

### Test Categories

```
tests/unit/state_machine/
├── test_transitions.py       # Valid transitions
├── test_invalid.py           # Invalid transitions raise errors
├── test_cas.py               # Concurrent access handling
├── test_guards.py            # Guard conditions
├── test_context.py           # Event context handling
├── test_audit.py             # Transition recording
├── test_migration.py         # Legacy status mapping
└── test_exit_code.py         # Exit code detection
```

### Example Tests

```python
class TestValidTransitions:
    @pytest.mark.parametrize("from_state,event,to_state", [
        ("preparing", "build_start", "building"),
        ("building", "build_ok", "launching"),
        ("launching", "launch_ok", "running"),
        ("running", "exit_success", "finalizing"),
        ("finalizing", "finalize_ok", "completed"),
    ])
    def test_happy_path_transitions(self, from_state, event, to_state):
        sm = StageStateMachine(initial=from_state)
        result = sm.handle_event(event, EventContext())
        assert result.success
        assert sm.state == to_state

class TestExitCodeDistinction:
    def test_exit_missing_goes_to_terminated_not_failed(self):
        sm = StageStateMachine(initial="running")
        ctx = EventContext(exit_code_exists=False, termination_cause="crashed")
        result = sm.handle_event("exit_missing", ctx)
        assert sm.state == "terminated"
        assert sm.termination_cause == "crashed"

    def test_exit_failure_goes_to_failed(self):
        sm = StageStateMachine(initial="running")
        ctx = EventContext(exit_code=1, exit_code_exists=True)
        result = sm.handle_event("exit_failure", ctx)
        assert sm.state == "failed"

class TestCAS:
    def test_concurrent_transitions_one_wins(self, test_db):
        run_id = create_test_run(test_db, state="running")

        # Simulate concurrent transitions
        result1 = transition(run_id, "exit_success", EventContext())
        result2 = transition(run_id, "user_cancel", EventContext())

        # One succeeds, one fails
        assert result1.success != result2.success

        # Final state is consistent
        run = test_db.get_stage_run(run_id)
        assert run["state"] in ("finalizing", "canceled")
```

---

## Implementation Phases

### Phase 1: Foundation (Current)
- [x] Write spec (this document)
- [ ] Fix `_get_exit_code()` to return `ExitCodeResult`
- [ ] Write comprehensive test suite (TDD)
- [ ] Implement state machine core (transitions, CAS, audit)
- [ ] Write migration script
- [ ] Add backwards compatibility layer
- [ ] Add `force_terminate_run` admin tool

### Phase 2: Integration
- [ ] Wire state machine into `stage_executor.py`
- [ ] Update all status readers to use new columns
- [ ] Add transition logging/monitoring
- [ ] Test with real workloads

### Phase 3: Cleanup
- [ ] Remove backwards compatibility layer
- [ ] Drop old `status`/`progress` columns
- [ ] Update documentation

### Phase 4: Daemon (Future)
- [ ] Implement daemon polling
- [ ] Add intelligent timeout detection
- [ ] Add AI-initiated stop (STOPPED state)
- [ ] Add auto-recovery from TERMINATED

---

## Open Questions

### Resolved in v3

- ✅ **Phase-only updates**: Use CAS with `expected_state` guard, atomic with audit (see "Phase Updates" section)
- ✅ **Initial state for new runs**: PREPARING with phase=gcs_check, set atomically on creation (see "Initial State" section)
- ✅ **EXIT_MISSING guard**: Requires `instance_confirmed_dead=True` via `verify_instance_stopped()` (see "Exit Code Detection" section)
- ✅ **infra_outcome mapping**: FAILED → completed (infra worked), TERMINATED → termination_cause (see "Relationship to run_results" section)

### Still Open

1. **Daemon deployment model**: Separate process vs. integrated?
2. **Metrics sync during polling**: Every poll or only on transitions?
3. **Partial finalization outputs**: What to do with them on CANCELED?
4. **UNKNOWN state cleanup**: Periodic job to investigate and resolve?
5. **AI_STOPPED trigger mechanism**: How does AI/SVS signal that a run should stop?
