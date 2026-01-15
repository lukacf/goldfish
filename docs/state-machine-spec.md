# Stage Execution State Machine Specification

> **Status**: Draft v3.2 (Daemon Rewrite Strategy)
> **Author**: Claude + Luka
> **Created**: 2025-01-15
> **Updated**: 2025-01-15

## Overview

This document specifies a state machine to **replace** the ad-hoc if/then/else state management scattered across Goldfish stage execution. The current daemon, stage executor, and MCP tools all have their own logic for deciding and updating state - this creates race conditions, silent failures, and untraceable state corruption. The state machine becomes the **single source of truth**, with all existing state-mutation code rewritten to emit events instead.

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
def update_phase(run_id: str, expected_state: str, new_phase: ProgressPhase, timestamp: datetime) -> bool:
    """Update phase within a state using CAS.

    IMPORTANT: Updates phase_updated_at, NOT state_entered_at.
    state_entered_at is only updated on state transitions (for timeout calculations).

    Args:
        run_id: Stage run ID
        expected_state: State we expect the run to be in (CAS guard)
        new_phase: New phase to set
        timestamp: Canonical timestamp for this update

    Returns True if update succeeded, False if state changed underneath us.
    """
    with db._conn() as conn:
        result = conn.execute(
            """UPDATE stage_runs
               SET phase = ?, phase_updated_at = ?
               WHERE id = ? AND state = ?""",
            (new_phase.value, timestamp.isoformat(), run_id, expected_state)
        )
        if result.rowcount == 0:
            return False  # State changed, phase update rejected

        # Record phase change in audit (same transaction)
        # Note: 'phase_update' is a pseudo-event for audit, not in StageEvent enum
        conn.execute(
            """INSERT INTO stage_state_transitions
               (stage_run_id, from_state, event, to_state, phase, source, created_at)
               VALUES (?, ?, 'phase_update', ?, ?, ?, ?)""",
            (run_id, expected_state, expected_state, new_phase.value, 'executor', timestamp.isoformat())
        )
        return True
```

**Key principles**:
- Phase updates are rejected if state changed (CAS guard prevents desync)
- `phase_updated_at` is updated, NOT `state_entered_at` (preserves timeout calculations)
- Timestamp is passed in (canonical time source, not `now()`)
- `'phase_update'` is a pseudo-event for audit trail, not a real state machine event

### Pseudo-Events (Audit-Only)

These event strings appear in `stage_state_transitions.event` but are NOT in `StageEvent` enum:

| Pseudo-Event | Purpose | When Used |
|--------------|---------|-----------|
| `run_start` | Records initial state machine entry | `create_stage_run()` |
| `phase_update` | Records within-state phase changes | `update_phase()` |

These are allowed in the audit trail for observability but don't drive state machine transitions.

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
    source: str                              # See SOURCE_VALUES below

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
    critical_phases_done: bool = False       # For TIMEOUT in FINALIZING: True → COMPLETED, False → FAILED

    # SVS context
    svs_decision: str | None = None          # 'approved', 'blocked', 'warning'
    svs_findings: list[dict] | None = None


# Allowed source values for EventContext.source
SOURCE_VALUES = {
    'mcp_tool',   # MCP tool invocation (cancel, force_* admin tools)
    'executor',   # StageExecutor during run orchestration
    'daemon',     # Background daemon polling
    'container',  # Container-side events (via metadata/GCS)
    'admin',      # Administrative operations
    'migration',  # Migration script
}
```

**Timestamp usage**: `EventContext.timestamp` is the canonical time source. The `state_entered_at` column uses `context.timestamp.isoformat()`, not `now()`. This ensures audit trail ordering matches actual event sequence.

**critical_phases_done**: For TIMEOUT events in FINALIZING state, this field determines whether we transition to COMPLETED (outputs were saved) or FAILED (outputs may be lost). Set to True if OUTPUT_SYNC and OUTPUT_RECORDING phases completed successfully before the timeout.

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

**UNKNOWN state**: Migration fallback only.

### Default Phases Per State

When a transition doesn't provide `context.phase`, use COALESCE to preserve the existing phase. However, to prevent invalid state/phase combinations (e.g., state=RUNNING with phase=docker_build), define entry phases for state transitions:

```python
# Entry phase when transitioning INTO this state (if context.phase not provided)
STATE_ENTRY_PHASES = {
    'preparing': 'gcs_check',
    'building': 'image_check',
    'launching': 'instance_create',
    'running': 'container_init',
    'finalizing': 'output_sync',
    # Terminal states don't have phases
    'completed': None,
    'failed': None,
    'terminated': None,
    'canceled': None,
    'unknown': None,
}
```

**Usage in transition()**: When `context.phase` is None and the state is changing, use `STATE_ENTRY_PHASES[to_state]` instead of COALESCE. COALESCE only applies for within-state phase preservation (shouldn't happen in transitions since transitions always change state). Admin tools provide escape hatches. No automatic transitions into or out of UNKNOWN.

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

    # 4. Determine if this is a "completed with warnings" case
    # Two scenarios: FINALIZE_FAIL(critical=False) or TIMEOUT in FINALIZING with outputs saved
    completed_with_warnings = (
        transition_def.to_state == 'completed'
        and (
            (event == StageEvent.FINALIZE_FAIL and not context.critical)
            or (event == StageEvent.TIMEOUT and context.critical_phases_done)
        )
    )

    # 5. Default termination_cause for TIMEOUT events
    # TIMEOUT transitions always need cause=TIMEOUT for proper infra_outcome mapping
    termination_cause = context.termination_cause
    if event == StageEvent.TIMEOUT and termination_cause is None:
        termination_cause = TerminationCause.TIMEOUT

    # 6. Determine phase for the new state
    # If context.phase provided, use it; otherwise use entry phase for new state
    new_phase = (
        context.phase.value if context.phase
        else STATE_ENTRY_PHASES.get(transition_def.to_state)
    )

    # 6. ATOMIC: CAS update + audit insert in same transaction
    with db._conn() as conn:
        # CAS update - only succeeds if state hasn't changed
        # Update both state_entered_at (for timeouts) and phase_updated_at (observability)
        result = conn.execute(
            """UPDATE stage_runs
               SET state = ?,
                   phase = ?,
                   state_entered_at = ?,
                   phase_updated_at = ?,
                   termination_cause = ?,
                   error = ?,
                   completed_with_warnings = ?
               WHERE id = ? AND state = ?""",
            (transition_def.to_state,
             new_phase,
             context.timestamp.isoformat(),  # state_entered_at: state change
             context.timestamp.isoformat(),  # phase_updated_at: phase also changes
             termination_cause.value if termination_cause else None,  # Uses defaulted value
             context.error_message,
             1 if completed_with_warnings else 0,
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
             new_phase,
             termination_cause.value if termination_cause else None,  # Uses defaulted value
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
    now = datetime.now(UTC)

    if result.exists:
        if result.code == 0:
            return StageEvent.EXIT_SUCCESS, EventContext(
                exit_code=0, exit_code_exists=True,
                timestamp=now, source="daemon"
            )
        else:
            return StageEvent.EXIT_FAILURE, EventContext(
                exit_code=result.code, exit_code_exists=True,
                timestamp=now, source="daemon"
            )
    else:
        # No exit code - but is the instance actually dead?
        instance_dead = verify_instance_stopped(run_id, backend)

        if not instance_dead:
            # Instance still running, exit code just not written yet
            # Do NOT emit EXIT_MISSING - wait for next poll
            return None

        # Instance confirmed dead + no exit code = need to detect WHY
        cause = detect_termination_cause(run_id, backend)
        return StageEvent.EXIT_MISSING, EventContext(
            exit_code_exists=False,
            instance_confirmed_dead=True,  # Guard requirement satisfied
            termination_cause=cause,
            timestamp=now, source="daemon"
        )


def detect_termination_cause(run_id: str, backend: str) -> TerminationCause:
    """Detect why an instance terminated without writing exit code.

    For GCE: Check operations API for preemption, check instance deletion reason
    For local: Generally assume crash (local doesn't have preemption)

    Returns: PREEMPTED, CRASHED, or ORPHANED
    """
    if backend == "gce":
        try:
            # Check GCE operations for preemption
            if gce_launcher.was_preempted(run_id):
                return TerminationCause.PREEMPTED

            # Check if instance has any termination evidence (delete operations, errors)
            if gce_launcher.has_termination_evidence(run_id):
                return TerminationCause.CRASHED

            # No evidence of how it terminated - orphaned
            return TerminationCause.ORPHANED
        except Exception as e:
            # Can't determine cause - treat as orphaned (unknown state)
            logger.warning(f"Can't detect termination cause for {run_id}: {e}")
            return TerminationCause.ORPHANED
    else:
        # Local Docker: check container exit reason
        try:
            exit_reason = docker_client.get_container_exit_reason(run_id)
            if exit_reason in ("OOMKilled", "Error", "ContainerCannotRun"):
                return TerminationCause.CRASHED
            if exit_reason is None:
                # Container vanished with no trace - orphaned
                return TerminationCause.ORPHANED
            # Unknown exit reason, assume crash
            return TerminationCause.CRASHED
        except Exception:
            return TerminationCause.ORPHANED


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

**Cause detection**: EXIT_MISSING must always detect the termination cause (PREEMPTED, CRASHED, or ORPHANED). For GCE, we check the operations API for preemption events. For local Docker, we assume crash since local doesn't have preemption.

---

## Admin Tools

### force_terminate_run

For stuck runs that can't be cleaned up normally:

```python
@mcp.tool()
def force_terminate_run(
    run_id: str,
    reason: str,
    termination_cause: str = "manual"
) -> dict:
    """Force a stuck run to TERMINATED state.

    Use when normal cleanup fails. Records admin action in audit trail.

    Args:
        run_id: Stage run ID
        reason: Why forcing termination (required for audit)
        termination_cause: One of 'orphaned', 'crashed', 'timeout', 'manual'
    """
    context = EventContext(
        timestamp=datetime.now(UTC),
        source="admin",
        termination_cause=TerminationCause(termination_cause),
        error_message=f"Admin force terminate: {reason}"
    )
    result = state_machine.transition(run_id, StageEvent.FORCE_TERMINATE, context)
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
ALTER TABLE stage_runs ADD COLUMN state_entered_at TEXT;    -- When state was entered (for timeouts)
ALTER TABLE stage_runs ADD COLUMN phase_updated_at TEXT;    -- When phase last changed (observability)
ALTER TABLE stage_runs ADD COLUMN completed_with_warnings INTEGER DEFAULT 0;  -- Non-critical finalization failures

-- Keep old columns during migration (remove after verified)
-- status TEXT (keep for backwards compat)
-- progress TEXT (keep for backwards compat)
```

**Timestamp semantics**:
- `state_entered_at`: Updated ONLY on state transitions. Used for timeout calculations.
- `phase_updated_at`: Updated on phase changes within a state. Observability only.
- `started_at`: When run was created (existing column). Used for total runtime limits.

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
    source TEXT,               -- See SOURCE_VALUES: mcp_tool, executor, daemon, container, admin, migration

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
                state, phase, state_entered_at, phase_updated_at, started_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, workspace_name, version, stage_name, backend_type,
             'preparing',        # Initial state
             'gcs_check',        # Initial phase
             now,                # state_entered_at (for timeout calcs)
             now,                # phase_updated_at (observability)
             now)                # started_at (total runtime)
        )

        # Record initial transition (state machine entry point)
        conn.execute(
            """INSERT INTO stage_state_transitions
               (stage_run_id, from_state, event, to_state, phase, source, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (run_id, 'none', 'run_start', 'preparing', 'gcs_check', 'executor', now)
        )

    return run_id
```

**Invariants for new runs**:
- `state` = 'preparing' (always start here)
- `phase` = 'gcs_check' (first phase of PREPARING)
- `state_entered_at` = `phase_updated_at` = `started_at` = creation timestamp
- Initial transition recorded in audit trail with `from_state='none'`

---

## Migration Strategy

### Phase 1: Add New Columns (Non-Breaking)

```sql
-- Migration 001: Add state machine columns
ALTER TABLE stage_runs ADD COLUMN state TEXT;
ALTER TABLE stage_runs ADD COLUMN phase TEXT;
ALTER TABLE stage_runs ADD COLUMN termination_cause TEXT;
ALTER TABLE stage_runs ADD COLUMN state_entered_at TEXT;
ALTER TABLE stage_runs ADD COLUMN phase_updated_at TEXT;
ALTER TABLE stage_runs ADD COLUMN completed_with_warnings INTEGER DEFAULT 0;
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
            "SELECT id, status, progress, error, started_at, completed_at, backend_type "
            "FROM stage_runs WHERE state IS NULL"
        ).fetchall()

    for row in rows:
        state, phase, cause = determine_migration_state(row)
        log_migration(row["id"], row["status"], state, cause)

        now = datetime.now(UTC).isoformat()
        with db._conn() as conn:
            conn.execute(
                """UPDATE stage_runs
                   SET state = ?, phase = ?, termination_cause = ?,
                       state_entered_at = COALESCE(completed_at, ?),
                       phase_updated_at = COALESCE(completed_at, ?)
                   WHERE id = ?""",
                (state, phase, cause, now, now, row["id"])
            )


def determine_migration_state(row: dict) -> tuple[str, str | None, str | None]:
    """Determine state from legacy columns with careful heuristics."""
    status = row["status"]
    progress = row["progress"]
    error = row["error"] or ""
    started_at = row["started_at"]
    backend_type = row.get("backend_type", "gce")  # Default to GCE for old runs

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
        orphan_status = check_orphan_status(row["id"], started_at, backend_type)
        if orphan_status == "confirmed_orphaned":
            return ("terminated", phase, "orphaned")
        elif orphan_status == "possibly_orphaned":
            # Can't confirm - mark UNKNOWN for manual review
            return ("unknown", phase, None)

        # Still potentially active
        return (state, phase, None)

    # Pending
    if status == "pending":
        return ("preparing", "gcs_check", None)

    # Unknown status - flag for manual review
    return ("unknown", None, None)


def check_orphan_status(run_id: str, started_at: str, backend_type: str) -> str:
    """Check if a 'running' run is actually orphaned.

    Returns:
        'not_orphaned': Instance still exists
        'confirmed_orphaned': Instance confirmed gone
        'possibly_orphaned': Can't check, but old enough to suspect
    """
    # 1. Check backend for instance status
    try:
        if backend_type == "gce":
            instance_status = gce_launcher.get_instance_status(run_id)
            if instance_status in (None, "TERMINATED", "STOPPED", "DELETED"):
                return "confirmed_orphaned"
            return "not_orphaned"
        else:
            # Local Docker
            container_status = docker_client.get_container_status(run_id)
            if container_status in (None, "exited", "dead", "not_found"):
                return "confirmed_orphaned"
            return "not_orphaned"
    except Exception as e:
        logger.warning(f"Can't check {backend_type} status for {run_id}: {e}")

    # 2. Fallback: Use time threshold (30 days)
    # But mark as POSSIBLY orphaned - we can't confirm
    if started_at:
        started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        age = datetime.now(UTC) - started
        if age > timedelta(days=30):
            logger.warning(f"Run {run_id} is 30+ days old, can't verify status - marking for review")
            return "possibly_orphaned"

    return "not_orphaned"
```

**Migration improvements over SQL-only**:
1. **Backend-aware**: Checks GCE or Docker based on `backend_type` column
2. **Three-way status**: Distinguishes "confirmed orphaned" from "possibly orphaned"
3. **UNKNOWN for ambiguous**: Can't confirm status → mark UNKNOWN for manual review
4. **Audit logging**: Every decision logged for review
5. **Conservative fallback**: 30 days threshold only when backend check fails

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

## Daemon Rewrite Strategy

**The current daemon (`daemon.py`) is an if/then/else mess that needs to be replaced with proper state machine logic.** While infrastructure components exist (metadata bus, GCE APIs), the control flow must be rewritten around the state machine as the single source of truth.

### Current Problems with Daemon

The existing daemon suffers from:
- **Scattered state logic**: Multiple code paths check status with subtly different conditions
- **If/then/else chains**: Nested conditionals like "if running and progress == X and elapsed > Y..."
- **No central authority**: Status updates happen in 70+ places with no coordination
- **Silent failures**: Edge cases fall through without proper handling
- **Exit code bug**: Returns `1` when file doesn't exist (conflates "missing" with "failed")

### The Rewrite Approach

**The state machine becomes the ONLY way to change stage run state.** All existing code paths that update `status`/`progress` columns must be converted to emit events instead.

```python
# BEFORE (scattered if/then/else mess)
def _check_orphaned_runs(self):
    for run in running_runs:
        if run.progress == "building" and elapsed > build_timeout:
            self._mark_failed(run, "build timeout")
        elif run.progress == "launching" and elapsed > launch_timeout:
            self._mark_failed(run, "launch timeout")
        elif run.status == "running" and not instance_exists:
            exit_code = self._get_exit_code(run)
            if exit_code == 0:
                self._finalize(run, "completed")
            else:
                self._mark_failed(run, "crashed")
        # ... 50 more lines of conditionals

# AFTER (state machine is single source of truth)
def _check_runs(self):
    for run in active_runs:
        event, context = self._determine_event(run)
        if event:
            state_machine.transition(run.id, event, context)
```

### What Gets Rewritten

| Component | Current State | Rewrite |
|-----------|--------------|---------|
| `daemon._check_orphaned_runs()` | If/then/else on status/progress | Emit events to state machine |
| `daemon._process_run()` | Direct status updates | Emit events to state machine |
| `stage_executor.refresh_status_once()` | Complex conditionals | Return event + context, let caller transition |
| `cancel()` MCP tool | Direct SQL update | `state_machine.transition(run_id, USER_CANCEL, ctx)` |
| `_finalize_stage_run()` | Called directly | Called by state machine on FINALIZING→terminal |

### What Gets Preserved

Some infrastructure is well-designed and should be kept:

- **Metadata Event System** (`infra/metadata/`): Clean protocol-based design
  - `MetadataSignal`, `MetadataBus`, GCP/Local implementations
  - ACK handshake pattern for container communication
  - Extend with `"stop"` command for AI_STOPPED

- **GCE APIs**: Instance lifecycle management
  - `get_instance_status()`, `delete_instance()`
  - Preemption detection via operations API

- **Docker APIs**: Container management
  - `docker stop`, status queries

- **Exit code retrieval** (after bug fix): GCS `exit_code.txt` pattern

### New Daemon Structure

```python
class StageDaemon:
    """Daemon that emits events to state machine.

    The daemon's job is to OBSERVE infrastructure state and EMIT events.
    The state machine's job is to DECIDE what those events mean.
    """

    def __init__(self, state_machine: StageStateMachine, ...):
        self.state_machine = state_machine

    def poll_active_runs(self):
        """Main polling loop - determine and emit events."""
        for run in self.db.get_active_runs():
            event, context = self._determine_event(run)
            if event:
                result = self.state_machine.transition(run["id"], event, context)
                if not result.success:
                    logger.warning(f"Transition failed: {result.reason}")

    def _determine_event(self, run: dict) -> tuple[StageEvent | None, EventContext | None]:
        """Determine what event (if any) should be emitted for this run.

        This is pure logic - no state mutations.
        """
        state = run["state"]
        backend = run["backend_type"]

        # Check timeouts first (state-specific)
        if timeout_event := self._check_timeout(run):
            return timeout_event

        # Check backend status
        if backend == "gce":
            return self._determine_gce_event(run)
        else:
            return self._determine_local_event(run)

    def _determine_gce_event(self, run: dict) -> tuple[StageEvent | None, EventContext | None]:
        """Determine event for GCE-backed run."""
        instance_status = self.gce.get_instance_status(run["backend_handle"])

        # Instance still running
        if instance_status in ("RUNNING", "STAGING", "PROVISIONING"):
            return None, None

        # Instance gone - check exit code
        exit_result = self._get_exit_code(run["id"])

        if exit_result.exists:
            if exit_result.code == 0:
                return StageEvent.EXIT_SUCCESS, EventContext(
                    exit_code=0, exit_code_exists=True,
                    timestamp=datetime.now(UTC), source="daemon"
                )
            else:
                return StageEvent.EXIT_FAILURE, EventContext(
                    exit_code=exit_result.code, exit_code_exists=True,
                    timestamp=datetime.now(UTC), source="daemon"
                )
        else:
            # No exit code + instance gone = crash/preemption
            cause = self._detect_termination_cause(run)
            return StageEvent.EXIT_MISSING, EventContext(
                exit_code_exists=False,
                instance_confirmed_dead=True,
                termination_cause=cause,
                timestamp=datetime.now(UTC), source="daemon"
            )
```

### Exit Code Bug (Must Fix)

**daemon.py:844**:
```python
return 1  # Default to failure if not found  ← BUG
```

**gce_launcher.py:738**:
```python
return 1  # Returns 1 when file doesn't exist  ← SAME BUG
```

Both must be fixed to use `ExitCodeResult` pattern (see "Exit Code Detection" section).

### Timeout Configuration

Keep existing timeout defaults:
```python
GOLDFISH_GCE_NOT_FOUND_TIMEOUT = 300   # 5 min for RUNNING phase
GOLDFISH_GCE_LAUNCH_TIMEOUT = 1200     # 20 min for BUILD/LAUNCH phases
```

### Cancel Flow (Rewritten)

```python
# server_tools/execution_tools.py
@mcp.tool()
def cancel(run_id: str, reason: str) -> CancelRunResponse:
    """Cancel a running stage.

    Uses state machine for atomic state change.
    """
    context = EventContext(
        timestamp=datetime.now(UTC),
        source="mcp_tool",
        error_message=f"Canceled: {reason}"
    )

    result = state_machine.transition(run_id, StageEvent.USER_CANCEL, context)

    if result.success:
        # Best-effort backend cleanup (non-blocking)
        _cleanup_backend(run_id)
        return CancelRunResponse(success=True, new_state="canceled")
    else:
        return CancelRunResponse(success=False, error=result.reason)
```

### AI_STOPPED via Metadata Bus

The existing metadata bus supports this cleanly:

```python
# Emit stop signal via metadata bus
signal = MetadataSignal(
    command="stop",
    request_id=uuid4().hex,
    payload={"reason": "ai_stopped", "svs_finding_id": "..."}
)
metadata_bus.set_signal(run_id, signal)

# Container receives, writes exit code, shuts down
# Daemon sees EXIT_SUCCESS/FAILURE, emits event
# State machine handles normally
```

The container-side polling is already implemented - just need to add "stop" command handler.

---

## Container ↔ Outside Communication

### Bidirectional Metadata Bus

The GCP instance metadata system provides **bidirectional** communication:

**Outside → Container (daemon triggers container):**
```python
# Daemon sets signal
metadata_bus.set_signal("goldfish", MetadataSignal(command="sync", request_id=uuid4().hex, ...))

# Container polls (shell script in startup_builder.py)
# curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/goldfish
```

**Container → Outside (container acknowledges, uploads):**
```bash
# Container sets ACK (from metadata_syncer_section in startup_builder.py)
gcloud compute instances add-metadata "$INSTANCE_NAME" \
    --zone="$INSTANCE_ZONE" --project="$PROJECT_ID" \
    --metadata "goldfish_ack=$REQ_ID"

# Daemon polls for ACK
ack = metadata_bus.get_ack("goldfish", target=instance_name)
```

### Container-Side Event Emission

The container CAN emit events to the outside:

1. **Via Metadata**: Container sets `goldfish_ack` or custom metadata keys that daemon polls
2. **Via GCS Files**: Container writes to GCS, daemon polls:
   - `.goldfish/svs_findings_during.json` - real-time SVS findings
   - `.goldfish/metrics.json` - training metrics
   - `exit_code.txt` - completion status

### Current SVS Pattern

The "Overdrive" system already provides on-demand sync:
1. Daemon sets `MetadataSignal(command="sync", ...)`
2. Container polls, sees new request_id
3. Container sets ACK immediately (tells daemon "I received it")
4. Container uploads metrics/SVS files to GCS
5. Daemon polls ACK, then reads GCS files

This same pattern works for any container→outside event emission. The container writes state to GCS or metadata, daemon polls and emits corresponding state machine events.

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
# Mapping from termination_cause to valid infra_outcome values
# infra_outcome CHECK constraint: 'completed', 'preempted', 'crashed', 'canceled', 'unknown'
TERMINATION_CAUSE_TO_INFRA_OUTCOME = {
    'preempted': 'preempted',   # Direct mapping
    'crashed': 'crashed',       # Direct mapping
    'timeout': 'crashed',       # Timeout is an infra failure (didn't complete)
    'orphaned': 'unknown',      # We don't know what happened
    'ai_stopped': 'canceled',   # Intentional stop requested by AI
    'manual': 'canceled',       # Admin intervention counts as cancellation
}

def derive_infra_outcome(state: str, termination_cause: str | None) -> str:
    """Derive infra_outcome from state machine state.

    infra_outcome answers: "Did the infrastructure successfully run the code?"
    NOT: "Did the code succeed?"

    IMPORTANT: Must return value that satisfies CHECK constraint:
    ('completed', 'preempted', 'crashed', 'canceled', 'unknown')
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
        # Map termination_cause to valid infra_outcome
        return TERMINATION_CAUSE_TO_INFRA_OUTCOME.get(termination_cause, 'unknown')
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
- [ ] Fix `_get_exit_code()` in BOTH `daemon.py:844` and `gce_launcher.py:738`
- [ ] Write comprehensive test suite (TDD)
- [ ] Implement state machine core (transitions, CAS, audit)
- [ ] Write migration script
- [ ] Add backwards compatibility layer
- [ ] Add `force_terminate_run`, `force_complete_run`, `force_fail_run` admin tools
- [ ] Add `completed_with_warnings` column for non-critical finalization failures

### Phase 2: Daemon Rewrite + Integration
- [ ] **Rewrite `daemon.py`** around state machine (not wrap - replace if/then/else mess)
- [ ] Rewrite `stage_executor.refresh_status_once()` to return events instead of mutating
- [ ] Wire state machine into `stage_executor.run_stage()`
- [ ] Update `cancel()` MCP tool to emit `USER_CANCEL` event
- [ ] Update all status readers to use new columns (70+ call sites)
- [ ] Add transition logging/monitoring
- [ ] **Add UNKNOWN cleanup job** (periodic investigation + admin tool resolution)
- [ ] Add audit retention policy (90 days / 100 per run)
- [ ] Test with real workloads

### Phase 3: Cleanup
- [ ] Remove backwards compatibility layer
- [ ] Drop old `status`/`progress` columns
- [ ] Update documentation

### Phase 4: Enhancements (Future)
- [ ] Add `"stop"` signal to metadata bus for AI_STOPPED
- [ ] Add intelligent timeout detection
- [ ] Add auto-recovery from TERMINATED

---

## Feedback Responses (v3 Review)

### Q1: FINALIZE_FAIL with critical=False → COMPLETED visibility?

**Answer**: Yes, this needs surfacing. Add `completed_with_warnings: bool` column:

```sql
ALTER TABLE stage_runs ADD COLUMN completed_with_warnings INTEGER DEFAULT 0;
```

Set to 1 when `FINALIZE_FAIL(critical=False)` → COMPLETED. Dashboard shows warning icon.

The audit trail (`stage_state_transitions`) already captures the `FINALIZE_FAIL` event with details, so the full history is preserved.

### Q2: Audit table growth with phase updates?

**Answer**: Phase updates are lower frequency than implied:
- Phases change ~7 times per run (not per poll)
- Polling doesn't emit events unless state/phase actually changes
- Still, add retention policy:

```sql
-- Retention: Keep all from last 90 days, plus last 100 per run for older runs
-- This ensures recent history is complete and old runs still have audit trail

-- Delete old transitions, keeping most recent 100 per run
DELETE FROM stage_state_transitions
WHERE id IN (
    SELECT t.id
    FROM stage_state_transitions t
    WHERE t.created_at < datetime('now', '-90 days')
      AND (
          SELECT COUNT(*)
          FROM stage_state_transitions t2
          WHERE t2.stage_run_id = t.stage_run_id
            AND t2.created_at > t.created_at
      ) >= 100  -- Keep 100 most recent per run
);
```

Run weekly via daemon or cron. The 100-per-run limit is conservative (most runs have <20 transitions).

### Q3: Phase update retry on rejection?

**Answer**: **Drop, don't retry.** If state changed, the phase info is stale. The new state transition will set appropriate phase anyway.

```python
def update_phase(run_id: str, expected_state: str, new_phase: ProgressPhase) -> bool:
    success = _do_cas_update(...)
    if not success:
        # State changed underneath us - our phase info is stale
        # Don't retry, don't log error - this is expected behavior
        logger.debug(f"Phase update rejected for {run_id} (state changed)")
    return success
```

### Q4: Migration edge case - 25-day orphaned run?

**Answer**: Good catch. Migration script uses `check_orphan_status()` (see Migration section) with three-way return:
- `confirmed_orphaned`: Backend API confirms instance/container gone → TERMINATED
- `possibly_orphaned`: Can't verify but >30 days old → UNKNOWN (manual review)
- `not_orphaned`: Instance still exists → keep as active state

This avoids falsely marking active runs and provides an escape hatch (UNKNOWN) for ambiguous cases. Daemon cleans up UNKNOWN runs in Phase 2 via admin tool investigation.

### Suggestions Incorporated

1. **completed_at on terminal transitions**: Already set by existing `_finalize_stage_run()`. The state machine preserves this - `completed_at` is set when transitioning to any terminal state.

2. **UNKNOWN cleanup job**: Moved to Phase 2. Periodic job investigates UNKNOWN runs and resolves via admin tools.

---

## Open Questions

### Resolved in v3

- ✅ **Phase-only updates**: Use CAS with `expected_state` guard, atomic with audit (see "Phase Updates" section)
- ✅ **Initial state for new runs**: PREPARING with phase=gcs_check, set atomically on creation (see "Initial State" section)
- ✅ **EXIT_MISSING guard**: Requires `instance_confirmed_dead=True` via `verify_instance_stopped()` (see "Exit Code Detection" section)
- ✅ **infra_outcome mapping**: FAILED → completed (infra worked), TERMINATED → termination_cause (see "Relationship to run_results" section)

### Still Open

1. **Daemon deployment model**: Keep as separate process (current) or integrate into MCP server?
2. **Metrics sync during polling**: Every poll or only on transitions?
3. **Partial finalization outputs**: What to do with them on CANCELED?
4. **AI_STOPPED container handling**: Container-side handler for "stop" metadata signal (write exit code, cleanup, shut down gracefully)
