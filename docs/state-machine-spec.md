# Stage Execution State Machine Specification

> **Status**: Draft v3.5 (Minor Fixes)
> **Author**: Claude + Luka
> **Created**: 2025-01-15
> **Updated**: 2025-01-15

## v3.5 Changes (Minor Fixes)

**MEDIUM Fixes:**
1. **UNKNOWN auto-cleanup contradiction**: Clarified that UNKNOWN has TIMEOUT escape (not "no automatic transitions out")
2. **GCS outage path**: Added `gcs_error` and `gcs_outage_started` fields to EventContext for >1h escalation
3. **Guard-aware idempotency**: Idempotency check now verifies guard passes for this context (not just any transition)
4. **critical_phases_done persistence**: Added `output_sync_done`, `output_recording_done` columns to schema
5. **Q1 feedback update**: Documented that `completed_with_warnings` also set for TIMEOUT→COMPLETED

## v3.4 Changes (Agent Review Fixes)

This version addresses all CRITICAL and HIGH findings from the 5-agent review:

**CRITICAL Fixes:**
1. **TOCTOU race in `transition()`**: Moved database read inside the transaction
2. **`find_transition()` guard bug**: Now iterates ALL matches until a guard passes
3. **Leader election race**: Added `BEGIN IMMEDIATE` for atomic lease acquisition
4. **Missing INSTANCE_LOST from PREPARING**: Added transition for Cloud Build failures
5. **EXIT_MISSING GCS unavailability**: Added documentation for prolonged outage handling
6. **Non-existent functions**: Added clarification that code examples are proposals
7. **SQLite DROP COLUMN rollback**: Changed to table recreation (works on all SQLite versions)

**HIGH Fixes:**
8. **Guard context critical=None**: Changed guards to use explicit `is True`/`is False`
9. **No escape from UNKNOWN**: Added TIMEOUT transition from UNKNOWN
10. **Idempotency in `transition()`**: Returns success if already in target state
11. **`critical_phases_done` tracking**: Added detailed mechanism specification
12. **Migration transaction boundaries**: Fixed batch transactions with proper atomicity

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

### Code Examples in This Spec

**IMPORTANT**: All code examples in this specification are **proposals for implementation**, not references to existing code. Functions like `transition()`, `find_transition()`, `get_exit_code()`, `verify_instance_stopped()`, `detect_termination_cause()`, etc. are specifications of what needs to be built - they do not currently exist in the codebase.

Existing functions that need to be **fixed or replaced** during implementation:
- `daemon.py:_get_exit_code()` - returns `1` when file doesn't exist (bug)
- `gce_launcher.py:_get_exit_code()` - same bug
- `daemon.py:_check_orphaned_runs()` - if/then/else mess to be replaced by event emission
- `stage_executor.py:refresh_status_once()` - to be rewritten to return events

The spec code shows the **target design**. Implementation will adapt these to fit the existing Goldfish patterns (TypedDict returns, db._conn() context managers, error types, etc.).

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
    MANUAL = "manual"             # User cancel via MCP tool
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

    # GCS context (for handling outages)
    gcs_error: bool = False               # GCS unavailable when checking exit code
    gcs_outage_started: datetime | None = None  # When GCS outage was first detected (for >1h escalation)

    # Finalization context
    critical: bool = False                   # For FINALIZE_FAIL: True → FAILED, False → COMPLETED
    critical_phases_done: bool = False       # For TIMEOUT in FINALIZING: True → COMPLETED, False → FAILED

    # SVS context
    svs_decision: str | None = None          # 'approved', 'blocked', 'warning'
    svs_findings: list[dict] | None = None


# Allowed source values for EventContext.source
SOURCE_VALUES = {
    'mcp_tool',   # MCP tool invocation (cancel)
    'executor',   # StageExecutor during run orchestration
    'daemon',     # Background daemon polling
    'container',  # Container-side events (via metadata/GCS)
    'migration',  # Migration script
}
```

**Timestamp usage**: `EventContext.timestamp` is the canonical time source. The `state_entered_at` column uses `context.timestamp.isoformat()`, not `now()`. This ensures audit trail ordering matches actual event sequence.

**critical_phases_done**: For TIMEOUT events in FINALIZING state, this field determines whether we transition to COMPLETED (outputs were saved) or FAILED (outputs may be lost). Set to True if OUTPUT_SYNC and OUTPUT_RECORDING phases completed successfully before the timeout.

### Tracking critical_phases_done

The `critical_phases_done` field must be tracked during FINALIZING to determine outcome on TIMEOUT. The mechanism:

```python
# In finalization code (within _finalize_stage_run or equivalent)
class FinalizationTracker:
    """Tracks completion of critical finalization phases.

    Critical phases (in order):
    1. OUTPUT_SYNC - gsutil rsync from container to GCS
    2. OUTPUT_RECORDING - register outputs in signal_lineage table

    Non-critical phases:
    3. METRICS_SYNC - metrics.json upload (nice to have)
    4. CLEANUP - instance deletion, docker cleanup

    If timeout occurs after OUTPUT_SYNC + OUTPUT_RECORDING complete,
    the run is COMPLETED (with warnings). Otherwise, it's FAILED.
    """

    def __init__(self, run_id: str):
        self.run_id = run_id
        self.output_sync_done = False
        self.output_recording_done = False

    @property
    def critical_phases_done(self) -> bool:
        """True if all critical phases completed."""
        return self.output_sync_done and self.output_recording_done

    def mark_output_sync_done(self):
        """Called after gsutil rsync succeeds."""
        self.output_sync_done = True
        self._persist_progress()

    def mark_output_recording_done(self):
        """Called after signal_lineage INSERT succeeds."""
        self.output_recording_done = True
        self._persist_progress()

    def _persist_progress(self):
        """Persist progress to DB so daemon can check on timeout.

        Uses stage_runs.output_sync_done and output_recording_done columns.
        This allows daemon to determine critical_phases_done if
        the executor process dies during finalization.
        """
        with db._conn() as conn:
            conn.execute(
                """UPDATE stage_runs
                   SET output_sync_done = ?, output_recording_done = ?
                   WHERE id = ?""",
                (1 if self.output_sync_done else 0,
                 1 if self.output_recording_done else 0,
                 self.run_id)
            )

# Usage in finalization
tracker = FinalizationTracker(run_id)

# Step 1: Sync outputs (critical)
rsync_outputs_to_gcs(run_id)
tracker.mark_output_sync_done()

# Step 2: Record in signal_lineage (critical)
register_outputs_in_db(run_id, outputs)
tracker.mark_output_recording_done()

# Step 3: Sync metrics (non-critical)
try:
    sync_metrics(run_id)
except Exception as e:
    logger.warning(f"Metrics sync failed (non-critical): {e}")

# Step 4: Cleanup (non-critical)
try:
    cleanup_resources(run_id)
except Exception as e:
    logger.warning(f"Cleanup failed (non-critical): {e}")


# When daemon detects TIMEOUT in FINALIZING state:
def determine_timeout_outcome(run_id: str) -> EventContext:
    progress = db.get_finalization_progress(run_id)
    critical_done = (
        progress.get("output_sync_done") is True
        and progress.get("output_recording_done") is True
    )
    return EventContext(
        timestamp=datetime.now(UTC),
        source="daemon",
        critical_phases_done=critical_done,
    )
```

**Database storage**: The `phase_data` JSON column in `stage_runs` (or a separate `finalization_progress` table) stores the progress markers. This ensures the daemon can determine `critical_phases_done` even if the executor process died mid-finalization.

---

## Transition Table

### Core Transitions

```
From State    | Event              | To State    | Guard                    | Notes
--------------|--------------------| ------------|--------------------------|------------------
PREPARING     | BUILD_START        | BUILDING    |                          | Preparation complete
PREPARING     | PREPARE_FAIL       | FAILED      |                          | Pipeline/version/input error
PREPARING     | SVS_BLOCK          | FAILED      |                          | Preflight or pre-run blocked
PREPARING     | INSTANCE_LOST      | TERMINATED  |                          | Cloud Build disappeared
PREPARING     | TIMEOUT            | TERMINATED  | cause=TIMEOUT            | Stuck in preparation
PREPARING     | USER_CANCEL        | CANCELED    |                          |
              |                    |             |                          |
BUILDING      | BUILD_OK           | LAUNCHING   |                          |
BUILDING      | BUILD_FAIL         | FAILED      |                          |
BUILDING      | INSTANCE_LOST      | TERMINATED  |                          | Cloud Build instance died
BUILDING      | TIMEOUT            | TERMINATED  | cause=TIMEOUT            | Build took too long
BUILDING      | USER_CANCEL        | CANCELED    |                          |
              |                    |             |                          |
LAUNCHING     | LAUNCH_OK          | RUNNING     |                          | Instance confirmed running
LAUNCHING     | LAUNCH_FAIL        | FAILED      |                          | Quota, capacity, config error
LAUNCHING     | INSTANCE_LOST      | TERMINATED  |                          | Preemption during startup
LAUNCHING     | TIMEOUT            | TERMINATED  | cause=TIMEOUT            | No capacity after timeout
LAUNCHING     | USER_CANCEL        | CANCELED    |                          |
              |                    |             |                          |
RUNNING       | EXIT_SUCCESS       | FINALIZING  |                          | exit_code.txt=0
RUNNING       | EXIT_FAILURE       | FAILED      |                          | exit_code.txt!=0
RUNNING       | EXIT_MISSING       | TERMINATED  | instance_confirmed_dead  | No exit code + instance dead
RUNNING       | INSTANCE_LOST      | TERMINATED  |                          | Preemption, crash, etc.
RUNNING       | TIMEOUT            | TERMINATED  | cause=TIMEOUT            | Exceeded max runtime
RUNNING       | USER_CANCEL        | CANCELED    |                          |
              |                    |             |                          |
FINALIZING    | FINALIZE_OK        | COMPLETED   |                          | All finalization done
FINALIZING    | FINALIZE_FAIL      | FAILED      | ctx.critical=True        | Critical step failed
FINALIZING    | FINALIZE_FAIL      | COMPLETED   | ctx.critical=False       | Non-critical, still complete
FINALIZING    | INSTANCE_LOST      | TERMINATED  |                          | Preempted during finalization
FINALIZING    | TIMEOUT            | COMPLETED   | critical_phases_done     | Timeout but outputs saved
FINALIZING    | TIMEOUT            | FAILED      | !critical_phases_done    | Timeout, outputs lost
FINALIZING    | USER_CANCEL        | CANCELED    |                          | Partial outputs possible
              |                    |             |                          |
UNKNOWN       | TIMEOUT            | TERMINATED  |                          | Auto-cleanup after 24h
```

**UNKNOWN state**: Requires investigation. Auto-cleaned via TIMEOUT after 24h investigation period (configurable). TIMEOUT transition prevents runs from being stuck in UNKNOWN forever.

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

**Usage in transition()**: When `context.phase` is None and the state is changing, use `STATE_ENTRY_PHASES[to_state]` instead of COALESCE. COALESCE only applies for within-state phase preservation (shouldn't happen in transitions since transitions always change state).

**UNKNOWN transitions**: Runs can only enter UNKNOWN via migration (not normal operation). Once in UNKNOWN, runs exit via auto-cleanup: TIMEOUT after 24h investigation period (configurable) to prevent runs stuck forever.

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

### Supporting Types and Functions

```python
@dataclass
class TransitionResult:
    """Result of a state transition attempt."""
    success: bool
    new_state: str | None = None
    reason: str | None = None     # 'not_found', 'invalid_transition', 'guard_failed', 'state_changed'
    details: str | None = None    # Additional context for failures


@dataclass
class TransitionDef:
    """Definition of a valid state transition."""
    from_state: str
    event: StageEvent
    to_state: str
    guard: Callable[[EventContext], bool] | None = None


# Transition table - all valid transitions
# IMPORTANT: Guards must use explicit `is True` / `is False` checks, never truthiness.
# Using `not ctx.critical` is a bug because None is falsy (treats missing data as False).
TRANSITIONS: list[TransitionDef] = [
    # PREPARING: GCS checks, pre-run validation
    TransitionDef('preparing', StageEvent.BUILD_START, 'building'),
    TransitionDef('preparing', StageEvent.PREPARE_FAIL, 'failed'),
    TransitionDef('preparing', StageEvent.SVS_BLOCK, 'failed'),
    TransitionDef('preparing', StageEvent.INSTANCE_LOST, 'terminated'),  # Cloud Build disappeared
    TransitionDef('preparing', StageEvent.TIMEOUT, 'terminated'),
    TransitionDef('preparing', StageEvent.USER_CANCEL, 'canceled'),

    # BUILDING: Docker image build (local or Cloud Build)
    TransitionDef('building', StageEvent.BUILD_OK, 'launching'),
    TransitionDef('building', StageEvent.BUILD_FAIL, 'failed'),
    TransitionDef('building', StageEvent.INSTANCE_LOST, 'terminated'),
    TransitionDef('building', StageEvent.TIMEOUT, 'terminated'),
    TransitionDef('building', StageEvent.USER_CANCEL, 'canceled'),

    # LAUNCHING: Instance creation, container start
    TransitionDef('launching', StageEvent.LAUNCH_OK, 'running'),
    TransitionDef('launching', StageEvent.LAUNCH_FAIL, 'failed'),
    TransitionDef('launching', StageEvent.INSTANCE_LOST, 'terminated'),
    TransitionDef('launching', StageEvent.TIMEOUT, 'terminated'),
    TransitionDef('launching', StageEvent.USER_CANCEL, 'canceled'),

    # RUNNING: Stage code executing
    TransitionDef('running', StageEvent.EXIT_SUCCESS, 'finalizing'),
    TransitionDef('running', StageEvent.EXIT_FAILURE, 'failed'),
    TransitionDef('running', StageEvent.EXIT_MISSING, 'terminated',
                  guard=lambda ctx: ctx.instance_confirmed_dead is True),
    TransitionDef('running', StageEvent.INSTANCE_LOST, 'terminated'),
    TransitionDef('running', StageEvent.TIMEOUT, 'terminated'),
    TransitionDef('running', StageEvent.USER_CANCEL, 'canceled'),

    # FINALIZING: Output registration, metrics sync, cleanup
    # Guards use explicit `is True`/`is False` to avoid None truthiness bugs
    TransitionDef('finalizing', StageEvent.FINALIZE_OK, 'completed'),
    TransitionDef('finalizing', StageEvent.FINALIZE_FAIL, 'failed',
                  guard=lambda ctx: ctx.critical is True),
    TransitionDef('finalizing', StageEvent.FINALIZE_FAIL, 'completed',
                  guard=lambda ctx: ctx.critical is False),
    TransitionDef('finalizing', StageEvent.INSTANCE_LOST, 'terminated'),
    TransitionDef('finalizing', StageEvent.TIMEOUT, 'completed',
                  guard=lambda ctx: ctx.critical_phases_done is True),
    TransitionDef('finalizing', StageEvent.TIMEOUT, 'failed',
                  guard=lambda ctx: ctx.critical_phases_done is False),
    TransitionDef('finalizing', StageEvent.USER_CANCEL, 'canceled'),

    # UNKNOWN: Auto-cleanup via timeout
    TransitionDef('unknown', StageEvent.TIMEOUT, 'terminated'),  # Auto-cleanup after investigation period
]


def find_transition(from_state: str, event: StageEvent, context: EventContext) -> TransitionDef | None:
    """Find a valid transition definition for the given state and event.

    Iterates ALL matching transitions and returns the first one whose guard passes.
    This fixes the bug where FINALIZE_FAIL with critical=False would hit the wrong
    guard (the first match has guard=lambda ctx: ctx.critical which fails, but we
    need to try the second match with guard=lambda ctx: not ctx.critical).

    Returns None if no valid transition exists or no guard passes.
    """
    for t in TRANSITIONS:
        if t.from_state == from_state and t.event == event:
            # If no guard, or guard passes, this is our transition
            if t.guard is None or t.guard(context):
                return t
            # Otherwise, keep looking for another matching transition
    return None
```

### The Solution

```python
def transition(db: Database, run_id: str, event: StageEvent, context: EventContext) -> TransitionResult:
    """Atomically transition state using compare-and-swap.

    CRITICAL: State read AND state update happen in the SAME transaction.
    This eliminates TOCTOU (time-of-check-time-of-use) races where state
    could change between read and update.

    CRITICAL: State update AND audit insert happen in the SAME transaction.
    This ensures we never have state changes without audit or vice versa.

    IDEMPOTENCY: If we're already in a valid target state for this event,
    return success without making changes. This handles retries gracefully.
    """

    # ATOMIC TRANSACTION: Read + validate + update + audit all happen together
    # This fixes the TOCTOU race where state could change between read and update.
    with db._conn() as conn:
        # 1. Read current state INSIDE transaction
        row = conn.execute(
            "SELECT state, phase FROM stage_runs WHERE id = ?",
            (run_id,)
        ).fetchone()

        if not row:
            return TransitionResult(success=False, reason="not_found")

        current_state = row["state"]
        current_phase = row["phase"]

        # 2. Find valid transition (passes context so guards can be checked)
        transition_def = find_transition(current_state, event, context)

        # 3. Handle idempotency: if already in a valid target state, return success
        # This handles retries where the first attempt succeeded but response was lost
        #
        # IMPORTANT: Guard-aware idempotency check
        # We must verify that THIS context would have led to the current state,
        # not just that SOME transition for this event leads to current state.
        # Example: FINALIZE_FAIL with critical=True should NOT be idempotent
        # if we're in COMPLETED (that transition requires critical=False).
        if transition_def is None:
            # Check if we're already in a valid target state for this event
            # Must match: event, to_state=current_state, AND guard passes for this context
            for t in TRANSITIONS:
                if t.event == event and t.to_state == current_state:
                    # Check guard - must pass for THIS context
                    if t.guard is None or t.guard(context):
                        # Already in target state AND guard matches - idempotent success
                        return TransitionResult(
                            success=True,
                            new_state=current_state,
                            reason="already_in_target_state"
                        )
            # Not a valid transition and not already in valid target state
            return TransitionResult(
                success=False,
                reason="invalid_transition",
                details=f"No transition from {current_state} on {event}"
            )

        # 4. Determine if this is a "completed with warnings" case
        # Two scenarios: FINALIZE_FAIL(critical=False) or TIMEOUT in FINALIZING with outputs saved
        # IMPORTANT: Use explicit None check for critical to avoid truthiness bugs
        # (critical=None should NOT be treated as critical=False)
        completed_with_warnings = (
            transition_def.to_state == 'completed'
            and (
                (event == StageEvent.FINALIZE_FAIL and context.critical is False)
                or (event == StageEvent.TIMEOUT and context.critical_phases_done is True)
            )
        )

        # 5. Default termination_cause for TIMEOUT → TERMINATED only
        # termination_cause is ONLY for TERMINATED state (see "Termination Cause" section)
        # TIMEOUT → COMPLETED/FAILED should NOT set termination_cause
        termination_cause = context.termination_cause
        if (event == StageEvent.TIMEOUT
            and transition_def.to_state == 'terminated'
            and termination_cause is None):
            termination_cause = TerminationCause.TIMEOUT

        # 6. Determine phase for the new state
        # If context.phase provided, use it; otherwise use entry phase for new state
        new_phase = (
            context.phase.value if context.phase
            else STATE_ENTRY_PHASES.get(transition_def.to_state)
        )

        # 7. CAS update - only succeeds if state hasn't changed since our read
        # Since we read inside the same transaction, this is guaranteed to match
        # (no other writer can modify the row while we hold the transaction)
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
             context.timestamp.isoformat(),
             context.timestamp.isoformat(),
             termination_cause.value if termination_cause else None,
             context.error_message,
             1 if completed_with_warnings else 0,
             run_id, current_state)
        )

        if result.rowcount == 0:
            # This should not happen since we read inside the same transaction
            # But handle defensively in case of edge cases
            return TransitionResult(success=False, reason="state_changed")

        # 8. Audit insert - SAME transaction, guaranteed atomic with state change
        conn.execute(
            """INSERT INTO stage_state_transitions
               (stage_run_id, from_state, event, to_state, phase,
                termination_cause, exit_code, exit_code_exists,
                error_message, source, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, current_state, event.value, transition_def.to_state,
             new_phase,
             termination_cause.value if termination_cause else None,
             context.exit_code, 1 if context.exit_code_exists else 0,
             context.error_message, context.source,
             context.timestamp.isoformat())
        )
        # Transaction commits here - read + update + audit all succeed or all fail

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
    """Result of exit code retrieval with explicit status.

    CRITICAL: Distinguish between:
    - exists=True, code=N: File found with exit code N
    - exists=False, gcs_error=False: File genuinely doesn't exist (404)
    - exists=False, gcs_error=True: GCS unavailable, can't determine
    """
    exists: bool
    code: int | None
    gcs_error: bool = False        # NEW: True if GCS API failed (not 404)
    error: str | None = None


class GCSError(Exception):
    """GCS API error (not 404 NotFound)."""
    pass


def get_exit_code(run_id: str) -> ExitCodeResult:
    """Get exit code, distinguishing 'file missing' from 'GCS unavailable'.

    CRITICAL: GCS unavailability should NOT be treated as "file missing".
    If GCS is down, we can't know if the file exists - must retry later.
    """
    gcs_path = f"gs://{bucket}/runs/{run_id}/logs/exit_code.txt"

    for attempt in range(max_attempts):
        try:
            content = gsutil_cat(gcs_path)
            return ExitCodeResult(exists=True, code=int(content.strip()))

        except NotFoundError:
            # 404 - file genuinely doesn't exist
            if attempt < max_attempts - 1:
                time.sleep(retry_delay)  # GCS eventual consistency
                continue
            return ExitCodeResult(exists=False, code=None, gcs_error=False)

        except (ConnectionError, TimeoutError, GCSError) as e:
            # GCS unavailable - NOT the same as "file doesn't exist"
            # Do NOT return exists=False here - we don't know!
            logger.warning(f"GCS unavailable for {run_id} (attempt {attempt + 1}): {e}")
            if attempt < max_attempts - 1:
                time.sleep(retry_delay * 2)  # Longer wait for API issues
                continue
            return ExitCodeResult(
                exists=False, code=None,
                gcs_error=True,  # CRITICAL: Mark as GCS error, not "file missing"
                error=f"GCS unavailable: {e}"
            )

        except Exception as e:
            # Unknown error - treat as GCS error (conservative)
            logger.error(f"Unexpected error reading exit code for {run_id}: {e}")
            return ExitCodeResult(
                exists=False, code=None,
                gcs_error=True,
                error=str(e)
            )

    return ExitCodeResult(exists=False, code=None, gcs_error=False)
```

### Usage in Event Emission

```python
def determine_exit_event(run_id: str, backend: str) -> tuple[StageEvent, EventContext] | None:
    """Determine exit event based on exit code AND instance status.

    CRITICAL: EXIT_MISSING requires verification that instance is actually dead.
    Otherwise we risk marking running jobs as terminated due to GCS eventual
    consistency or delayed log sync.

    CRITICAL: GCS unavailability is NOT the same as "exit code missing".
    If we can't reach GCS, we must wait and retry - not assume failure.
    """
    result = get_exit_code(run_id)
    now = datetime.now(UTC)

    # Case 1: Exit code found
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

    # Case 2: GCS unavailable - can't determine, retry later
    # CRITICAL: This is NOT the same as "exit code missing"!
    # If GCS is down, we genuinely don't know the state - must wait for recovery.
    #
    # Track GCS outage start time in stage_runs.gcs_outage_started:
    # - First GCS error: set gcs_outage_started = now()
    # - GCS recovers: clear gcs_outage_started = NULL
    # - Outage duration = now() - gcs_outage_started
    #
    # For prolonged GCS outages (>1hr), the daemon should:
    # 1. Check if instance is still running (via GCE API, not GCS)
    # 2. If instance is dead AND GCS unavailable >1hr, emit EXIT_MISSING with
    #    termination_cause=ORPHANED and gcs_error=True in context for audit
    # 3. This prevents runs from being stuck forever due to GCS issues
    if result.gcs_error:
        # Track outage start time for escalation
        outage_started = get_gcs_outage_started(run_id)  # Returns datetime or None
        if outage_started is None:
            set_gcs_outage_started(run_id, now)
            outage_started = now

        outage_duration = now - outage_started
        if outage_duration > timedelta(hours=1):
            # Prolonged outage - check if instance is dead
            instance_dead = verify_instance_stopped(run_id, backend)
            if instance_dead:
                # Instance is dead but we can't read exit code - orphaned
                return StageEvent.EXIT_MISSING, EventContext(
                    exit_code_exists=False,
                    instance_confirmed_dead=True,
                    termination_cause=TerminationCause.ORPHANED,
                    gcs_error=True,
                    gcs_outage_started=outage_started,
                    error_message=f"GCS unavailable for {outage_duration}, instance dead",
                    timestamp=now, source="daemon"
                )
            # Instance still running - keep waiting
        logger.warning(f"GCS unavailable for {run_id} ({outage_duration}), will retry: {result.error}")
        return None

    # Case 3: Exit code genuinely doesn't exist (404)
    # But is the instance actually dead?
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
    For local: Check Docker exit reason; return ORPHANED if container vanished without trace

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

**Cause detection**: EXIT_MISSING must always detect the termination cause (PREEMPTED, CRASHED, or ORPHANED). For GCE, we check the operations API for preemption events. For local Docker, we check the container exit reason and return ORPHANED if the container vanished without trace, CRASHED for known failure modes (OOMKilled, Error).

---

## Database Schema

### Modified Table: `stage_runs`

```sql
-- Add new columns
ALTER TABLE stage_runs ADD COLUMN state TEXT
    CHECK(state IN ('preparing', 'building', 'launching', 'running', 'finalizing',
                    'completed', 'failed', 'terminated', 'canceled', 'unknown'));
ALTER TABLE stage_runs ADD COLUMN phase TEXT;
ALTER TABLE stage_runs ADD COLUMN termination_cause TEXT
    CHECK(termination_cause IS NULL OR termination_cause IN
          ('preempted', 'crashed', 'orphaned', 'timeout', 'ai_stopped', 'manual'));
ALTER TABLE stage_runs ADD COLUMN state_entered_at TEXT;    -- When state was entered (for timeouts)
ALTER TABLE stage_runs ADD COLUMN phase_updated_at TEXT;    -- When phase last changed (observability)
ALTER TABLE stage_runs ADD COLUMN completed_with_warnings INTEGER DEFAULT 0;  -- Non-critical finalization failures OR finalization timeout with outputs saved
ALTER TABLE stage_runs ADD COLUMN error TEXT;               -- Error message for failed/terminated states

-- Finalization progress tracking (for critical_phases_done determination)
-- If executor dies during finalization, daemon can check these flags
ALTER TABLE stage_runs ADD COLUMN output_sync_done INTEGER DEFAULT 0;      -- 1 if outputs synced to GCS
ALTER TABLE stage_runs ADD COLUMN output_recording_done INTEGER DEFAULT 0; -- 1 if signal_lineage recorded

-- GCS outage tracking (for >1h escalation)
ALTER TABLE stage_runs ADD COLUMN gcs_outage_started TEXT;  -- ISO timestamp when GCS outage first detected

-- Index for daemon polling (get_active_runs query)
CREATE INDEX idx_stage_runs_state ON stage_runs(state)
    WHERE state IN ('preparing', 'building', 'launching', 'running', 'finalizing');

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
    source TEXT NOT NULL
        CHECK(source IN ('mcp_tool', 'executor', 'daemon', 'container', 'migration')),

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

### Phase 1c: Migration Rollback Strategy

**CRITICAL**: Migrations can fail partway. Without rollback, the database is left in an inconsistent state.

```python
def migrate_with_rollback():
    """Migrate stage_runs with rollback capability.

    Strategy:
    1. Create backup table before migration
    2. Migrate in batches with progress tracking
    3. On failure, restore from backup
    """
    backup_table = f"stage_runs_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    with db._conn() as conn:
        # Step 1: Create backup
        conn.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM stage_runs")
        logger.info(f"Created backup: {backup_table}")

        # Step 2: Track migration progress
        conn.execute("""
            CREATE TABLE IF NOT EXISTS migration_progress (
                id INTEGER PRIMARY KEY,
                run_id TEXT UNIQUE,
                status TEXT,  -- 'pending', 'migrated', 'failed'
                error TEXT,
                migrated_at TEXT
            )
        """)

        # Step 3: Mark all runs as pending
        conn.execute("""
            INSERT OR IGNORE INTO migration_progress (run_id, status)
            SELECT id, 'pending' FROM stage_runs WHERE state IS NULL
        """)

    try:
        # Step 4: Migrate in batches
        # CRITICAL: Each batch is a single transaction. This ensures:
        # - Atomic commit of batch (all-or-nothing)
        # - Progress can be resumed if migration is interrupted
        # - Database isn't locked for the entire migration
        batch_size = 100
        while True:
            # Read pending runs outside transaction (non-blocking read)
            with db._conn() as conn:
                pending = conn.execute("""
                    SELECT run_id FROM migration_progress
                    WHERE status = 'pending' LIMIT ?
                """, (batch_size,)).fetchall()

            if not pending:
                break

            # Migrate batch in a SINGLE transaction
            # This ensures the batch is atomic: if any row fails, the batch rolls back
            # and we retry the individual failures in a separate pass
            with db._conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                batch_success = True
                failed_runs = []

                try:
                    for row in pending:
                        run_id = row["run_id"]
                        try:
                            # migrate_single_run should NOT open its own transaction
                            # It receives the connection and operates within our transaction
                            migrate_single_run_in_txn(conn, run_id)
                            conn.execute("""
                                UPDATE migration_progress
                                SET status = 'migrated', migrated_at = ?
                                WHERE run_id = ?
                            """, (datetime.now(UTC).isoformat(), run_id))
                        except Exception as e:
                            # Record failure but continue batch
                            logger.warning(f"Run {run_id} failed: {e}")
                            failed_runs.append((run_id, str(e)))

                    # Update failed runs within same transaction
                    for run_id, error in failed_runs:
                        conn.execute("""
                            UPDATE migration_progress
                            SET status = 'failed', error = ?
                            WHERE run_id = ?
                        """, (error, run_id))

                    conn.execute("COMMIT")
                    logger.info(f"Migrated batch: {len(pending) - len(failed_runs)} success, {len(failed_runs)} failed")

                except Exception as e:
                    conn.execute("ROLLBACK")
                    logger.error(f"Batch failed, rolling back: {e}")
                    raise

        # Step 5: Check for failures
        with db._conn() as conn:
            failures = conn.execute(
                "SELECT COUNT(*) FROM migration_progress WHERE status = 'failed'"
            ).fetchone()[0]

        if failures > 0:
            raise MigrationError(f"{failures} runs failed to migrate - manual review required")

        logger.info("Migration completed successfully")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        rollback_migration(backup_table)
        raise


def migrate_single_run_in_txn(conn, run_id: str):
    """Migrate a single run within an existing transaction.

    IMPORTANT: This function does NOT open a transaction - it operates
    within the caller's transaction. This allows batch atomicity.
    """
    row = conn.execute(
        "SELECT status, progress FROM stage_runs WHERE id = ?",
        (run_id,)
    ).fetchone()

    if not row:
        raise MigrationError(f"Run {run_id} not found")

    # Map old status/progress to new state
    new_state = map_legacy_to_state(row["status"], row["progress"])

    conn.execute("""
        UPDATE stage_runs
        SET state = ?, state_entered_at = ?
        WHERE id = ?
    """, (new_state, datetime.now(UTC).isoformat(), run_id))


def rollback_migration(backup_table: str):
    """Restore stage_runs from backup using table recreation.

    IMPORTANT: SQLite DROP COLUMN was added in version 3.35.0 (2021-03-12).
    Many production systems run older SQLite versions, so we use the
    portable table recreation approach that works on ALL SQLite versions.

    This approach:
    1. Drops the migrated table
    2. Renames backup to original name
    3. Recreates any indexes that were on the original table
    """
    logger.warning(f"Rolling back migration from {backup_table}")

    # Validate backup table name to prevent SQL injection
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', backup_table):
        raise MigrationError(f"Invalid backup table name: {backup_table}")

    with db._conn() as conn:
        # Use a single transaction for atomic rollback
        conn.execute("BEGIN IMMEDIATE")
        try:
            # Step 1: Verify backup table exists and has data
            backup_count = conn.execute(
                f"SELECT COUNT(*) FROM {backup_table}"  # noqa: S608 - validated above
            ).fetchone()[0]
            if backup_count == 0:
                raise MigrationError(f"Backup table {backup_table} is empty")

            # Step 2: Drop the partially-migrated table
            conn.execute("DROP TABLE IF EXISTS stage_runs")

            # Step 3: Rename backup to original name
            conn.execute(f"ALTER TABLE {backup_table} RENAME TO stage_runs")  # noqa: S608

            # Step 4: Recreate indexes (backup table won't have them)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_stage_runs_workspace
                ON stage_runs(workspace_name, version)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_stage_runs_status
                ON stage_runs(status)
            """)
            # Note: Don't create idx_stage_runs_state since we rolled back

            conn.execute("COMMIT")
            logger.info(f"Rollback complete: restored {backup_count} rows from {backup_table}")

        except Exception as e:
            conn.execute("ROLLBACK")
            logger.error(f"Rollback failed: {e}")
            raise MigrationError(f"Rollback failed: {e}") from e
```

### Phase 1d: Handling Active Runs During Migration

**Problem**: What if runs are actively executing while migration runs?

**Solution**: Migration window + drain mode

```python
def safe_migration():
    """Migration with active run handling."""
    # Step 1: Enable drain mode - no new runs can start
    with db._conn() as conn:
        conn.execute("INSERT INTO config (key, value) VALUES ('drain_mode', 'true')")

    # Step 2: Wait for active runs to complete (with timeout)
    timeout = timedelta(hours=2)
    start = datetime.now(UTC)

    while datetime.now(UTC) - start < timeout:
        with db._conn() as conn:
            active = conn.execute("""
                SELECT COUNT(*) FROM stage_runs
                WHERE status = 'running' AND progress NOT IN ('completed', 'failed')
            """).fetchone()[0]

        if active == 0:
            break

        logger.info(f"Waiting for {active} active runs to complete...")
        time.sleep(60)

    # Step 3: Check for stragglers
    with db._conn() as conn:
        active = conn.execute("""
            SELECT id FROM stage_runs
            WHERE status = 'running' AND progress NOT IN ('completed', 'failed')
        """).fetchall()

    if active:
        # Mark stragglers as UNKNOWN for manual review
        for row in active:
            logger.warning(f"Run {row['id']} still active after timeout - marking UNKNOWN")
            with db._conn() as conn:
                conn.execute("""
                    UPDATE stage_runs SET state = 'unknown'
                    WHERE id = ?
                """, (row["id"],))

    # Step 4: Run migration
    migrate_with_rollback()

    # Step 5: Disable drain mode
    with db._conn() as conn:
        conn.execute("DELETE FROM config WHERE key = 'drain_mode'")
```

**Key principles**:
1. **Drain mode**: Stop new runs before migration
2. **Graceful wait**: Give active runs time to complete
3. **Timeout handling**: Mark long-running stragglers as UNKNOWN
4. **Atomic transition**: Old code still works until migration complete

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

### Active Runs Query

The daemon polls `get_active_runs()` to find runs that need monitoring:

```python
def get_active_runs() -> list[dict]:
    """Get all runs in active (non-terminal) states for daemon polling.

    Uses the partial index on state column for efficient filtering.
    """
    with db._conn() as conn:
        rows = conn.execute("""
            SELECT id, workspace_name, version, stage_name, backend_type, backend_handle,
                   state, phase, state_entered_at, started_at
            FROM stage_runs
            WHERE state IN ('preparing', 'building', 'launching', 'running', 'finalizing')
            ORDER BY started_at ASC
        """).fetchall()
    return [dict(row) for row in rows]
```

**Performance note**: The partial index `idx_stage_runs_state` ensures this query remains fast even with millions of historical runs - it only indexes active states.

### Daemon Leader Election

**Problem**: Multiple daemon instances polling simultaneously could emit duplicate events, causing CAS failures and wasted work.

**Solution**: Use SQLite advisory locking with a lease-based leader election:

```python
class DaemonLeaderElection:
    """Simple leader election using SQLite row locking.

    Only one daemon can hold the lease at a time. Other daemons
    wait until the lease expires or is released.
    """

    LEASE_DURATION_SECONDS = 60
    LEASE_KEY = "daemon_leader"

    def __init__(self, db: Database):
        self.db = db
        self._ensure_table()

    def _ensure_table(self):
        with self.db._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daemon_leases (
                    key TEXT PRIMARY KEY,
                    holder_id TEXT NOT NULL,
                    acquired_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                )
            """)

    def try_acquire_lease(self, holder_id: str) -> bool:
        """Try to acquire or renew the daemon lease.

        Returns True if this daemon is the leader.

        CRITICAL: Uses BEGIN IMMEDIATE to acquire a write lock at transaction start.
        This prevents the race condition where:
        1. Two daemons both read "no lease exists"
        2. Both try to INSERT
        3. One wins, but both think they're leader

        With BEGIN IMMEDIATE, the second daemon blocks on step 1 until the first
        daemon's transaction commits, then sees the updated lease.
        """
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=self.LEASE_DURATION_SECONDS)

        with self.db._conn() as conn:
            # BEGIN IMMEDIATE acquires write lock immediately
            # This prevents TOCTOU race between INSERT and SELECT
            conn.execute("BEGIN IMMEDIATE")
            try:
                # Try to acquire expired or non-existent lease
                conn.execute("""
                    INSERT INTO daemon_leases (key, holder_id, acquired_at, expires_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        holder_id = excluded.holder_id,
                        acquired_at = excluded.acquired_at,
                        expires_at = excluded.expires_at
                    WHERE daemon_leases.holder_id = excluded.holder_id
                       OR daemon_leases.expires_at < ?
                """, (self.LEASE_KEY, holder_id, now.isoformat(),
                      expires_at.isoformat(), now.isoformat()))

                # Check if we hold the lease - guaranteed accurate due to write lock
                row = conn.execute("""
                    SELECT holder_id FROM daemon_leases WHERE key = ?
                """, (self.LEASE_KEY,)).fetchone()

                is_leader = row and row["holder_id"] == holder_id
                conn.execute("COMMIT")
                return is_leader
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def release_lease(self, holder_id: str):
        """Release the lease on shutdown."""
        with self.db._conn() as conn:
            conn.execute("""
                DELETE FROM daemon_leases
                WHERE key = ? AND holder_id = ?
            """, (self.LEASE_KEY, holder_id))


class StageDaemon:
    """Daemon with leader election."""

    def __init__(self, ...):
        self.holder_id = f"daemon-{uuid.uuid4().hex[:8]}"
        self.leader_election = DaemonLeaderElection(self.db)

    def run(self):
        """Main loop with leader election."""
        try:
            while not self._shutdown:
                if self.leader_election.try_acquire_lease(self.holder_id):
                    self.poll_active_runs()
                else:
                    logger.debug(f"Not leader, waiting...")
                time.sleep(self.poll_interval)
        finally:
            self.leader_election.release_lease(self.holder_id)
```

**Key properties**:
1. **Exactly-once delivery**: Only one daemon processes events at a time
2. **Automatic failover**: If leader dies, lease expires and another daemon takes over
3. **No external dependencies**: Uses SQLite, no need for Redis/etcd
4. **Graceful handoff**: Leader releases lease on shutdown

### Clock Skew Handling

**Problem**: Distributed timestamps (daemon, executor, container) could have different clock values, causing:
- Timeout calculations to be incorrect
- Audit trail ordering to be wrong
- CAS operations to behave unexpectedly

**Solution**: Use relative timestamps for timeouts, tolerate skew in audit trail:

```python
# 1. Timeout calculations use state_entered_at stored in DB
#    NOT "now() - run.started_at" which crosses clock domains
def check_timeout(run: dict) -> bool:
    """Check if run has timed out using stored timestamp."""
    state_entered = datetime.fromisoformat(run["state_entered_at"])
    timeout = get_timeout_for_state(run["state"])
    # Both timestamps from same source (daemon's clock)
    return datetime.now(UTC) - state_entered > timeout

# 2. EventContext.timestamp is set by the emitter
#    This means audit trail reflects emitter's clock, which is acceptable
#    for debugging/observability
context = EventContext(
    timestamp=datetime.now(UTC),  # Daemon's clock
    source="daemon"
)

# 3. CAS semantics don't depend on timestamps
#    We compare state values, not timestamps
result = conn.execute(
    "UPDATE stage_runs SET state = ? WHERE id = ? AND state = ?",
    (new_state, run_id, expected_state)  # No timestamp comparison
)
```

**Acceptable skew**: Up to 30 seconds of clock skew is tolerable:
- Timeouts have minutes of margin (300s, 1200s)
- Audit trail ordering is "best effort" - small inversions are fine for debugging
- CAS operations are timestamp-independent

**Mitigation for larger skew**:
- Ensure all Goldfish components use NTP-synced clocks
- GCE instances auto-sync via Google NTP
- Log warnings when container timestamps differ significantly from daemon timestamps

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
- [ ] Add `completed_with_warnings` column for non-critical finalization failures and finalization timeouts with outputs saved

### Phase 2: Daemon Rewrite + Integration
- [ ] **Rewrite `daemon.py`** around state machine (not wrap - replace if/then/else mess)
- [ ] Rewrite `stage_executor.refresh_status_once()` to return events instead of mutating
- [ ] Wire state machine into `stage_executor.run_stage()`
- [ ] Update `cancel()` MCP tool to emit `USER_CANCEL` event
- [ ] Update all status readers to use new columns (70+ call sites)
- [ ] Add transition logging/monitoring
- [ ] **Add UNKNOWN cleanup job** (auto-timeout after 24h investigation period)
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

Set to 1 in two scenarios:
1. `FINALIZE_FAIL(critical=False)` → COMPLETED (non-critical finalization failure)
2. `TIMEOUT(critical_phases_done=True)` → COMPLETED (finalization timed out but outputs were saved)

Dashboard shows warning icon. The audit trail (`stage_state_transitions`) captures the event with details, so the full history is preserved.

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

This avoids falsely marking active runs and provides an escape hatch (UNKNOWN) for ambiguous cases. Daemon auto-cleans UNKNOWN runs via TIMEOUT after 24h investigation period.

### Suggestions Incorporated

1. **completed_at on terminal transitions**: Already set by existing `_finalize_stage_run()`. The state machine preserves this - `completed_at` is set when transitioning to any terminal state.

2. **UNKNOWN cleanup job**: Moved to Phase 2. Periodic job investigates UNKNOWN runs and auto-resolves via TIMEOUT after 24h.

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
