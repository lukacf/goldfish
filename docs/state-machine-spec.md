# Stage Execution State Machine Specification

> **Status**: Draft v2 (Post-Review)
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
- **AI-initiated stop (STOPPED state)** - Deferred until trigger mechanism exists
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
    AI_STOPPED = "ai_stopped"     # Future: AI requested stop
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

---

## Event Model

### Events

```python
class StageEvent(str, Enum):
    # Progress events
    PREPARE_COMPLETE = "prepare_complete"    # All preparation done
    BUILD_START = "build_start"              # Starting Docker build
    BUILD_OK = "build_ok"                    # Build succeeded
    BUILD_FAIL = "build_fail"                # Build failed
    LAUNCH_OK = "launch_ok"                  # Instance/container running
    FINALIZE_START = "finalize_start"        # Starting finalization
    FINALIZE_OK = "finalize_ok"              # Finalization complete
    FINALIZE_FAIL = "finalize_fail"          # Critical finalization failed

    # Exit events (CRITICAL: must distinguish these!)
    EXIT_SUCCESS = "exit_success"            # exit_code.txt exists, value=0
    EXIT_FAILURE = "exit_failure"            # exit_code.txt exists, value!=0
    EXIT_MISSING = "exit_missing"            # exit_code.txt does NOT exist

    # Failure/termination events
    SVS_BLOCK = "svs_block"                  # SVS preflight or pre-run review blocked
    INSTANCE_LOST = "instance_lost"          # Instance gone, termination_cause in context

    # User events
    USER_CANCEL = "user_cancel"

    # Admin events
    FORCE_TERMINATE = "force_terminate"      # Admin override for stuck runs
    FORCE_COMPLETE = "force_complete"        # Admin override to mark complete
```

### Event Context

```python
@dataclass
class EventContext:
    """Context attached to each event for audit and decision-making."""
    timestamp: datetime
    source: str                              # 'mcp_tool', 'daemon', 'container', 'admin'

    # Exit context
    exit_code: int | None = None
    exit_code_exists: bool = False           # CRITICAL: distinguishes missing from failure

    # Termination context
    termination_cause: TerminationCause | None = None

    # Error context
    error_message: str | None = None

    # Progress context
    phase: ProgressPhase | None = None

    # SVS context
    svs_decision: str | None = None          # 'approved', 'blocked', 'warning'
    svs_findings: list[dict] | None = None
```

---

## Transition Table

### Core Transitions

```
From State    | Event              | To State    | Guard                    | Notes
--------------|--------------------| ------------|--------------------------|------------------
PREPARING     | SVS_BLOCK          | FAILED      |                          | Preflight or pre-run blocked
PREPARING     | BUILD_START        | BUILDING    |                          | Preparation complete
PREPARING     | USER_CANCEL        | CANCELED    |                          |
PREPARING     | FORCE_TERMINATE    | TERMINATED  |                          | Admin override
              |                    |             |                          |
BUILDING      | BUILD_OK           | LAUNCHING   |                          |
BUILDING      | BUILD_FAIL         | FAILED      |                          |
BUILDING      | USER_CANCEL        | CANCELED    |                          |
BUILDING      | FORCE_TERMINATE    | TERMINATED  |                          | Admin override
              |                    |             |                          |
LAUNCHING     | LAUNCH_OK          | RUNNING     |                          | Instance confirmed running
LAUNCHING     | INSTANCE_LOST      | TERMINATED  |                          | Preemption, timeout, etc.
LAUNCHING     | USER_CANCEL        | CANCELED    |                          |
LAUNCHING     | FORCE_TERMINATE    | TERMINATED  |                          | Admin override
              |                    |             |                          |
RUNNING       | EXIT_SUCCESS       | FINALIZING  |                          | exit_code.txt=0
RUNNING       | EXIT_FAILURE       | FAILED      |                          | exit_code.txt!=0
RUNNING       | EXIT_MISSING       | TERMINATED  | cause=CRASHED            | No exit code written
RUNNING       | INSTANCE_LOST      | TERMINATED  |                          | Preemption, timeout, etc.
RUNNING       | USER_CANCEL        | CANCELED    |                          |
RUNNING       | FORCE_TERMINATE    | TERMINATED  |                          | Admin override
              |                    |             |                          |
FINALIZING    | FINALIZE_OK        | COMPLETED   |                          | All finalization done
FINALIZING    | FINALIZE_FAIL      | FAILED      | critical=True            | Critical step failed
FINALIZING    | FINALIZE_FAIL      | COMPLETED   | critical=False           | Non-critical, still complete
FINALIZING    | USER_CANCEL        | CANCELED    |                          | Partial outputs possible
FINALIZING    | FORCE_COMPLETE     | COMPLETED   |                          | Admin override
FINALIZING    | FORCE_TERMINATE    | TERMINATED  |                          | Admin override
```

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
def transition(run_id: str, event: Event, context: EventContext) -> TransitionResult:
    """Atomically transition state using compare-and-swap."""

    # 1. Read current state
    current = db.get_stage_run(run_id)
    if not current:
        return TransitionResult(success=False, reason="not_found")

    # 2. Find valid transition
    transition = find_transition(current.state, event)
    if not transition:
        return TransitionResult(success=False, reason="invalid_transition")

    # 3. Check guard
    if transition.guard and not transition.guard(context):
        return TransitionResult(success=False, reason="guard_failed")

    # 4. CAS update - only succeeds if state hasn't changed
    with db._conn() as conn:
        result = conn.execute(
            """UPDATE stage_runs
               SET state = ?, phase = ?, state_updated_at = ?,
                   termination_cause = ?, error = ?
               WHERE id = ? AND state = ?""",
            (transition.to_state, context.phase, now_iso(),
             context.termination_cause, context.error_message,
             run_id, current.state)
        )
        if result.rowcount == 0:
            return TransitionResult(success=False, reason="state_changed")

    # 5. Record audit trail
    db.record_transition(run_id, current.state, event, transition.to_state, context)

    return TransitionResult(success=True, new_state=transition.to_state)
```

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
def determine_exit_event(run_id: str) -> tuple[Event, EventContext]:
    result = get_exit_code(run_id)

    if result.exists:
        if result.code == 0:
            return Event.EXIT_SUCCESS, EventContext(exit_code=0, exit_code_exists=True)
        else:
            return Event.EXIT_FAILURE, EventContext(exit_code=result.code, exit_code_exists=True)
    else:
        return Event.EXIT_MISSING, EventContext(
            exit_code_exists=False,
            termination_cause=TerminationCause.CRASHED
        )
```

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

For runs stuck in FINALIZING where outputs are safe:

```python
@mcp.tool()
def force_complete_run(run_id: str, reason: str) -> dict:
    """Force a stuck FINALIZING run to COMPLETED.

    Use when finalization hangs but outputs are recorded.
    """
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

---

## Migration Strategy

### Phase 1: Add New Columns (Non-Breaking)

```sql
-- Migration 001: Add state machine columns
ALTER TABLE stage_runs ADD COLUMN state TEXT;
ALTER TABLE stage_runs ADD COLUMN phase TEXT;
ALTER TABLE stage_runs ADD COLUMN termination_cause TEXT;
ALTER TABLE stage_runs ADD COLUMN state_updated_at TEXT;

-- Populate from existing data
UPDATE stage_runs SET state = CASE
    WHEN status = 'pending' THEN 'preparing'
    WHEN status = 'running' AND progress = 'building' THEN 'building'
    WHEN status = 'running' AND progress = 'launching' THEN 'launching'
    WHEN status = 'running' AND progress = 'running' THEN 'running'
    WHEN status = 'running' AND progress = 'finalizing' THEN 'finalizing'
    WHEN status = 'running' AND started_at < datetime('now', '-7 days') THEN 'terminated'
    WHEN status = 'running' THEN 'running'
    WHEN status = 'completed' THEN 'completed'
    WHEN status = 'failed' AND error LIKE '%preempted%' THEN 'terminated'
    WHEN status = 'failed' THEN 'failed'
    WHEN status = 'canceled' THEN 'canceled'
    ELSE 'unknown'
END;

-- Set termination_cause for terminated runs
UPDATE stage_runs SET termination_cause = 'preempted'
WHERE state = 'terminated' AND error LIKE '%preempted%';

UPDATE stage_runs SET termination_cause = 'orphaned'
WHERE state = 'terminated' AND termination_cause IS NULL;
```

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

```python
def derive_infra_outcome(state: str, termination_cause: str | None) -> str:
    if state == 'completed':
        return 'completed'
    if state == 'canceled':
        return 'canceled'
    if state == 'terminated':
        return termination_cause or 'unknown'
    if state == 'failed':
        return 'crashed'  # Code failure, not infra
    return 'unknown'
```

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

1. **Daemon deployment model**: Separate process vs. integrated?
2. **Metrics sync during polling**: Every poll or only on transitions?
3. **Partial finalization outputs**: What to do with them on CANCELED?
4. **UNKNOWN state cleanup**: Periodic job to investigate and resolve?
