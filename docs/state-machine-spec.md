# Stage Execution State Machine Specification

> **Status**: Draft
> **Author**: Claude + Luka
> **Created**: 2025-01-15

## Overview

This document specifies a hierarchical state machine to replace the current ad-hoc state management in Goldfish stage execution. The goal is to eliminate state synchronization bugs, provide full audit trails, and enable reliable ghost run cleanup.

### Problems Being Solved

1. **Ghost/Zombie Runs**: ~70+ runs stuck in `status=running` for weeks with no cleanup mechanism
2. **False Exit Code Detection**: `_get_exit_code()` returns `1` when file doesn't exist, causing false failures
3. **Lost State Tracking**: Runs complete successfully but database state never updates
4. **State Desync**: `status` and `progress` columns managed independently, can get out of sync
5. **Scattered Updates**: 14+ places in code that mutate status with subtly different logic
6. **No Audit Trail**: No record of state transitions for debugging

### Design Principles

1. **Single Source of Truth**: State machine owns the state. No scattered updates.
2. **Event-Driven**: External systems emit events. State machine processes them.
3. **Explicit Transitions**: Every valid state change is documented in a transition table.
4. **Hierarchical**: Top-level states have sub-states for fine-grained observability.
5. **Fail Loudly**: Invalid transitions raise errors during development.
6. **Full Audit**: Every transition recorded with timestamp, event, and context.

---

## State Model

### Top-Level States

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
│  FAILED       │ Explicit failure (build, code, validation)              │
│  PREEMPTED    │ Spot instance preempted (terminal, future: retryable)   │
│  CRASHED      │ Instance disappeared without exit code                  │
│  CANCELED     │ User-initiated cancellation                             │
│  STOPPED      │ AI-initiated stop (during-run SVS detected issue)       │
│  ORPHANED     │ Lost track of instance (ghost run detection)            │
└─────────────────────────────────────────────────────────────────────────┘
```

### Hierarchical Sub-States

Each active top-level state has sub-states for detailed progress tracking.

#### PREPARING Sub-States

```python
class PreparingPhase(str, Enum):
    GCS_CHECK = "gcs_check"              # Verify GCS access
    VERSIONING = "versioning"            # Sync + commit + tag
    PIPELINE_LOAD = "pipeline_load"      # Load pipeline.yaml
    SVS_PREFLIGHT = "svs_preflight"      # Mechanistic validation
    CONFIG_LOAD = "config_load"          # Load stage config
    RECORD_CREATE = "record_create"      # Create DB record
    INPUT_RESOLVE = "input_resolve"      # Resolve input sources
    PRE_RUN_REVIEW = "pre_run_review"    # AI code review
    RECORD_UPDATE = "record_update"      # Update with resolved values
```

#### BUILDING Sub-States

```python
class BuildingPhase(str, Enum):
    IMAGE_CHECK = "image_check"          # Check if image exists
    DOCKERFILE_GEN = "dockerfile_gen"    # Generate Dockerfile
    DOCKER_BUILD = "docker_build"        # Execute docker build
    IMAGE_TAG = "image_tag"              # Tag image
```

#### LAUNCHING Sub-States

```python
class LaunchingPhase(str, Enum):
    ENTRYPOINT_GEN = "entrypoint_gen"    # Generate startup script
    INSTANCE_CREATE = "instance_create"  # Create GCE instance / container
    INSTANCE_PROVISIONING = "provisioning"  # GCE: PROVISIONING state
    INSTANCE_STAGING = "staging"         # GCE: STAGING state
    INSTANCE_STARTING = "starting"       # Container starting
```

#### RUNNING Sub-States

```python
class RunningPhase(str, Enum):
    CONTAINER_INIT = "container_init"    # goldfish.io bootstrap
    CODE_EXECUTION = "code_execution"    # User code running
    # Note: Schema validation, stats computation, and during-run SVS
    # happen within CODE_EXECUTION but are tracked via events, not sub-states
```

#### FINALIZING Sub-States

```python
class FinalizingPhase(str, Enum):
    OUTPUT_SYNC = "output_sync"          # Sync outputs to GCS
    OUTPUT_RECORDING = "output_recording"  # Record in signal_lineage
    LOG_FETCH = "log_fetch"              # Fetch logs from GCS/container
    METRICS_COLLECTION = "metrics_collection"  # Collect metrics.jsonl
    AUTO_RESULTS = "auto_results"        # Extract auto-results
    POST_RUN_REVIEW = "post_run_review"  # AI output review
    SVS_MANIFEST = "svs_manifest"        # Collect SVS findings
    PATTERN_EXTRACT = "pattern_extract"  # Extract failure patterns
    CLEANUP = "cleanup"                  # Delete instance/container
```

---

## Event Model

### Event Categories

#### Progress Events

```python
# Phase completion
PHASE_COMPLETE = "phase_complete"    # Sub-phase completed successfully
PHASE_FAIL = "phase_fail"            # Sub-phase failed

# Build events
BUILD_START = "build_start"
BUILD_OK = "build_ok"
BUILD_FAIL = "build_fail"

# Instance lifecycle (from GCE API / Docker)
INSTANCE_PROVISIONING = "instance_provisioning"
INSTANCE_STAGING = "instance_staging"
INSTANCE_RUNNING = "instance_running"
INSTANCE_TERMINATED = "instance_terminated"
INSTANCE_NOT_FOUND = "instance_not_found"
```

#### Exit Events

```python
# Exit code detection (CRITICAL: must distinguish these!)
EXIT_CODE_0 = "exit_code_0"              # exit_code.txt exists, value=0
EXIT_CODE_NONZERO = "exit_code_nonzero"  # exit_code.txt exists, value!=0
EXIT_CODE_MISSING = "exit_code_missing"  # exit_code.txt does NOT exist
```

#### SVS Events

```python
# Pre-execution SVS
SVS_PREFLIGHT_PASS = "svs_preflight_pass"
SVS_PREFLIGHT_BLOCK = "svs_preflight_block"
PRE_RUN_REVIEW_PASS = "pre_run_review_pass"
PRE_RUN_REVIEW_BLOCK = "pre_run_review_block"

# During execution SVS (informational, not state-changing unless stop requested)
SCHEMA_VALIDATION_PASS = "schema_validation_pass"
SCHEMA_VALIDATION_WARN = "schema_validation_warn"
SCHEMA_VALIDATION_BLOCK = "schema_validation_block"
DURING_RUN_FINDING = "during_run_finding"
DURING_RUN_STOP_REQUEST = "during_run_stop_request"

# Post execution SVS (informational)
POST_RUN_REVIEW_COMPLETE = "post_run_review_complete"
```

#### Failure Events

```python
PREEMPTION = "preemption"        # Spot instance preempted
TIMEOUT = "timeout"              # Heartbeat timeout exceeded
CRASH = "crash"                  # Instance gone, no exit code
```

#### User Events

```python
USER_CANCEL = "user_cancel"      # User requested cancellation
```

#### Recovery Events

```python
RECOVER = "recover"              # Retry orphaned run (future)
```

---

## Transition Table

### Core Transitions

```
From State    | Event                  | To State    | Guard/Notes
--------------|------------------------|-------------|---------------------------
PREPARING     | SVS_PREFLIGHT_BLOCK    | FAILED      | Preflight validation failed
PREPARING     | PRE_RUN_REVIEW_BLOCK   | FAILED      | AI review blocked run
PREPARING     | PHASE_FAIL             | FAILED      | Any preparation phase failed
PREPARING     | BUILD_START            | BUILDING    | Preparation complete
PREPARING     | USER_CANCEL            | CANCELED    |
              |                        |             |
BUILDING      | BUILD_OK               | LAUNCHING   |
BUILDING      | BUILD_FAIL             | FAILED      |
BUILDING      | USER_CANCEL            | CANCELED    |
BUILDING      | TIMEOUT                | FAILED      | Build timeout exceeded
              |                        |             |
LAUNCHING     | INSTANCE_RUNNING       | RUNNING     | Instance/container up
LAUNCHING     | PREEMPTION             | PREEMPTED   | Preempted during launch
LAUNCHING     | TIMEOUT                | ORPHANED    | Guard: no exit_code exists
LAUNCHING     | TIMEOUT                | FAILED      | Guard: exit_code exists (rare)
LAUNCHING     | USER_CANCEL            | CANCELED    |
              |                        |             |
RUNNING       | EXIT_CODE_0            | FINALIZING  | Successful exit
RUNNING       | EXIT_CODE_NONZERO      | FAILED      | Non-zero exit code
RUNNING       | EXIT_CODE_MISSING      | CRASHED     | No exit code = crashed
RUNNING       | PREEMPTION             | PREEMPTED   |
RUNNING       | DURING_RUN_STOP_REQUEST| STOPPED     | AI requested stop
RUNNING       | TIMEOUT                | ORPHANED    | Guard: no exit_code
RUNNING       | TIMEOUT                | CRASHED     | Guard: instance terminated
RUNNING       | USER_CANCEL            | CANCELED    |
              |                        |             |
FINALIZING    | PHASE_COMPLETE         | COMPLETED   | All finalization done
FINALIZING    | PHASE_FAIL             | FAILED      | Critical finalization failed
FINALIZING    | USER_CANCEL            | CANCELED    | (outputs may be partial)
              |                        |             |
ORPHANED      | RECOVER                | PREPARING   | Future: retry mechanism
```

### SVS Enforcement Behavior

SVS validation failures are configurable per-check type. The enforcement level determines whether a validation failure emits a blocking event or just a warning.

```yaml
# Example configuration (goldfish.yaml)
svs:
  enabled: true
  enforcement:
    preflight: blocking        # SVS_PREFLIGHT_BLOCK → FAILED
    pre_run_review: blocking   # PRE_RUN_REVIEW_BLOCK → FAILED
    schema_validation: warning # SCHEMA_VALIDATION_WARN (no state change)
    during_run: warning        # DURING_RUN_FINDING (logged only)
    post_run: silent           # POST_RUN_REVIEW_COMPLETE (informational)
```

When `enforcement=blocking`, the corresponding `*_BLOCK` event is emitted and triggers a transition to FAILED. When `enforcement=warning` or `silent`, only informational events are emitted (no state transition).

---

## State Diagram

```
                                    USER_CANCEL (from any active state)
                                              │
                                              ▼
                                        ┌──────────┐
                                        │ CANCELED │
                                        └──────────┘

┌───────────┐                                              ┌───────────┐
│  PENDING  │─────────────────────────────────────────────▶│  FAILED   │
└───────────┘  SVS_PREFLIGHT_BLOCK / PRE_RUN_REVIEW_BLOCK  └───────────┘
      │                                                          ▲
      │ (implicit: record created)                               │
      ▼                                                          │
┌───────────┐  BUILD_FAIL                                        │
│ PREPARING │────────────────────────────────────────────────────┤
└───────────┘                                                    │
      │                                                          │
      │ BUILD_START                                              │
      ▼                                                          │
┌───────────┐  BUILD_FAIL                                        │
│ BUILDING  │────────────────────────────────────────────────────┤
└───────────┘                                                    │
      │                                                          │
      │ BUILD_OK                                                 │
      ▼                                                          │
┌───────────┐  PREEMPTION    ┌───────────┐                       │
│ LAUNCHING │───────────────▶│ PREEMPTED │                       │
└───────────┘                └───────────┘                       │
      │                                                          │
      │ INSTANCE_RUNNING           TIMEOUT (no exit code)        │
      │                                   │                      │
      ▼                                   ▼                      │
┌───────────┐  EXIT_CODE_NONZERO   ┌───────────┐                 │
│  RUNNING  │─────────────────────▶│  FAILED   │◀────────────────┤
└───────────┘                      └───────────┘                 │
      │                                   ▲                      │
      │                                   │                      │
      │ EXIT_CODE_MISSING                 │                      │
      │         │                         │                      │
      │         ▼                         │                      │
      │   ┌───────────┐                   │                      │
      │   │  CRASHED  │                   │                      │
      │   └───────────┘                   │                      │
      │                                   │                      │
      │ DURING_RUN_STOP_REQUEST           │                      │
      │         │                         │                      │
      │         ▼                         │                      │
      │   ┌───────────┐                   │                      │
      │   │  STOPPED  │                   │                      │
      │   └───────────┘                   │                      │
      │                                   │                      │
      │ PREEMPTION                        │                      │
      │         │                         │                      │
      │         ▼                         │                      │
      │   ┌───────────┐                   │                      │
      │   │ PREEMPTED │                   │                      │
      │   └───────────┘                   │                      │
      │                                   │                      │
      │ TIMEOUT (no exit code)            │                      │
      │         │                         │                      │
      │         ▼                         │                      │
      │   ┌───────────┐  RECOVER          │                      │
      │   │ ORPHANED  │─────────▶ PREPARING (future)             │
      │   └───────────┘                   │                      │
      │                                   │                      │
      │ EXIT_CODE_0                       │                      │
      ▼                                   │                      │
┌─────────────┐  PHASE_FAIL              │                      │
│ FINALIZING  │──────────────────────────┘                      │
└─────────────┘                                                  │
      │                                                          │
      │ PHASE_COMPLETE                                           │
      ▼                                                          │
┌───────────┐                                                    │
│ COMPLETED │                                                    │
└───────────┘                                                    │
```

---

## Database Schema

### New Table: `stage_state_transitions`

Records every state transition for audit trail and debugging.

```sql
CREATE TABLE stage_state_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage_run_id TEXT NOT NULL REFERENCES stage_runs(id),

    -- Transition details
    from_state TEXT NOT NULL,
    from_phase TEXT,              -- Sub-state (nullable)
    event TEXT NOT NULL,
    to_state TEXT NOT NULL,
    to_phase TEXT,                -- Sub-state (nullable)

    -- Context
    context_json TEXT,            -- Event-specific data (exit_code, error, etc.)
    created_at TEXT NOT NULL DEFAULT (datetime('now')),

    -- Indexes
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id)
);

CREATE INDEX idx_stage_transitions_run ON stage_state_transitions(stage_run_id, created_at);
CREATE INDEX idx_stage_transitions_state ON stage_state_transitions(to_state, created_at);
```

### Modified Table: `stage_runs`

Replace `status` + `progress` with unified `state` + `phase`:

```sql
-- Remove: status TEXT, progress TEXT
-- Add:
ALTER TABLE stage_runs ADD COLUMN state TEXT NOT NULL DEFAULT 'preparing';
ALTER TABLE stage_runs ADD COLUMN phase TEXT;  -- Current sub-state (nullable)
ALTER TABLE stage_runs ADD COLUMN state_updated_at TEXT;  -- Last state change
```

**Migration Note**: The `status` column will be replaced in-place. During development, invalid transitions will raise errors loudly to catch any code paths still using old patterns.

---

## Daemon Specification

### Purpose

The MCP-side daemon is the **event emitter** for infrastructure state changes. It polls active runs and emits events to the state machine.

### Polling Behavior

```python
class StageDaemon:
    """Background daemon that polls active runs and emits state events."""

    poll_interval: int = 30  # seconds

    def poll_active_runs(self):
        """Poll all runs in active states."""
        active_states = ['preparing', 'building', 'launching', 'running', 'finalizing']
        runs = db.get_runs_by_states(active_states)

        for run in runs:
            events = self.check_run(run)
            for event in events:
                state_machine.handle_event(run.id, event)

    def check_run(self, run: StageRun) -> list[Event]:
        """Check a single run and return events to emit."""
        events = []

        if run.state in ('launching', 'running'):
            # Check GCE instance status
            instance_state = gce.get_instance_status(run.id)

            if instance_state == 'RUNNING' and run.state == 'launching':
                events.append(Event.INSTANCE_RUNNING)

            elif instance_state == 'TERMINATED':
                exit_result = gcs.get_exit_code(run.id)
                if exit_result.exists:
                    if exit_result.code == 0:
                        events.append(Event.EXIT_CODE_0)
                    else:
                        events.append(Event.EXIT_CODE_NONZERO)
                else:
                    events.append(Event.EXIT_CODE_MISSING)

            elif instance_state is None:  # Not found
                elapsed = now() - run.started_at
                if elapsed > timeout_threshold:
                    exit_result = gcs.get_exit_code(run.id)
                    if exit_result.exists:
                        # Had exit code but instance gone
                        events.append(Event.CRASH)
                    else:
                        # Never got exit code
                        events.append(Event.TIMEOUT)

        return events
```

### Exit Code Detection (Critical Fix)

The current `_get_exit_code()` returns `1` when the file doesn't exist. This MUST be fixed:

```python
@dataclass
class ExitCodeResult:
    exists: bool
    code: int | None

def get_exit_code(run_id: str) -> ExitCodeResult:
    """Get exit code from GCS, distinguishing missing from failure."""
    try:
        content = gcs_cat(f"gs://{bucket}/runs/{run_id}/logs/exit_code.txt")
        return ExitCodeResult(exists=True, code=int(content.strip()))
    except NotFoundError:
        return ExitCodeResult(exists=False, code=None)
    except Exception:
        return ExitCodeResult(exists=False, code=None)
```

---

## Event Context

Each event carries context for debugging and audit:

```python
@dataclass
class EventContext:
    """Context attached to each event."""
    timestamp: datetime
    source: str              # 'daemon', 'mcp_tool', 'container'

    # Optional context fields
    exit_code: int | None = None
    error_message: str | None = None
    instance_state: str | None = None
    gcs_artifacts: list[str] | None = None
    svs_findings: list[dict] | None = None
    metrics_summary: dict | None = None
```

---

## Terminal State Semantics

| State | Meaning | Recoverable? | User Action |
|-------|---------|--------------|-------------|
| COMPLETED | Success | N/A | Finalize results |
| FAILED | Explicit failure | Manual retry | Fix issue, re-run |
| PREEMPTED | Spot preemption | Future: auto | Re-run (maybe on-demand) |
| CRASHED | Unexpected death | Manual retry | Check logs, re-run |
| CANCELED | User stopped | Manual retry | Re-run if needed |
| STOPPED | AI stopped | Manual retry | Review findings, fix, re-run |
| ORPHANED | Lost track | Future: auto | Manual cleanup or recover |

---

## Future Roadmap

### Phase 1: Core State Machine (Current)
- Implement state machine with transitions
- Fix `_get_exit_code()` to return `ExitCodeResult`
- Add `stage_state_transitions` table
- Replace scattered status updates

### Phase 2: Daemon Integration
- Implement polling daemon
- Emit events from daemon to state machine
- Add ORPHANED detection and cleanup

### Phase 3: Auto-Recovery
- PREEMPTED → auto-retry with backoff
- ORPHANED → auto-recover mechanism
- Configurable retry policies

### Phase 4: SVS Integration
- Granular enforcement per SVS check type
- STOPPED state triggers and handling
- During-run stop request flow

---

## Open Questions

1. **Library vs Custom**: Use `transitions` library or roll our own? Custom gives full control, library gives battle-tested edge case handling.

2. **Daemon Deployment**: Should daemon run as separate process, thread in MCP server, or triggered by cron/scheduler?

3. **Metrics Sync**: Should daemon sync metrics to DB on every poll, or only on state transitions?

4. **Partial Finalization**: If CANCELED during FINALIZING, how much cleanup do we do? Record partial outputs?
