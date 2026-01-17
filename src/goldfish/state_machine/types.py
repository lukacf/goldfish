"""Type definitions for the Stage Execution State Machine.

This module defines all enums, dataclasses, and type aliases used by the
state machine. All definitions match the specification in docs/state-machine-spec.md.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Literal


class StageState(str, Enum):
    """All possible states for a stage run.

    Active states (v1.2):
    - PREPARING, BUILDING, LAUNCHING, RUNNING: Execution phases
    - POST_RUN: Infrastructure wrap-up (was FINALIZING)
    - AWAITING_USER_FINALIZATION: Requires explicit user finalization

    Terminal states: COMPLETED, FAILED, TERMINATED, CANCELED
    Limbo states: UNKNOWN (for runs in indeterminate state, auto-cleanup after 24h)
    """

    # Active states
    PREPARING = "preparing"
    BUILDING = "building"
    LAUNCHING = "launching"
    RUNNING = "running"
    POST_RUN = "post_run"  # v1.2: renamed from FINALIZING (infrastructure wrap-up)
    AWAITING_USER_FINALIZATION = "awaiting_user_finalization"  # v1.2: new state

    # Terminal states
    COMPLETED = "completed"
    FAILED = "failed"
    TERMINATED = "terminated"
    CANCELED = "canceled"

    # Limbo state (not terminal - can transition out via timeout or admin actions)
    UNKNOWN = "unknown"


class StageEvent(str, Enum):
    """All events that can trigger state transitions.

    Events are emitted by:
    - Executor: BUILD_START, BUILD_OK, BUILD_FAIL, LAUNCH_OK, LAUNCH_FAIL, etc.
    - Daemon: EXIT_SUCCESS, EXIT_FAILURE, EXIT_MISSING, INSTANCE_LOST, TIMEOUT
    - MCP tools: USER_CANCEL, USER_FINALIZE
    - SVS: SVS_BLOCK
    """

    # Build events
    BUILD_START = "build_start"
    BUILD_OK = "build_ok"
    BUILD_FAIL = "build_fail"

    # Launch events
    LAUNCH_OK = "launch_ok"
    LAUNCH_FAIL = "launch_fail"

    # Exit events (from container)
    EXIT_SUCCESS = "exit_success"
    EXIT_FAILURE = "exit_failure"
    EXIT_MISSING = "exit_missing"  # No exit code file (crash/preemption)

    # Post-run events (v1.2: renamed from finalization)
    POST_RUN_OK = "post_run_ok"  # v1.2: renamed from FINALIZE_OK
    POST_RUN_FAIL = "post_run_fail"  # v1.2: renamed from FINALIZE_FAIL

    # Infrastructure events
    INSTANCE_LOST = "instance_lost"  # Instance disappeared (preemption, crash)
    TIMEOUT = "timeout"  # State-specific timeout exceeded

    # User events
    USER_CANCEL = "user_cancel"
    USER_FINALIZE = "user_finalize"  # v1.2: explicit finalization from finalize_run tool

    # Preparation events
    PREPARE_FAIL = "prepare_fail"  # Pre-execution validation failed
    SVS_BLOCK = "svs_block"  # SVS pre-run review blocked execution

    # AI/SVS events
    AI_STOP = "ai_stop"  # During-run SVS requested stop (via stop_requested file)


class TerminationCause(str, Enum):
    """Reason for TERMINATED state.

    Used to distinguish between different infrastructure failures
    that all result in TERMINATED state.
    """

    PREEMPTED = "preempted"  # Spot instance preempted (detected via GCE API)
    CRASHED = "crashed"  # Instance died without exit code
    ORPHANED = "orphaned"  # Lost track of instance (timeout, no evidence of run)
    TIMEOUT = "timeout"  # Exceeded configured timeout threshold
    AI_STOPPED = "ai_stopped"  # AI/SVS requested stop
    MANUAL = "manual"  # Manual termination (reserved for future use)


class ProgressPhase(str, Enum):
    """Sub-phases within each state for observability.

    These are metadata for UI/debugging, not true states with their own
    transition tables. They indicate "what's happening within this state"
    but don't affect transition logic.
    """

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

    # POST_RUN phases (v1.2: renamed from FINALIZING)
    OUTPUT_SYNC = "output_sync"
    OUTPUT_RECORDING = "output_recording"
    LOG_FETCH = "log_fetch"
    METRICS_COLLECTION = "metrics_collection"
    POST_RUN_REVIEW = "post_run_review"
    CLEANUP = "cleanup"


# Source types for audit trail
SourceType = Literal["mcp_tool", "executor", "daemon", "container", "migration"]


@dataclass
class EventContext:
    """Context attached to each event for audit and decision-making.

    This context is passed with every event and contains:
    - Information needed by guards to make decisions
    - Details to record in the audit trail
    - Timestamps for consistent ordering
    """

    # Required fields
    timestamp: datetime
    source: SourceType

    # Exit context
    exit_code: int | None = None
    exit_code_exists: bool = False  # CRITICAL: distinguishes missing from failure

    # Termination context
    termination_cause: TerminationCause | None = None
    instance_confirmed_dead: bool = False  # For EXIT_MISSING: verified instance is not running

    # Error context
    error_message: str | None = None

    # Progress context
    phase: ProgressPhase | None = None

    # GCS context (for handling outages)
    gcs_error: bool = False  # GCS unavailable when checking exit code
    gcs_outage_started: datetime | None = None  # When GCS outage was first detected

    # Post-run context (v1.2: renamed from finalization)
    critical: bool | None = None  # For POST_RUN_FAIL: True → FAILED, False → AWAITING_USER_FINALIZATION
    critical_phases_done: bool | None = None  # For TIMEOUT in POST_RUN: True → AWAITING_USER_FINALIZATION

    # SVS context
    svs_review_id: str | None = None  # FK to svs_reviews.id for SVS_BLOCK and AI_STOP events


@dataclass
class TransitionResult:
    """Result of attempting a state transition.

    Returned by transition() to indicate success/failure and provide details.
    """

    success: bool
    new_state: StageState | None = None
    reason: str | None = (
        None  # "ok", "not_found", "state_not_set", "invalid_state", "no_transition", "stale_state", "already_in_target_state"
    )
    details: str | None = None  # Additional error details


# Type alias for guard functions
GuardFunc = Callable[[EventContext], bool]


@dataclass
class TransitionDef:
    """Definition of a single state transition.

    Defines what happens when a specific event is received in a specific state.
    Guards are optional conditions that must pass for the transition to occur.
    """

    from_state: StageState
    event: StageEvent
    to_state: StageState
    guard: GuardFunc | None = None
    guard_name: str | None = None  # Human-readable name for debugging

    def __post_init__(self) -> None:
        """Set guard_name from guard function if not provided."""
        if self.guard is not None and self.guard_name is None:
            # Try to extract name from lambda or function
            self.guard_name = getattr(self.guard, "__name__", "anonymous")
