"""Type definitions for the Instance State Machine.

Defines states, events, and context for warm pool instance lifecycle.
Mirrors the pattern in types.py (stage run state machine).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Literal


class InstanceState(str, Enum):
    """All possible states for a warm pool instance.

    launching   - VM booting (first boot, not yet in pool)
    busy        - Executing a Docker job
    draining    - Job done, uploading outputs/logs
    idle_ready  - In idle loop, polling for signals, ready for claims
    claimed     - Claim sent, waiting for ACK
    deleting    - gcloud delete issued
    gone        - Confirmed deleted (terminal, row removable)
    """

    LAUNCHING = "launching"
    BUSY = "busy"
    DRAINING = "draining"
    IDLE_READY = "idle_ready"
    CLAIMED = "claimed"
    DELETING = "deleting"
    GONE = "gone"


class InstanceEvent(str, Enum):
    """Events that trigger instance state transitions."""

    BOOT_REGISTERED = "boot_registered"
    JOB_STARTED = "job_started"
    JOB_FINISHED = "job_finished"
    DRAIN_COMPLETE = "drain_complete"
    IDLE_READY = "idle_ready"
    CLAIM_SENT = "claim_sent"
    CLAIM_ACKED = "claim_acked"
    CLAIM_TIMEOUT = "claim_timeout"
    LAUNCH_FAILED = "launch_failed"
    PREEMPTED = "preempted"
    DELETE_REQUESTED = "delete_requested"
    DELETE_CONFIRMED = "delete_confirmed"
    DELETE_FAILED = "delete_failed"


# Source types for instance audit trail
InstanceSourceType = Literal["controller", "daemon", "executor", "warm_pool"]


@dataclass
class InstanceEventContext:
    """Context attached to each instance event for audit and decision-making."""

    timestamp: datetime
    source: InstanceSourceType

    # Run context
    stage_run_id: str | None = None

    # Error context
    error_message: str | None = None

    # Reason for deletion/transition
    reason: str | None = None


@dataclass
class InstanceTransitionResult:
    """Result of attempting an instance state transition."""

    success: bool
    new_state: InstanceState | None = None
    reason: str | None = None  # "ok", "not_found", "no_transition", "stale_state"
    details: str | None = None


@dataclass
class InstanceTransitionDef:
    """Definition of a single instance state transition."""

    from_state: InstanceState
    event: InstanceEvent
    to_state: InstanceState
