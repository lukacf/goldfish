"""Utility functions for state machine operations.

Shared helpers used by cancel and other state machine modules.
"""

from __future__ import annotations

from typing import Any

from goldfish.state_machine.types import TransitionResult


def format_transition_result(
    result: TransitionResult,
    run_id: str,
    previous_state: str | None,
) -> dict[str, Any]:
    """Format TransitionResult into API response dict.

    Args:
        result: TransitionResult from transition().
        run_id: The run ID that was operated on.
        previous_state: The state before transition attempt.

    Returns:
        Dict with success, previous_state, new_state, and reason.
    """
    return {
        "success": result.success,
        "run_id": run_id,
        "previous_state": previous_state,
        "new_state": result.new_state.value if result.new_state else None,
        "reason": result.reason or "",
    }
