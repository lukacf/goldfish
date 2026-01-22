"""Tests for state machine utility functions."""

from __future__ import annotations

from goldfish.state_machine.types import StageState, TransitionResult
from goldfish.state_machine.utils import format_transition_result


class TestFormatTransitionResult:
    """Tests for format_transition_result()."""

    def test_format_successful_transition(self) -> None:
        """format_transition_result formats successful transition correctly."""
        result = TransitionResult(
            success=True,
            new_state=StageState.CANCELED,
            reason=None,
        )

        formatted = format_transition_result(result, "stage-abc123", "running")

        assert formatted["success"] is True
        assert formatted["run_id"] == "stage-abc123"
        assert formatted["previous_state"] == "running"
        assert formatted["new_state"] == "canceled"
        assert formatted["reason"] == ""

    def test_format_failed_transition(self) -> None:
        """format_transition_result formats failed transition correctly."""
        result = TransitionResult(
            success=False,
            new_state=None,
            reason="no_valid_transition",
        )

        formatted = format_transition_result(result, "stage-xyz789", "completed")

        assert formatted["success"] is False
        assert formatted["run_id"] == "stage-xyz789"
        assert formatted["previous_state"] == "completed"
        assert formatted["new_state"] is None
        assert formatted["reason"] == "no_valid_transition"

    def test_format_with_none_reason(self) -> None:
        """format_transition_result returns empty string when reason is None."""
        result = TransitionResult(
            success=True,
            new_state=StageState.COMPLETED,
            reason=None,
        )

        formatted = format_transition_result(result, "stage-def456", "finalizing")

        assert formatted["reason"] == ""

    def test_format_idempotent_transition(self) -> None:
        """format_transition_result handles idempotent success case."""
        result = TransitionResult(
            success=True,
            new_state=StageState.CANCELED,
            reason="already_in_target_state",
        )

        formatted = format_transition_result(result, "stage-111222", "canceled")

        assert formatted["success"] is True
        assert formatted["previous_state"] == "canceled"
        assert formatted["new_state"] == "canceled"
        assert formatted["reason"] == "already_in_target_state"

    def test_format_with_none_previous_state(self) -> None:
        """format_transition_result handles None previous_state."""
        result = TransitionResult(
            success=False,
            new_state=None,
            reason="not_found",
        )

        formatted = format_transition_result(result, "stage-000000", None)

        assert formatted["success"] is False
        assert formatted["run_id"] == "stage-000000"
        assert formatted["previous_state"] is None
        assert formatted["new_state"] is None
        assert formatted["reason"] == "not_found"
