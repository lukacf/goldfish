"""Tests for backwards compatibility module (legacy status/progress mapping)."""

from __future__ import annotations

import pytest

from goldfish.state_machine.backwards_compat import (
    _STATE_TO_LEGACY_PROGRESS,
    _STATE_TO_LEGACY_STATUS,
    _TERMINATION_CAUSE_TO_STATUS,
    get_legacy_progress,
    get_legacy_status,
    state_from_legacy,
)
from goldfish.state_machine.types import (
    ProgressPhase,
    StageState,
    TerminationCause,
)


class TestGetLegacyStatus:
    """Tests for get_legacy_status function."""

    def test_running_state(self) -> None:
        """Active states map to status='running'."""
        assert get_legacy_status(StageState.RUNNING) == "running"
        assert get_legacy_status(StageState.BUILDING) == "running"
        assert get_legacy_status(StageState.LAUNCHING) == "running"
        assert get_legacy_status(StageState.FINALIZING) == "running"

    def test_preparing_state(self) -> None:
        """Test mapping for PREPARING state (maps to pending)."""
        assert get_legacy_status(StageState.PREPARING) == "pending"

    def test_completed_state(self) -> None:
        """Test mapping for COMPLETED state."""
        assert get_legacy_status(StageState.COMPLETED) == "completed"

    def test_failed_terminated_unknown_map_to_failed(self) -> None:
        """FAILED, TERMINATED, and UNKNOWN map to status='failed'."""
        assert get_legacy_status(StageState.FAILED) == "failed"
        assert get_legacy_status(StageState.TERMINATED) == "failed"
        assert get_legacy_status(StageState.UNKNOWN) == "failed"

    def test_canceled_maps_to_canceled(self) -> None:
        """CANCELED maps to status='canceled'."""
        assert get_legacy_status(StageState.CANCELED) == "canceled"

    def test_all_states_have_mappings(self) -> None:
        """Test that all StageState values have explicit mappings in the dict."""
        for state in StageState:
            assert (
                state in _STATE_TO_LEGACY_STATUS
            ), f"StageState.{state.name} has no explicit mapping in _STATE_TO_LEGACY_STATUS"

    def test_all_termination_causes_have_mappings(self) -> None:
        """Test that all TerminationCause values have explicit mappings in the dict."""
        for cause in TerminationCause:
            assert (
                cause in _TERMINATION_CAUSE_TO_STATUS
            ), f"TerminationCause.{cause.name} has no explicit mapping in _TERMINATION_CAUSE_TO_STATUS"


class TestGetLegacyProgress:
    """Tests for get_legacy_progress function."""

    def test_progress_for_active_states(self) -> None:
        assert get_legacy_progress(StageState.BUILDING, ProgressPhase.DOCKER_BUILD) == "building"
        assert get_legacy_progress(StageState.LAUNCHING, ProgressPhase.INSTANCE_CREATE) == "launching"
        assert get_legacy_progress(StageState.RUNNING, ProgressPhase.CODE_EXECUTION) == "running"
        assert get_legacy_progress(StageState.FINALIZING, ProgressPhase.OUTPUT_SYNC) == "finalizing"

    def test_progress_none_for_preparing_and_terminal(self) -> None:
        assert get_legacy_progress(StageState.PREPARING, ProgressPhase.GCS_CHECK) is None
        assert get_legacy_progress(StageState.COMPLETED, None) is None
        assert get_legacy_progress(StageState.FAILED, None) is None
        assert get_legacy_progress(StageState.TERMINATED, None) is None
        assert get_legacy_progress(StageState.CANCELED, None) is None
        assert get_legacy_progress(StageState.UNKNOWN, None) is None

    def test_all_states_have_progress_mappings(self) -> None:
        for state in StageState:
            assert state in _STATE_TO_LEGACY_PROGRESS


class TestStateFromLegacy:
    """Tests for state_from_legacy function."""

    def test_running_status(self) -> None:
        """Test conversion of running status."""
        state, cause = state_from_legacy("running")
        assert state == StageState.RUNNING
        assert cause is None

    def test_pending_status(self) -> None:
        """Test conversion of pending status to PREPARING."""
        state, cause = state_from_legacy("pending")
        assert state == StageState.PREPARING
        assert cause is None

    def test_completed_status(self) -> None:
        state, cause = state_from_legacy("completed")
        assert state == StageState.COMPLETED
        assert cause is None

    def test_failed_status(self) -> None:
        state, cause = state_from_legacy("failed")
        assert state == StageState.FAILED
        assert cause is None

    def test_canceled_status(self) -> None:
        state, cause = state_from_legacy("canceled")
        assert state == StageState.CANCELED
        assert cause is None

    def test_preempted_status(self) -> None:
        """Test conversion of preempted status to TERMINATED with PREEMPTED cause."""
        state, cause = state_from_legacy("preempted")
        assert state == StageState.TERMINATED
        assert cause == TerminationCause.PREEMPTED

    def test_crashed_status(self) -> None:
        """Test conversion of crashed status to TERMINATED with CRASHED cause."""
        state, cause = state_from_legacy("crashed")
        assert state == StageState.TERMINATED
        assert cause == TerminationCause.CRASHED

    def test_timed_out_status(self) -> None:
        """Test conversion of timed_out status to TERMINATED with TIMEOUT cause."""
        state, cause = state_from_legacy("timed_out")
        assert state == StageState.TERMINATED
        assert cause == TerminationCause.TIMEOUT

    def test_terminated_status(self) -> None:
        """Test conversion of terminated status to TERMINATED with None cause."""
        state, cause = state_from_legacy("terminated")
        assert state == StageState.TERMINATED
        assert cause is None

    def test_unknown_status_raises(self) -> None:
        """Test that unknown status raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            state_from_legacy("nonexistent_status")
        assert "Unknown legacy status" in str(exc_info.value)

    def test_roundtrip_for_standard_statuses(self) -> None:
        """Standard StageRunStatus values should roundtrip."""
        for status in ("pending", "running", "completed", "failed", "canceled"):
            recovered, cause = state_from_legacy(status)
            assert cause is None
            assert recovered in (
                StageState.PREPARING,
                StageState.RUNNING,
                StageState.COMPLETED,
                StageState.FAILED,
                StageState.CANCELED,
            )
