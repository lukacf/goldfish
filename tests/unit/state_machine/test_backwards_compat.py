"""Tests for backwards compatibility module."""

from __future__ import annotations

import warnings

import pytest

from goldfish.state_machine.backwards_compat import (
    _STATE_TO_LEGACY_STATUS,
    _TERMINATION_CAUSE_TO_STATUS,
    get_legacy_status,
    state_from_legacy,
)
from goldfish.state_machine.types import (
    StageState,
    TerminationCause,
)


class TestGetLegacyStatus:
    """Tests for get_legacy_status function."""

    def test_emits_deprecation_warning(self) -> None:
        """Test that get_legacy_status emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            get_legacy_status(StageState.RUNNING)
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "deprecated" in str(w[0].message).lower()

    def test_running_state(self) -> None:
        """Test mapping for RUNNING state."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            assert get_legacy_status(StageState.RUNNING) == "running"

    def test_preparing_state(self) -> None:
        """Test mapping for PREPARING state (maps to pending)."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            assert get_legacy_status(StageState.PREPARING) == "pending"

    def test_completed_state(self) -> None:
        """Test mapping for COMPLETED state."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            assert get_legacy_status(StageState.COMPLETED) == "completed"

    def test_terminated_without_cause(self) -> None:
        """Test TERMINATED state without cause returns terminated."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            assert get_legacy_status(StageState.TERMINATED) == "terminated"

    def test_terminated_with_preempted(self) -> None:
        """Test TERMINATED with PREEMPTED cause returns preempted."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = get_legacy_status(
                StageState.TERMINATED,
                TerminationCause.PREEMPTED,
            )
            assert result == "preempted"

    def test_terminated_with_crashed(self) -> None:
        """Test TERMINATED with CRASHED cause returns crashed."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = get_legacy_status(
                StageState.TERMINATED,
                TerminationCause.CRASHED,
            )
            assert result == "crashed"

    def test_terminated_with_timeout(self) -> None:
        """Test TERMINATED with TIMEOUT cause returns timed_out."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = get_legacy_status(
                StageState.TERMINATED,
                TerminationCause.TIMEOUT,
            )
            assert result == "timed_out"

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


class TestStateFromLegacy:
    """Tests for state_from_legacy function."""

    def test_emits_deprecation_warning(self) -> None:
        """Test that state_from_legacy emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            state_from_legacy("running")
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)

    def test_running_status(self) -> None:
        """Test conversion of running status."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            state, cause = state_from_legacy("running")
            assert state == StageState.RUNNING
            assert cause is None

    def test_pending_status(self) -> None:
        """Test conversion of pending status to PREPARING."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            state, cause = state_from_legacy("pending")
            assert state == StageState.PREPARING
            assert cause is None

    def test_preempted_status(self) -> None:
        """Test conversion of preempted status to TERMINATED with PREEMPTED cause."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            state, cause = state_from_legacy("preempted")
            assert state == StageState.TERMINATED
            assert cause == TerminationCause.PREEMPTED

    def test_crashed_status(self) -> None:
        """Test conversion of crashed status to TERMINATED with CRASHED cause."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            state, cause = state_from_legacy("crashed")
            assert state == StageState.TERMINATED
            assert cause == TerminationCause.CRASHED

    def test_timed_out_status(self) -> None:
        """Test conversion of timed_out status to TERMINATED with TIMEOUT cause."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            state, cause = state_from_legacy("timed_out")
            assert state == StageState.TERMINATED
            assert cause == TerminationCause.TIMEOUT

    def test_terminated_status(self) -> None:
        """Test conversion of terminated status to TERMINATED with None cause."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            state, cause = state_from_legacy("terminated")
            assert state == StageState.TERMINATED
            # Roundtrip consistency: get_legacy_status(TERMINATED, None) → "terminated"
            # so state_from_legacy("terminated") → (TERMINATED, None)
            assert cause is None

    def test_unknown_status_raises(self) -> None:
        """Test that unknown status raises ValueError."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with pytest.raises(ValueError) as exc_info:
                state_from_legacy("nonexistent_status")
            assert "Unknown legacy status" in str(exc_info.value)

    def test_roundtrip_for_all_states(self) -> None:
        """Test roundtrip conversion for all states."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for state in StageState:
                legacy = get_legacy_status(state)
                recovered, cause = state_from_legacy(legacy)
                assert recovered == state
                assert cause is None

    def test_roundtrip_for_termination_causes_except_manual(self) -> None:
        """Test roundtrip conversion for termination causes.

        Note: MANUAL maps to "terminated" which maps back to (TERMINATED, None),
        not (TERMINATED, MANUAL). This is by design for roundtrip consistency
        with get_legacy_status(TERMINATED, None) → "terminated".
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for cause in TerminationCause:
                if cause == TerminationCause.MANUAL:
                    # MANUAL → "terminated" → (TERMINATED, None), not (TERMINATED, MANUAL)
                    legacy = get_legacy_status(StageState.TERMINATED, cause)
                    assert legacy == "terminated"
                    recovered_state, recovered_cause = state_from_legacy(legacy)
                    assert recovered_state == StageState.TERMINATED
                    assert recovered_cause is None  # Not MANUAL
                else:
                    legacy = get_legacy_status(StageState.TERMINATED, cause)
                    recovered_state, recovered_cause = state_from_legacy(legacy)
                    assert recovered_state == StageState.TERMINATED
                    assert recovered_cause == cause
