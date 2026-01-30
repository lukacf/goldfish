"""Tests for event emission functions.

This module tests the event emission layer that replaces direct status updates.
Functions here determine what event to emit based on exit codes, GCS status,
and backend state.

TDD: These tests are written BEFORE the implementation.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

if TYPE_CHECKING:
    pass


class TestDetermineExitEvent:
    """Tests for determine_exit_event() function."""

    def test_exit_success_returns_exit_success_event(self) -> None:
        """Exit code 0 must return EXIT_SUCCESS event."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.exit_code import ExitCodeResult
        from goldfish.state_machine.types import StageEvent, StageState

        run = {"id": "stage-123", "state": StageState.RUNNING.value}
        exit_result = ExitCodeResult.from_code(0)

        result = determine_exit_event(run, exit_result, db=MagicMock())

        assert result is not None
        event, context = result
        assert event == StageEvent.EXIT_SUCCESS
        assert context.exit_code == 0
        assert context.exit_code_exists is True

    def test_exit_failure_returns_exit_failure_event(self) -> None:
        """Exit code non-zero must return EXIT_FAILURE event."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.exit_code import ExitCodeResult
        from goldfish.state_machine.types import StageEvent, StageState

        run = {"id": "stage-123", "state": StageState.RUNNING.value}
        exit_result = ExitCodeResult.from_code(1)

        result = determine_exit_event(run, exit_result, db=MagicMock())

        assert result is not None
        event, context = result
        assert event == StageEvent.EXIT_FAILURE
        assert context.exit_code == 1
        assert context.exit_code_exists is True

    def test_exit_missing_without_instance_verification_returns_none(self) -> None:
        """Missing exit code must return None unless instance is confirmed dead."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.exit_code import ExitCodeResult
        from goldfish.state_machine.types import StageState

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "local",
            "backend_handle": "container-123",
        }
        exit_result = ExitCodeResult.from_not_found()

        with patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = False
            result = determine_exit_event(run, exit_result, db=MagicMock())

        assert result is None

    def test_exit_missing_with_instance_confirmed_dead_returns_exit_missing_event(self) -> None:
        """Missing exit code with confirmed dead instance emits EXIT_MISSING with termination_cause."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.exit_code import ExitCodeResult
        from goldfish.state_machine.types import StageEvent, StageState, TerminationCause

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "local",
            "backend_handle": "container-123",
        }
        exit_result = ExitCodeResult.from_not_found()

        with (
            patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify,
            patch("goldfish.state_machine.event_emission.detect_termination_cause") as mock_cause,
        ):
            mock_verify.return_value = True
            mock_cause.return_value = TerminationCause.PREEMPTED
            result = determine_exit_event(run, exit_result, db=MagicMock())

        assert result is not None
        event, context = result
        assert event == StageEvent.EXIT_MISSING
        assert context.exit_code is None
        assert context.exit_code_exists is False
        assert context.instance_confirmed_dead is True
        assert context.termination_cause == TerminationCause.PREEMPTED

    def test_gcs_error_first_time_returns_none(self) -> None:
        """First GCS error must start outage clock and return None (wait and retry)."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.exit_code import ExitCodeResult
        from goldfish.state_machine.types import StageState

        run = {"id": "stage-123", "state": StageState.RUNNING.value, "gcs_outage_started": None}
        exit_result = ExitCodeResult.from_gcs_error("ServiceUnavailable")

        db = MagicMock()
        result = determine_exit_event(run, exit_result, db=db)

        # First GCS error: don't emit event, just record outage start
        assert result is None
        db.update_stage_run_gcs_outage.assert_called_once()

    def test_gcs_error_over_1h_returns_exit_missing(self) -> None:
        """GCS error >1h emits EXIT_MISSING only when instance is confirmed dead."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.exit_code import ExitCodeResult
        from goldfish.state_machine.types import StageEvent, StageState, TerminationCause

        # GCS outage started 2 hours ago
        outage_start = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "gcs_outage_started": outage_start,
            "backend_type": "local",
            "backend_handle": "container-123",
        }
        exit_result = ExitCodeResult.from_gcs_error("ServiceUnavailable")

        with (
            patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify,
            patch("goldfish.state_machine.event_emission.detect_termination_cause") as mock_cause,
        ):
            mock_verify.return_value = True
            mock_cause.return_value = TerminationCause.CRASHED
            result = determine_exit_event(run, exit_result, db=MagicMock())

        assert result is not None
        event, context = result
        assert event == StageEvent.EXIT_MISSING
        assert context.gcs_error is True
        assert context.instance_confirmed_dead is True
        assert context.termination_cause == TerminationCause.CRASHED

    def test_gcs_error_over_1h_without_instance_confirmed_dead_returns_none(self) -> None:
        """GCS error >1h does NOT emit EXIT_MISSING if instance is still running."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.exit_code import ExitCodeResult
        from goldfish.state_machine.types import StageState

        outage_start = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "gcs_outage_started": outage_start,
            "backend_type": "local",
            "backend_handle": "container-123",
        }
        exit_result = ExitCodeResult.from_gcs_error("ServiceUnavailable")

        with patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = False
            result = determine_exit_event(run, exit_result, db=MagicMock())

        assert result is None

    def test_gcs_error_under_1h_returns_none(self) -> None:
        """GCS error <1h must return None (keep waiting)."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.exit_code import ExitCodeResult
        from goldfish.state_machine.types import StageState

        # GCS outage started 30 minutes ago
        outage_start = (datetime.now(UTC) - timedelta(minutes=30)).isoformat()
        run = {"id": "stage-123", "state": StageState.RUNNING.value, "gcs_outage_started": outage_start}
        exit_result = ExitCodeResult.from_gcs_error("ServiceUnavailable")

        result = determine_exit_event(run, exit_result, db=MagicMock())

        assert result is None  # Keep waiting

    def test_exit_137_indicates_killed(self) -> None:
        """Exit code 137 (SIGKILL) must be treated as definite failure."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.exit_code import ExitCodeResult
        from goldfish.state_machine.types import StageEvent, StageState

        run = {"id": "stage-123", "state": StageState.RUNNING.value}
        exit_result = ExitCodeResult.from_code(137)

        result = determine_exit_event(run, exit_result, db=MagicMock())

        assert result is not None
        event, context = result
        assert event == StageEvent.EXIT_FAILURE
        assert context.exit_code == 137


class TestGCSOutageTracking:
    """Tests for GCS outage tracking functions."""

    def test_get_gcs_outage_started_returns_none_when_not_set(self) -> None:
        """get_gcs_outage_started must return None when not set."""
        from goldfish.state_machine.event_emission import get_gcs_outage_started

        run = {"id": "stage-123", "gcs_outage_started": None}
        result = get_gcs_outage_started(run)
        assert result is None

    def test_get_gcs_outage_started_returns_datetime(self) -> None:
        """get_gcs_outage_started must parse ISO timestamp."""
        from goldfish.state_machine.event_emission import get_gcs_outage_started

        now = datetime.now(UTC)
        run = {"id": "stage-123", "gcs_outage_started": now.isoformat()}
        result = get_gcs_outage_started(run)
        assert result is not None
        # Allow small time difference due to serialization
        assert abs((result - now).total_seconds()) < 1

    def test_set_gcs_outage_started_stores_timestamp(self) -> None:
        """set_gcs_outage_started must store timestamp in database."""
        from goldfish.state_machine.event_emission import set_gcs_outage_started

        db = MagicMock()
        db.update_stage_run_gcs_outage = MagicMock()
        now = datetime.now(UTC)

        set_gcs_outage_started(db, "stage-123", now)

        db.update_stage_run_gcs_outage.assert_called_once()
        call_args = db.update_stage_run_gcs_outage.call_args[0]
        assert call_args[0] == "stage-123"
        assert call_args[1] == now.isoformat()

    def test_clear_gcs_outage_started_clears_timestamp(self) -> None:
        """clear_gcs_outage_started must clear timestamp in database."""
        from goldfish.state_machine.event_emission import clear_gcs_outage_started

        db = MagicMock()
        db.update_stage_run_gcs_outage = MagicMock()

        clear_gcs_outage_started(db, "stage-123")

        db.update_stage_run_gcs_outage.assert_called_once()
        call_args = db.update_stage_run_gcs_outage.call_args[0]
        assert call_args[0] == "stage-123"
        assert call_args[1] is None


class TestVerifyInstanceStopped:
    """Tests for verify_instance_stopped() function."""

    def test_gce_instance_not_found_returns_true(self) -> None:
        """GCE instance not found must return True (confirmed dead)."""
        from goldfish.state_machine.event_emission import verify_instance_stopped

        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            from goldfish.errors import NotFoundError

            mock_backend = MagicMock()
            mock_backend.get_status.side_effect = NotFoundError("instance:instance-123")
            mock_factory.return_value = mock_backend

            result = verify_instance_stopped(
                run_id="stage-123",
                backend_type="gce",
                backend_handle="instance-123",
                project_id="test-project",
            )

        assert result is True

    def test_gce_instance_running_returns_false(self) -> None:
        """GCE instance RUNNING must return False."""
        from goldfish.state_machine.event_emission import verify_instance_stopped

        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            from goldfish.cloud.contracts import BackendStatus, RunStatus

            mock_backend = MagicMock()
            mock_backend.get_status.return_value = BackendStatus(status=RunStatus.RUNNING)
            mock_factory.return_value = mock_backend

            result = verify_instance_stopped(
                run_id="stage-123",
                backend_type="gce",
                backend_handle="instance-123",
                project_id="test-project",
            )

        assert result is False

    def test_gce_instance_terminated_returns_true(self) -> None:
        """GCE instance TERMINATED must return True."""
        from goldfish.state_machine.event_emission import verify_instance_stopped

        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            from goldfish.cloud.contracts import BackendStatus, RunStatus

            mock_backend = MagicMock()
            mock_backend.get_status.return_value = BackendStatus(status=RunStatus.TERMINATED)
            mock_factory.return_value = mock_backend

            result = verify_instance_stopped(
                run_id="stage-123",
                backend_type="gce",
                backend_handle="instance-123",
                project_id="test-project",
            )

        assert result is True

    def test_docker_container_not_found_returns_true(self) -> None:
        """Docker container not found must return True."""
        from goldfish.state_machine.event_emission import verify_instance_stopped

        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            from goldfish.errors import NotFoundError

            mock_backend = MagicMock()
            mock_backend.get_status.side_effect = NotFoundError("container:container-123")
            mock_factory.return_value = mock_backend

            result = verify_instance_stopped(
                run_id="stage-123",
                backend_type="local",
                backend_handle="container-123",
            )

        assert result is True

    def test_docker_container_running_returns_false(self) -> None:
        """Docker container running must return False."""
        from goldfish.state_machine.event_emission import verify_instance_stopped

        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            from goldfish.cloud.contracts import BackendStatus, RunStatus

            mock_backend = MagicMock()
            mock_backend.get_status.return_value = BackendStatus(status=RunStatus.RUNNING)
            mock_factory.return_value = mock_backend

            result = verify_instance_stopped(
                run_id="stage-123",
                backend_type="local",
                backend_handle="container-123",
            )

        assert result is False

    def test_docker_container_exited_returns_true(self) -> None:
        """Docker container exited must return True."""
        from goldfish.state_machine.event_emission import verify_instance_stopped

        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            from goldfish.cloud.contracts import BackendStatus, RunStatus

            mock_backend = MagicMock()
            mock_backend.get_status.return_value = BackendStatus(status=RunStatus.COMPLETED)
            mock_factory.return_value = mock_backend

            result = verify_instance_stopped(
                run_id="stage-123",
                backend_type="local",
                backend_handle="container-123",
            )

        assert result is True


class TestDetectTerminationCause:
    """Tests for detect_termination_cause() function."""

    def test_gce_preemption_detected(self) -> None:
        """GCE preemption must be detected via backend status."""
        from goldfish.state_machine.event_emission import detect_termination_cause
        from goldfish.state_machine.types import TerminationCause

        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            from goldfish.cloud.contracts import BackendStatus, RunStatus

            mock_backend = MagicMock()
            mock_backend.get_status.return_value = BackendStatus(
                status=RunStatus.TERMINATED,
                termination_cause="preemption",
            )
            mock_factory.return_value = mock_backend

            result = detect_termination_cause(
                run_id="stage-123",
                backend_type="gce",
                backend_handle="instance-123",
                project_id="test-project",
            )

        assert result == TerminationCause.PREEMPTED

    def test_gce_no_preemption_returns_crashed(self) -> None:
        """GCE instance stopped without preemption must return CRASHED."""
        from goldfish.state_machine.event_emission import detect_termination_cause
        from goldfish.state_machine.types import TerminationCause

        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            from goldfish.cloud.contracts import BackendStatus, RunStatus

            mock_backend = MagicMock()
            mock_backend.get_status.return_value = BackendStatus(status=RunStatus.TERMINATED)
            mock_factory.return_value = mock_backend

            result = detect_termination_cause(
                run_id="stage-123",
                backend_type="gce",
                backend_handle="instance-123",
                project_id="test-project",
            )

        assert result == TerminationCause.CRASHED

    def test_docker_oom_returns_crashed(self) -> None:
        """Docker OOM kill must return CRASHED."""
        from goldfish.state_machine.event_emission import detect_termination_cause
        from goldfish.state_machine.types import TerminationCause

        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            from goldfish.cloud.contracts import BackendStatus, RunStatus

            mock_backend = MagicMock()
            mock_backend.get_status.return_value = BackendStatus(
                status=RunStatus.TERMINATED,
                termination_cause="oom",
            )
            mock_factory.return_value = mock_backend

            result = detect_termination_cause(
                run_id="stage-123",
                backend_type="local",
                backend_handle="container-123",
            )

        assert result == TerminationCause.CRASHED

    def test_docker_no_oom_returns_orphaned(self) -> None:
        """Docker container stopped without OOM must return ORPHANED."""
        from goldfish.state_machine.event_emission import detect_termination_cause
        from goldfish.state_machine.types import TerminationCause

        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            from goldfish.cloud.contracts import BackendStatus, RunStatus

            mock_backend = MagicMock()
            mock_backend.get_status.return_value = BackendStatus(status=RunStatus.TERMINATED)
            mock_factory.return_value = mock_backend

            result = detect_termination_cause(
                run_id="stage-123",
                backend_type="local",
                backend_handle="container-123",
            )

        # Without OOM, we can't be sure why it stopped
        assert result == TerminationCause.ORPHANED

    def test_api_error_returns_orphaned(self) -> None:
        """API error must return ORPHANED (can't determine cause)."""
        from goldfish.state_machine.event_emission import detect_termination_cause
        from goldfish.state_machine.types import TerminationCause

        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            mock_backend = MagicMock()
            mock_backend.get_status.side_effect = Exception("API error")
            mock_factory.return_value = mock_backend

            result = detect_termination_cause(
                run_id="stage-123",
                backend_type="gce",
                backend_handle="instance-123",
                project_id="test-project",
            )

        # When we can't determine the cause, default to ORPHANED
        assert result == TerminationCause.ORPHANED


class TestEventContextCreation:
    """Tests for EventContext creation in event emission."""

    def test_context_has_correct_source(self) -> None:
        """EventContext must have source='daemon' for daemon-emitted events."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.exit_code import ExitCodeResult
        from goldfish.state_machine.types import StageState

        run = {"id": "stage-123", "state": StageState.RUNNING.value}
        exit_result = ExitCodeResult.from_code(0)

        result = determine_exit_event(run, exit_result)
        assert result is not None
        _, context = result
        assert context.source == "daemon"

    def test_context_has_timestamp(self) -> None:
        """EventContext must have timestamp."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.exit_code import ExitCodeResult
        from goldfish.state_machine.types import StageState

        run = {"id": "stage-123", "state": StageState.RUNNING.value}
        exit_result = ExitCodeResult.from_code(0)

        result = determine_exit_event(run, exit_result)
        assert result is not None
        _, context = result
        assert context.timestamp is not None


class TestInstanceLostEvent:
    """Tests for INSTANCE_LOST event emission."""

    def test_instance_lost_when_instance_disappeared(self) -> None:
        """INSTANCE_LOST must be emitted when instance disappears unexpectedly.

        This should only happen once the backend instance is confirmed dead.
        """
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageEvent, StageState

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "gce",
            "backend_handle": "instance-123",
        }

        with (
            patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify,
            patch("goldfish.state_machine.event_emission.detect_termination_cause") as mock_cause,
        ):
            from goldfish.state_machine.types import TerminationCause

            mock_verify.return_value = True
            mock_cause.return_value = TerminationCause.ORPHANED
            result = determine_instance_event(run, project_id="test-project")

        assert result is not None
        event, _ = result
        assert event == StageEvent.INSTANCE_LOST

    def test_instance_still_running_returns_none(self) -> None:
        """No event when instance is still running normally."""
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageState

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "gce",
            "backend_handle": "instance-123",
        }

        with patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = False
            result = determine_instance_event(run, project_id="test-project")

        assert result is None  # No event, instance is fine

    def test_preparing_state_never_emits_instance_lost(self) -> None:
        """REGRESSION: PREPARING state must never emit INSTANCE_LOST.

        PREPARING runs don't have instances yet - instances are only created
        during LAUNCHING. Even if a PREPARING run has a backend_handle (from
        migration or bug), we must not check instance status.
        """
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageState

        run = {
            "id": "stage-123",
            "state": StageState.PREPARING.value,
            "backend_type": "gce",
            "backend_handle": "instance-123",  # Should be ignored
        }

        # verify_instance_stopped should NOT be called for PREPARING state
        with patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = True  # Would trigger INSTANCE_LOST if called

            result = determine_instance_event(run, project_id="test-project")

            assert result is None  # Must not emit any event
            mock_verify.assert_not_called()  # Must not even check instance status

    def test_building_state_never_emits_instance_lost(self) -> None:
        """REGRESSION: BUILDING state must never emit INSTANCE_LOST.

        BUILDING runs don't have instances yet - instances are only created
        during LAUNCHING. Even if a BUILDING run has a backend_handle (from
        migration or bug), we must not check instance status.
        """
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageState

        run = {
            "id": "stage-123",
            "state": StageState.BUILDING.value,
            "backend_type": "local",
            "backend_handle": "container-123",  # Should be ignored
        }

        with patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = True

            result = determine_instance_event(run, project_id="test-project")

            assert result is None
            mock_verify.assert_not_called()

    def test_launching_state_without_handle_does_not_emit_instance_lost(self) -> None:
        """LAUNCHING state without backend_handle should not emit INSTANCE_LOST.

        Early in LAUNCHING, before the instance is created, backend_handle is None.
        The function should return None without checking instance status.
        """
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageState

        run = {
            "id": "stage-123",
            "state": StageState.LAUNCHING.value,
            "backend_type": "gce",
            "backend_handle": None,  # No handle yet
        }

        with patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = True  # Would trigger INSTANCE_LOST if called

            result = determine_instance_event(run, project_id="test-project")

            assert result is None  # Must not emit any event
            mock_verify.assert_not_called()  # Must not even check instance status

    def test_launching_state_with_handle_can_detect_preemption(self) -> None:
        """REGRESSION: LAUNCHING state with backend_handle should detect preemption.

        Bug: Preemption during LAUNCHING (after instance created, before ACK) was
        not detected. The run would timeout after 20 minutes instead of properly
        reporting preemption.

        Fix: Allow LAUNCHING to check instance status if backend_handle exists.
        If the instance was created and then preempted, we should detect it.
        """
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageEvent, StageState

        run = {
            "id": "stage-123",
            "state": StageState.LAUNCHING.value,
            "backend_type": "gce",
            "backend_handle": "instance-123",  # Handle exists - instance was created
        }

        with (
            patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify,
            patch("goldfish.state_machine.event_emission.detect_termination_cause") as mock_detect,
        ):
            from goldfish.state_machine.types import TerminationCause

            mock_verify.return_value = True  # Instance is stopped
            mock_detect.return_value = TerminationCause.PREEMPTED

            result = determine_instance_event(run, project_id="test-project")

            assert result is not None
            event, context = result
            assert event == StageEvent.INSTANCE_LOST
            assert context.termination_cause == TerminationCause.PREEMPTED
            mock_verify.assert_called_once()
            mock_detect.assert_called_once()

    def test_awaiting_user_finalization_state_never_emits_instance_lost(self) -> None:
        """AWAITING_USER_FINALIZATION state must never emit INSTANCE_LOST.

        After a run completes, the instance may be cleaned up while the run
        waits for user finalization. Checking instance status would incorrectly
        report INSTANCE_LOST for completed runs.
        """
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageState

        run = {
            "id": "stage-123",
            "state": StageState.AWAITING_USER_FINALIZATION.value,
            "backend_type": "local",
            "backend_handle": "container-123",
        }

        with patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = True

            result = determine_instance_event(run)

            assert result is None
            mock_verify.assert_not_called()

    def test_unknown_state_never_emits_instance_lost(self) -> None:
        """UNKNOWN state must never emit INSTANCE_LOST.

        When state is UNKNOWN, we can't assume anything about whether an
        instance should exist.
        """
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageState

        run = {
            "id": "stage-123",
            "state": StageState.UNKNOWN.value,
            "backend_type": "gce",
            "backend_handle": "instance-123",
        }

        with patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = True

            result = determine_instance_event(run, project_id="test-project")

            assert result is None
            mock_verify.assert_not_called()

    def test_post_run_state_never_emits_instance_lost(self) -> None:
        """POST_RUN state must NOT emit INSTANCE_LOST.

        In POST_RUN, the instance being stopped is EXPECTED because the process
        already exited. The executor handles POST_RUN → AWAITING_USER_FINALIZATION
        via POST_RUN_OK. Emitting INSTANCE_LOST would race with that and
        incorrectly terminate successful runs.
        """
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageState

        run = {
            "id": "stage-123",
            "state": StageState.POST_RUN.value,
            "backend_type": "local",
            "backend_handle": "container-123",
        }

        with patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = True  # Instance is stopped

            result = determine_instance_event(run)

            # POST_RUN should NOT emit INSTANCE_LOST
            assert result is None
            # verify_instance_stopped should not even be called
            mock_verify.assert_not_called()


class TestEdgeCases:
    """Tests for edge cases in event emission."""

    def test_none_exit_result_returns_none(self) -> None:
        """None exit result must return None."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.types import StageState

        run = {"id": "stage-123", "state": StageState.RUNNING.value}

        # This shouldn't happen, but handle gracefully
        result = determine_exit_event(run, None)  # type: ignore[arg-type]
        assert result is None

    def test_non_running_state_returns_none(self) -> None:
        """Non-RUNNING state must return None (already transitioned)."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.exit_code import ExitCodeResult
        from goldfish.state_machine.types import StageState

        # Already completed - shouldn't emit exit events
        run = {"id": "stage-123", "state": StageState.COMPLETED.value}
        exit_result = ExitCodeResult.from_code(0)

        result = determine_exit_event(run, exit_result)
        assert result is None


class TestAIStopDetection:
    """Tests for AI_STOP event detection.

    When user code exits with code 0 but stop_requested file exists,
    we should emit AI_STOP instead of EXIT_SUCCESS.
    """

    def test_exit_success_with_stop_requested_returns_ai_stop(self) -> None:
        """Exit code 0 with stop_requested file emits AI_STOP."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.exit_code import ExitCodeResult
        from goldfish.state_machine.types import StageEvent, StageState, TerminationCause

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "local",
            "outputs_dir": "/tmp/test-outputs",
        }
        exit_result = ExitCodeResult.from_code(0)

        with patch("goldfish.state_machine.event_emission.check_ai_stop_requested") as mock_check:
            mock_check.return_value = {"stop_requested": True, "svs_review_id": "42"}
            result = determine_exit_event(run, exit_result, db=MagicMock())

        assert result is not None
        event, context = result
        assert event == StageEvent.AI_STOP
        assert context.termination_cause == TerminationCause.AI_STOPPED
        assert context.svs_review_id == "42"

    def test_exit_success_without_stop_requested_returns_exit_success(self) -> None:
        """Exit code 0 without stop_requested file emits EXIT_SUCCESS."""
        from goldfish.state_machine.event_emission import determine_exit_event
        from goldfish.state_machine.exit_code import ExitCodeResult
        from goldfish.state_machine.types import StageEvent, StageState

        run = {"id": "stage-123", "state": StageState.RUNNING.value}
        exit_result = ExitCodeResult.from_code(0)

        with patch("goldfish.state_machine.event_emission.check_ai_stop_requested") as mock_check:
            mock_check.return_value = None  # No stop requested
            result = determine_exit_event(run, exit_result, db=MagicMock())

        assert result is not None
        event, context = result
        assert event == StageEvent.EXIT_SUCCESS

    def test_check_ai_stop_requested_local_with_file(self) -> None:
        """check_ai_stop_requested detects local stop_requested file."""
        import tempfile
        from pathlib import Path

        from goldfish.state_machine.event_emission import check_ai_stop_requested

        with tempfile.TemporaryDirectory() as tmpdir:
            outputs_dir = Path(tmpdir)
            goldfish_dir = outputs_dir / ".goldfish"
            goldfish_dir.mkdir()
            stop_file = goldfish_dir / "stop_requested"
            stop_file.write_text("AI requested stop")

            run = {
                "id": "stage-123",
                "backend_type": "local",
                "outputs_dir": str(outputs_dir),
            }

            result = check_ai_stop_requested(run)

            assert result is not None
            assert result["stop_requested"] is True

    def test_check_ai_stop_requested_local_without_file(self) -> None:
        """check_ai_stop_requested returns None when no stop_requested file."""
        import tempfile
        from pathlib import Path

        from goldfish.state_machine.event_emission import check_ai_stop_requested

        with tempfile.TemporaryDirectory() as tmpdir:
            outputs_dir = Path(tmpdir)
            goldfish_dir = outputs_dir / ".goldfish"
            goldfish_dir.mkdir()
            # No stop_requested file

            run = {
                "id": "stage-123",
                "backend_type": "local",
                "outputs_dir": str(outputs_dir),
            }

            result = check_ai_stop_requested(run)

            assert result is None

    def test_check_ai_stop_requested_remote_with_storage(self) -> None:
        """check_ai_stop_requested uses storage adapter for remote stop file."""
        from goldfish.cloud.contracts import StorageURI
        from goldfish.state_machine.event_emission import check_ai_stop_requested

        storage = MagicMock()
        storage.exists.return_value = True
        bucket_uri = StorageURI("gs", "test-bucket", "")

        run = {"id": "stage-123"}

        result = check_ai_stop_requested(run, storage=storage, bucket_uri=bucket_uri)

        assert result is not None
        assert result["stop_requested"] is True
        storage.exists.assert_called_once_with(
            bucket_uri.join("runs", "stage-123", "outputs", ".goldfish", "stop_requested")
        )

    def test_check_ai_stop_requested_looks_up_svs_review_id(self) -> None:
        """check_ai_stop_requested looks up svs_review_id from database."""
        import tempfile
        from pathlib import Path

        from goldfish.state_machine.event_emission import check_ai_stop_requested

        with tempfile.TemporaryDirectory() as tmpdir:
            outputs_dir = Path(tmpdir)
            goldfish_dir = outputs_dir / ".goldfish"
            goldfish_dir.mkdir()
            stop_file = goldfish_dir / "stop_requested"
            stop_file.write_text("AI requested stop")

            run = {
                "id": "stage-123",
                "backend_type": "local",
                "outputs_dir": str(outputs_dir),
            }

            # Mock database lookup
            mock_db = MagicMock()
            mock_conn = MagicMock()
            mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)
            mock_conn.execute.return_value.fetchone.return_value = {"id": 99}

            result = check_ai_stop_requested(run, db=mock_db)

            assert result is not None
            assert result["svs_review_id"] == "99"


class TestStateBasedInstanceChecks:
    """Comprehensive tests for state-based instance checking.

    This test class captures a general class of bugs: the daemon polls faster
    than the executor can set up infrastructure. Without proper state filtering,
    the daemon can incorrectly emit events for runs that haven't finished setup.

    General Rule:
    - Only check for INSTANCE_LOST in states where an instance is CONFIRMED to exist
    - Confirmed states: RUNNING, POST_RUN
    - Unconfirmed states: PREPARING, BUILDING, LAUNCHING, AWAITING_USER_FINALIZATION, UNKNOWN

    This pattern applies whenever:
    1. A background process (daemon) monitors state
    2. A foreground process (executor) sets up resources asynchronously
    3. The monitor polls faster than the setup completes
    """

    def test_all_states_instance_lost_behavior(self) -> None:
        """Verify INSTANCE_LOST behavior is correct for ALL states.

        This comprehensive test ensures we don't miss any state when adding
        new states to the state machine.
        """
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageEvent, StageState

        # States where instance check should be SKIPPED (no INSTANCE_LOST)
        # These are states where INSTANCE_LOST should NOT be emitted
        states_skip_instance_check = {
            StageState.PREPARING,  # Code syncing, no instance
            StageState.BUILDING,  # Docker build, no instance
            # LAUNCHING: removed - now checks for preemption if backend_handle exists
            StageState.POST_RUN,  # Instance stopping is EXPECTED after exit
            StageState.AWAITING_USER_FINALIZATION,  # Instance may be cleaned up
            StageState.UNKNOWN,  # Can't assume anything
        }

        # States where instance check SHOULD happen
        # These are states where an instance being lost is unexpected
        states_do_instance_check = {
            StageState.RUNNING,  # Instance must exist while code runs
            StageState.LAUNCHING,  # Instance may be preempted before ACK
        }

        # Terminal states - no checks needed, run is done
        terminal_states = {
            StageState.COMPLETED,
            StageState.TERMINATED,
            StageState.CANCELED,
        }

        for state in StageState:
            run = {
                "id": "stage-123",
                "state": state.value,
                "backend_type": "gce",
                "backend_handle": "instance-123",
            }

            with patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify:
                # Make verify return True (instance "not found")
                mock_verify.return_value = True

                with patch("goldfish.state_machine.event_emission.detect_termination_cause"):
                    result = determine_instance_event(run, project_id="test-project")

                    if state in states_skip_instance_check:
                        assert result is None, f"State {state.value} should NOT emit INSTANCE_LOST"
                        mock_verify.assert_not_called()
                    elif state in states_do_instance_check:
                        assert result is not None, f"State {state.value} SHOULD emit INSTANCE_LOST"
                        event, _ = result
                        assert event == StageEvent.INSTANCE_LOST
                    elif state in terminal_states:
                        # Terminal states aren't polled by daemon, but if they were,
                        # we'd check (result depends on backend_handle presence)
                        pass  # Not relevant to this test

    def test_daemon_poll_vs_executor_setup_timing(self) -> None:
        """Document the timing invariant between daemon and executor.

        The daemon polls every 2 seconds. If the executor takes longer than
        2 seconds to set up infrastructure, the daemon must NOT emit spurious
        events during setup phases.

        This test documents the expected timing relationships.
        """
        # These are the approximate times for each phase:
        # PREPARING: 1-5 seconds (git sync)
        # BUILDING: 10-300 seconds (Docker build)
        # LAUNCHING: 5-60 seconds (instance creation)
        # RUNNING: indefinite (user code)
        # POST_RUN: 5-30 seconds (cleanup)

        # Daemon poll interval
        DAEMON_POLL_INTERVAL_SECONDS = 2.0

        # Minimum time for instance to be visible after LAUNCHING starts
        MIN_INSTANCE_VISIBILITY_SECONDS = 5.0

        # This inequality MUST hold to avoid spurious INSTANCE_LOST:
        # If daemon polls during LAUNCHING before instance is visible,
        # it would incorrectly see "instance not found".
        assert MIN_INSTANCE_VISIBILITY_SECONDS > DAEMON_POLL_INTERVAL_SECONDS, (
            "Instance visibility time must be > daemon poll interval. "
            "This means LAUNCHING state MUST be excluded from instance checks."
        )

    def test_running_is_only_state_with_instance_checks(self) -> None:
        """Ensure ONLY RUNNING performs instance checks.

        RUNNING is the only state where instance being lost is unexpected
        and indicates a failure. All other states either don't have an
        instance yet, or the instance stopping is expected.
        """
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageEvent, StageState

        # Only RUNNING should emit INSTANCE_LOST
        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "local",
            "backend_handle": "container-123",
        }

        with patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = True

            with patch("goldfish.state_machine.event_emission.detect_termination_cause"):
                result = determine_instance_event(run)

                assert result is not None, "RUNNING state should emit INSTANCE_LOST"
                event, _ = result
                assert event == StageEvent.INSTANCE_LOST
                mock_verify.assert_called_once()

    def test_no_backend_handle_never_emits_instance_lost(self) -> None:
        """Runs without backend_handle should never emit INSTANCE_LOST.

        Even in RUNNING state, if there's no backend_handle, we can't
        check instance status.
        """
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageState

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "gce",
            "backend_handle": None,  # No handle
        }

        with patch("goldfish.state_machine.event_emission.verify_instance_stopped") as mock_verify:
            result = determine_instance_event(run, project_id="test-project")

            assert result is None
            mock_verify.assert_not_called()


class TestExitCodeMetadataPrimary:
    """REGRESSION TESTS for exit code signaling.

    The instance writes two signals:
    - Instance metadata `goldfish_exit_code` (best-effort, in-instance)
    - `exit_code.txt` in object storage (authoritative server-side signal)

    Core state machine code reads `exit_code.txt` via the ObjectStorage adapter.
    Provider-specific probing stays behind the adapter boundary.
    """

    def test_startup_script_sets_exit_code_metadata(self) -> None:
        """Startup script must set exit code in instance metadata."""
        from goldfish.cloud.adapters.gcp.startup_builder import build_startup_script

        script = build_startup_script(
            bucket="test-bucket",
            bucket_prefix="artifacts",
            run_path="runs/stage-123",
            image="test-image:latest",
            entrypoint="/bin/bash",
            env_map={"TEST": "value"},
        )

        # Must set goldfish_exit_code metadata
        assert (
            "goldfish_exit_code=$EXIT_CODE" in script
        ), "Startup script must set exit code in instance metadata as fallback"
        assert "gcloud compute instances add-metadata" in script

    def test_startup_script_sets_metadata_before_gcs_upload(self) -> None:
        """Exit code metadata must be set BEFORE GCS upload call (for reliability)."""
        from goldfish.cloud.adapters.gcp.startup_builder import build_startup_script

        script = build_startup_script(
            bucket="test-bucket",
            bucket_prefix="artifacts",
            run_path="runs/stage-123",
            image="test-image:latest",
            entrypoint="/bin/bash",
            env_map={"TEST": "value"},
        )

        # Find positions of metadata set and GCS upload CALL (not function definition)
        metadata_pos = script.find("goldfish_exit_code=$EXIT_CODE")
        # Look for the actual call pattern: upload_exit_code "$EXIT_CODE_FILE"
        gcs_upload_call_pos = script.find('upload_exit_code "$EXIT_CODE_FILE"')

        assert metadata_pos != -1, "Should have metadata set"
        assert gcs_upload_call_pos != -1, "Should have GCS upload call"
        assert metadata_pos < gcs_upload_call_pos, (
            f"Metadata (pos={metadata_pos}) must be set BEFORE GCS upload call "
            f"(pos={gcs_upload_call_pos}) so it's available as primary channel"
        )

    def test_get_exit_code_gce_reads_from_storage(self) -> None:
        """get_exit_code_gce reads exit_code.txt from the storage adapter."""
        from unittest.mock import MagicMock

        from goldfish.state_machine.exit_code import get_exit_code_gce

        storage = MagicMock()
        storage.get.return_value = b"0\n"

        result = get_exit_code_gce(
            bucket_uri="gs://test-bucket",
            stage_run_id="stage-abc123",
            storage=storage,
            project_id="test-project",
        )

        assert result.exists is True
        assert result.code == 0
        assert storage.get.call_count == 1

        uri = storage.get.call_args.args[0]
        assert str(uri) == "gs://test-bucket/runs/stage-abc123/logs/exit_code.txt"

    def test_get_exit_code_gce_returns_not_found_when_object_missing(self) -> None:
        """Missing exit_code.txt must return exists=False."""
        from unittest.mock import MagicMock

        from goldfish.errors import NotFoundError
        from goldfish.state_machine.exit_code import get_exit_code_gce

        storage = MagicMock()
        storage.get.side_effect = NotFoundError("gs://test-bucket/runs/stage-abc123/logs/exit_code.txt")

        result = get_exit_code_gce(
            bucket_uri="gs://test-bucket",
            stage_run_id="stage-abc123",
            storage=storage,
            project_id="test-project",
            max_attempts=1,
        )

        assert result.exists is False
        assert result.gcs_error is False

    def test_get_exit_code_gce_retries_when_object_missing_then_succeeds(self) -> None:
        """Retries on missing object to handle eventual consistency."""
        from unittest.mock import MagicMock, patch

        from goldfish.errors import NotFoundError
        from goldfish.state_machine.exit_code import get_exit_code_gce

        storage = MagicMock()
        storage.get.side_effect = [
            NotFoundError("gs://test-bucket/runs/stage-abc123/logs/exit_code.txt"),
            b"1\n",
        ]

        with patch("time.sleep"):
            result = get_exit_code_gce(
                bucket_uri="gs://test-bucket",
                stage_run_id="stage-abc123",
                storage=storage,
                project_id="test-project",
                max_attempts=2,
            )

        assert result.exists is True
        assert result.code == 1
        assert storage.get.call_count == 2

    def test_get_exit_code_gce_without_uri_scheme_defaults_to_gs(self) -> None:
        """Legacy bucket config without scheme defaults to gs://."""
        from unittest.mock import MagicMock

        from goldfish.state_machine.exit_code import get_exit_code_gce

        storage = MagicMock()
        storage.get.return_value = b"0\n"

        _result = get_exit_code_gce(
            bucket_uri="test-bucket",
            stage_run_id="stage-abc123",
            storage=storage,
            project_id="test-project",
        )

        uri = storage.get.call_args.args[0]
        assert str(uri).startswith("gs://test-bucket/")


class TestExitCodeUploadMandatory:
    """REGRESSION TESTS for exit_code.txt upload reliability.

    THE BUG (Found 2026-01-19):
    - Instance completes successfully, attempts to upload exit_code.txt to GCS
    - If upload fails (timeout, network issue), script logs "WARNING" but continues
    - Script exits → triggers self-delete → daemon sees "instance dead + no exit code"
    - Daemon assumes crash → emits EXIT_MISSING → transitions to TERMINATED
    - But the actual exit code was 0!

    THE SYMPTOM:
    - Run shows "Stage completed successfully" in logs
    - But state = "terminated" instead of "completed"
    - post_run SVS never triggered

    THE FIX:
    - Make exit_code.txt upload MANDATORY with extended retries
    - exit_code.txt is critical for state machine, unlike stdout/stderr
    - Use a dedicated upload function with more retries and longer timeout
    - If upload truly fails after extended retries, block (watchdog will eventually kill)

    THE PROPER SOLUTION:
    - Fix the root cause (startup script) rather than daemon-side workarounds
    - Daemon-side grace periods are flakey and add complexity
    """

    def test_startup_script_has_critical_exit_code_upload(self) -> None:
        """REGRESSION: exit_code.txt upload must use critical (blocking) upload.

        Bug: used `upload_logs_with_retry ... || echo "WARNING"` which allowed
        script to continue even if upload failed.

        Fix: use `upload_exit_code` which has more retries and NO fallback.
        """
        from goldfish.cloud.adapters.gcp.startup_builder import build_startup_script

        # Generate a startup script
        script = build_startup_script(
            bucket="test-bucket",
            bucket_prefix="artifacts",
            run_path="runs/stage-123",
            image="test-image:latest",
            entrypoint="/bin/bash",
            env_map={"TEST": "value"},
        )

        # Find the exit_code.txt upload line
        lines = script.split("\n")
        exit_code_upload_line = None
        for line in lines:
            if "exit_code.txt" in line and "upload" in line.lower():
                exit_code_upload_line = line
                break

        assert exit_code_upload_line is not None, "Should have exit_code upload line"

        # Must NOT have a fallback that allows continuation on failure
        assert '|| echo "WARNING' not in exit_code_upload_line, (
            "exit_code.txt upload must NOT have || echo fallback - "
            "it's critical for state machine and must block until success"
        )

    def test_upload_critical_helper_exists(self) -> None:
        """Startup script should have upload_critical helper for mandatory uploads."""
        from goldfish.cloud.adapters.gcp.startup_builder import upload_helper_section

        helper = upload_helper_section()

        # Should have upload_critical function with more retries
        assert (
            "upload_critical" in helper or "upload_exit_code" in helper
        ), "Should have a dedicated critical upload function for exit_code.txt"
