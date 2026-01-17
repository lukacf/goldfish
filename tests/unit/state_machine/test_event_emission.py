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

        with patch("subprocess.run") as mock_run:
            import subprocess

            error = subprocess.CalledProcessError(1, "gcloud")
            error.stderr = "not found"
            mock_run.side_effect = error

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

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="RUNNING\n", returncode=0)

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

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="TERMINATED\n", returncode=0)

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

        with patch("subprocess.run") as mock_run:
            import subprocess

            error = subprocess.CalledProcessError(1, "docker")
            error.stderr = "No such container"
            mock_run.side_effect = error

            result = verify_instance_stopped(
                run_id="stage-123",
                backend_type="local",
                backend_handle="container-123",
            )

            assert result is True

    def test_docker_container_running_returns_false(self) -> None:
        """Docker container running must return False."""
        from goldfish.state_machine.event_emission import verify_instance_stopped

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="running\n", returncode=0)

            result = verify_instance_stopped(
                run_id="stage-123",
                backend_type="local",
                backend_handle="container-123",
            )

            assert result is False

    def test_docker_container_exited_returns_true(self) -> None:
        """Docker container exited must return True."""
        from goldfish.state_machine.event_emission import verify_instance_stopped

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="exited\n", returncode=0)

            result = verify_instance_stopped(
                run_id="stage-123",
                backend_type="local",
                backend_handle="container-123",
            )

            assert result is True


class TestDetectTerminationCause:
    """Tests for detect_termination_cause() function."""

    def test_gce_preemption_detected(self) -> None:
        """GCE preemption must be detected via operations API."""
        from goldfish.state_machine.event_emission import detect_termination_cause
        from goldfish.state_machine.types import TerminationCause

        with patch("subprocess.run") as mock_run:
            # Return preemption operation
            mock_run.return_value = MagicMock(
                stdout='[{"operationType": "compute.instances.preempted"}]',
                returncode=0,
            )

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

        with patch("subprocess.run") as mock_run:
            # Return no preemption operations
            mock_run.return_value = MagicMock(stdout="[]", returncode=0)

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

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="true\n", returncode=0)

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

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="false\n", returncode=0)

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

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = Exception("API error")

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
        """INSTANCE_LOST must be emitted when instance disappears unexpectedly."""
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageEvent, StageState

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "gce",
            "backend_handle": "instance-123",
        }

        with patch("subprocess.run") as mock_run:
            import subprocess

            error = subprocess.CalledProcessError(1, "gcloud")
            error.stderr = "not found"
            mock_run.side_effect = error

            result = determine_instance_event(run, project_id="test-project")

            assert result is not None
            event, context = result
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

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="RUNNING\n", returncode=0)

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

    def test_launching_state_can_emit_instance_lost(self) -> None:
        """LAUNCHING state CAN emit INSTANCE_LOST (instance creation in progress)."""
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageEvent, StageState

        run = {
            "id": "stage-123",
            "state": StageState.LAUNCHING.value,
            "backend_type": "gce",
            "backend_handle": "instance-123",
        }

        with patch("subprocess.run") as mock_run:
            import subprocess

            error = subprocess.CalledProcessError(1, "gcloud")
            error.stderr = "not found"
            mock_run.side_effect = error

            result = determine_instance_event(run, project_id="test-project")

            assert result is not None
            event, _ = result
            assert event == StageEvent.INSTANCE_LOST

    def test_post_run_state_can_emit_instance_lost(self) -> None:
        """POST_RUN state CAN emit INSTANCE_LOST (instance may still exist)."""
        from goldfish.state_machine.event_emission import determine_instance_event
        from goldfish.state_machine.types import StageEvent, StageState

        run = {
            "id": "stage-123",
            "state": StageState.POST_RUN.value,
            "backend_type": "local",
            "backend_handle": "container-123",
        }

        with patch("subprocess.run") as mock_run:
            import subprocess

            error = subprocess.CalledProcessError(1, "docker")
            error.stderr = "No such container"
            mock_run.side_effect = error

            result = determine_instance_event(run)

            assert result is not None
            event, _ = result
            assert event == StageEvent.INSTANCE_LOST


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
