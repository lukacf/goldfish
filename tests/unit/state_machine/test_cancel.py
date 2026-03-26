"""Tests for cancel flow using state machine transitions.

These tests verify that canceling a run properly uses the state machine
to emit USER_CANCEL events and trigger backend cleanup.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from goldfish.db.database import Database
from goldfish.errors import ReasonTooShortError
from goldfish.state_machine.cancel import _cleanup_backend, cancel_run
from goldfish.state_machine.types import StageEvent, StageState
from goldfish.validation import (
    InvalidContainerIdError,
    InvalidInstanceNameError,
    InvalidStageRunIdError,
)


def _create_run_in_state(
    db: Database,
    state: StageState,
    backend_type: str | None = "local",
    backend_handle: str | None = None,
) -> str:
    """Create a stage run in a specific state for testing."""
    run_id = f"stage-{uuid.uuid4().hex[:8]}"
    workspace_name = "test-workspace"
    version = "v1"
    now = datetime.now(UTC).isoformat()

    with db._conn() as conn:
        # Create workspace lineage if not exists
        conn.execute(
            """INSERT OR IGNORE INTO workspace_lineage
            (workspace_name, created_at)
            VALUES (?, ?)""",
            (workspace_name, now),
        )

        # Create workspace version if not exists
        conn.execute(
            """INSERT OR IGNORE INTO workspace_versions
            (workspace_name, version, git_tag, git_sha, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (workspace_name, version, f"{workspace_name}-{version}", "abc123", now, "test"),
        )

        # Create stage run
        conn.execute(
            """INSERT INTO stage_runs
            (id, workspace_name, version, stage_name, status, started_at, state, state_entered_at,
             backend_type, backend_handle)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                run_id,
                workspace_name,
                version,
                "test-stage",
                "running",
                now,
                state.value,
                now,
                backend_type,
                backend_handle,
            ),
        )

    return run_id


class TestCancelRunStateMachine:
    """Tests for cancel_run() using state machine transitions."""

    def test_cancel_from_preparing_succeeds(self, test_db: Database) -> None:
        """Cancel from PREPARING state transitions to CANCELED."""
        run_id = _create_run_in_state(test_db, StageState.PREPARING)

        result = cancel_run(test_db, run_id, "User requested cancellation")

        assert result["success"] is True
        assert result["previous_state"] == "preparing"
        assert result["new_state"] == "canceled"

    def test_cancel_from_building_succeeds(self, test_db: Database) -> None:
        """Cancel from BUILDING state transitions to CANCELED."""
        run_id = _create_run_in_state(test_db, StageState.BUILDING)

        result = cancel_run(test_db, run_id, "User requested cancellation")

        assert result["success"] is True
        assert result["previous_state"] == "building"
        assert result["new_state"] == "canceled"

    def test_cancel_from_launching_succeeds(self, test_db: Database) -> None:
        """Cancel from LAUNCHING state transitions to CANCELED."""
        run_id = _create_run_in_state(test_db, StageState.LAUNCHING)

        result = cancel_run(test_db, run_id, "User requested cancellation")

        assert result["success"] is True
        assert result["previous_state"] == "launching"
        assert result["new_state"] == "canceled"

    def test_cancel_from_running_succeeds(self, test_db: Database) -> None:
        """Cancel from RUNNING state transitions to CANCELED."""
        run_id = _create_run_in_state(test_db, StageState.RUNNING)

        result = cancel_run(test_db, run_id, "User requested cancellation")

        assert result["success"] is True
        assert result["previous_state"] == "running"
        assert result["new_state"] == "canceled"

    def test_cancel_from_post_run_succeeds(self, test_db: Database) -> None:
        """Cancel from POST_RUN state transitions to CANCELED."""
        run_id = _create_run_in_state(test_db, StageState.POST_RUN)

        result = cancel_run(test_db, run_id, "User requested cancellation")

        assert result["success"] is True
        assert result["previous_state"] == "post_run"
        assert result["new_state"] == "canceled"

    def test_cancel_from_awaiting_user_finalization_succeeds(self, test_db: Database) -> None:
        """Cancel from AWAITING_USER_FINALIZATION state transitions to CANCELED."""
        run_id = _create_run_in_state(test_db, StageState.AWAITING_USER_FINALIZATION)

        result = cancel_run(test_db, run_id, "User requested cancellation")

        assert result["success"] is True
        assert result["previous_state"] == "awaiting_user_finalization"
        assert result["new_state"] == "canceled"

    def test_cancel_from_completed_fails(self, test_db: Database) -> None:
        """Cancel from COMPLETED (terminal) state fails."""
        run_id = _create_run_in_state(test_db, StageState.COMPLETED)

        result = cancel_run(test_db, run_id, "User requested cancellation")

        assert result["success"] is False

    def test_cancel_from_failed_fails(self, test_db: Database) -> None:
        """Cancel from FAILED (terminal) state fails."""
        run_id = _create_run_in_state(test_db, StageState.FAILED)

        result = cancel_run(test_db, run_id, "User requested cancellation")

        assert result["success"] is False

    def test_cancel_from_terminated_fails(self, test_db: Database) -> None:
        """Cancel from TERMINATED (terminal) state fails."""
        run_id = _create_run_in_state(test_db, StageState.TERMINATED)

        result = cancel_run(test_db, run_id, "User requested cancellation")

        assert result["success"] is False

    def test_cancel_from_canceled_is_idempotent(self, test_db: Database) -> None:
        """Cancel from CANCELED state returns idempotent success."""
        run_id = _create_run_in_state(test_db, StageState.CANCELED)

        result = cancel_run(test_db, run_id, "User requested cancellation")

        # Idempotent success - already in target state
        assert result["success"] is True
        assert result["reason"] == "already_in_target_state"

    def test_cancel_from_unknown_fails(self, test_db: Database) -> None:
        """Cancel from UNKNOWN state is not allowed (no transition defined)."""
        run_id = _create_run_in_state(test_db, StageState.UNKNOWN)

        result = cancel_run(test_db, run_id, "User requested cancellation")

        # UNKNOWN state has no USER_CANCEL transition - use force_terminate instead
        assert result["success"] is False


class TestCancelRecordsAudit:
    """Tests for cancel audit trail."""

    def test_cancel_records_audit_with_mcp_tool_source(self, test_db: Database) -> None:
        """Cancel records audit entry with source='mcp_tool'."""
        run_id = _create_run_in_state(test_db, StageState.RUNNING)

        cancel_run(test_db, run_id, "User cancelled the run")

        # Check audit trail
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT source FROM stage_state_transitions WHERE stage_run_id = ? ORDER BY id DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            assert row is not None
            assert row["source"] == "mcp_tool"

    def test_cancel_records_event_as_user_cancel(self, test_db: Database) -> None:
        """Cancel records the event as USER_CANCEL."""
        run_id = _create_run_in_state(test_db, StageState.RUNNING)

        cancel_run(test_db, run_id, "User cancelled the run")

        # Check audit trail
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT event FROM stage_state_transitions WHERE stage_run_id = ? ORDER BY id DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            assert row is not None
            assert row["event"] == StageEvent.USER_CANCEL.value


class TestCancelBackendCleanup:
    """Tests for cancel backend cleanup."""

    def test_cancel_triggers_local_backend_cleanup(self, test_db: Database) -> None:
        """Cancel triggers cleanup for local Docker backend."""
        run_id = _create_run_in_state(test_db, StageState.RUNNING, backend_type="local", backend_handle="container-123")

        with patch("goldfish.state_machine.cancel._cleanup_backend") as mock_cleanup:
            result = cancel_run(test_db, run_id, "User cancellation")

            assert result["success"] is True
            mock_cleanup.assert_called_once_with(run_id, "local", "container-123")

    def test_cancel_triggers_gce_backend_cleanup(self, test_db: Database) -> None:
        """Cancel triggers cleanup for GCE backend."""
        run_id = _create_run_in_state(test_db, StageState.RUNNING, backend_type="gce", backend_handle="instance-456")

        with patch("goldfish.state_machine.cancel._cleanup_backend") as mock_cleanup:
            result = cancel_run(test_db, run_id, "User cancellation")

            assert result["success"] is True
            mock_cleanup.assert_called_once_with(run_id, "gce", "instance-456")

    def test_cancel_succeeds_even_if_cleanup_fails(self, test_db: Database) -> None:
        """Cancel succeeds even if backend cleanup raises an exception."""
        run_id = _create_run_in_state(test_db, StageState.RUNNING, backend_type="local", backend_handle="container-789")

        with patch("goldfish.state_machine.cancel._cleanup_backend", side_effect=Exception("Cleanup failed")):
            result = cancel_run(test_db, run_id, "User cancellation")

            # State transition should still succeed
            assert result["success"] is True
            # Backend cleanup is best-effort
            assert result.get("cleanup_error") is not None

    def test_cancel_logs_warning_when_cleanup_fails(self, test_db: Database) -> None:
        """Cancel logs warning when backend cleanup fails."""
        run_id = _create_run_in_state(test_db, StageState.RUNNING, backend_type="local", backend_handle="container-789")

        with patch("goldfish.state_machine.cancel._cleanup_backend", side_effect=Exception("Cleanup failed")):
            with patch("goldfish.state_machine.cancel.logger") as mock_logger:
                cancel_run(test_db, run_id, "User cancellation for testing purposes")

                mock_logger.warning.assert_called_once()
                call_args = mock_logger.warning.call_args[0]
                assert "Failed to cleanup backend" in call_args[0]

    def test_cancel_does_not_cleanup_if_no_handle(self, test_db: Database) -> None:
        """Cancel does not attempt cleanup if no backend handle."""
        run_id = _create_run_in_state(test_db, StageState.PREPARING, backend_type=None, backend_handle=None)

        with patch("goldfish.state_machine.cancel._cleanup_backend") as mock_cleanup:
            result = cancel_run(test_db, run_id, "User cancellation")

            assert result["success"] is True
            mock_cleanup.assert_not_called()

    def test_cancel_does_not_cleanup_if_handle_is_none_but_type_set(self, test_db: Database) -> None:
        """Cancel does not attempt cleanup if backend_type set but backend_handle is None."""
        run_id = _create_run_in_state(test_db, StageState.RUNNING, backend_type="local", backend_handle=None)

        with patch("goldfish.state_machine.cancel._cleanup_backend") as mock_cleanup:
            result = cancel_run(test_db, run_id, "User cancellation")

            assert result["success"] is True
            # Cleanup should NOT be called because backend_handle is None
            mock_cleanup.assert_not_called()

    def test_cancel_does_not_cleanup_if_handle_is_empty_string(self, test_db: Database) -> None:
        """Cancel does not attempt cleanup if backend_handle is empty string."""
        run_id = _create_run_in_state(test_db, StageState.RUNNING, backend_type="local", backend_handle="")

        with patch("goldfish.state_machine.cancel._cleanup_backend") as mock_cleanup:
            result = cancel_run(test_db, run_id, "User cancellation")

            assert result["success"] is True
            # Cleanup should NOT be called because empty string is falsy
            mock_cleanup.assert_not_called()

    def test_cancel_does_not_cleanup_when_transition_fails(self, test_db: Database) -> None:
        """Cancel does not attempt cleanup when transition fails (e.g., from terminal state)."""
        # Create a run in COMPLETED state - transition should fail
        run_id = _create_run_in_state(
            test_db, StageState.COMPLETED, backend_type="local", backend_handle="container-123"
        )

        with patch("goldfish.state_machine.cancel._cleanup_backend") as mock_cleanup:
            result = cancel_run(test_db, run_id, "User cancellation")

            # Transition should fail
            assert result["success"] is False
            # Cleanup should NOT be called because transition failed
            mock_cleanup.assert_not_called()


class TestCleanupBackendDirectly:
    """Tests for _cleanup_backend function directly testing each branch."""

    def test_cleanup_backend_local_calls_terminate(self) -> None:
        """_cleanup_backend calls RunBackend.terminate for local backend via factory."""
        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            mock_backend = MagicMock()
            mock_factory.return_value = mock_backend

            _cleanup_backend("stage-123", "local", "container-abc123")

            mock_factory.assert_called_once_with("local")
            mock_backend.terminate.assert_called_once()
            # Verify the handle passed has correct values
            call_args = mock_backend.terminate.call_args[0][0]
            assert call_args.stage_run_id == "stage-123"
            assert call_args.backend_type == "local"
            assert call_args.backend_handle == "container-abc123"

    def test_cleanup_backend_gce_calls_terminate(self) -> None:
        """_cleanup_backend calls RunBackend.terminate for GCE backend via factory."""
        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            mock_backend = MagicMock()
            mock_factory.return_value = mock_backend

            _cleanup_backend("stage-456", "gce", "instance-xyz789")

            mock_factory.assert_called_once_with("gce")
            mock_backend.terminate.assert_called_once()
            # Verify the handle passed has correct values
            call_args = mock_backend.terminate.call_args[0][0]
            assert call_args.stage_run_id == "stage-456"
            assert call_args.backend_type == "gce"
            assert call_args.backend_handle == "instance-xyz789"

    def test_cleanup_backend_unknown_type_logs_warning(self) -> None:
        """_cleanup_backend logs warning for unknown backend type."""
        # Unknown backend type should log warning but not raise
        with patch("goldfish.state_machine.cancel.logger") as mock_logger:
            _cleanup_backend("stage-123", "unknown_type", "some-handle")

            mock_logger.warning.assert_called_once()
            call_args = mock_logger.warning.call_args[0]
            assert "Unknown backend type" in call_args[0]
            assert "unknown_type" in call_args


class TestCleanupBackendValidation:
    """Tests for _cleanup_backend validation of backend_handle."""

    def test_cleanup_backend_local_validates_container_id(self) -> None:
        """_cleanup_backend validates container ID before calling stop_container."""
        # Invalid container ID with shell injection attempt
        with pytest.raises(InvalidContainerIdError):
            _cleanup_backend("stage-123", "local", "container; rm -rf /")

    def test_cleanup_backend_local_rejects_path_traversal(self) -> None:
        """_cleanup_backend rejects path traversal in container ID."""
        with pytest.raises(InvalidContainerIdError):
            _cleanup_backend("stage-123", "local", "../../../etc/passwd")

    def test_cleanup_backend_gce_validates_instance_name(self) -> None:
        """_cleanup_backend validates instance name before calling delete_instance."""
        # Invalid instance name with shell injection attempt
        with pytest.raises(InvalidInstanceNameError):
            _cleanup_backend("stage-123", "gce", "instance; rm -rf /")

    def test_cleanup_backend_gce_rejects_path_traversal(self) -> None:
        """_cleanup_backend rejects path traversal in instance name."""
        with pytest.raises(InvalidInstanceNameError):
            _cleanup_backend("stage-123", "gce", "../../../etc/passwd")

    def test_cleanup_backend_gce_rejects_uppercase(self) -> None:
        """_cleanup_backend rejects uppercase in GCE instance name."""
        with pytest.raises(InvalidInstanceNameError):
            _cleanup_backend("stage-123", "gce", "Instance-Name")

    def test_cleanup_backend_local_accepts_valid_container_id(self) -> None:
        """_cleanup_backend accepts valid container ID via factory."""
        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            mock_backend = MagicMock()
            mock_factory.return_value = mock_backend
            # Valid 12-char hex container ID
            _cleanup_backend("stage-123", "local", "abc123def456")
            mock_factory.assert_called_once_with("local")
            mock_backend.terminate.assert_called_once()
            # Verify the handle passed has correct container ID
            call_args = mock_backend.terminate.call_args[0][0]
            assert call_args.backend_handle == "abc123def456"

    def test_cleanup_backend_gce_accepts_valid_instance_name(self) -> None:
        """_cleanup_backend accepts valid GCE instance name via factory."""
        with patch("goldfish.cloud.factory.create_backend_for_cleanup") as mock_factory:
            mock_backend = MagicMock()
            mock_factory.return_value = mock_backend
            # Valid lowercase instance name
            _cleanup_backend("stage-123", "gce", "goldfish-stage-abc123")
            mock_factory.assert_called_once_with("gce")
            mock_backend.terminate.assert_called_once()
            # Verify the handle passed has correct instance name
            call_args = mock_backend.terminate.call_args[0][0]
            assert call_args.backend_handle == "goldfish-stage-abc123"


class TestCancelReasonValidation:
    """Tests for cancel_run reason validation."""

    def test_cancel_rejects_short_reason(self, test_db: Database) -> None:
        """Cancel with reason < 15 chars raises ReasonTooShortError."""
        run_id = _create_run_in_state(test_db, StageState.RUNNING)

        with pytest.raises(ReasonTooShortError):
            cancel_run(test_db, run_id, "too short")

    def test_cancel_accepts_15_char_reason(self, test_db: Database) -> None:
        """Cancel with exactly 15 char reason succeeds."""
        run_id = _create_run_in_state(test_db, StageState.RUNNING)

        # Exactly 15 characters
        result = cancel_run(test_db, run_id, "123456789012345")

        assert result["success"] is True

    def test_cancel_rejects_14_char_reason(self, test_db: Database) -> None:
        """Cancel with exactly 14 char reason raises ReasonTooShortError."""
        run_id = _create_run_in_state(test_db, StageState.RUNNING)

        with pytest.raises(ReasonTooShortError):
            cancel_run(test_db, run_id, "12345678901234")  # 14 chars

    def test_cancel_rejects_empty_reason(self, test_db: Database) -> None:
        """Cancel with empty reason raises ReasonTooShortError."""
        run_id = _create_run_in_state(test_db, StageState.RUNNING)

        with pytest.raises(ReasonTooShortError):
            cancel_run(test_db, run_id, "")

    def test_cancel_rejects_short_whitespace_reason(self, test_db: Database) -> None:
        """Cancel with short whitespace reason raises ReasonTooShortError (14 spaces < 15)."""
        run_id = _create_run_in_state(test_db, StageState.RUNNING)

        with pytest.raises(ReasonTooShortError):
            cancel_run(test_db, run_id, "              ")  # 14 spaces - fails length check


class TestCancelRunIdValidation:
    """Tests for cancel_run run_id validation."""

    def test_cancel_rejects_invalid_run_id_format(self, test_db: Database) -> None:
        """Cancel with invalid run_id format raises InvalidStageRunIdError."""
        with pytest.raises(InvalidStageRunIdError):
            cancel_run(test_db, "../../../etc/passwd", "Valid reason over 15 chars")

    def test_cancel_rejects_malformed_run_id(self, test_db: Database) -> None:
        """Cancel with malformed run_id raises InvalidStageRunIdError."""
        with pytest.raises(InvalidStageRunIdError):
            cancel_run(test_db, "not-a-valid-format", "Valid reason over 15 chars")

    def test_cancel_rejects_empty_run_id(self, test_db: Database) -> None:
        """Cancel with empty run_id raises InvalidStageRunIdError."""
        with pytest.raises(InvalidStageRunIdError):
            cancel_run(test_db, "", "Valid reason over 15 chars")


class TestCancelWarmPoolLaunchingInstance:
    """Tests for cancel during warm pool instance launch."""

    def test_cancel_cleans_up_launching_warm_instance(self, test_db: Database) -> None:
        """Cancel during launch should clean up even when lease is NULL.

        During fresh launch, the warm instance row is pre-registered with
        current_lease_run_id=NULL (on_fresh_launch sets it later). If the
        user cancels before that, the cleanup guard must NOT skip cleanup
        just because the lease doesn't match the run_id.
        """
        now = datetime.now(UTC).isoformat()
        run_id = _create_run_in_state(
            test_db,
            StageState.LAUNCHING,
            backend_type="gce",
            backend_handle="goldfish-warm-001",
        )

        # Pre-register warm instance with NULL lease (simulating pre on_fresh_launch)
        with test_db._conn() as conn:
            conn.execute(
                """INSERT INTO warm_instances
                (instance_name, zone, project_id, machine_type, gpu_count,
                 image_family, image_project, preemptible, state, state_entered_at,
                 current_lease_run_id, created_at)
                VALUES ('goldfish-warm-001', 'us-central1-a', 'proj', 'a3-highgpu-1g', 1,
                        'debian-12', 'debian-cloud', 0, 'launching', ?, NULL, ?)""",
                (now, now),
            )

        with patch("goldfish.state_machine.cancel._cleanup_backend") as mock_cleanup:
            result = cancel_run(test_db, run_id, "User cancellation during launch")

            assert result["success"] is True
            # Cleanup MUST be called — the VM is ours even though lease is NULL
            mock_cleanup.assert_called_once_with(run_id, "gce", "goldfish-warm-001")

            # The warm_instances row must be transitioned to deleting so it stops
            # consuming pool capacity immediately (not waiting 15-min daemon timeout)
            inst = test_db.get_warm_instance("goldfish-warm-001")
            assert inst is not None
            assert inst["state"] == "deleting"

    def test_cancel_skips_cleanup_for_warm_instance_owned_by_other_run(self, test_db: Database) -> None:
        """Cancel should NOT clean up a warm instance owned by a different run."""
        now = datetime.now(UTC).isoformat()
        run_id = _create_run_in_state(
            test_db,
            StageState.RUNNING,
            backend_type="gce",
            backend_handle="goldfish-warm-001",
        )

        # Warm instance exists but is leased to a different run
        with test_db._conn() as conn:
            conn.execute(
                """INSERT INTO warm_instances
                (instance_name, zone, project_id, machine_type, gpu_count,
                 image_family, image_project, preemptible, state, state_entered_at,
                 current_lease_run_id, created_at)
                VALUES ('goldfish-warm-001', 'us-central1-a', 'proj', 'a3-highgpu-1g', 1,
                        'debian-12', 'debian-cloud', 0, 'busy', ?, 'stage-other-run', ?)""",
                (now, now),
            )

        with patch("goldfish.state_machine.cancel._cleanup_backend") as mock_cleanup:
            result = cancel_run(test_db, run_id, "User cancellation test reason")

            assert result["success"] is True
            # Cleanup must NOT be called — instance belongs to another run
            mock_cleanup.assert_not_called()

    def test_cancel_finalized_run_does_not_kill_idle_warm_vm(self, test_db: Database) -> None:
        """Cancel after finalization must NOT delete an idle warm VM.

        When a run reaches awaiting_user_finalization, the executor releases
        the lease and the instance returns to idle_ready for reuse. If the
        user cancels instead of finalizing, the NULL lease must NOT be treated
        as "still launching" — the instance is already back in the pool.
        """
        now = datetime.now(UTC).isoformat()
        run_id = _create_run_in_state(
            test_db,
            StageState.AWAITING_USER_FINALIZATION,
            backend_type="gce",
            backend_handle="goldfish-warm-001",
        )

        # Instance is idle_ready with NULL lease (lease released during finalization)
        with test_db._conn() as conn:
            conn.execute(
                """INSERT INTO warm_instances
                (instance_name, zone, project_id, machine_type, gpu_count,
                 image_family, image_project, preemptible, state, state_entered_at,
                 current_lease_run_id, created_at)
                VALUES ('goldfish-warm-001', 'us-central1-a', 'proj', 'a3-highgpu-1g', 1,
                        'debian-12', 'debian-cloud', 0, 'idle_ready', ?, NULL, ?)""",
                (now, now),
            )

        with patch("goldfish.state_machine.cancel._cleanup_backend") as mock_cleanup:
            result = cancel_run(test_db, run_id, "User canceled instead of finalizing")

            assert result["success"] is True
            # Cleanup must NOT be called — instance is back in the pool
            mock_cleanup.assert_not_called()

            # Instance must remain idle_ready (not deleted)
            inst = test_db.get_warm_instance("goldfish-warm-001")
            assert inst is not None
            assert inst["state"] == "idle_ready"

    def test_cancel_running_warm_instance_still_does_best_effort_cleanup(self, test_db: Database) -> None:
        """Even after controller finalization, cancel should still terminate the live VM promptly."""
        now = datetime.now(UTC).isoformat()
        run_id = _create_run_in_state(
            test_db,
            StageState.RUNNING,
            backend_type="gce",
            backend_handle="goldfish-warm-001",
        )

        with test_db._conn() as conn:
            conn.execute(
                """INSERT INTO warm_instances
                (instance_name, zone, project_id, machine_type, gpu_count,
                 image_family, image_project, preemptible, state, state_entered_at,
                 current_lease_run_id, created_at)
                VALUES ('goldfish-warm-001', 'us-central1-a', 'proj', 'a3-highgpu-1g', 1,
                        'debian-12', 'debian-cloud', 0, 'busy', ?, ?, ?)""",
                (now, run_id, now),
            )

        with patch("goldfish.state_machine.cancel._cleanup_backend") as mock_cleanup:
            result = cancel_run(test_db, run_id, "User canceled a running warm-pool job")

            assert result["success"] is True
            mock_cleanup.assert_called_once_with(run_id, "gce", "goldfish-warm-001")

            inst = test_db.get_warm_instance("goldfish-warm-001")
            assert inst is not None
            assert inst["state"] == "deleting"


class TestCancelNonexistentRun:
    """Tests for canceling non-existent runs."""

    def test_cancel_nonexistent_run_fails(self, test_db: Database) -> None:
        """Cancel on non-existent run returns failure."""
        # Use valid format but nonexistent run
        result = cancel_run(test_db, "stage-00000000", "User cancellation")

        assert result["success"] is False
        assert "not_found" in result["reason"]
