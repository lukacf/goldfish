"""Tests for stage daemon - Phase 5.2.

Tests for the StageDaemon class that uses event-driven architecture
with the state machine instead of if/then/else status updates.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from goldfish.state_machine.stage_daemon import StageDaemon
from goldfish.state_machine.types import (
    StageEvent,
    StageState,
    TerminationCause,
)


@pytest.fixture
def daemon() -> StageDaemon:
    """Create a StageDaemon with mock dependencies for testing."""
    return StageDaemon(db=MagicMock(), config=MagicMock())


class TestStageDaemonInit:
    """Tests for StageDaemon.__init__() holder_id validation."""

    def test_init_with_valid_holder_id(self) -> None:
        """Valid holder_id should be accepted."""
        daemon = StageDaemon(db=MagicMock(), config=MagicMock(), holder_id="valid-holder-123")
        assert daemon._holder_id == "valid-holder-123"

    def test_init_with_invalid_holder_id_raises_value_error(self) -> None:
        """Invalid holder_id should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid holder_id"):
            StageDaemon(db=MagicMock(), config=MagicMock(), holder_id="invalid/holder/id")

    def test_init_with_none_holder_id_auto_generates(self) -> None:
        """None holder_id should auto-generate a valid ID."""
        daemon = StageDaemon(db=MagicMock(), config=MagicMock(), holder_id=None)
        assert daemon._holder_id is not None
        assert daemon._holder_id.startswith("daemon-")

    def test_init_with_empty_holder_id_raises_value_error(self) -> None:
        """Empty holder_id should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid holder_id"):
            StageDaemon(db=MagicMock(), config=MagicMock(), holder_id="")

    def test_init_with_config_none(self) -> None:
        """StageDaemon should work with config=None."""
        daemon = StageDaemon(db=MagicMock(), config=None)
        assert daemon._config is None
        assert daemon._holder_id is not None
        assert daemon._leader is not None


class TestDetermineEvent:
    """Tests for _determine_event() method."""

    def test_determine_event_preparing_timeout(self, daemon: StageDaemon) -> None:
        """PREPARING state timeout should return TIMEOUT event."""
        run = {
            "id": "stage-123",
            "state": StageState.PREPARING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(minutes=20)).isoformat(),
            "backend_type": "local",
        }

        event, ctx = daemon._determine_event(run)

        assert event == StageEvent.TIMEOUT
        assert ctx.source == "daemon"
        assert ctx.timestamp is not None

    def test_determine_event_building_timeout(self, daemon: StageDaemon) -> None:
        """BUILDING state timeout should return TIMEOUT event."""
        run = {
            "id": "stage-123",
            "state": StageState.BUILDING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(minutes=35)).isoformat(),
            "backend_type": "local",
        }

        event, ctx = daemon._determine_event(run)

        assert event == StageEvent.TIMEOUT
        assert ctx.source == "daemon"

    def test_determine_event_launching_timeout(self, daemon: StageDaemon) -> None:
        """LAUNCHING state timeout should return TIMEOUT event."""
        run = {
            "id": "stage-123",
            "state": StageState.LAUNCHING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(minutes=25)).isoformat(),
            "backend_type": "gce",
        }

        event, ctx = daemon._determine_event(run)

        assert event == StageEvent.TIMEOUT
        assert ctx.source == "daemon"

    def test_determine_event_running_timeout(self, daemon: StageDaemon) -> None:
        """RUNNING state timeout should return TIMEOUT event."""
        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(hours=25)).isoformat(),
            "backend_type": "local",
        }

        event, ctx = daemon._determine_event(run)

        assert event == StageEvent.TIMEOUT

    def test_determine_event_finalizing_timeout_critical_phases_done(self, daemon: StageDaemon) -> None:
        """FINALIZING timeout with both phases done should set critical_phases_done=True."""
        run = {
            "id": "stage-123",
            "state": StageState.FINALIZING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(minutes=35)).isoformat(),
            "backend_type": "local",
            "output_sync_done": 1,
            "output_recording_done": 1,
        }

        event, ctx = daemon._determine_event(run)

        assert event == StageEvent.TIMEOUT
        assert ctx.critical_phases_done is True

    def test_determine_event_finalizing_timeout_critical_phases_not_done(self, daemon: StageDaemon) -> None:
        """FINALIZING timeout with phases not done should set critical_phases_done=False."""
        run = {
            "id": "stage-123",
            "state": StageState.FINALIZING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(minutes=35)).isoformat(),
            "backend_type": "local",
            "output_sync_done": 0,
            "output_recording_done": 1,
        }

        event, ctx = daemon._determine_event(run)

        assert event == StageEvent.TIMEOUT
        assert ctx.critical_phases_done is False

    def test_determine_event_finalizing_timeout_sync_done_but_recording_not(self, daemon: StageDaemon) -> None:
        """FINALIZING timeout with sync done but recording not should set critical_phases_done=False."""
        run = {
            "id": "stage-123",
            "state": StageState.FINALIZING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(minutes=35)).isoformat(),
            "backend_type": "local",
            "output_sync_done": 1,
            "output_recording_done": 0,
        }

        event, ctx = daemon._determine_event(run)

        assert event == StageEvent.TIMEOUT
        assert ctx.critical_phases_done is False

    def test_determine_event_finalizing_timeout_missing_phase_fields(self, daemon: StageDaemon) -> None:
        """FINALIZING timeout with missing phase fields should set critical_phases_done=False."""
        run = {
            "id": "stage-123",
            "state": StageState.FINALIZING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(minutes=35)).isoformat(),
            "backend_type": "local",
            # No output_sync_done or output_recording_done
        }

        event, ctx = daemon._determine_event(run)

        assert event == StageEvent.TIMEOUT
        assert ctx.critical_phases_done is False

    def test_determine_event_unknown_24h_cleanup(self, daemon: StageDaemon) -> None:
        """UNKNOWN state should timeout after 24 hours."""
        run = {
            "id": "stage-123",
            "state": StageState.UNKNOWN.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(hours=25)).isoformat(),
            "backend_type": "local",
        }

        event, ctx = daemon._determine_event(run)

        assert event == StageEvent.TIMEOUT

    def test_determine_event_no_timeout_within_limit(self, daemon: StageDaemon) -> None:
        """No timeout event when within time limit."""
        run = {
            "id": "stage-123",
            "state": StageState.PREPARING.value,
            "state_entered_at": datetime.now(UTC).isoformat(),
            "backend_type": "local",
        }

        result = daemon._determine_event(run)

        assert result is None

    def test_determine_event_terminal_state_skipped(self, daemon: StageDaemon) -> None:
        """Terminal states should not generate events."""
        for state in [
            StageState.COMPLETED,
            StageState.FAILED,
            StageState.TERMINATED,
            StageState.CANCELED,
        ]:
            run = {
                "id": "stage-123",
                "state": state.value,
                "state_entered_at": (datetime.now(UTC) - timedelta(hours=48)).isoformat(),
                "backend_type": "local",
            }

            result = daemon._determine_event(run)

            assert result is None

    def test_determine_event_gce_config_valueerror_handled(self) -> None:
        """GCE config ValueError for effective_project_id should be handled."""
        mock_config = MagicMock()
        mock_gce = MagicMock()
        type(mock_gce).effective_project_id = property(
            lambda self: (_ for _ in ()).throw(ValueError("No project configured"))
        )
        mock_config.gce = mock_gce

        daemon = StageDaemon(db=MagicMock(), config=mock_config)

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "state_entered_at": datetime.now(UTC).isoformat(),
            "backend_type": "gce",
            "backend_handle": "instance-123",
        }

        # Should not raise - ValueError should be caught
        with patch("goldfish.state_machine.stage_daemon.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = False
            result = daemon._determine_event(run)

        # No event expected since instance is running
        assert result is None

    def test_determine_event_gce_backend_with_config_gce_none(self) -> None:
        """GCE backend with config.gce=None should not get project_id."""
        mock_config = MagicMock()
        mock_config.gce = None

        daemon = StageDaemon(db=MagicMock(), config=mock_config)

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "state_entered_at": datetime.now(UTC).isoformat(),
            "backend_type": "gce",
            "backend_handle": "instance-123",
        }

        with patch("goldfish.state_machine.stage_daemon.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = False
            result = daemon._determine_event(run)

        # No event expected since instance is running
        assert result is None
        # Verify verify_instance_stopped was called with project_id=None
        mock_verify.assert_called_once()
        call_kwargs = mock_verify.call_args.kwargs
        assert call_kwargs.get("project_id") is None

    def test_determine_event_finalizing_instance_lost(self, daemon: StageDaemon) -> None:
        """FINALIZING state with instance lost should emit INSTANCE_LOST (not timeout).

        critical_phases_done should be None (not set) for INSTANCE_LOST events.
        Only TIMEOUT events on FINALIZING set critical_phases_done.
        """
        run = {
            "id": "stage-123",
            "state": StageState.FINALIZING.value,
            "state_entered_at": datetime.now(UTC).isoformat(),  # Not timed out
            "backend_type": "local",
            "backend_handle": "container-abc123",
            "output_sync_done": 0,
            "output_recording_done": 0,
        }

        with patch("goldfish.state_machine.stage_daemon.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = True  # Instance is stopped

            event, ctx = daemon._determine_event(run)

        assert event == StageEvent.INSTANCE_LOST
        assert ctx.instance_confirmed_dead is True
        assert ctx.source == "daemon"
        # CRITICAL: instance_lost should NOT set critical_phases_done
        assert ctx.critical_phases_done is None

    def test_determine_event_unknown_instance_lost(self, daemon: StageDaemon) -> None:
        """UNKNOWN state with instance lost should emit INSTANCE_LOST (not wait for timeout)."""
        run = {
            "id": "stage-123",
            "state": StageState.UNKNOWN.value,
            "state_entered_at": datetime.now(UTC).isoformat(),  # Not timed out
            "backend_type": "local",
            "backend_handle": "container-abc123",
        }

        with patch("goldfish.state_machine.stage_daemon.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = True  # Instance is stopped

            event, ctx = daemon._determine_event(run)

        assert event == StageEvent.INSTANCE_LOST
        assert ctx.instance_confirmed_dead is True
        assert ctx.source == "daemon"

    def test_determine_event_with_config_none(self) -> None:
        """_determine_event with config=None should handle GCE backend gracefully."""
        daemon = StageDaemon(db=MagicMock(), config=None)

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "state_entered_at": datetime.now(UTC).isoformat(),
            "backend_type": "gce",
            "backend_handle": "instance-123",
        }

        with patch("goldfish.state_machine.stage_daemon.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = False
            result = daemon._determine_event(run)

        # No event expected since instance is running
        assert result is None
        # Verify verify_instance_stopped was called with project_id=None
        mock_verify.assert_called_once()
        call_kwargs = mock_verify.call_args.kwargs
        assert call_kwargs.get("project_id") is None


class TestDetermineBackendEvent:
    """Tests for _determine_backend_event() method (unified GCE/local)."""

    @patch("goldfish.state_machine.stage_daemon.verify_instance_stopped")
    def test_gce_instance_lost_when_stopped(self, mock_verify, daemon: StageDaemon) -> None:
        """INSTANCE_LOST event when GCE instance is confirmed stopped."""
        mock_verify.return_value = True

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "gce",
            "backend_handle": "instance-123",
        }

        event, ctx = daemon._determine_backend_event(run, "gce", project_id="my-project")

        assert event == StageEvent.INSTANCE_LOST
        assert ctx.instance_confirmed_dead is True
        assert ctx.source == "daemon"

    @patch("goldfish.state_machine.stage_daemon.verify_instance_stopped")
    def test_gce_no_event_when_running(self, mock_verify, daemon: StageDaemon) -> None:
        """No event when GCE instance is still running."""
        mock_verify.return_value = False

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "gce",
            "backend_handle": "instance-123",
        }

        result = daemon._determine_backend_event(run, "gce", project_id="my-project")

        assert result is None

    @patch("goldfish.state_machine.stage_daemon.verify_instance_stopped")
    def test_local_container_lost_when_stopped(self, mock_verify, daemon: StageDaemon) -> None:
        """INSTANCE_LOST event when Docker container is confirmed stopped."""
        mock_verify.return_value = True

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "local",
            "backend_handle": "container-abc123",
        }

        event, ctx = daemon._determine_backend_event(run, "local")

        assert event == StageEvent.INSTANCE_LOST
        assert ctx.instance_confirmed_dead is True
        assert ctx.source == "daemon"

    @patch("goldfish.state_machine.stage_daemon.verify_instance_stopped")
    def test_local_container_no_event_when_running(self, mock_verify, daemon: StageDaemon) -> None:
        """No event when Docker container is still running."""
        mock_verify.return_value = False

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "local",
            "backend_handle": "container-abc123",
        }

        result = daemon._determine_backend_event(run, "local")

        assert result is None


class TestCheckTimeout:
    """Tests for _check_timeout() method."""

    def test_preparing_timeout_15min(self, daemon: StageDaemon) -> None:
        """PREPARING should timeout after 15 minutes."""
        run = {
            "state": StageState.PREPARING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(minutes=16)).isoformat(),
        }

        assert daemon._check_timeout(run) is True

        run["state_entered_at"] = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
        assert daemon._check_timeout(run) is False

    def test_building_timeout_30min(self, daemon: StageDaemon) -> None:
        """BUILDING should timeout after 30 minutes."""
        run = {
            "state": StageState.BUILDING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(minutes=31)).isoformat(),
        }

        assert daemon._check_timeout(run) is True

    def test_launching_timeout_20min(self, daemon: StageDaemon) -> None:
        """LAUNCHING should timeout after 20 minutes."""
        run = {
            "state": StageState.LAUNCHING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(minutes=21)).isoformat(),
        }

        assert daemon._check_timeout(run) is True

    def test_running_timeout_24h(self, daemon: StageDaemon) -> None:
        """RUNNING should timeout after 24 hours."""
        run = {
            "state": StageState.RUNNING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(hours=25)).isoformat(),
        }

        assert daemon._check_timeout(run) is True

    def test_finalizing_timeout_30min(self, daemon: StageDaemon) -> None:
        """FINALIZING should timeout after 30 minutes."""
        run = {
            "state": StageState.FINALIZING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(minutes=31)).isoformat(),
        }

        assert daemon._check_timeout(run) is True

    def test_unknown_timeout_24h(self, daemon: StageDaemon) -> None:
        """UNKNOWN should timeout after 24 hours."""
        run = {
            "state": StageState.UNKNOWN.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(hours=25)).isoformat(),
        }

        assert daemon._check_timeout(run) is True


class TestPollActiveRuns:
    """Tests for poll_active_runs() method."""

    def test_poll_acquires_lease_before_processing(self, test_db) -> None:
        """poll_active_runs should acquire lease before processing."""
        config = MagicMock()
        config.gce = None

        daemon = StageDaemon(db=test_db, config=config)
        daemon._leader = MagicMock()
        daemon._leader.try_acquire_lease.return_value = False

        daemon.poll_active_runs()

        # Should have tried to acquire lease
        daemon._leader.try_acquire_lease.assert_called()

    def test_poll_skips_processing_without_lease(self, test_db) -> None:
        """poll_active_runs should skip processing if lease not acquired."""
        config = MagicMock()
        config.gce = None

        daemon = StageDaemon(db=test_db, config=config)
        daemon._leader = MagicMock()
        daemon._leader.try_acquire_lease.return_value = False

        # Create a run that would generate an event
        test_db.create_workspace_lineage("test_ws", description="Test")
        test_db.create_version("test_ws", "v1", "tag-1", "abc123", "run")
        test_db.create_stage_run(
            stage_run_id="stage-test",
            workspace_name="test_ws",
            version="v1",
            stage_name="train",
        )
        with test_db._conn() as conn:
            conn.execute(
                """UPDATE stage_runs SET state = ?, state_entered_at = datetime('now', '-1 hour')
                WHERE id = ?""",
                (StageState.PREPARING.value, "stage-test"),
            )

        # Mock transition to track if it's called
        daemon._transition = MagicMock()

        daemon.poll_active_runs()

        # Should NOT have called transition
        daemon._transition.assert_not_called()

    def test_poll_emits_events_to_state_machine(self, test_db) -> None:
        """poll_active_runs should emit events to state machine."""
        config = MagicMock()
        config.gce = None

        daemon = StageDaemon(db=test_db, config=config)
        daemon._leader = MagicMock()
        daemon._leader.try_acquire_lease.return_value = True

        # Create a run that should timeout
        test_db.create_workspace_lineage("test_ws", description="Test")
        test_db.create_version("test_ws", "v1", "tag-1", "abc123", "run")
        test_db.create_stage_run(
            stage_run_id="stage-test",
            workspace_name="test_ws",
            version="v1",
            stage_name="train",
        )
        with test_db._conn() as conn:
            conn.execute(
                """UPDATE stage_runs SET state = ?, state_entered_at = datetime('now', '-1 hour')
                WHERE id = ?""",
                (StageState.PREPARING.value, "stage-test"),
            )

        # Mock transition
        daemon._transition = MagicMock()

        daemon.poll_active_runs()

        # Should have emitted TIMEOUT event
        daemon._transition.assert_called()
        call_args = daemon._transition.call_args
        assert call_args[0][0] == "stage-test"  # run_id
        assert call_args[0][1] == StageEvent.TIMEOUT  # event

    def test_poll_processes_multiple_runs_in_sequence(self, test_db) -> None:
        """poll_active_runs should process all runs, not just the first one."""
        config = MagicMock()
        config.gce = None

        daemon = StageDaemon(db=test_db, config=config)
        daemon._leader = MagicMock()
        daemon._leader.try_acquire_lease.return_value = True

        # Create multiple runs that should timeout
        test_db.create_workspace_lineage("test_ws", description="Test")
        test_db.create_version("test_ws", "v1", "tag-1", "abc123", "run")

        for i in range(3):
            test_db.create_stage_run(
                stage_run_id=f"stage-{i}",
                workspace_name="test_ws",
                version="v1",
                stage_name="train",
            )
            with test_db._conn() as conn:
                conn.execute(
                    """UPDATE stage_runs SET state = ?, state_entered_at = datetime('now', '-1 hour')
                    WHERE id = ?""",
                    (StageState.PREPARING.value, f"stage-{i}"),
                )

        # Mock transition to track all calls
        daemon._transition = MagicMock()

        daemon.poll_active_runs()

        # Should have emitted events for ALL 3 runs
        assert daemon._transition.call_count == 3

        # Verify all run IDs were processed
        processed_ids = {call[0][0] for call in daemon._transition.call_args_list}
        assert processed_ids == {"stage-0", "stage-1", "stage-2"}

    def test_poll_does_not_release_lease_after_processing(self, test_db) -> None:
        """poll_active_runs should NOT release the lease after processing.

        Per the design comment: 'We don't release the lease - it will auto-expire if we crash'.
        The lease should persist after poll completes (held until expiry or next renewal).
        """
        from goldfish.state_machine.leader_election import DaemonLeaderElection

        config = MagicMock()
        config.gce = None

        daemon = StageDaemon(db=test_db, config=config)
        # Use the real leader election, not a mock
        daemon._leader = DaemonLeaderElection(test_db)

        # Poll (no runs to process, but lease should be acquired)
        daemon.poll_active_runs()

        # Verify the lease is still held after poll completes
        assert daemon._leader.is_leader(daemon._holder_id) is True


class TestGetActiveRuns:
    """Tests for get_active_runs() method."""

    def test_get_active_runs_returns_active_states(self, test_db) -> None:
        """get_active_runs should return runs in active states."""
        daemon = StageDaemon(db=test_db, config=MagicMock())

        # Create runs in various states
        test_db.create_workspace_lineage("test_ws", description="Test")
        test_db.create_version("test_ws", "v1", "tag-1", "abc123", "run")

        active_states = [
            StageState.PREPARING,
            StageState.BUILDING,
            StageState.LAUNCHING,
            StageState.RUNNING,
            StageState.FINALIZING,
        ]

        for i, state in enumerate(active_states):
            test_db.create_stage_run(
                stage_run_id=f"stage-{i}",
                workspace_name="test_ws",
                version="v1",
                stage_name="train",
            )
            with test_db._conn() as conn:
                conn.execute(
                    "UPDATE stage_runs SET state = ? WHERE id = ?",
                    (state.value, f"stage-{i}"),
                )

        # Create a completed run (should not be returned)
        test_db.create_stage_run(
            stage_run_id="stage-completed",
            workspace_name="test_ws",
            version="v1",
            stage_name="train",
        )
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE stage_runs SET state = ? WHERE id = ?",
                (StageState.COMPLETED.value, "stage-completed"),
            )

        runs = daemon.get_active_runs()

        # Should return 5 active runs
        assert len(runs) == 5
        run_ids = {r["id"] for r in runs}
        assert "stage-completed" not in run_ids

    def test_get_active_runs_includes_unknown_state(self, test_db) -> None:
        """get_active_runs should include UNKNOWN state for cleanup."""
        daemon = StageDaemon(db=test_db, config=MagicMock())

        test_db.create_workspace_lineage("test_ws", description="Test")
        test_db.create_version("test_ws", "v1", "tag-1", "abc123", "run")

        test_db.create_stage_run(
            stage_run_id="stage-unknown",
            workspace_name="test_ws",
            version="v1",
            stage_name="train",
        )
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE stage_runs SET state = ? WHERE id = ?",
                (StageState.UNKNOWN.value, "stage-unknown"),
            )

        runs = daemon.get_active_runs()

        assert len(runs) == 1
        assert runs[0]["state"] == StageState.UNKNOWN.value

    def test_get_active_runs_limit_parameter(self, test_db) -> None:
        """get_active_runs should respect the limit parameter."""
        daemon = StageDaemon(db=test_db, config=MagicMock())

        test_db.create_workspace_lineage("test_ws", description="Test")
        test_db.create_version("test_ws", "v1", "tag-1", "abc123", "run")

        # Create 5 runs
        for i in range(5):
            test_db.create_stage_run(
                stage_run_id=f"stage-{i}",
                workspace_name="test_ws",
                version="v1",
                stage_name="train",
            )
            with test_db._conn() as conn:
                conn.execute(
                    "UPDATE stage_runs SET state = ? WHERE id = ?",
                    (StageState.RUNNING.value, f"stage-{i}"),
                )

        # Limit to 3
        runs = daemon.get_active_runs(limit=3)
        assert len(runs) == 3

        # Default limit should return all
        runs_all = daemon.get_active_runs()
        assert len(runs_all) == 5

    def test_get_active_runs_empty_database_returns_empty_list(self, test_db) -> None:
        """get_active_runs should return empty list when no runs exist."""
        daemon = StageDaemon(db=test_db, config=MagicMock())

        runs = daemon.get_active_runs()

        assert runs == []

    def test_get_active_runs_default_limit_is_constant(self, test_db) -> None:
        """get_active_runs default limit should match DEFAULT_ACTIVE_RUNS_LIMIT constant."""
        from goldfish.state_machine.stage_daemon import DEFAULT_ACTIVE_RUNS_LIMIT

        daemon = StageDaemon(db=test_db, config=MagicMock())

        # Verify the constant value is 1000
        assert DEFAULT_ACTIVE_RUNS_LIMIT == 1000

        # Verify the method signature uses the constant as default
        import inspect

        sig = inspect.signature(daemon.get_active_runs)
        limit_param = sig.parameters["limit"]
        assert limit_param.default == DEFAULT_ACTIVE_RUNS_LIMIT

    def test_get_active_runs_ordered_by_started_at_asc(self, test_db) -> None:
        """get_active_runs should return runs ordered by started_at ascending."""
        daemon = StageDaemon(db=test_db, config=MagicMock())

        test_db.create_workspace_lineage("test_ws", description="Test")
        test_db.create_version("test_ws", "v1", "tag-1", "abc123", "run")

        # Create runs with specific started_at times (oldest first should be returned first)
        for i in range(3):
            test_db.create_stage_run(
                stage_run_id=f"stage-{i}",
                workspace_name="test_ws",
                version="v1",
                stage_name="train",
            )
            # Set started_at: stage-0 is oldest (-2 hours), stage-2 is newest (0 hours)
            with test_db._conn() as conn:
                offset = 2 - i  # stage-0: -2 hours, stage-1: -1 hour, stage-2: -0 hours
                conn.execute(
                    f"""UPDATE stage_runs SET state = ?,
                    started_at = datetime('now', '-{offset} hours')
                    WHERE id = ?""",
                    (StageState.RUNNING.value, f"stage-{i}"),
                )

        runs = daemon.get_active_runs()

        # Should be ordered by started_at ASC (oldest first)
        # stage-0 is oldest (-2 hours), stage-1 is middle (-1 hour), stage-2 is newest (0 hours)
        run_ids = [r["id"] for r in runs]
        assert run_ids == ["stage-0", "stage-1", "stage-2"]


class TestTransitionMethod:
    """Tests for _transition() method."""

    def test_transition_calls_state_machine_transition(self, daemon: StageDaemon) -> None:
        """_transition should call the state machine transition function."""
        from goldfish.state_machine.types import EventContext as EC

        with patch("goldfish.state_machine.stage_daemon.transition") as mock_transition:
            context = EC(
                timestamp=datetime.now(UTC),
                source="daemon",
            )

            daemon._transition("stage-123", StageEvent.TIMEOUT, context)

            mock_transition.assert_called_once()
            call_args = mock_transition.call_args
            assert call_args[0][0] == daemon._db  # db
            assert call_args[0][1] == "stage-123"  # run_id
            assert call_args[0][2] == StageEvent.TIMEOUT  # event
            assert call_args[0][3] == context  # context


class TestDetermineEventEdgeCases:
    """Tests for _determine_event() edge cases."""

    def test_determine_event_missing_state_returns_none(self, daemon: StageDaemon) -> None:
        """_determine_event should return None when state is missing."""
        run = {
            "id": "stage-123",
            "state": None,
            "backend_type": "local",
        }

        result = daemon._determine_event(run)
        assert result is None

    def test_determine_event_empty_state_returns_none(self, daemon: StageDaemon) -> None:
        """_determine_event should return None when state is empty string."""
        run = {
            "id": "stage-123",
            "state": "",
            "backend_type": "local",
        }

        result = daemon._determine_event(run)
        assert result is None

    def test_determine_event_invalid_state_returns_none(self, daemon: StageDaemon) -> None:
        """_determine_event should return None for invalid state value."""
        run = {
            "id": "stage-123",
            "state": "invalid_state_value",
            "backend_type": "local",
        }

        result = daemon._determine_event(run)
        assert result is None

    def test_determine_event_missing_backend_type_defaults_to_local(self, daemon: StageDaemon) -> None:
        """_determine_event with missing backend_type should default to 'local'."""
        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "state_entered_at": datetime.now(UTC).isoformat(),  # Not timed out
            "backend_handle": "container-abc123",
            # backend_type is missing - should default to "local"
        }

        with patch("goldfish.state_machine.stage_daemon.verify_instance_stopped") as mock_verify:
            mock_verify.return_value = True  # Instance is stopped

            event, ctx = daemon._determine_event(run)

        assert event == StageEvent.INSTANCE_LOST
        # Verify local backend was used (verify_instance_stopped called with "local")
        mock_verify.assert_called_once()
        call_kwargs = mock_verify.call_args.kwargs
        assert call_kwargs.get("backend_type") == "local"


class TestDetermineBackendEventEdgeCases:
    """Tests for _determine_backend_event() edge cases."""

    def test_gce_event_missing_backend_handle_returns_none(self, daemon: StageDaemon) -> None:
        """_determine_backend_event should return None when backend_handle is missing."""
        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "gce",
            "backend_handle": None,
        }

        result = daemon._determine_backend_event(run, "gce", project_id="my-project")
        assert result is None

    def test_local_event_missing_backend_handle_returns_none(self, daemon: StageDaemon) -> None:
        """_determine_backend_event should return None when backend_handle is missing."""
        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "local",
            "backend_handle": None,
        }

        result = daemon._determine_backend_event(run, "local")
        assert result is None

    def test_gce_event_empty_string_backend_handle_returns_none(self, daemon: StageDaemon) -> None:
        """_determine_backend_event should return None when backend_handle is empty string."""
        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "gce",
            "backend_handle": "",
        }

        result = daemon._determine_backend_event(run, "gce", project_id="my-project")
        assert result is None

    def test_local_event_empty_string_backend_handle_returns_none(self, daemon: StageDaemon) -> None:
        """_determine_backend_event should return None when backend_handle is empty string."""
        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "local",
            "backend_handle": "",
        }

        result = daemon._determine_backend_event(run, "local")
        assert result is None

    @patch("goldfish.state_machine.stage_daemon.verify_instance_stopped")
    def test_gce_event_verify_instance_error_returns_none(self, mock_verify, daemon: StageDaemon) -> None:
        """_determine_backend_event should return None on verify_instance_stopped error."""
        mock_verify.side_effect = RuntimeError("GCE API error")

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "gce",
            "backend_handle": "instance-123",
        }

        result = daemon._determine_backend_event(run, "gce", project_id="my-project")
        assert result is None

    @patch("goldfish.state_machine.stage_daemon.verify_instance_stopped")
    def test_local_event_verify_instance_error_returns_none(self, mock_verify, daemon: StageDaemon) -> None:
        """_determine_backend_event should return None on verify_instance_stopped error."""
        mock_verify.side_effect = RuntimeError("Docker API error")

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "local",
            "backend_handle": "container-123",
        }

        result = daemon._determine_backend_event(run, "local")
        assert result is None

    @patch("goldfish.state_machine.stage_daemon.detect_termination_cause")
    @patch("goldfish.state_machine.stage_daemon.verify_instance_stopped")
    def test_gce_event_detect_termination_error_defaults_orphaned(
        self, mock_verify, mock_detect, daemon: StageDaemon
    ) -> None:
        """_determine_backend_event should default to ORPHANED on termination cause error."""
        mock_verify.return_value = True
        mock_detect.side_effect = RuntimeError("API error")

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "gce",
            "backend_handle": "instance-123",
        }

        event, ctx = daemon._determine_backend_event(run, "gce", project_id="my-project")
        assert event == StageEvent.INSTANCE_LOST
        assert ctx.termination_cause == TerminationCause.ORPHANED

    @patch("goldfish.state_machine.stage_daemon.detect_termination_cause")
    @patch("goldfish.state_machine.stage_daemon.verify_instance_stopped")
    def test_local_event_detect_termination_error_defaults_orphaned(
        self, mock_verify, mock_detect, daemon: StageDaemon
    ) -> None:
        """_determine_backend_event should default to ORPHANED on termination cause error."""
        mock_verify.return_value = True
        mock_detect.side_effect = RuntimeError("Docker API error")

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "local",
            "backend_handle": "container-123",
        }

        event, ctx = daemon._determine_backend_event(run, "local")
        assert event == StageEvent.INSTANCE_LOST
        assert ctx.termination_cause == TerminationCause.ORPHANED

    @patch("goldfish.state_machine.stage_daemon.detect_termination_cause")
    @patch("goldfish.state_machine.stage_daemon.verify_instance_stopped")
    def test_gce_event_termination_cause_in_context(self, mock_verify, mock_detect, daemon: StageDaemon) -> None:
        """_determine_backend_event should include termination cause in context."""
        mock_verify.return_value = True
        mock_detect.return_value = TerminationCause.PREEMPTED

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "gce",
            "backend_handle": "instance-123",
        }

        event, ctx = daemon._determine_backend_event(run, "gce", project_id="my-project")
        assert event == StageEvent.INSTANCE_LOST
        assert ctx.termination_cause == TerminationCause.PREEMPTED

    @patch("goldfish.state_machine.stage_daemon.detect_termination_cause")
    @patch("goldfish.state_machine.stage_daemon.verify_instance_stopped")
    def test_local_event_termination_cause_in_context(self, mock_verify, mock_detect, daemon: StageDaemon) -> None:
        """_determine_backend_event should include termination cause in context."""
        mock_verify.return_value = True
        mock_detect.return_value = TerminationCause.CRASHED

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "local",
            "backend_handle": "container-123",
        }

        event, ctx = daemon._determine_backend_event(run, "local")
        assert event == StageEvent.INSTANCE_LOST
        assert ctx.termination_cause == TerminationCause.CRASHED


class TestCheckTimeoutEdgeCases:
    """Tests for _check_timeout() edge cases."""

    def test_check_timeout_missing_state_returns_false(self, daemon: StageDaemon) -> None:
        """_check_timeout should return False when state is None."""
        run = {
            "state": None,
            "state_entered_at": (datetime.now(UTC) - timedelta(hours=1)).isoformat(),
        }

        assert daemon._check_timeout(run) is False

    def test_check_timeout_terminal_state_returns_false(self, daemon: StageDaemon) -> None:
        """_check_timeout should return False for terminal states (no timeout defined)."""
        # Terminal states like COMPLETED, FAILED have no timeout in STATE_TIMEOUTS
        run = {
            "state": StageState.COMPLETED.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(hours=48)).isoformat(),
        }

        # Should return False because COMPLETED has no timeout defined
        assert daemon._check_timeout(run) is False

    def test_check_timeout_missing_state_entered_at_returns_false(self, daemon: StageDaemon) -> None:
        """_check_timeout should return False when state_entered_at is missing."""
        run = {
            "state": StageState.PREPARING.value,
            "state_entered_at": None,
        }

        assert daemon._check_timeout(run) is False

    def test_check_timeout_invalid_timestamp_returns_false(self, daemon: StageDaemon) -> None:
        """_check_timeout should return False for invalid timestamp format."""
        run = {
            "state": StageState.PREPARING.value,
            "state_entered_at": "not-a-valid-timestamp",
        }

        assert daemon._check_timeout(run) is False

    def test_check_timeout_timezone_naive_timestamp_works(self, daemon: StageDaemon) -> None:
        """_check_timeout should handle timezone-naive timestamps."""
        # Create timezone-naive timestamp (no +00:00 suffix)
        # This simulates how SQLite might return timestamps without timezone info
        # Use a time clearly in the past to ensure timeout is detected
        naive_timestamp = "2020-01-01T00:00:00"

        run = {
            "state": StageState.PREPARING.value,
            "state_entered_at": naive_timestamp,
        }

        # Should still work and detect timeout (timestamp is years ago)
        assert daemon._check_timeout(run) is True

    def test_check_timeout_exactly_at_boundary_returns_false(self, daemon: StageDaemon) -> None:
        """_check_timeout at or just under the timeout boundary should return False.

        The implementation uses 'elapsed > timeout', not 'elapsed >= timeout'.
        So at or just under the boundary should NOT trigger timeout.
        """
        # PREPARING has 15-minute timeout
        # Set state_entered_at to 14 minutes 59 seconds ago (just under boundary)
        run = {
            "state": StageState.PREPARING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(minutes=14, seconds=59)).isoformat(),
        }

        # Should return False (not yet exceeded)
        assert daemon._check_timeout(run) is False

        # 15 minutes and 1 second should trigger timeout
        run["state_entered_at"] = (datetime.now(UTC) - timedelta(minutes=15, seconds=1)).isoformat()
        assert daemon._check_timeout(run) is True


class TestProcessRunErrorHandling:
    """Tests for _process_run() error handling."""

    def test_process_run_catches_exceptions(self, daemon: StageDaemon) -> None:
        """_process_run should catch and log exceptions without crashing."""
        # Make _determine_event raise an exception
        daemon._determine_event = MagicMock(side_effect=RuntimeError("Test error"))

        run = {"id": "stage-123", "state": "running"}

        # Should not raise - should catch and log
        daemon._process_run(run)

        # Verify _determine_event was called
        daemon._determine_event.assert_called_once_with(run)

    def test_process_run_handles_missing_id_key(self, daemon: StageDaemon) -> None:
        """_process_run should handle run dict without 'id' key."""
        # Create a run without "id" key - should default to "unknown"
        run = {"state": StageState.RUNNING.value}

        # Should not raise
        daemon._process_run(run)


class TestPollActiveRunsErrorHandling:
    """Tests for poll_active_runs() error handling."""

    def test_poll_active_runs_catches_exceptions(self, test_db) -> None:
        """poll_active_runs should catch and log exceptions without crashing."""
        daemon = StageDaemon(db=test_db, config=MagicMock())
        daemon._leader = MagicMock()
        daemon._leader.try_acquire_lease.return_value = True

        # Make get_active_runs raise an exception
        daemon.get_active_runs = MagicMock(side_effect=RuntimeError("DB error"))

        # Should not raise - should catch and log
        daemon.poll_active_runs()

        # Verify get_active_runs was called
        daemon.get_active_runs.assert_called_once()

    def test_poll_continues_after_single_run_exception(self, test_db) -> None:
        """poll_active_runs should continue processing runs after one fails."""
        config = MagicMock()
        config.gce = None

        daemon = StageDaemon(db=test_db, config=config)
        daemon._leader = MagicMock()
        daemon._leader.try_acquire_lease.return_value = True

        # Create multiple runs
        test_db.create_workspace_lineage("test_ws", description="Test")
        test_db.create_version("test_ws", "v1", "tag-1", "abc123", "run")

        for i in range(3):
            test_db.create_stage_run(
                stage_run_id=f"stage-{i}",
                workspace_name="test_ws",
                version="v1",
                stage_name="train",
            )
            with test_db._conn() as conn:
                conn.execute(
                    """UPDATE stage_runs SET state = ?, state_entered_at = datetime('now', '-1 hour')
                    WHERE id = ?""",
                    (StageState.PREPARING.value, f"stage-{i}"),
                )

        # Track processed runs
        processed_runs = []
        call_count = [0]

        def failing_transition(run_id, event, context):
            call_count[0] += 1
            processed_runs.append(run_id)
            if run_id == "stage-1":
                raise RuntimeError("Simulated failure on stage-1")

        daemon._transition = failing_transition

        # Should not raise - should catch per-run exception and continue
        daemon.poll_active_runs()

        # All 3 runs should have been attempted (despite stage-1 failing)
        assert len(processed_runs) == 3
        assert "stage-0" in processed_runs
        assert "stage-1" in processed_runs
        assert "stage-2" in processed_runs


class TestEventContextProperties:
    """Tests for EventContext properties set by daemon."""

    def test_timeout_event_has_correct_source(self, daemon: StageDaemon) -> None:
        """EventContext should have source='daemon' for timeout events."""
        run = {
            "id": "stage-123",
            "state": StageState.PREPARING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(minutes=20)).isoformat(),
            "backend_type": "local",
        }

        event, ctx = daemon._determine_event(run)

        assert ctx.source == "daemon"

    def test_timeout_event_has_timestamp(self, daemon: StageDaemon) -> None:
        """EventContext should have a timestamp set."""
        run = {
            "id": "stage-123",
            "state": StageState.PREPARING.value,
            "state_entered_at": (datetime.now(UTC) - timedelta(minutes=20)).isoformat(),
            "backend_type": "local",
        }

        event, ctx = daemon._determine_event(run)

        assert ctx.timestamp is not None
        assert isinstance(ctx.timestamp, datetime)
        # Should be recent (within last minute)
        assert (datetime.now(UTC) - ctx.timestamp).total_seconds() < 60

    @patch("goldfish.state_machine.stage_daemon.verify_instance_stopped")
    def test_instance_lost_event_has_correct_source(self, mock_verify, daemon: StageDaemon) -> None:
        """EventContext should have source='daemon' for instance lost events."""
        mock_verify.return_value = True

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "local",
            "backend_handle": "container-123",
        }

        event, ctx = daemon._determine_backend_event(run, "local")

        assert ctx.source == "daemon"

    @patch("goldfish.state_machine.stage_daemon.verify_instance_stopped")
    def test_instance_lost_event_has_timestamp(self, mock_verify, daemon: StageDaemon) -> None:
        """EventContext should have a timestamp set for instance lost events."""
        mock_verify.return_value = True

        run = {
            "id": "stage-123",
            "state": StageState.RUNNING.value,
            "backend_type": "local",
            "backend_handle": "container-123",
        }

        event, ctx = daemon._determine_backend_event(run, "local")

        assert ctx.timestamp is not None
        assert isinstance(ctx.timestamp, datetime)


class TestStateTimeoutsConstant:
    """Tests for STATE_TIMEOUTS constant values."""

    def test_state_timeouts_has_all_active_states(self) -> None:
        """STATE_TIMEOUTS should have entries for all active states."""
        from goldfish.state_machine.stage_daemon import STATE_TIMEOUTS

        expected_states = [
            StageState.PREPARING,
            StageState.BUILDING,
            StageState.LAUNCHING,
            StageState.RUNNING,
            StageState.FINALIZING,
            StageState.UNKNOWN,
        ]

        for state in expected_states:
            assert state in STATE_TIMEOUTS, f"Missing timeout for {state}"

    def test_state_timeouts_values_match_spec(self) -> None:
        """STATE_TIMEOUTS values should match spec requirements."""
        from goldfish.state_machine.stage_daemon import STATE_TIMEOUTS

        # Verify specific timeout values from implementation
        assert STATE_TIMEOUTS[StageState.PREPARING] == timedelta(minutes=15)
        assert STATE_TIMEOUTS[StageState.BUILDING] == timedelta(minutes=30)
        assert STATE_TIMEOUTS[StageState.LAUNCHING] == timedelta(minutes=20)
        assert STATE_TIMEOUTS[StageState.RUNNING] == timedelta(hours=24)
        assert STATE_TIMEOUTS[StageState.FINALIZING] == timedelta(minutes=30)
        assert STATE_TIMEOUTS[StageState.UNKNOWN] == timedelta(hours=24)

    def test_state_timeouts_has_no_terminal_states(self) -> None:
        """STATE_TIMEOUTS should NOT have entries for terminal states."""
        from goldfish.state_machine.stage_daemon import STATE_TIMEOUTS
        from goldfish.state_machine.transitions import TERMINAL_STATES

        for state in TERMINAL_STATES:
            assert state not in STATE_TIMEOUTS, f"Unexpected timeout for terminal state {state}"
