"""Integration tests for backend-primary exit code retrieval.

StageDaemon should not shell out to provider CLIs from core code. Instead, it
queries the configured RunBackend adapter and emits state machine events based
on the normalized BackendStatus.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from goldfish.cloud.contracts import BackendStatus
from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig
from goldfish.db.database import Database
from goldfish.state_machine.stage_daemon import StageDaemon
from goldfish.state_machine.types import StageState


def _create_workspace_and_version(db: Database, workspace: str = "test-ws", version: str = "v1") -> None:
    """Create workspace lineage and version for testing."""
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO workspace_lineage
            (workspace_name, created_at)
            VALUES (?, ?)""",
            (workspace, now),
        )
        conn.execute(
            """INSERT OR IGNORE INTO workspace_versions
            (workspace_name, version, git_tag, git_sha, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (workspace, version, f"{workspace}-{version}", "abc123", now, "test"),
        )


def _create_run(
    db: Database,
    run_id: str,
    state: StageState = StageState.RUNNING,
    *,
    backend_type: str,
    backend_handle: str,
) -> None:
    """Create a stage run for testing."""
    now = datetime.now(UTC).isoformat()
    with db._conn() as conn:
        conn.execute(
            """INSERT INTO stage_runs
            (id, workspace_name, version, stage_name, status, started_at,
             state, state_entered_at, backend_type, backend_handle)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                run_id,
                "test-ws",
                "v1",
                "train",
                "running",
                now,
                state.value,
                now,
                backend_type,
                backend_handle,
            ),
        )


def _get_run_state(db: Database, run_id: str) -> str | None:
    """Get current state of a run."""
    with db._conn() as conn:
        row = conn.execute("SELECT state FROM stage_runs WHERE id = ?", (run_id,)).fetchone()
        return row["state"] if row else None


class TestDaemonExitCodeViaBackendStatus:
    """Integration tests for StageDaemon exit transitions via RunBackend.get_status()."""

    def test_exit_success_transitions_to_post_run(self, test_db: Database) -> None:
        """Exit code 0 should emit EXIT_SUCCESS and transition RUNNING -> POST_RUN."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run(
            test_db,
            run_id,
            backend_type="gce",
            backend_handle=f"instance-{run_id}",
        )

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(backend="gce"),
            gce=GCEConfig(project="test-gcp-project", zones=["us-central1-a"]),
            gcs=GCSConfig(bucket="test-bucket"),
        )

        daemon = StageDaemon(test_db, config)

        backend = MagicMock()
        backend.get_status.return_value = BackendStatus.from_exit_code(0)

        captured: dict[str, str | None] = {}

        def fake_get_backend(backend_type: str, *, project_id: str | None):
            captured["backend_type"] = backend_type
            captured["project_id"] = project_id
            return backend

        with (
            patch.object(daemon._leader, "try_acquire_lease", return_value=True),
            patch.object(daemon, "_get_backend", side_effect=fake_get_backend),
            patch("goldfish.state_machine.stage_daemon.determine_instance_event", side_effect=AssertionError),
        ):
            daemon.poll_active_runs()

        assert captured["backend_type"] == "gce"
        assert captured["project_id"] == "test-gcp-project"
        assert _get_run_state(test_db, run_id) == StageState.POST_RUN.value

    def test_exit_failure_transitions_to_failed(self, test_db: Database) -> None:
        """Exit code non-zero should emit EXIT_FAILURE and transition RUNNING -> FAILED."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run(
            test_db,
            run_id,
            backend_type="gce",
            backend_handle=f"instance-{run_id}",
        )

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(backend="gce"),
            gce=GCEConfig(project="test-gcp-project", zones=["us-central1-a"]),
            gcs=GCSConfig(bucket="test-bucket"),
        )

        daemon = StageDaemon(test_db, config)

        backend = MagicMock()
        backend.get_status.return_value = BackendStatus.from_exit_code(1)

        with (
            patch.object(daemon._leader, "try_acquire_lease", return_value=True),
            patch.object(daemon, "_get_backend", return_value=backend),
            patch("goldfish.state_machine.stage_daemon.determine_instance_event", side_effect=AssertionError),
        ):
            daemon.poll_active_runs()

        assert _get_run_state(test_db, run_id) == StageState.FAILED.value

    def test_local_exit_success_transitions_to_post_run(self, test_db: Database) -> None:
        """Local backend should also transition RUNNING -> POST_RUN on exit code 0."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run(
            test_db,
            run_id,
            backend_type="local",
            backend_handle=f"container-{run_id}",
        )

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(backend="local"),
        )

        daemon = StageDaemon(test_db, config)

        backend = MagicMock()
        backend.get_status.return_value = BackendStatus.from_exit_code(0)

        with (
            patch.object(daemon._leader, "try_acquire_lease", return_value=True),
            patch.object(daemon, "_get_backend", return_value=backend),
            patch("goldfish.state_machine.stage_daemon.determine_instance_event", side_effect=AssertionError),
        ):
            daemon.poll_active_runs()

        assert _get_run_state(test_db, run_id) == StageState.POST_RUN.value


class TestPostRunInstanceLostRaceCondition:
    """Regression tests for POST_RUN vs INSTANCE_LOST behavior."""

    def test_daemon_poll_sequence_exit_success_then_instance_stopped(self, test_db: Database) -> None:
        """Sequence: RUNNING -> POST_RUN, then poll again should not emit INSTANCE_LOST."""
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_run(
            test_db,
            run_id,
            state=StageState.RUNNING,
            backend_type="gce",
            backend_handle=f"instance-{run_id}",
        )

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            jobs=JobsConfig(backend="gce"),
            gce=GCEConfig(project="test-gcp-project", zones=["us-central1-a"]),
            gcs=GCSConfig(bucket="test-bucket"),
        )

        daemon = StageDaemon(test_db, config)

        backend = MagicMock()
        backend.get_status.return_value = BackendStatus.from_exit_code(0)

        with (
            patch.object(daemon._leader, "try_acquire_lease", return_value=True),
            patch.object(daemon, "_get_backend", return_value=backend),
            patch("goldfish.state_machine.stage_daemon.determine_instance_event", side_effect=AssertionError),
        ):
            daemon.poll_active_runs()

        assert _get_run_state(test_db, run_id) == StageState.POST_RUN.value

        # Second poll: POST_RUN should not consult backend status for exit codes.
        with (
            patch.object(daemon._leader, "try_acquire_lease", return_value=True),
            patch.object(daemon, "_get_backend", side_effect=AssertionError),
        ):
            daemon.poll_active_runs()

        assert _get_run_state(test_db, run_id) == StageState.POST_RUN.value
