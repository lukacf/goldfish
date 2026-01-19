"""Integration tests for metadata-primary exit code retrieval.

These tests verify the end-to-end flow of exit code retrieval through the
state machine daemon, ensuring that instance metadata is tried first (PRIMARY)
and GCS is used as fallback (SECONDARY).

The tests mock the subprocess calls to gcloud/gsutil but exercise the actual
integration between:
- StageDaemon (polls active runs)
- get_exit_code_gce (metadata-first retrieval)
- State machine transitions (EXIT_SUCCESS, EXIT_FAILURE, EXIT_MISSING)
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig
from goldfish.db.database import Database
from goldfish.state_machine.exit_code import ExitCodeResult, get_exit_code_gce
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


def _create_gce_run(
    db: Database,
    run_id: str,
    state: StageState = StageState.RUNNING,
    instance_name: str | None = None,
) -> None:
    """Create a GCE stage run for testing."""
    now = datetime.now(UTC).isoformat()
    handle = instance_name or f"instance-{run_id}"
    with db._conn() as conn:
        conn.execute(
            """INSERT INTO stage_runs
            (id, workspace_name, version, stage_name, status, started_at,
             state, state_entered_at, backend_type, backend_handle)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, "test-ws", "v1", "train", "running", now, state.value, now, "gce", handle),
        )


def _get_run_state(db: Database, run_id: str) -> str | None:
    """Get current state of a run."""
    with db._conn() as conn:
        row = conn.execute("SELECT state FROM stage_runs WHERE id = ?", (run_id,)).fetchone()
        return row["state"] if row else None


class TestMetadataPrimaryFlow:
    """Integration tests for metadata-primary exit code retrieval through daemon."""

    def test_daemon_uses_metadata_first_for_gce_runs(self, test_db: Database) -> None:
        """Test that daemon passes instance info enabling metadata-first retrieval.

        This verifies the integration between StageDaemon and get_exit_code_gce,
        ensuring instance_name and instance_zone are passed for GCE runs.
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        instance_name = f"instance-{run_id}"
        _create_gce_run(test_db, run_id, instance_name=instance_name)

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            gce=GCEConfig(project="test-gcp-project", zones=["us-central1-a"]),
            gcs=GCSConfig(bucket="test-bucket"),
        )

        daemon = StageDaemon(test_db, config)

        # Track calls to get_exit_code_gce
        captured_calls: list[dict] = []

        def mock_get_exit_code_gce(
            bucket_uri: str,
            stage_run_id: str,
            project_id: str | None = None,
            max_attempts: int = 5,  # noqa: ARG001
            retry_delay: float = 2.0,  # noqa: ARG001
            instance_name: str | None = None,
            instance_zone: str | None = None,
        ) -> ExitCodeResult:
            captured_calls.append(
                {
                    "bucket_uri": bucket_uri,
                    "stage_run_id": stage_run_id,
                    "project_id": project_id,
                    "instance_name": instance_name,
                    "instance_zone": instance_zone,
                }
            )
            return ExitCodeResult.from_code(0)

        with patch(
            "goldfish.state_machine.stage_daemon.get_exit_code_gce",
            side_effect=mock_get_exit_code_gce,
        ):
            daemon.poll_active_runs()

        # Verify get_exit_code_gce was called with instance info
        assert len(captured_calls) == 1
        call = captured_calls[0]
        assert call["instance_name"] == instance_name
        assert call["instance_zone"] == "us-central1-a"
        assert call["project_id"] == "test-gcp-project"

    def test_metadata_success_transitions_to_post_run(self, test_db: Database) -> None:
        """Test that successful exit code via metadata triggers correct state transition.

        When metadata returns exit code 0, daemon should emit EXIT_SUCCESS
        which transitions RUNNING -> POST_RUN.
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_gce_run(test_db, run_id)

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            gce=GCEConfig(project="test-gcp-project", zones=["us-central1-a"]),
            gcs=GCSConfig(bucket="test-bucket"),
        )

        daemon = StageDaemon(test_db, config)

        # Mock metadata returning exit code 0 (metadata-first success)
        with patch(
            "goldfish.state_machine.stage_daemon.get_exit_code_gce",
            return_value=ExitCodeResult.from_code(0),
        ):
            daemon.poll_active_runs()

        # Verify state transitioned to POST_RUN
        assert _get_run_state(test_db, run_id) == StageState.POST_RUN.value

    def test_metadata_failure_exit_code_transitions_to_failed(self, test_db: Database) -> None:
        """Test that non-zero exit code via metadata triggers FAILED transition.

        When metadata returns exit code != 0, daemon should emit EXIT_FAILURE
        which transitions RUNNING -> FAILED.
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_gce_run(test_db, run_id)

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            gce=GCEConfig(project="test-gcp-project", zones=["us-central1-a"]),
            gcs=GCSConfig(bucket="test-bucket"),
        )

        daemon = StageDaemon(test_db, config)

        # Mock metadata returning exit code 1 (process failure)
        with patch(
            "goldfish.state_machine.stage_daemon.get_exit_code_gce",
            return_value=ExitCodeResult.from_code(1),
        ):
            daemon.poll_active_runs()

        # Verify state transitioned to FAILED
        assert _get_run_state(test_db, run_id) == StageState.FAILED.value


class TestMetadataGCSFallback:
    """Integration tests for GCS fallback when metadata is unavailable."""

    def test_gcs_fallback_when_metadata_fails(self) -> None:
        """Test that GCS is used when metadata lookup fails.

        Scenario: Instance metadata is unavailable (instance deleted),
        but exit_code.txt exists in GCS.
        """
        # Mock subprocess for both gcloud (metadata) and gsutil (GCS)
        with patch("subprocess.run") as mock_run:

            def subprocess_handler(cmd, **_kw):
                result = MagicMock()
                if "gcloud" in cmd[0]:
                    # Metadata lookup fails (instance not found)
                    from subprocess import CalledProcessError

                    raise CalledProcessError(1, cmd, stderr="instance not found")
                elif "gsutil" in cmd[0]:
                    # GCS returns exit code 0
                    result.stdout = "0"
                    result.returncode = 0
                return result

            mock_run.side_effect = subprocess_handler

            result = get_exit_code_gce(
                bucket_uri="gs://test-bucket",
                stage_run_id="stage-abc123",
                project_id="test-project",
                instance_name="deleted-instance",
                instance_zone="us-central1-a",
                max_attempts=1,
                retry_delay=0,
            )

        assert result.exists is True
        assert result.code == 0
        assert result.gcs_error is False

    def test_gcs_only_when_no_instance_info(self) -> None:
        """Test that only GCS is used when instance info is not available.

        Scenario: No instance_name/zone provided (maybe local run or info lost).
        Should go straight to GCS without attempting metadata.
        """
        gcloud_called = False

        with patch("subprocess.run") as mock_run:

            def subprocess_handler(cmd, **_kw):
                nonlocal gcloud_called
                result = MagicMock()
                if "gcloud" in cmd[0]:
                    gcloud_called = True
                    raise RuntimeError("Should not call gcloud")
                elif "gsutil" in cmd[0]:
                    result.stdout = "42"
                    result.returncode = 0
                return result

            mock_run.side_effect = subprocess_handler

            result = get_exit_code_gce(
                bucket_uri="gs://test-bucket",
                stage_run_id="stage-abc123",
                project_id="test-project",
                # No instance_name or instance_zone
                max_attempts=1,
                retry_delay=0,
            )

        assert not gcloud_called, "Should not call gcloud when instance info missing"
        assert result.exists is True
        assert result.code == 42


class TestMetadataRetrievalOrder:
    """Integration tests verifying the order of retrieval attempts."""

    def test_metadata_checked_before_gcs(self) -> None:
        """Test that metadata is checked BEFORE GCS (not after).

        The primary design is: metadata first, GCS second.
        This test verifies that order is respected.
        """
        call_order: list[str] = []

        with patch("subprocess.run") as mock_run:

            def subprocess_handler(cmd, **_kw):
                result = MagicMock()
                if "gcloud" in cmd[0]:
                    call_order.append("metadata")
                    # Metadata succeeds with exit code 0
                    result.stdout = "0"
                    result.returncode = 0
                elif "gsutil" in cmd[0]:
                    call_order.append("gcs")
                    result.stdout = "0"
                    result.returncode = 0
                return result

            mock_run.side_effect = subprocess_handler

            result = get_exit_code_gce(
                bucket_uri="gs://test-bucket",
                stage_run_id="stage-abc123",
                project_id="test-project",
                instance_name="my-instance",
                instance_zone="us-central1-a",
                max_attempts=1,
                retry_delay=0,
            )

        # Metadata should be called first and succeed, GCS should NOT be called
        assert call_order == ["metadata"]
        assert result.code == 0

    def test_gcs_called_only_after_metadata_fails(self) -> None:
        """Test that GCS is called only when metadata doesn't return a result.

        If metadata returns empty/None, GCS should be tried next.
        """
        call_order: list[str] = []

        with patch("subprocess.run") as mock_run:

            def subprocess_handler(cmd, **_kw):
                result = MagicMock()
                if "gcloud" in cmd[0]:
                    call_order.append("metadata")
                    # Metadata returns empty (no exit code set yet)
                    result.stdout = ""
                    result.returncode = 0
                elif "gsutil" in cmd[0]:
                    call_order.append("gcs")
                    result.stdout = "0"
                    result.returncode = 0
                return result

            mock_run.side_effect = subprocess_handler

            result = get_exit_code_gce(
                bucket_uri="gs://test-bucket",
                stage_run_id="stage-abc123",
                project_id="test-project",
                instance_name="my-instance",
                instance_zone="us-central1-a",
                max_attempts=1,
                retry_delay=0,
            )

        # Both should be called in order: metadata first, then GCS
        assert call_order == ["metadata", "gcs"]
        assert result.code == 0


class TestDaemonGCEConfiguration:
    """Integration tests for daemon behavior with various GCE configurations."""

    def test_daemon_with_multi_zone_config_uses_first_zone(self, test_db: Database) -> None:
        """Test that daemon uses first zone from config for metadata lookup.

        When multiple zones are configured, the daemon should use the first
        zone for metadata lookup (since we don't yet store zone per-run).
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        _create_gce_run(test_db, run_id)

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            gce=GCEConfig(
                project="test-gcp-project",
                zones=["europe-west4-a", "us-central1-a", "asia-east1-b"],  # Multiple zones
            ),
            gcs=GCSConfig(bucket="test-bucket"),
        )

        daemon = StageDaemon(test_db, config)

        captured_zone: str | None = None

        def mock_get_exit_code_gce(
            bucket_uri: str,  # noqa: ARG001
            stage_run_id: str,  # noqa: ARG001
            project_id: str | None = None,  # noqa: ARG001
            max_attempts: int = 5,  # noqa: ARG001
            retry_delay: float = 2.0,  # noqa: ARG001
            instance_name: str | None = None,  # noqa: ARG001
            instance_zone: str | None = None,
        ) -> ExitCodeResult:
            nonlocal captured_zone
            captured_zone = instance_zone
            return ExitCodeResult.from_code(0)

        with patch(
            "goldfish.state_machine.stage_daemon.get_exit_code_gce",
            side_effect=mock_get_exit_code_gce,
        ):
            daemon.poll_active_runs()

        # Should use first zone from config
        assert captured_zone == "europe-west4-a"

    def test_daemon_without_gce_config_skips_metadata(self, test_db: Database) -> None:
        """Test that daemon gracefully handles missing GCE config.

        Without GCE config, daemon should still work but won't pass
        instance info for metadata lookup.
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create a local run (not GCE)
        now = datetime.now(UTC).isoformat()
        with test_db._conn() as conn:
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
                    StageState.RUNNING.value,
                    now,
                    "local",
                    f"container-{run_id}",
                ),
            )

        # Config without GCE
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
        )

        daemon = StageDaemon(test_db, config)

        # Mock docker inspect for local backend
        with patch(
            "goldfish.state_machine.stage_daemon.get_exit_code_docker",
            return_value=ExitCodeResult.from_code(0),
        ):
            daemon.poll_active_runs()

        # Should transition to POST_RUN
        assert _get_run_state(test_db, run_id) == StageState.POST_RUN.value


class TestPostRunInstanceLostRaceCondition:
    """Regression tests for POST_RUN INSTANCE_LOST race condition.

    Bug: When a run successfully transitions to POST_RUN (after EXIT_SUCCESS),
    the daemon's next poll would detect that the instance is stopped and
    emit INSTANCE_LOST, causing the run to transition to TERMINATED instead
    of completing normally via POST_RUN_OK.

    Root cause: The instance being stopped in POST_RUN is EXPECTED (the process
    already exited), not a failure. The INSTANCE_LOST check should only apply
    to RUNNING state where instance disappearance is unexpected.

    Fix: POST_RUN was added to the states that skip INSTANCE_LOST checks.
    """

    def test_post_run_state_not_affected_by_stopped_instance(self, test_db: Database) -> None:
        """POST_RUN run should not transition to TERMINATED when instance is stopped.

        This is the core regression test. After EXIT_SUCCESS, the run goes to
        POST_RUN. The instance stopping at this point is normal and expected.
        The daemon should NOT emit INSTANCE_LOST.
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in POST_RUN state (as if EXIT_SUCCESS just happened)
        now = datetime.now(UTC).isoformat()
        with test_db._conn() as conn:
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
                    StageState.POST_RUN.value,
                    now,
                    "gce",
                    f"instance-{run_id}",
                ),
            )

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            gce=GCEConfig(project="test-gcp-project", zones=["us-central1-a"]),
            gcs=GCSConfig(bucket="test-bucket"),
        )

        daemon = StageDaemon(test_db, config)

        # Mock instance as STOPPED (normal after process exit)
        # The old buggy code would emit INSTANCE_LOST here
        with patch("subprocess.run") as mock_run:

            def subprocess_handler(cmd, **_kw):
                result = MagicMock()
                if "gcloud" in cmd[0] and "instances" in cmd and "list" in cmd:
                    # Instance is TERMINATED (stopped)
                    result.stdout = "TERMINATED"
                    result.returncode = 0
                return result

            mock_run.side_effect = subprocess_handler

            # Poll - this should NOT emit INSTANCE_LOST for POST_RUN state
            daemon.poll_active_runs()

        # State should remain POST_RUN, NOT transition to TERMINATED
        final_state = _get_run_state(test_db, run_id)
        assert final_state == StageState.POST_RUN.value, (
            f"Expected POST_RUN but got {final_state}. " "Bug: Daemon emitted INSTANCE_LOST for POST_RUN state."
        )

    def test_running_state_still_detects_instance_lost(self, test_db: Database) -> None:
        """RUNNING run should still transition to TERMINATED when instance is lost.

        This ensures the fix didn't break the legitimate INSTANCE_LOST detection
        for RUNNING state, where instance disappearance IS unexpected.
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"

        # Create run in RUNNING state
        now = datetime.now(UTC).isoformat()
        with test_db._conn() as conn:
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
                    StageState.RUNNING.value,
                    now,
                    "gce",
                    f"instance-{run_id}",
                ),
            )

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            gce=GCEConfig(project="test-gcp-project", zones=["us-central1-a"]),
            gcs=GCSConfig(bucket="test-bucket"),
        )

        daemon = StageDaemon(test_db, config)

        # Mock: no exit code available, instance is gone
        with (
            patch(
                "goldfish.state_machine.stage_daemon.get_exit_code_gce",
                return_value=ExitCodeResult.from_not_found(),
            ),
            patch("subprocess.run") as mock_run,
        ):

            def subprocess_handler(cmd, **_kw):
                result = MagicMock()
                if "gcloud" in cmd[0] and "instances" in cmd and "list" in cmd:
                    # Instance not found (deleted)
                    result.stdout = ""
                    result.returncode = 0
                elif "gcloud" in cmd[0] and "operations" in cmd:
                    # No preemption events
                    result.stdout = "[]"
                    result.returncode = 0
                return result

            mock_run.side_effect = subprocess_handler

            daemon.poll_active_runs()

        # RUNNING state should transition to TERMINATED when instance is lost
        final_state = _get_run_state(test_db, run_id)
        assert final_state == StageState.TERMINATED.value, (
            f"Expected TERMINATED but got {final_state}. " "RUNNING state should still detect INSTANCE_LOST."
        )

    def test_daemon_poll_sequence_exit_success_then_instance_stopped(self, test_db: Database) -> None:
        """Simulate the full sequence: RUNNING → POST_RUN, then instance stops.

        This tests the actual race condition scenario:
        1. First poll: RUNNING, exit_code=0 → EXIT_SUCCESS → POST_RUN
        2. Second poll: POST_RUN, instance stopped → should NOT emit INSTANCE_LOST
        """
        _create_workspace_and_version(test_db)
        run_id = f"stage-{uuid.uuid4().hex[:8]}"
        instance_name = f"instance-{run_id}"
        _create_gce_run(test_db, run_id, state=StageState.RUNNING, instance_name=instance_name)

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            gce=GCEConfig(project="test-gcp-project", zones=["us-central1-a"]),
            gcs=GCSConfig(bucket="test-bucket"),
        )

        daemon = StageDaemon(test_db, config)

        # Poll 1: RUNNING state, exit code 0 → should transition to POST_RUN
        with patch(
            "goldfish.state_machine.stage_daemon.get_exit_code_gce",
            return_value=ExitCodeResult.from_code(0),
        ):
            daemon.poll_active_runs()

        assert _get_run_state(test_db, run_id) == StageState.POST_RUN.value

        # Poll 2: POST_RUN state, instance is stopped → should NOT emit INSTANCE_LOST
        with patch("subprocess.run") as mock_run:

            def subprocess_handler(cmd, **_kw):
                result = MagicMock()
                if "gcloud" in cmd[0] and "instances" in cmd and "list" in cmd:
                    # Instance is TERMINATED
                    result.stdout = "TERMINATED"
                    result.returncode = 0
                return result

            mock_run.side_effect = subprocess_handler

            daemon.poll_active_runs()

        # State should still be POST_RUN, NOT TERMINATED
        final_state = _get_run_state(test_db, run_id)
        assert final_state == StageState.POST_RUN.value, (
            f"Expected POST_RUN but got {final_state}. "
            "Race condition bug: daemon emitted INSTANCE_LOST after EXIT_SUCCESS."
        )
