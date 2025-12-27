from unittest.mock import MagicMock, patch

from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig
from goldfish.daemon import GoldfishDaemon
from goldfish.db.database import Database
from goldfish.models import StageRunStatus


class TestDaemonRaceCondition:
    @patch("subprocess.run")
    def test_daemon_marks_completed_instead_of_failed(self, mock_run, temp_dir):
        """Test that daemon checks for exit_code.txt before marking as failed."""

        # 1. Setup DB and Config
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            gce=GCEConfig(project="test-project", zones=["us-central1-a"]),
            gcs=GCSConfig(bucket="test-bucket"),
            jobs=JobsConfig(backend="gce"),
        )

        daemon = GoldfishDaemon(project_root=temp_dir)
        daemon.config = config
        daemon._db = db

        # 2. Insert a 'RUNNING' GCE stage run
        stage_run_id = "stage-race-123"
        with db._conn() as conn:
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute(
                """
                INSERT INTO stage_runs (
                    id, workspace_name, stage_name, version, status,
                    started_at, backend_type, backend_handle
                ) VALUES (?, ?, ?, ?, ?, datetime('now', '-21 minutes'), ?, ?)
                """,
                (stage_run_id, "w1", "train", "v1", StageRunStatus.RUNNING, "gce", stage_run_id),
            )

        # 3. Mock gcloud list to show NO instances (instance disappeared)
        mock_run.side_effect = [
            # First call: gcloud compute instances list
            MagicMock(stdout="", returncode=0),
            # Second call: gsutil cat exit_code.txt (to be implemented in fix)
            MagicMock(stdout="0", returncode=0),
        ]

        # 4. Run the check
        # We need to ensure _check_orphaned_instances is called.
        daemon._check_orphaned_instances()

        # 5. Verify outcome
        stage_run = db.get_stage_run(stage_run_id)

        # EXPECTATION: It should be COMPLETED now that we check GCS
        assert stage_run["status"] == StageRunStatus.COMPLETED
        assert stage_run["error"] is None

    @patch("subprocess.run")
    def test_daemon_marks_failed_on_nonzero_exit_code(self, mock_run, temp_dir):
        """Test that daemon marks as FAILED if exit_code.txt is non-zero."""
        db_path = temp_dir / "test_fail.db"
        db = Database(db_path)

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            gce=GCEConfig(project="test-project", zones=["us-central1-a"]),
            gcs=GCSConfig(bucket="test-bucket"),
            jobs=JobsConfig(backend="gce"),
        )

        daemon = GoldfishDaemon(project_root=temp_dir)
        daemon.config = config
        daemon._db = db

        stage_run_id = "stage-fail-123"
        with db._conn() as conn:
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute(
                """
                INSERT INTO stage_runs (
                    id, workspace_name, stage_name, version, status,
                    started_at, backend_type, backend_handle
                ) VALUES (?, ?, ?, ?, ?, datetime('now', '-21 minutes'), ?, ?)
                """,
                (stage_run_id, "w1", "train", "v1", StageRunStatus.RUNNING, "gce", stage_run_id),
            )

        mock_run.side_effect = [
            MagicMock(stdout="", returncode=0),  # instances list
            MagicMock(stdout="1", returncode=0),  # exit_code.txt = 1
            MagicMock(stdout="", returncode=1),  # _check_if_preempted (not preempted)
        ]

        daemon._check_orphaned_instances()

        stage_run = db.get_stage_run(stage_run_id)
        assert stage_run["status"] == StageRunStatus.FAILED
        assert "exit_code=1" in stage_run["error"]
