from unittest.mock import MagicMock, patch

from goldfish.cloud.contracts import BackendStatus
from goldfish.db.database import Database
from goldfish.state_machine.stage_daemon import StageDaemon
from goldfish.state_machine.types import StageState


class TestDaemonRaceCondition:
    def test_daemon_marks_completed_instead_of_failed(self, temp_dir):
        """StageDaemon should prioritize exit_code over instance-loss detection."""

        # 1. Setup DB and Config
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # 2. Insert a 'RUNNING' GCE stage run
        stage_run_id = "stage-acde01"
        with db._conn() as conn:
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute(
                """
                INSERT INTO stage_runs (
                    id, workspace_name, stage_name, version, state,
                    started_at, backend_type, backend_handle
                ) VALUES (?, ?, ?, ?, ?, datetime('now', '-21 minutes'), ?, ?)
                """,
                (stage_run_id, "w1", "train", "v1", "running", "gce", stage_run_id),
            )

        daemon = StageDaemon(db=db, config=None)

        backend = MagicMock()
        backend.get_status.return_value = BackendStatus.from_exit_code(0)

        # 3. If determine_instance_event were called, this would raise. It should
        # not be reached when an exit_code is available.
        with (
            patch.object(daemon, "_get_backend", return_value=backend),
            patch("goldfish.state_machine.stage_daemon.determine_instance_event", side_effect=AssertionError),
        ):
            run = db.get_stage_run(stage_run_id)
            assert run is not None
            daemon._process_run(run)

        # 5. Verify outcome
        stage_run = db.get_stage_run(stage_run_id)

        # EXPECTATION: EXIT_SUCCESS transitions RUNNING → POST_RUN
        assert stage_run is not None
        assert stage_run["state"] == StageState.POST_RUN.value
        assert stage_run["error"] is None

    def test_daemon_marks_failed_on_nonzero_exit_code(self, temp_dir):
        """StageDaemon should transition to FAILED when exit_code is non-zero."""
        db_path = temp_dir / "test_fail.db"
        db = Database(db_path)

        stage_run_id = "stage-acde02"
        with db._conn() as conn:
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute(
                """
                INSERT INTO stage_runs (
                    id, workspace_name, stage_name, version, state,
                    started_at, backend_type, backend_handle
                ) VALUES (?, ?, ?, ?, ?, datetime('now', '-21 minutes'), ?, ?)
                """,
                (stage_run_id, "w1", "train", "v1", "running", "gce", stage_run_id),
            )

        daemon = StageDaemon(db=db, config=None)
        backend = MagicMock()
        backend.get_status.return_value = BackendStatus.from_exit_code(1)

        with (
            patch.object(daemon, "_get_backend", return_value=backend),
            patch("goldfish.state_machine.stage_daemon.determine_instance_event", side_effect=AssertionError),
        ):
            run = db.get_stage_run(stage_run_id)
            assert run is not None
            daemon._process_run(run)

        stage_run = db.get_stage_run(stage_run_id)
        assert stage_run is not None
        assert stage_run["state"] == StageState.FAILED.value
        assert stage_run["error"] is None
