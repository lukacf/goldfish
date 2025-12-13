"""Tests for database transaction management - P1.

TDD: Write failing tests first, then implement.
"""

import sqlite3

import pytest

from goldfish.db.database import Database


class TestTransactionManagement:
    """Tests for database transaction context manager."""

    def test_transaction_context_manager_exists(self, temp_dir):
        """Database should have a transaction context manager."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # Should have transaction method
        assert hasattr(db, "transaction")

    def test_transaction_commits_on_success(self, temp_dir):
        """Transaction should commit when block completes normally."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        with db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sources (id, name, created_at, created_by, gcs_location, status)
                VALUES ('test-source', 'Test', '2024-01-01', 'test', 'gs://bucket', 'available')
                """
            )

        # Should be persisted
        source = db.get_source("test-source")
        assert source is not None
        assert source["name"] == "Test"

    def test_transaction_rolls_back_on_exception(self, temp_dir):
        """Transaction should rollback when exception is raised."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        with pytest.raises(ValueError):
            with db.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO sources (id, name, created_at, created_by, gcs_location, status)
                    VALUES ('test-source', 'Test', '2024-01-01', 'test', 'gs://bucket', 'available')
                    """
                )
                raise ValueError("Intentional failure")

        # Should NOT be persisted
        source = db.get_source("test-source")
        assert source is None

    def test_transaction_allows_multiple_operations(self, temp_dir):
        """Multiple operations in transaction should all succeed or all fail."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        with db.transaction() as conn:
            # Create a source
            conn.execute(
                """
                INSERT INTO sources (id, name, created_at, created_by, gcs_location, status)
                VALUES ('source-1', 'Source 1', '2024-01-01', 'test', 'gs://bucket/1', 'available')
                """
            )
            # Create another source
            conn.execute(
                """
                INSERT INTO sources (id, name, created_at, created_by, gcs_location, status)
                VALUES ('source-2', 'Source 2', '2024-01-01', 'test', 'gs://bucket/2', 'available')
                """
            )
            # Add lineage
            conn.execute(
                """
                INSERT INTO source_lineage (source_id, parent_source_id, created_at)
                VALUES ('source-2', 'source-1', '2024-01-01')
                """
            )

        # All should be persisted
        assert db.get_source("source-1") is not None
        assert db.get_source("source-2") is not None
        lineage = db.get_lineage("source-2")
        assert len(lineage) == 1
        assert lineage[0]["parent_source_id"] == "source-1"

    def test_partial_failure_rolls_back_all(self, temp_dir):
        """If later operation fails, earlier ones should be rolled back."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # First, create source-1 outside transaction to test rollback
        db.create_source(
            source_id="existing-source",
            name="Existing",
            gcs_location="gs://bucket/existing",
            created_by="test",
        )

        with pytest.raises(sqlite3.IntegrityError):
            with db.transaction() as conn:
                # Create a new source
                conn.execute(
                    """
                    INSERT INTO sources (id, name, created_at, created_by, gcs_location, status)
                    VALUES ('new-source', 'New', '2024-01-01', 'test', 'gs://bucket/new', 'available')
                    """
                )
                # Try to create duplicate (should fail)
                conn.execute(
                    """
                    INSERT INTO sources (id, name, created_at, created_by, gcs_location, status)
                    VALUES ('existing-source', 'Dupe', '2024-01-01', 'test', 'gs://bucket/dupe', 'available')
                    """
                )

        # new-source should NOT exist due to rollback
        source = db.get_source("new-source")
        assert source is None

        # existing-source should still have original values
        existing = db.get_source("existing-source")
        assert existing["name"] == "Existing"


class TestDatabaseOperationHelpers:
    """Tests for helper methods that use transactions internally."""

    def test_create_job_with_inputs_atomic(self, temp_dir):
        """Creating job and adding inputs should be atomic."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # Create a source first
        db.create_source(
            source_id="input-source",
            name="Input",
            gcs_location="gs://bucket/input",
            created_by="test",
        )

        # Test atomic creation
        db.create_job_with_inputs(
            job_id="job-a1b2c3d4",
            workspace="test-ws",
            snapshot_id="snap-abc",
            script="run.py",
            experiment_dir="/exp/test",
            inputs={"raw": "input-source"},
        )

        job = db.get_job("job-a1b2c3d4")
        assert job is not None

        inputs = db.get_job_inputs("job-a1b2c3d4")
        assert len(inputs) == 1
        assert inputs[0]["source_id"] == "input-source"
        assert inputs[0]["input_name"] == "raw"

    def test_create_job_with_inputs_rolls_back_on_failure(self, temp_dir):
        """If adding inputs fails, job creation should be rolled back."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # Don't create the source - this should cause failure (FK constraint)
        with pytest.raises(sqlite3.IntegrityError):
            db.create_job_with_inputs(
                job_id="job-b5c6d7e8",
                workspace="test-ws",
                snapshot_id="snap-def",
                script="run.py",
                experiment_dir="/exp/test",
                inputs={"raw": "nonexistent-source"},  # Should fail FK constraint
            )

        # Job should NOT exist
        job = db.get_job("job-b5c6d7e8")
        assert job is None


class TestPipelineRunStatus:
    """Tests for pipeline run status and queue queries."""

    def test_get_pipeline_run_status_returns_none_for_nonexistent(self, temp_dir):
        """Should return None when pipeline run doesn't exist."""
        db = Database(temp_dir / "test.db")
        result = db.get_pipeline_run_status("prun-nonexistent")
        assert result is None

    def test_get_pipeline_run_status_returns_pipeline_info(self, temp_dir):
        """Should return pipeline run info with queue status."""
        db = Database(temp_dir / "test.db")

        # Create pipeline run
        with db._conn() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_runs (id, workspace_name, pipeline_name, status, started_at)
                VALUES ('prun-test123', 'my_workspace', 'pipeline.yaml', 'running', '2024-01-01T12:00:00')
                """
            )
            # Add queue entries
            conn.execute(
                """
                INSERT INTO pipeline_stage_queue (pipeline_run_id, stage_name, status, deps)
                VALUES ('prun-test123', 'preprocess', 'completed', '[]')
                """
            )
            conn.execute(
                """
                INSERT INTO pipeline_stage_queue (pipeline_run_id, stage_name, status, deps)
                VALUES ('prun-test123', 'train', 'running', '["preprocess"]')
                """
            )
            conn.execute(
                """
                INSERT INTO pipeline_stage_queue (pipeline_run_id, stage_name, status, deps)
                VALUES ('prun-test123', 'evaluate', 'pending', '["train"]')
                """
            )

        result = db.get_pipeline_run_status("prun-test123")

        assert result is not None
        assert result["pipeline_run_id"] == "prun-test123"
        assert result["workspace"] == "my_workspace"
        assert result["pipeline"] == "pipeline.yaml"
        assert result["status"] == "running"
        assert result["started_at"] == "2024-01-01T12:00:00"
        assert result["completed_at"] is None
        assert result["error"] is None
        assert len(result["queue"]) == 3
        assert result["queue"][0]["stage_name"] == "preprocess"
        assert result["queue"][0]["status"] == "completed"
        assert result["queue"][1]["stage_name"] == "train"
        assert result["queue"][1]["status"] == "running"
        assert result["queue"][2]["stage_name"] == "evaluate"
        assert result["queue"][2]["status"] == "pending"

    def test_get_queued_stages_for_pipeline_returns_pending_entries(self, temp_dir):
        """Should return stages that are queued but not yet have stage_runs."""
        db = Database(temp_dir / "test.db")

        # Create pipeline run
        with db._conn() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_runs (id, workspace_name, pipeline_name, status, started_at)
                VALUES ('prun-queue123', 'test_ws', 'pipeline.yaml', 'running', '2024-01-01T12:00:00')
                """
            )
            # Add queue entries - some with stage_run_id (already started), some without
            conn.execute(
                """
                INSERT INTO pipeline_stage_queue (pipeline_run_id, stage_name, status, stage_run_id)
                VALUES ('prun-queue123', 'preprocess', 'completed', 'stage-abc123')
                """
            )
            conn.execute(
                """
                INSERT INTO pipeline_stage_queue (pipeline_run_id, stage_name, status, stage_run_id)
                VALUES ('prun-queue123', 'train', 'pending', NULL)
                """
            )
            conn.execute(
                """
                INSERT INTO pipeline_stage_queue (pipeline_run_id, stage_name, status, stage_run_id)
                VALUES ('prun-queue123', 'evaluate', 'pending', NULL)
                """
            )

        result = db.get_queued_stages_for_pipeline("prun-queue123")

        # Should only return entries without stage_run_id that are pending/running
        assert len(result) == 2
        assert result[0]["stage_name"] == "train"
        assert result[0]["status"] == "pending"
        assert result[1]["stage_name"] == "evaluate"
        assert result[1]["status"] == "pending"

    def test_get_queued_stages_for_pipeline_excludes_completed(self, temp_dir):
        """Should not return completed/failed/canceled entries without stage_run_id."""
        db = Database(temp_dir / "test.db")

        # Create pipeline run
        with db._conn() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_runs (id, workspace_name, pipeline_name, status, started_at)
                VALUES ('prun-exclude', 'test_ws', 'pipeline.yaml', 'completed', '2024-01-01T12:00:00')
                """
            )
            # All entries don't have stage_run_id but are in terminal states
            conn.execute(
                """
                INSERT INTO pipeline_stage_queue (pipeline_run_id, stage_name, status, stage_run_id)
                VALUES ('prun-exclude', 'stage1', 'completed', NULL)
                """
            )
            conn.execute(
                """
                INSERT INTO pipeline_stage_queue (pipeline_run_id, stage_name, status, stage_run_id)
                VALUES ('prun-exclude', 'stage2', 'failed', NULL)
                """
            )
            conn.execute(
                """
                INSERT INTO pipeline_stage_queue (pipeline_run_id, stage_name, status, stage_run_id)
                VALUES ('prun-exclude', 'stage3', 'canceled', NULL)
                """
            )

        result = db.get_queued_stages_for_pipeline("prun-exclude")

        # Should return empty - all entries are in terminal states
        assert len(result) == 0

    def test_get_queued_stages_for_pipeline_empty_for_nonexistent(self, temp_dir):
        """Should return empty list for non-existent pipeline."""
        db = Database(temp_dir / "test.db")
        result = db.get_queued_stages_for_pipeline("prun-nonexistent")
        assert result == []


class TestDatabaseInitialization:
    """Tests for database initialization."""

    def test_creates_directory_if_not_exists(self, temp_dir):
        """Should create parent directory for database file."""
        db_path = temp_dir / "nested" / "deep" / "test.db"
        db = Database(db_path)

        assert db_path.parent.exists()

    def test_initializes_schema(self, temp_dir):
        """Should initialize schema on creation."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # Should have expected tables
        with db._conn() as conn:
            tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            table_names = {t[0] for t in tables}

            assert "audit" in table_names
            assert "sources" in table_names
            assert "jobs" in table_names
            assert "source_lineage" in table_names
            assert "job_inputs" in table_names

    def test_foreign_keys_enabled(self, temp_dir):
        """Foreign key constraints should be enabled."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # Try to add job_input for non-existent job (should fail)
        with pytest.raises(sqlite3.IntegrityError):
            with db._conn() as conn:
                conn.execute(
                    "INSERT INTO job_inputs (job_id, source_id, input_name) VALUES ('fake-job', 'fake-source', 'test')"
                )


class TestAttemptGrouping:
    """Tests for attempt grouping feature."""

    def _setup_workspace(self, db: Database, workspace: str = "test_ws") -> None:
        """Helper to create workspace lineage and versions."""
        db.create_workspace_lineage(workspace, None, None, "Test workspace")
        for i in range(1, 6):
            db.create_version(workspace, f"v{i}", f"{workspace}-v{i}", f"sha{i}", "run")

    def test_first_run_gets_attempt_one(self, temp_dir):
        """First run for a stage should be attempt #1."""
        db = Database(temp_dir / "test.db")
        self._setup_workspace(db)

        db.create_stage_run("stage-1", "test_ws", "v1", "train")
        run = db.get_stage_run("stage-1")

        assert run["attempt_num"] == 1

    def test_subsequent_runs_stay_in_same_attempt(self, temp_dir):
        """Runs without success outcome stay in the same attempt."""
        db = Database(temp_dir / "test.db")
        self._setup_workspace(db)

        # Create multiple runs without marking success
        db.create_stage_run("stage-1", "test_ws", "v1", "train")
        db.create_stage_run("stage-2", "test_ws", "v2", "train")
        db.create_stage_run("stage-3", "test_ws", "v3", "train")

        # All should be in attempt #1
        for run_id in ["stage-1", "stage-2", "stage-3"]:
            run = db.get_stage_run(run_id)
            assert run["attempt_num"] == 1, f"{run_id} should be attempt 1"

    def test_success_outcome_closes_attempt(self, temp_dir):
        """Marking success should cause next run to start new attempt."""
        db = Database(temp_dir / "test.db")
        self._setup_workspace(db)

        # Run 1 and 2 in attempt 1
        db.create_stage_run("stage-1", "test_ws", "v1", "train")
        db.create_stage_run("stage-2", "test_ws", "v2", "train")

        # Mark run 2 as success
        db.update_stage_run_status("stage-2", "completed")
        db.update_run_outcome("stage-2", "success")

        # Run 3 should be in attempt 2
        db.create_stage_run("stage-3", "test_ws", "v3", "train")
        run3 = db.get_stage_run("stage-3")

        assert run3["attempt_num"] == 2

    def test_bad_results_does_not_close_attempt(self, temp_dir):
        """Marking bad_results should keep same attempt open."""
        db = Database(temp_dir / "test.db")
        self._setup_workspace(db)

        db.create_stage_run("stage-1", "test_ws", "v1", "train")
        db.update_stage_run_status("stage-1", "completed")
        db.update_run_outcome("stage-1", "bad_results")

        # Next run should still be attempt 1
        db.create_stage_run("stage-2", "test_ws", "v2", "train")
        run2 = db.get_stage_run("stage-2")

        assert run2["attempt_num"] == 1

    def test_different_stages_have_independent_attempts(self, temp_dir):
        """Different stages should have independent attempt numbering."""
        db = Database(temp_dir / "test.db")
        self._setup_workspace(db)

        # Train stage - attempt 1
        db.create_stage_run("train-1", "test_ws", "v1", "train")

        # Preprocess stage - also attempt 1 (independent)
        db.create_stage_run("preprocess-1", "test_ws", "v1", "preprocess")

        train = db.get_stage_run("train-1")
        preprocess = db.get_stage_run("preprocess-1")

        assert train["attempt_num"] == 1
        assert preprocess["attempt_num"] == 1

    def test_update_run_outcome_only_works_on_completed(self, temp_dir):
        """update_run_outcome should only update completed runs."""
        db = Database(temp_dir / "test.db")
        self._setup_workspace(db)

        db.create_stage_run("stage-1", "test_ws", "v1", "train")
        # Run is still 'pending', not completed

        result = db.update_run_outcome("stage-1", "success")
        assert result is False  # Should fail

        run = db.get_stage_run("stage-1")
        assert run["outcome"] is None  # Should not be set

    def test_update_run_outcome_validates_outcome_value(self, temp_dir):
        """update_run_outcome should reject invalid outcome values."""
        db = Database(temp_dir / "test.db")
        self._setup_workspace(db)

        db.create_stage_run("stage-1", "test_ws", "v1", "train")
        db.update_stage_run_status("stage-1", "completed")

        with pytest.raises(ValueError, match="Invalid outcome"):
            db.update_run_outcome("stage-1", "invalid_value")

    def test_list_attempts_groups_runs(self, temp_dir):
        """list_attempts should return grouped attempt summaries."""
        db = Database(temp_dir / "test.db")
        self._setup_workspace(db)

        # Attempt 1: 3 runs, 2 failed, 1 success
        db.create_stage_run("stage-1", "test_ws", "v1", "train")
        db.update_stage_run_status("stage-1", "failed", error="crash")

        db.create_stage_run("stage-2", "test_ws", "v2", "train")
        db.update_stage_run_status("stage-2", "failed", error="crash")

        db.create_stage_run("stage-3", "test_ws", "v3", "train")
        db.update_stage_run_status("stage-3", "completed")
        db.update_run_outcome("stage-3", "success")

        # Attempt 2: 1 run, still open
        db.create_stage_run("stage-4", "test_ws", "v4", "train")

        attempts = db.list_attempts("test_ws", stage_name="train")

        assert len(attempts) == 2

        # Attempt 2 (most recent first)
        assert attempts[0]["attempt"] == 2
        assert attempts[0]["runs"] == 1
        assert attempts[0]["status"] == "open"

        # Attempt 1
        assert attempts[1]["attempt"] == 1
        assert attempts[1]["runs"] == 3
        assert attempts[1]["failed"] == 2
        assert attempts[1]["success"] == 1
        assert attempts[1]["status"] == "closed"

    def test_list_attempts_shows_version_range(self, temp_dir):
        """list_attempts should show version range for multi-run attempts."""
        db = Database(temp_dir / "test.db")
        self._setup_workspace(db)

        db.create_stage_run("stage-1", "test_ws", "v1", "train")
        db.create_stage_run("stage-2", "test_ws", "v3", "train")
        db.create_stage_run("stage-3", "test_ws", "v5", "train")

        attempts = db.list_attempts("test_ws", stage_name="train")

        assert len(attempts) == 1
        assert attempts[0]["versions"] == "v1→v5"

    def test_list_attempts_single_version(self, temp_dir):
        """list_attempts should show single version when only one run."""
        db = Database(temp_dir / "test.db")
        self._setup_workspace(db)

        db.create_stage_run("stage-1", "test_ws", "v2", "train")

        attempts = db.list_attempts("test_ws", stage_name="train")

        assert len(attempts) == 1
        assert attempts[0]["versions"] == "v2"
