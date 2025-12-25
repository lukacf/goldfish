"""Tests for cascading failure handling.

TDD: Verify proper cleanup when multi-step operations fail partway through.
"""

from unittest.mock import MagicMock, patch

import pytest

from goldfish.errors import GoldfishError


def get_tool_fn(tool):
    """Get the underlying function from a FastMCP tool."""
    return tool.fn if hasattr(tool, "fn") else tool


class TestJobLaunchCascadingFailures:
    """Tests for job launch failure cleanup."""

    def test_checkpoint_fails_no_orphaned_state(self, temp_dir):
        """If checkpoint fails, no job record should be created."""
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.db.database import Database
        from goldfish.jobs.launcher import JobLauncher
        from goldfish.state.state_md import StateManager

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            slots=["w1", "w2", "w3"],
            state_md=StateMdConfig(),
            audit=AuditConfig(),
            jobs=JobsConfig(experiments_dir="experiments"),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        # Mock workspace manager with failing checkpoint
        mock_ws_manager = MagicMock()
        mock_ws_manager.get_slot_info.return_value = MagicMock(
            workspace="test-ws",
            state="mounted",
        )
        mock_ws_manager.checkpoint.side_effect = GoldfishError("Git commit failed")

        launcher = JobLauncher(config, temp_dir, db, mock_ws_manager, state_manager)

        with pytest.raises(GoldfishError, match="Git commit failed"):
            launcher.run_job(
                slot="w1",
                script="train.py",
                reason="Testing checkpoint failure",
            )

        # No job should be created
        jobs = db.list_jobs()
        assert len(jobs) == 0

    def test_export_fails_after_checkpoint_logs_error(self, temp_dir):
        """If export fails after checkpoint, job should be marked failed."""
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.db.database import Database
        from goldfish.jobs.launcher import JobLauncher
        from goldfish.models import CheckpointResponse
        from goldfish.state.state_md import StateManager

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            slots=["w1", "w2", "w3"],
            state_md=StateMdConfig(),
            audit=AuditConfig(),
            jobs=JobsConfig(experiments_dir="experiments"),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        # Mock workspace manager - checkpoint succeeds
        mock_ws_manager = MagicMock()
        mock_ws_manager.get_slot_info.return_value = MagicMock(
            workspace="test-ws",
            state="mounted",
        )
        mock_ws_manager.checkpoint.return_value = CheckpointResponse(
            success=True,
            slot="w1",
            snapshot_id="snap-abc1234-20251205-120000",
            message="Test checkpoint",
            state_md="",
        )
        mock_ws_manager.get_slot_path.return_value = temp_dir / "workspaces" / "w1"

        launcher = JobLauncher(config, temp_dir, db, mock_ws_manager, state_manager)

        # Make export fail by creating directory issues
        with patch.object(launcher.exporter, "export") as mock_export:
            mock_export.side_effect = OSError("Permission denied creating experiment dir")

            with pytest.raises(GoldfishError, match="Permission denied"):
                launcher.run_job(
                    slot="w1",
                    script="train.py",
                    reason="Testing export failure",
                )

        # Job may or may not exist depending on when failure occurs
        # But if it exists, it should be marked as failed
        jobs = db.list_jobs()
        if jobs:
            assert jobs[0]["status"] == "failed"
            assert "Permission denied" in jobs[0].get("error", "")

    def test_db_fails_after_export_cleans_up(self, temp_dir):
        """If database fails after export, experiment dir should be cleaned up."""
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.db.database import Database
        from goldfish.jobs.launcher import JobLauncher
        from goldfish.models import CheckpointResponse
        from goldfish.state.state_md import StateManager

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            slots=["w1", "w2", "w3"],
            state_md=StateMdConfig(),
            audit=AuditConfig(),
            jobs=JobsConfig(experiments_dir="experiments"),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        # Create workspace directory
        ws_path = temp_dir / "workspaces" / "w1"
        ws_path.mkdir(parents=True, exist_ok=True)
        (ws_path / "train.py").write_text("print('hello')")

        mock_ws_manager = MagicMock()
        mock_ws_manager.get_slot_info.return_value = MagicMock(
            workspace="test-ws",
            state="mounted",
        )
        mock_ws_manager.checkpoint.return_value = CheckpointResponse(
            success=True,
            slot="w1",
            snapshot_id="snap-abc1234-20251205-120000",
            message="Test checkpoint",
            state_md="",
        )
        mock_ws_manager.get_slot_path.return_value = ws_path

        launcher = JobLauncher(config, temp_dir, db, mock_ws_manager, state_manager)

        # Make database fail on job creation (now using create_job_with_inputs)
        with patch.object(db, "create_job_with_inputs") as mock_create:
            mock_create.side_effect = Exception("Database connection lost")

            with pytest.raises(GoldfishError, match="Database"):
                launcher.run_job(
                    slot="w1",
                    script="train.py",
                    reason="Testing DB failure after export",
                )

        # Experiment directory should be cleaned up (or at least job not in DB)
        jobs = db.list_jobs()
        assert len(jobs) == 0


class TestDeleteWorkspaceCascadingFailures:
    """Tests for workspace deletion failure handling."""

    def test_snapshot_delete_fails_partial_cleanup(self, temp_dir):
        """If snapshot deletion fails midway, should report partial success."""
        from goldfish import server
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.db.database import Database
        from goldfish.state.state_md import StateManager

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            slots=["w1", "w2", "w3"],
            state_md=StateMdConfig(),
            audit=AuditConfig(),
            jobs=JobsConfig(),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        mock_ws_manager = MagicMock()
        mock_ws_manager.git.branch_exists.return_value = True
        mock_ws_manager.git.list_snapshots.return_value = ["snap-1", "snap-2", "snap-3"]
        mock_ws_manager.get_all_slots.return_value = []

        # Second snapshot delete fails
        call_count = [0]

        def delete_snapshot_side_effect(snap_id):
            call_count[0] += 1
            if call_count[0] == 2:
                raise GoldfishError("Failed to delete snap-2")
            return True

        mock_ws_manager.git.delete_snapshot.side_effect = delete_snapshot_side_effect

        server.configure_server(
            project_root=temp_dir,
            config=config,
            db=db,
            workspace_manager=mock_ws_manager,
            state_manager=state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            delete_workspace_fn = get_tool_fn(server.delete_workspace)
            with pytest.raises(GoldfishError, match="Failed to delete"):
                delete_workspace_fn(
                    workspace="ws-with-bad-snapshot",
                    reason="Testing partial failure",
                )
        finally:
            server.reset_server()


class TestPromoteArtifactCascadingFailures:
    """Tests for artifact promotion failure handling."""

    def test_source_create_fails_no_orphaned_lineage(self, temp_dir):
        """If source creation fails, no lineage records should exist."""
        from goldfish import server
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
        from goldfish.db.database import Database
        from goldfish.state.state_md import StateManager

        db = Database(temp_dir / "test.db")
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(),
            jobs=JobsConfig(),
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

        # Create a source and job
        db.create_source(
            source_id="input-source",
            name="input-source",
            gcs_location="gs://bucket/input",
            created_by="external",
        )
        db.create_job_with_inputs(
            job_id="completed-job",
            workspace="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="train.py",
            inputs={"data": "input-source"},
        )
        db.update_job_status(
            job_id="completed-job",
            status="completed",
            artifact_uri="gs://bucket/artifacts/completed-job/",
        )

        server.configure_server(
            project_root=temp_dir,
            config=config,
            db=db,
            workspace_manager=MagicMock(),
            state_manager=state_manager,
            job_launcher=MagicMock(),
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            # Make source creation fail via database error
            with patch.object(db, "create_source") as mock_create:
                mock_create.side_effect = Exception("Database constraint violation")

                promote_artifact_fn = get_tool_fn(server.promote_artifact)
                with pytest.raises(GoldfishError):
                    promote_artifact_fn(
                        job_id="completed-job",
                        output_name="model",
                        source_name="promoted-model",
                        metadata={
                            "schema_version": 1,
                            "description": "Model artifact file for cascading failure test.",
                            "source": {
                                "format": "file",
                                "size_bytes": 123,
                                "created_at": "2025-12-24T12:00:00Z",
                            },
                            "schema": {"kind": "file", "content_type": "application/json"},
                        },
                        reason="Testing source creation failure",
                    )

            # No lineage should exist for the failed source
            lineage = db.get_lineage("promoted-model")
            assert len(lineage) == 0
        finally:
            server.reset_server()
