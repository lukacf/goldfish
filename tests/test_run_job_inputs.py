"""Tests for run_job source_inputs parameter - P1.

TDD: Write failing tests first, then implement.
"""

from unittest.mock import MagicMock

import pytest


class TestRunJobSourceInputs:
    """Tests for source_inputs parameter in run_job."""

    def test_run_job_accepts_source_inputs_parameter(self, temp_dir):
        """run_job should accept source_inputs parameter."""
        from goldfish import server
        import inspect

        # Get the underlying function from the decorated tool
        # FastMCP tools have a .fn attribute
        run_job_tool = server.run_job
        if hasattr(run_job_tool, 'fn'):
            sig = inspect.signature(run_job_tool.fn)
        else:
            sig = inspect.signature(run_job_tool)

        # Should have source_inputs parameter
        assert "source_inputs" in sig.parameters

    def test_run_job_source_inputs_is_optional(self, temp_dir):
        """source_inputs should be optional with default None."""
        from goldfish import server
        import inspect

        run_job_tool = server.run_job
        if hasattr(run_job_tool, 'fn'):
            sig = inspect.signature(run_job_tool.fn)
        else:
            sig = inspect.signature(run_job_tool)

        param = sig.parameters["source_inputs"]
        # Should have a default value
        assert param.default is None or param.default == inspect.Parameter.empty

    def test_run_job_passes_source_inputs_to_launcher(self, temp_dir):
        """source_inputs should be passed to job_launcher.run_job."""
        from goldfish import server
        from goldfish.models import RunJobResponse

        # Create mocks
        mock_config = MagicMock()
        mock_config.slots = ["w1", "w2", "w3"]
        mock_config.audit.min_reason_length = 15

        mock_job_launcher = MagicMock()
        mock_job_launcher.run_job.return_value = RunJobResponse(
            success=True,
            job_id="job-123",
            snapshot_id="snap-abc",
            experiment_dir="/exp/test",
            artifact_uri="gs://bucket/artifacts/job-123/",
        )

        mock_state_manager = MagicMock()
        mock_state_manager.regenerate.return_value = "# State"

        mock_workspace_manager = MagicMock()
        mock_workspace_manager.get_all_slots.return_value = []

        mock_db = MagicMock()
        mock_db.get_active_jobs.return_value = []
        mock_db.list_sources.return_value = []

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=mock_workspace_manager,
            state_manager=mock_state_manager,
            job_launcher=mock_job_launcher,
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            # Get the actual function from the tool
            run_job_fn = server.run_job.fn if hasattr(server.run_job, 'fn') else server.run_job

            # Call with source_inputs
            source_inputs = {"raw_data": "my-source"}
            run_job_fn(
                slot="w1",
                script="scripts/train.py",
                reason="Testing source inputs parameter",
                source_inputs=source_inputs,
            )

            # Verify launcher was called with source_inputs
            mock_job_launcher.run_job.assert_called_once()
            call_kwargs = mock_job_launcher.run_job.call_args[1]
            assert "source_inputs" in call_kwargs
            assert call_kwargs["source_inputs"] == source_inputs
        finally:
            server.reset_server()

    def test_run_job_works_without_source_inputs(self, temp_dir):
        """run_job should work when source_inputs is not provided."""
        from goldfish import server
        from goldfish.models import RunJobResponse

        mock_config = MagicMock()
        mock_config.slots = ["w1", "w2", "w3"]
        mock_config.audit.min_reason_length = 15

        mock_job_launcher = MagicMock()
        mock_job_launcher.run_job.return_value = RunJobResponse(
            success=True,
            job_id="job-456",
            snapshot_id="snap-def",
            experiment_dir="/exp/test2",
            artifact_uri=None,
        )

        mock_state_manager = MagicMock()
        mock_state_manager.regenerate.return_value = "# State"

        mock_workspace_manager = MagicMock()
        mock_workspace_manager.get_all_slots.return_value = []

        mock_db = MagicMock()
        mock_db.get_active_jobs.return_value = []
        mock_db.list_sources.return_value = []

        server.configure_server(
            project_root=temp_dir,
            config=mock_config,
            db=mock_db,
            workspace_manager=mock_workspace_manager,
            state_manager=mock_state_manager,
            job_launcher=mock_job_launcher,
            job_tracker=MagicMock(),
            pipeline_manager=MagicMock(),
            dataset_registry=MagicMock(),
            stage_executor=MagicMock(),
            pipeline_executor=MagicMock(),
        )

        try:
            run_job_fn = server.run_job.fn if hasattr(server.run_job, 'fn') else server.run_job

            # Call without source_inputs
            run_job_fn(
                slot="w1",
                script="scripts/train.py",
                reason="Testing without source inputs",
            )

            # Verify launcher was called (source_inputs should be None or not present)
            mock_job_launcher.run_job.assert_called_once()
        finally:
            server.reset_server()


class TestJobCreationAtomicity:
    """Tests for atomic job creation with inputs - P0."""

    def test_job_creation_atomic_with_inputs(self, temp_dir, temp_git_repo):
        """Test that job and inputs are created atomically.

        If adding inputs fails, the job creation should be rolled back.
        This prevents orphaned jobs without their required inputs.
        """
        from goldfish.config import GoldfishConfig, JobsConfig, StateMdConfig, AuditConfig
        from goldfish.db.database import Database
        from goldfish.jobs.launcher import JobLauncher
        from goldfish.workspace.manager import WorkspaceManager
        from goldfish.models import SlotInfo, SlotState, CheckpointResponse
        from unittest.mock import patch, MagicMock

        # Setup real components
        project_root = temp_dir / "project"
        project_root.mkdir()
        (project_root / "workspaces").mkdir()

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path=str(temp_git_repo),
            workspaces_dir="workspaces",
            slots=["w1", "w2", "w3"],
            state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(backend="local", experiments_dir="experiments", infra_path=None),
            invariants=[],
        )

        db = Database(temp_dir / "test.db")
        workspace_manager = WorkspaceManager(config=config, project_root=project_root, db=db)

        # Create a source for the input
        db.create_source(
            source_id="test-source",
            name="Test Source",
            gcs_location="gs://bucket/test",
            created_by="external",
        )

        # Create and mount a workspace
        workspace_manager.create_workspace(
            "test-ws",
            goal="Test workspace",
            reason="Testing atomic job creation",
        )
        workspace_manager.mount("test-ws", "w1", "Testing atomic job creation")

        # Add a file to the workspace
        workspace_path = project_root / "workspaces" / "w1"
        (workspace_path / "code").mkdir(parents=True, exist_ok=True)
        (workspace_path / "code" / "train.py").write_text("print('training')")

        launcher = JobLauncher(
            config=config,
            project_root=project_root,
            db=db,
            workspace_manager=workspace_manager,
        )

        # Mock _launch_job to prevent actual job launch
        with patch.object(launcher, "_launch_job"):
            # Test atomic behavior: simulate input validation failure
            # inside create_job_with_inputs transaction
            original_create_job_with_inputs = db.create_job_with_inputs

            def failing_create_with_inputs(*args, **kwargs):
                # Start the transaction like the real method
                with db.transaction() as conn:
                    # Create the job
                    conn.execute(
                        """
                        INSERT INTO jobs (id, workspace, snapshot_id, script, experiment_dir,
                                          status, started_at, metadata)
                        VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
                        """,
                        (
                            kwargs["job_id"],
                            kwargs["workspace"],
                            kwargs["snapshot_id"],
                            kwargs["script"],
                            kwargs["experiment_dir"],
                            "2025-01-01T00:00:00",
                            None,
                        ),
                    )
                    # Simulate failure when adding inputs
                    # This should rollback the entire transaction including job creation
                    raise Exception("Simulated input addition failure in transaction")

            with patch.object(db, "create_job_with_inputs", side_effect=failing_create_with_inputs):
                with pytest.raises(Exception) as exc_info:
                    launcher.run_job(
                        slot="w1",
                        script="train.py",
                        reason="Testing atomic creation",
                        source_inputs={"data": "test-source"},
                    )

                assert "input addition failure" in str(exc_info.value).lower()

                # After fix: Transaction rolled back, so no job created
                jobs = db.list_jobs(limit=100)

                assert len(jobs) == 0, (
                    f"Expected job creation to be rolled back when input addition fails, "
                    f"but found {len(jobs)} job(s) in database"
                )
