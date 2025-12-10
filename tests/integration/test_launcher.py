"""Tests for job launcher - P0 Core Functionality.

TDD: Write failing tests first, then implement.
"""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from goldfish.config import GCSConfig, GoldfishConfig, JobsConfig
from goldfish.errors import GoldfishError
from goldfish.jobs.launcher import JobLauncher


class TestGetArtifactUri:
    """Tests for _get_artifact_uri - P0."""

    def test_returns_uri_with_gcs_config(self, temp_dir):
        """With GCS config, should return properly formatted URI."""
        config = MagicMock(spec=GoldfishConfig)
        config.gcs = GCSConfig(
            bucket="my-bucket",
            artifacts_prefix="artifacts/",
        )
        config.jobs = JobsConfig()

        launcher = JobLauncher(
            config=config,
            project_root=temp_dir,
            db=MagicMock(),
            workspace_manager=MagicMock(),
        )

        uri = launcher._get_artifact_uri("job-e5f6a7b8")
        assert uri == "gs://my-bucket/artifacts/job-e5f6a7b8/"

    def test_handles_prefix_with_trailing_slash(self, temp_dir):
        """Prefix with trailing slash should not double-slash."""
        config = MagicMock(spec=GoldfishConfig)
        config.gcs = GCSConfig(
            bucket="bucket",
            artifacts_prefix="path/to/artifacts/",
        )
        config.jobs = JobsConfig()

        launcher = JobLauncher(
            config=config,
            project_root=temp_dir,
            db=MagicMock(),
            workspace_manager=MagicMock(),
        )

        uri = launcher._get_artifact_uri("job-xyz")
        assert uri == "gs://bucket/path/to/artifacts/job-xyz/"
        assert "//" not in uri.replace("gs://", "")

    def test_handles_prefix_without_trailing_slash(self, temp_dir):
        """Prefix without trailing slash should still work."""
        config = MagicMock(spec=GoldfishConfig)
        config.gcs = GCSConfig(
            bucket="bucket",
            artifacts_prefix="artifacts",
        )
        config.jobs = JobsConfig()

        launcher = JobLauncher(
            config=config,
            project_root=temp_dir,
            db=MagicMock(),
            workspace_manager=MagicMock(),
        )

        uri = launcher._get_artifact_uri("job-xyz")
        assert uri == "gs://bucket/artifacts/job-xyz/"


class TestLaunchViaInfra:
    """Tests for _launch_via_infra - P0."""

    def test_calls_subprocess_with_correct_args(self, temp_dir):
        """Should call create_run.py with correct arguments."""
        config = MagicMock(spec=GoldfishConfig)
        config.gcs = None
        config.jobs = JobsConfig()

        launcher = JobLauncher(
            config=config,
            project_root=temp_dir,
            db=MagicMock(),
            workspace_manager=MagicMock(),
        )

        # Create mock script
        create_run_script = temp_dir / "infra" / "create_run.py"
        create_run_script.parent.mkdir(parents=True)
        create_run_script.write_text("#!/usr/bin/env python\nprint('ok')")

        exp_dir = temp_dir / "experiments" / "test-exp"
        exp_dir.mkdir(parents=True)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="run-123\n", stderr="")

            launcher._launch_via_infra(
                job_id="job-c3d4e5f6",
                exp_dir=exp_dir,
                script="scripts/train.py",
                create_run_script=create_run_script,
            )

            mock_run.assert_called_once()
            call_args = mock_run.call_args

            # Check command structure
            cmd = call_args[0][0]
            assert "python" in cmd[0] or cmd[0].endswith("python3")
            assert str(create_run_script) in cmd
            assert "--experiment" in cmd
            assert str(exp_dir) in cmd
            assert "--job-id" in cmd
            assert "job-c3d4e5f6" in cmd

    def test_has_timeout(self, temp_dir):
        """Subprocess call should have a timeout."""
        config = MagicMock(spec=GoldfishConfig)
        config.gcs = None
        config.jobs = JobsConfig()

        launcher = JobLauncher(
            config=config,
            project_root=temp_dir,
            db=MagicMock(),
            workspace_manager=MagicMock(),
        )

        create_run_script = temp_dir / "create_run.py"
        create_run_script.write_text("#!/usr/bin/env python\nprint('ok')")

        exp_dir = temp_dir / "exp"
        exp_dir.mkdir()

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

            launcher._launch_via_infra(
                job_id="job-a1b2c3d4",
                exp_dir=exp_dir,
                script="run.py",
                create_run_script=create_run_script,
            )

            call_kwargs = mock_run.call_args[1]
            assert "timeout" in call_kwargs
            assert call_kwargs["timeout"] > 0  # Should have some timeout

    def test_raises_on_nonzero_exit(self, temp_dir):
        """Should raise GoldfishError on non-zero exit code."""
        config = MagicMock(spec=GoldfishConfig)
        config.gcs = None
        config.jobs = JobsConfig()

        launcher = JobLauncher(
            config=config,
            project_root=temp_dir,
            db=MagicMock(),
            workspace_manager=MagicMock(),
        )

        create_run_script = temp_dir / "create_run.py"
        create_run_script.write_text("#!/usr/bin/env python\nprint('ok')")

        exp_dir = temp_dir / "exp"
        exp_dir.mkdir()

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="Error: something failed",
            )

            with pytest.raises(GoldfishError) as exc_info:
                launcher._launch_via_infra(
                    job_id="job-a1b2c3d4",
                    exp_dir=exp_dir,
                    script="run.py",
                    create_run_script=create_run_script,
                )

            assert "failed" in str(exc_info.value).lower()

    def test_raises_on_timeout(self, temp_dir):
        """Should raise GoldfishError on subprocess timeout."""
        config = MagicMock(spec=GoldfishConfig)
        config.gcs = None
        config.jobs = JobsConfig()

        launcher = JobLauncher(
            config=config,
            project_root=temp_dir,
            db=MagicMock(),
            workspace_manager=MagicMock(),
        )

        create_run_script = temp_dir / "create_run.py"
        create_run_script.write_text("#!/usr/bin/env python\nprint('ok')")

        exp_dir = temp_dir / "exp"
        exp_dir.mkdir()

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="python", timeout=60)

            with pytest.raises(GoldfishError) as exc_info:
                launcher._launch_via_infra(
                    job_id="job-a1b2c3d4",
                    exp_dir=exp_dir,
                    script="run.py",
                    create_run_script=create_run_script,
                )

            assert "timed out" in str(exc_info.value).lower()


class TestRunJobExceptionHandling:
    """Tests for run_job exception handling - P0."""

    def test_preserves_goldfish_error_type(self, temp_dir):
        """GoldfishError from _launch_job should be re-raised, not wrapped."""
        from goldfish.models import CheckpointResponse, SlotInfo, SlotState

        config = MagicMock(spec=GoldfishConfig)
        config.gcs = None
        config.jobs = JobsConfig(infra_path="infra")

        # Create infra script
        infra_dir = temp_dir / "infra"
        infra_dir.mkdir()
        (infra_dir / "create_run.py").write_text("#!/usr/bin/env python")

        # Setup mocks
        mock_db = MagicMock()
        mock_workspace_manager = MagicMock()
        mock_workspace_manager.get_slot_info.return_value = SlotInfo(
            slot="w1", state=SlotState.MOUNTED, workspace="test-ws"
        )
        mock_workspace_manager.get_slot_path.return_value = temp_dir / "workspace"
        mock_workspace_manager.checkpoint.return_value = CheckpointResponse(
            success=True, slot="w1", snapshot_id="snap-abc1234-20251205-120000", message="test", state_md="# State"
        )

        # Create workspace for exporter
        (temp_dir / "workspace" / "code").mkdir(parents=True)

        launcher = JobLauncher(
            config=config,
            project_root=temp_dir,
            db=mock_db,
            workspace_manager=mock_workspace_manager,
        )

        # Make _launch_job raise a GoldfishError
        with patch.object(launcher, "_launch_job") as mock_launch:
            mock_launch.side_effect = GoldfishError("Launch failed: specific reason")

            with pytest.raises(GoldfishError) as exc_info:
                launcher.run_job(
                    slot="w1",
                    script="scripts/train.py",
                    reason="Testing exception handling",
                )

            # Should be the SAME error message, not wrapped
            assert "specific reason" in str(exc_info.value)
            # Should NOT be double-wrapped
            assert "Unexpected error" not in str(exc_info.value)

    def test_wraps_subprocess_error_with_context(self, temp_dir):
        """SubprocessError should be wrapped with proper context."""
        from goldfish.models import CheckpointResponse, SlotInfo, SlotState

        config = MagicMock(spec=GoldfishConfig)
        config.gcs = None
        config.jobs = JobsConfig(infra_path="infra")

        infra_dir = temp_dir / "infra"
        infra_dir.mkdir()
        (infra_dir / "create_run.py").write_text("#!/usr/bin/env python")

        mock_db = MagicMock()
        mock_workspace_manager = MagicMock()
        mock_workspace_manager.get_slot_info.return_value = SlotInfo(
            slot="w1", state=SlotState.MOUNTED, workspace="test-ws"
        )
        mock_workspace_manager.get_slot_path.return_value = temp_dir / "workspace"
        mock_workspace_manager.checkpoint.return_value = CheckpointResponse(
            success=True, slot="w1", snapshot_id="snap-abc1234-20251205-120000", message="test", state_md="# State"
        )

        (temp_dir / "workspace" / "code").mkdir(parents=True)

        launcher = JobLauncher(
            config=config,
            project_root=temp_dir,
            db=mock_db,
            workspace_manager=mock_workspace_manager,
        )

        with patch.object(launcher, "_launch_job") as mock_launch:
            mock_launch.side_effect = subprocess.CalledProcessError(1, "cmd")

            with pytest.raises(GoldfishError) as exc_info:
                launcher.run_job(
                    slot="w1",
                    script="scripts/train.py",
                    reason="Testing subprocess error",
                )

            # Should mention subprocess
            assert "subprocess" in str(exc_info.value).lower()
            # Should preserve context via __cause__
            assert exc_info.value.__cause__ is not None

    def test_wraps_oserror_with_context(self, temp_dir):
        """OSError should be wrapped with file system error context."""
        from goldfish.models import CheckpointResponse, SlotInfo, SlotState

        config = MagicMock(spec=GoldfishConfig)
        config.gcs = None
        config.jobs = JobsConfig(infra_path="infra")

        infra_dir = temp_dir / "infra"
        infra_dir.mkdir()
        (infra_dir / "create_run.py").write_text("#!/usr/bin/env python")

        mock_db = MagicMock()
        mock_workspace_manager = MagicMock()
        mock_workspace_manager.get_slot_info.return_value = SlotInfo(
            slot="w1", state=SlotState.MOUNTED, workspace="test-ws"
        )
        mock_workspace_manager.get_slot_path.return_value = temp_dir / "workspace"
        mock_workspace_manager.checkpoint.return_value = CheckpointResponse(
            success=True, slot="w1", snapshot_id="snap-abc1234-20251205-120000", message="test", state_md="# State"
        )

        (temp_dir / "workspace" / "code").mkdir(parents=True)

        launcher = JobLauncher(
            config=config,
            project_root=temp_dir,
            db=mock_db,
            workspace_manager=mock_workspace_manager,
        )

        with patch.object(launcher, "_launch_job") as mock_launch:
            mock_launch.side_effect = FileNotFoundError("Script not found")

            with pytest.raises(GoldfishError) as exc_info:
                launcher.run_job(
                    slot="w1",
                    script="scripts/train.py",
                    reason="Testing file system error",
                )

            # Should mention file system
            assert "file system" in str(exc_info.value).lower()
            # Should preserve context via __cause__
            assert exc_info.value.__cause__ is not None


class TestExpDirCleanup:
    """Tests for experiment directory cleanup on failures - P0."""

    def test_exp_dir_cleanup_on_launch_failure(self, temp_dir):
        """Test that exp_dir is cleaned up when job launch fails."""
        from goldfish.models import CheckpointResponse, SlotInfo, SlotState

        config = MagicMock(spec=GoldfishConfig)
        config.gcs = None
        config.jobs = JobsConfig(experiments_dir="experiments", infra_path="infra")

        # Create infra script
        infra_dir = temp_dir / "infra"
        infra_dir.mkdir()
        (infra_dir / "create_run.py").write_text("#!/usr/bin/env python")

        # Setup mocks
        mock_db = MagicMock()
        mock_workspace_manager = MagicMock()
        mock_workspace_manager.get_slot_info.return_value = SlotInfo(
            slot="w1", state=SlotState.MOUNTED, workspace="test-ws"
        )
        mock_workspace_manager.get_slot_path.return_value = temp_dir / "workspace"
        mock_workspace_manager.checkpoint.return_value = CheckpointResponse(
            success=True, slot="w1", snapshot_id="snap-abc1234-20251205-120000", message="test", state_md="# State"
        )

        # Create workspace for exporter
        (temp_dir / "workspace" / "code").mkdir(parents=True)
        (temp_dir / "workspace" / "code" / "test.py").write_text("print('test')")

        launcher = JobLauncher(
            config=config,
            project_root=temp_dir,
            db=mock_db,
            workspace_manager=mock_workspace_manager,
        )

        # Make _launch_job raise an exception
        with patch.object(launcher, "_launch_job") as mock_launch:
            mock_launch.side_effect = subprocess.CalledProcessError(1, "cmd", stderr="Launch failed")

            # Run the job - should raise GoldfishError
            with pytest.raises(GoldfishError):
                launcher.run_job(
                    slot="w1",
                    script="scripts/train.py",
                    reason="Testing cleanup on launch failure",
                )

            # Verify job status was updated to failed
            assert mock_db.update_job_status.called
            status_call = mock_db.update_job_status.call_args
            assert status_call[0][1] == "failed"  # status argument

            # The critical assertion: exp_dir should be cleaned up (deleted)
            # Find what exp_dir was created by checking exporter
            experiments_dir = temp_dir / "experiments"
            assert experiments_dir.exists(), "Experiments dir should exist"

            # List all experiment directories created
            exp_dirs = list(experiments_dir.iterdir())

            # The bug: exp_dir is NOT cleaned up, so it still exists
            # After fix: exp_dir SHOULD be deleted
            assert len(exp_dirs) == 0, (
                f"Expected exp_dir to be cleaned up after launch failure, but found: {[d.name for d in exp_dirs]}"
            )

    def test_exp_dir_cleanup_on_all_launch_exceptions(self, temp_dir):
        """Test cleanup happens for OSError, IOError, and unexpected errors."""
        from goldfish.models import CheckpointResponse, SlotInfo, SlotState

        config = MagicMock(spec=GoldfishConfig)
        config.gcs = None
        config.jobs = JobsConfig(experiments_dir="experiments", infra_path="infra")

        infra_dir = temp_dir / "infra"
        infra_dir.mkdir()
        (infra_dir / "create_run.py").write_text("#!/usr/bin/env python")

        mock_db = MagicMock()
        mock_workspace_manager = MagicMock()
        mock_workspace_manager.get_slot_info.return_value = SlotInfo(
            slot="w1", state=SlotState.MOUNTED, workspace="test-ws"
        )
        mock_workspace_manager.get_slot_path.return_value = temp_dir / "workspace"
        mock_workspace_manager.checkpoint.return_value = CheckpointResponse(
            success=True, slot="w1", snapshot_id="snap-abc1234-20251205-120000", message="test", state_md="# State"
        )

        (temp_dir / "workspace" / "code").mkdir(parents=True)
        (temp_dir / "workspace" / "code" / "test.py").write_text("print('test')")

        # Test different exception types
        test_cases = [
            ("OSError", OSError("Permission denied")),
            ("IOError", OSError("Disk full")),
            ("Unexpected", RuntimeError("Something weird")),
        ]

        for error_type, exception in test_cases:
            # Clean up experiments dir between tests
            experiments_dir = temp_dir / "experiments"
            if experiments_dir.exists():
                import shutil

                shutil.rmtree(experiments_dir)

            launcher = JobLauncher(
                config=config,
                project_root=temp_dir,
                db=mock_db,
                workspace_manager=mock_workspace_manager,
            )

            with patch.object(launcher, "_launch_job") as mock_launch:
                mock_launch.side_effect = exception

                with pytest.raises(GoldfishError):
                    launcher.run_job(
                        slot="w1",
                        script="scripts/train.py",
                        reason="Testing cleanup on error",
                    )

                # Verify exp_dir was cleaned up for this error type
                exp_dirs = list(experiments_dir.iterdir()) if experiments_dir.exists() else []
                assert len(exp_dirs) == 0, (
                    f"Expected cleanup after {error_type}, but found: {[d.name for d in exp_dirs]}"
                )


class TestLaunchJob:
    """Tests for _launch_job - integration of infra launch."""

    def test_calls_infra_when_script_exists(self, temp_dir):
        """When create_run.py exists, should call _launch_via_infra."""
        config = MagicMock(spec=GoldfishConfig)
        config.gcs = None
        config.jobs = JobsConfig(infra_path="infra")

        launcher = JobLauncher(
            config=config,
            project_root=temp_dir,
            db=MagicMock(),
            workspace_manager=MagicMock(),
        )

        # Create the script
        infra_dir = temp_dir / "infra"
        infra_dir.mkdir()
        create_run = infra_dir / "create_run.py"
        create_run.write_text("#!/usr/bin/env python\nprint('ok')")

        exp_dir = temp_dir / "exp"
        exp_dir.mkdir()

        with patch.object(launcher, "_launch_via_infra") as mock_launch:
            launcher._launch_job("job-a1b2c3d4", exp_dir, "run.py")

            mock_launch.assert_called_once_with(
                "job-a1b2c3d4",
                exp_dir,
                "run.py",
                create_run,
            )
