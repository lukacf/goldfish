"""Integration tests for infrastructure MCP tools.

Tests manage_base_images and get_build_status tools.
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from goldfish.config import (
    AuditConfig,
    DockerConfig,
    GCEConfig,
    GoldfishConfig,
    JobsConfig,
    StateMdConfig,
)
from goldfish.db.database import Database
from goldfish.state.state_md import StateManager

pytestmark = pytest.mark.filterwarnings("error::pytest.PytestUnhandledThreadExceptionWarning")


def _get_tool_fn(tool: Any) -> Callable:
    """Get the underlying function from a tool (handles FunctionTool wrapper)."""
    return tool.fn if hasattr(tool, "fn") else tool


@pytest.fixture
def configured_server(temp_dir: Path):
    """Configure server with test context and return cleanup function."""
    from goldfish import server
    from goldfish.server_tools import infra_tools

    db = Database(temp_dir / "test.db")
    config = GoldfishConfig(
        project_name="test-project",
        dev_repo_path="../test-dev",
        state_md=StateMdConfig(),
        audit=AuditConfig(min_reason_length=15),
        jobs=JobsConfig(),
        docker=DockerConfig(
            extra_packages={
                "gpu": ["flash-attn --no-build-isolation"],
                "cpu": ["lightgbm"],
            }
        ),
        gce=GCEConfig(project_id="my-gcp-project"),
    )
    state_manager = StateManager(temp_dir / "STATE.md", config)

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

    # Reset the manager singleton to pick up new config
    infra_tools._reset_base_image_manager()

    yield config

    # Clean up
    infra_tools._reset_base_image_manager()


class TestManageBaseImagesListAction:
    """Tests for manage_base_images list action."""

    def test_list_returns_project_info(self, configured_server, temp_dir: Path):
        """list action should return project and image info."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)

        with patch("subprocess.run") as mock_run:
            # Docker info succeeds, images don't exist
            mock_run.return_value = MagicMock(returncode=0)

            result = manage_fn(action="list")

        assert result["project"] == "test-project"
        # Now returns both base_images and project_images
        assert "base_images" in result
        assert "project_images" in result
        assert "cpu" in result["project_images"]
        assert "gpu" in result["project_images"]

    def test_list_shows_customizations(self, configured_server, temp_dir: Path):
        """list action should show configured extra packages."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            result = manage_fn(action="list")

        gpu_info = result["project_images"]["gpu"]
        assert "flash-attn --no-build-isolation" in gpu_info["customization"]["extra_packages"]


class TestManageBaseImagesInspectAction:
    """Tests for manage_base_images inspect action."""

    def test_inspect_requires_image_type(self, configured_server):
        """inspect action should require image_type parameter."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)
        result = manage_fn(action="inspect")

        assert result.get("success") is False
        assert "image_type required" in result["error"]

    def test_inspect_returns_dockerfile_content(self, configured_server):
        """inspect action should return effective Dockerfile."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)
        result = manage_fn(action="inspect", image_type="gpu")

        assert "effective_dockerfile" in result
        assert "FROM " in result["effective_dockerfile"]
        assert "flash-attn" in result["effective_dockerfile"]

    def test_inspect_shows_config_path(self, configured_server, temp_dir: Path):
        """inspect action should show path to goldfish.yaml."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)
        result = manage_fn(action="inspect", image_type="cpu")

        assert "customization" in result
        assert "config_path" in result["customization"]
        assert "goldfish.yaml" in result["customization"]["config_path"]

    def test_inspect_invalid_type_returns_error(self, configured_server):
        """inspect action should return error for invalid image type."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)
        result = manage_fn(action="inspect", image_type="invalid")

        assert result.get("success") is False
        assert "Invalid image type" in result["error"]


class TestManageBaseImagesCheckAction:
    """Tests for manage_base_images check action."""

    def test_check_returns_status_for_both_types(self, configured_server):
        """check action should return status for cpu and gpu."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            result = manage_fn(action="check")

        assert "checks" in result
        assert "cpu" in result["checks"]
        assert "gpu" in result["checks"]

    def test_check_shows_rebuild_needed(self, configured_server):
        """check action should indicate when rebuild is needed."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)

        with patch("subprocess.run") as mock_run:
            # Docker info succeeds, but image inspect fails (no local image)
            def side_effect(cmd, **kwargs):
                if "info" in cmd:
                    return MagicMock(returncode=0)
                if "inspect" in cmd:
                    return MagicMock(returncode=1)  # Image not found
                return MagicMock(returncode=0)

            mock_run.side_effect = side_effect

            result = manage_fn(action="check")

        assert result["checks"]["gpu"]["needs_rebuild"] is True


class TestManageBaseImagesBuildAction:
    """Tests for manage_base_images build action."""

    def test_build_requires_image_type(self, configured_server):
        """build action should require image_type parameter."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)
        result = manage_fn(action="build")

        assert result.get("success") is False
        assert "image_type required" in result["error"]

    def test_build_async_returns_build_id(self, configured_server):
        """build action with wait=False should return build_id."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            with patch("subprocess.Popen") as mock_popen:
                mock_process = MagicMock()
                mock_process.stdout = iter([])
                mock_process.wait.return_value = None
                mock_process.returncode = 0
                mock_popen.return_value = mock_process

                result = manage_fn(action="build", image_type="gpu", wait=False)

        assert "build_id" in result
        assert result["build_id"].startswith("build-")
        assert result["status"] == "pending"

    def test_build_invalid_type_returns_error(self, configured_server):
        """build action should return error for invalid image type."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)
        result = manage_fn(action="build", image_type="tpu")

        assert result.get("success") is False
        assert "Invalid image type" in result["error"]


class TestManageBaseImagesPushAction:
    """Tests for manage_base_images push action."""

    def test_push_requires_image_type(self, configured_server):
        """push action should require image_type parameter."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)
        result = manage_fn(action="push")

        assert result.get("success") is False
        assert "image_type required" in result["error"]

    def test_push_requires_local_image(self, configured_server):
        """push action should fail when local image doesn't exist."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)

        with patch("subprocess.run") as mock_run:

            def side_effect(cmd, **kwargs):
                if "info" in cmd:
                    return MagicMock(returncode=0)
                if "inspect" in cmd:
                    return MagicMock(returncode=1)  # Image not found
                return MagicMock(returncode=0)

            mock_run.side_effect = side_effect

            result = manage_fn(action="push", image_type="gpu")

        assert result.get("success") is False
        assert "not found" in result["error"].lower()


class TestManageBaseImagesInvalidAction:
    """Tests for invalid action handling."""

    def test_invalid_action_returns_error(self, configured_server):
        """Unknown action should return error."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)
        result = manage_fn(action="invalid_action")

        assert result.get("success") is False
        assert "Unknown action" in result["error"]


class TestGetBuildStatus:
    """Tests for get_build_status tool."""

    def test_unknown_build_id_returns_error(self, configured_server):
        """get_build_status should return error for unknown build ID."""
        from goldfish.server_tools.infra_tools import get_build_status

        status_fn = _get_tool_fn(get_build_status)
        result = status_fn(build_id="build-00000000")

        assert result.get("success") is False
        assert "Unknown build ID" in result["error"]

    def test_invalid_build_id_format_returns_error(self, configured_server):
        """get_build_status should return error for invalid build ID format."""
        from goldfish.server_tools.infra_tools import get_build_status

        status_fn = _get_tool_fn(get_build_status)
        result = status_fn(build_id="invalid-id")

        assert result.get("success") is False
        assert "Invalid build ID" in result["error"]


class TestDockerNotAvailable:
    """Tests for Docker unavailability handling."""

    def test_list_fails_gracefully_when_docker_unavailable(self, configured_server):
        """list action should return error when Docker not available."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="Cannot connect")

            result = manage_fn(action="list")

        assert result.get("success") is False
        assert "Docker" in result["error"]


class TestCloudBuildBackend:
    """Tests for Cloud Build backend parameter."""

    def test_build_with_invalid_backend_returns_error(self, configured_server):
        """build action should return error for invalid backend."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            result = manage_fn(action="build", image_type="gpu", backend="invalid")

        assert result.get("success") is False
        assert "Invalid backend" in result["error"]

    def test_build_local_backend_uses_docker(self, configured_server):
        """build with backend=local should use Docker."""
        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            with patch("subprocess.Popen") as mock_popen:
                mock_process = MagicMock()
                mock_process.stdout = iter([])
                mock_process.wait.return_value = None
                mock_process.returncode = 0
                mock_popen.return_value = mock_process

                result = manage_fn(action="build", image_type="gpu", backend="local")

        assert "build_id" in result
        assert result["backend"] == "local"

    def test_cloud_build_requires_gce_config(self, temp_dir: Path):
        """Cloud build should require GCE configuration."""
        from goldfish import server
        from goldfish.server_tools import infra_tools

        db = Database(temp_dir / "test.db")
        # Config without GCE
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(),
            docker=DockerConfig(),
            # No GCE config
        )
        state_manager = StateManager(temp_dir / "STATE.md", config)

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

        infra_tools._reset_base_image_manager()

        from goldfish.server_tools.infra_tools import manage_base_images

        manage_fn = _get_tool_fn(manage_base_images)
        result = manage_fn(action="build", image_type="gpu", backend="cloud")

        assert result.get("success") is False
        assert "GCE" in result["error"] or "project" in result["error"].lower()

        infra_tools._reset_base_image_manager()
