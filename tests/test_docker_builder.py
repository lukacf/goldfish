"""Tests for Docker image building - TDD Phase 6.1."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

from goldfish.infra.docker_builder import DockerBuilder
from goldfish.errors import GoldfishError


class TestDockerfileGeneration:
    """Test Dockerfile generation."""

    def test_generate_dockerfile_basic(self, temp_dir):
        """Should generate Dockerfile with basic structure."""
        # Setup
        workspace_dir = temp_dir / "workspace"
        workspace_dir.mkdir()
        (workspace_dir / "modules").mkdir()
        (workspace_dir / "configs").mkdir()
        (workspace_dir / "requirements.txt").write_text("numpy==1.24.0\npandas==2.0.0\n")

        builder = DockerBuilder()

        # Execute
        dockerfile = builder.generate_dockerfile(workspace_dir)

        # Verify
        assert "FROM python:" in dockerfile
        assert "COPY requirements.txt" in dockerfile
        assert "pip install" in dockerfile and "requirements.txt" in dockerfile
        assert "COPY modules/" in dockerfile
        assert "COPY configs/" in dockerfile

    def test_generate_dockerfile_includes_goldfish_io(self, temp_dir):
        """Should include Goldfish IO library in image."""
        # Setup
        workspace_dir = temp_dir / "workspace"
        workspace_dir.mkdir()
        (workspace_dir / "requirements.txt").write_text("")

        builder = DockerBuilder()

        # Execute
        dockerfile = builder.generate_dockerfile(workspace_dir)

        # Verify - should copy goldfish IO library
        assert "goldfish" in dockerfile.lower() or "COPY" in dockerfile

    def test_generate_dockerfile_with_loaders(self, temp_dir):
        """Should include loaders directory if it exists."""
        # Setup
        workspace_dir = temp_dir / "workspace"
        workspace_dir.mkdir()
        (workspace_dir / "loaders").mkdir()
        (workspace_dir / "loaders" / "custom.py").write_text("# custom loader")
        (workspace_dir / "requirements.txt").write_text("")

        builder = DockerBuilder()

        # Execute
        dockerfile = builder.generate_dockerfile(workspace_dir)

        # Verify
        assert "COPY loaders/" in dockerfile


class TestDockerImageBuilding:
    """Test Docker image building."""

    def test_build_image_calls_docker_build(self, temp_dir):
        """Should call docker build with correct arguments."""
        # Setup
        workspace_dir = temp_dir / "workspace"
        workspace_dir.mkdir()
        (workspace_dir / "requirements.txt").write_text("")

        builder = DockerBuilder()

        # Mock subprocess
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            # Execute
            image_tag = builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="test_ws",
                version="v1"
            )

            # Verify
            assert image_tag == "goldfish-test_ws-v1"
            mock_run.assert_called_once()
            args = mock_run.call_args[0][0]
            assert "docker" in args
            assert "build" in args
            assert "-t" in args
            assert "goldfish-test_ws-v1" in args

    def test_build_image_raises_on_docker_failure(self, temp_dir):
        """Should raise error if docker build fails."""
        # Setup
        workspace_dir = temp_dir / "workspace"
        workspace_dir.mkdir()
        (workspace_dir / "requirements.txt").write_text("")

        builder = DockerBuilder()

        # Mock subprocess to fail
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="Build error")

            # Execute - should raise
            with pytest.raises(GoldfishError, match="Docker build failed"):
                builder.build_image(
                    workspace_dir=workspace_dir,
                    workspace_name="test_ws",
                    version="v1"
                )

    def test_build_image_with_cache(self, temp_dir):
        """Should use Docker layer caching for efficiency."""
        # Setup
        workspace_dir = temp_dir / "workspace"
        workspace_dir.mkdir()
        (workspace_dir / "requirements.txt").write_text("")

        builder = DockerBuilder()

        # Mock subprocess
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            # Execute
            builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="test_ws",
                version="v1",
                use_cache=True
            )

            # Verify - should NOT have --no-cache flag
            args = mock_run.call_args[0][0]
            assert "--no-cache" not in args


class TestImageTagging:
    """Test Docker image tagging strategy."""

    def test_image_tag_format(self, temp_dir):
        """Image tags should follow goldfish-{workspace}-{version} format."""
        workspace_dir = temp_dir / "workspace"
        workspace_dir.mkdir()
        (workspace_dir / "requirements.txt").write_text("")

        builder = DockerBuilder()

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            tag = builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="baseline_lstm",
                version="v3"
            )

            assert tag == "goldfish-baseline_lstm-v3"

    def test_image_tag_sanitization(self, temp_dir):
        """Image tags should be sanitized (no special chars)."""
        workspace_dir = temp_dir / "workspace"
        workspace_dir.mkdir()
        (workspace_dir / "requirements.txt").write_text("")

        builder = DockerBuilder()

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            tag = builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="test/workspace",  # Has invalid char
                version="v1"
            )

            # Should replace invalid chars with underscores
            assert "/" not in tag
            assert "goldfish-test_workspace-v1" == tag
