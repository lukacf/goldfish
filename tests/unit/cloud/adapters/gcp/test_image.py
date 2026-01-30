"""Unit tests for GCP image adapters.

Tests CloudBuildImageBuilder and ArtifactRegistryImageRegistry.
All subprocess and gcloud calls are mocked.
"""

from __future__ import annotations

import json
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from goldfish.cloud.adapters.gcp.image import (
    ArtifactRegistryError,
    ArtifactRegistryImageRegistry,
    CloudBuildImageBuilder,
)
from goldfish.errors import CloudBuildError, CloudBuildNotConfiguredError

# --- CloudBuildImageBuilder Fixtures ---


@pytest.fixture
def mock_shutil_which():
    """Mock shutil.which to simulate gcloud availability."""
    with patch("goldfish.cloud.adapters.gcp.image.shutil.which") as mock:
        mock.return_value = "/usr/local/bin/gcloud"
        yield mock


@pytest.fixture
def cloud_builder(mock_shutil_which):
    """Create a CloudBuildImageBuilder for testing."""
    return CloudBuildImageBuilder(
        project_id="test-project",
        registry_url="us-docker.pkg.dev/test-project/images",
        machine_type="E2_HIGHCPU_32",
        timeout_minutes=30,
        disk_size_gb=100,
    )


# --- CloudBuildImageBuilder Tests ---


class TestCloudBuildImageBuilderInit:
    """Tests for CloudBuildImageBuilder initialization."""

    def test_init_sets_all_parameters(self, mock_shutil_which):
        """Init sets all provided parameters."""
        builder = CloudBuildImageBuilder(
            project_id="my-project",
            registry_url="us-docker.pkg.dev/my-project/repo",
            machine_type="E2_HIGHCPU_8",
            timeout_minutes=60,
            disk_size_gb=200,
        )

        assert builder._project_id == "my-project"
        assert builder._registry_url == "us-docker.pkg.dev/my-project/repo"
        assert builder._machine_type == "E2_HIGHCPU_8"
        assert builder._timeout_minutes == 60
        assert builder._disk_size_gb == 200

    def test_init_with_database(self, mock_shutil_which):
        """Init accepts optional database for build tracking."""
        mock_db = MagicMock()
        builder = CloudBuildImageBuilder(
            project_id="test",
            registry_url="us-docker.pkg.dev/test/repo",
            db=mock_db,
        )

        assert builder._db is mock_db


class TestCloudBuildImageBuilderCheckGcloud:
    """Tests for _check_gcloud_available method."""

    def test_check_gcloud_passes_when_installed(self, cloud_builder, mock_shutil_which):
        """Check passes when gcloud is installed."""
        mock_shutil_which.return_value = "/usr/local/bin/gcloud"

        # Should not raise
        cloud_builder._check_gcloud_available()

    def test_check_gcloud_raises_when_not_installed(self, mock_shutil_which):
        """Check raises CloudBuildNotConfiguredError when gcloud missing."""
        mock_shutil_which.return_value = None
        builder = CloudBuildImageBuilder(
            project_id="test",
            registry_url="us-docker.pkg.dev/test/repo",
        )

        with pytest.raises(CloudBuildNotConfiguredError):
            builder._check_gcloud_available()


class TestCloudBuildImageBuilderBuildAsync:
    """Tests for build_async method."""

    def test_build_async_submits_cloud_build(self, cloud_builder, mock_shutil_which, tmp_path):
        """Build async submits build to Cloud Build."""
        context = tmp_path / "context"
        context.mkdir()
        dockerfile = context / "Dockerfile"
        dockerfile.write_text("FROM python:3.11")

        with (
            patch("subprocess.run") as mock_run,
            patch("tempfile.NamedTemporaryFile") as mock_temp,
            patch("os.unlink"),
        ):
            mock_temp_file = MagicMock()
            mock_temp_file.name = "/tmp/cloudbuild.yaml"
            mock_temp.return_value = mock_temp_file

            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps({"id": "build-abc123"}),
                stderr="",
            )

            build_id = cloud_builder.build_async(context, dockerfile, "my-image:v1")

            assert build_id == "build-abc123"
            mock_run.assert_called_once()

    def test_build_async_adds_registry_url_to_tag(self, cloud_builder, mock_shutil_which, tmp_path):
        """Build async adds registry URL prefix if missing."""
        context = tmp_path / "context"
        context.mkdir()
        dockerfile = context / "Dockerfile"
        dockerfile.write_text("FROM python:3.11")

        with (
            patch("subprocess.run") as mock_run,
            patch("tempfile.NamedTemporaryFile") as mock_temp,
            patch("os.unlink"),
            patch("yaml.dump") as mock_yaml,
        ):
            mock_temp_file = MagicMock()
            mock_temp_file.name = "/tmp/cloudbuild.yaml"
            mock_temp.return_value = mock_temp_file

            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps({"id": "build-123"}),
                stderr="",
            )

            cloud_builder.build_async(context, dockerfile, "my-image:v1")

            # Check that the YAML config includes full registry tag
            config = mock_yaml.call_args[0][0]
            assert config["images"] == ["us-docker.pkg.dev/test-project/images/my-image:v1"]

    def test_build_async_includes_build_args(self, cloud_builder, mock_shutil_which, tmp_path):
        """Build async includes build arguments in config."""
        context = tmp_path / "context"
        context.mkdir()
        dockerfile = context / "Dockerfile"
        dockerfile.write_text("FROM python:3.11")

        with (
            patch("subprocess.run") as mock_run,
            patch("tempfile.NamedTemporaryFile") as mock_temp,
            patch("os.unlink"),
            patch("yaml.dump") as mock_yaml,
        ):
            mock_temp_file = MagicMock()
            mock_temp_file.name = "/tmp/cloudbuild.yaml"
            mock_temp.return_value = mock_temp_file

            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps({"id": "build-123"}),
                stderr="",
            )

            cloud_builder.build_async(context, dockerfile, "image:v1", build_args={"PYTHON_VERSION": "3.11"})

            config = mock_yaml.call_args[0][0]
            docker_args = config["steps"][0]["args"]
            assert "--build-arg" in docker_args
            assert "PYTHON_VERSION=3.11" in docker_args

    def test_build_async_includes_no_cache_flag(self, cloud_builder, mock_shutil_which, tmp_path):
        """Build async includes --no-cache when requested."""
        context = tmp_path / "context"
        context.mkdir()
        dockerfile = context / "Dockerfile"
        dockerfile.write_text("FROM python:3.11")

        with (
            patch("subprocess.run") as mock_run,
            patch("tempfile.NamedTemporaryFile") as mock_temp,
            patch("os.unlink"),
            patch("yaml.dump") as mock_yaml,
        ):
            mock_temp_file = MagicMock()
            mock_temp_file.name = "/tmp/cloudbuild.yaml"
            mock_temp.return_value = mock_temp_file

            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps({"id": "build-123"}),
                stderr="",
            )

            cloud_builder.build_async(context, dockerfile, "image:v1", no_cache=True)

            config = mock_yaml.call_args[0][0]
            docker_args = config["steps"][0]["args"]
            assert "--no-cache" in docker_args

    def test_build_async_raises_on_submit_failure(self, cloud_builder, mock_shutil_which, tmp_path):
        """Build async raises CloudBuildError on submit failure."""
        context = tmp_path / "context"
        context.mkdir()
        dockerfile = context / "Dockerfile"
        dockerfile.write_text("FROM python:3.11")

        with (
            patch("subprocess.run") as mock_run,
            patch("tempfile.NamedTemporaryFile") as mock_temp,
            patch("os.unlink"),
        ):
            mock_temp_file = MagicMock()
            mock_temp_file.name = "/tmp/cloudbuild.yaml"
            mock_temp.return_value = mock_temp_file

            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="Permission denied",
            )

            with pytest.raises(CloudBuildError) as exc_info:
                cloud_builder.build_async(context, dockerfile, "image:v1")

            assert "Failed to submit Cloud Build" in str(exc_info.value)

    def test_build_async_parses_id_from_name_field(self, cloud_builder, mock_shutil_which, tmp_path):
        """Build async parses build ID from 'name' field if 'id' missing."""
        context = tmp_path / "context"
        context.mkdir()
        dockerfile = context / "Dockerfile"
        dockerfile.write_text("FROM python:3.11")

        with (
            patch("subprocess.run") as mock_run,
            patch("tempfile.NamedTemporaryFile") as mock_temp,
            patch("os.unlink"),
        ):
            mock_temp_file = MagicMock()
            mock_temp_file.name = "/tmp/cloudbuild.yaml"
            mock_temp.return_value = mock_temp_file

            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps({"name": "projects/test/builds/build-xyz789"}),
                stderr="",
            )

            build_id = cloud_builder.build_async(context, dockerfile, "image:v1")

            assert build_id == "build-xyz789"


class TestCloudBuildImageBuilderGetBuildStatus:
    """Tests for get_build_status method."""

    def test_get_build_status_returns_completed(self, cloud_builder, mock_shutil_which):
        """Get build status returns completed status."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps(
                    {
                        "status": "SUCCESS",
                        "images": ["us-docker.pkg.dev/test/repo/image:v1"],
                    }
                ),
                stderr="",
            )

            status = cloud_builder.get_build_status("build-123")

            assert status["status"] == "completed"
            assert status["image_tag"] == "us-docker.pkg.dev/test/repo/image:v1"

    def test_get_build_status_returns_building(self, cloud_builder, mock_shutil_which):
        """Get build status returns building for WORKING status."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps({"status": "WORKING"}),
                stderr="",
            )

            status = cloud_builder.get_build_status("build-123")

            assert status["status"] == "building"

    def test_get_build_status_returns_pending(self, cloud_builder, mock_shutil_which):
        """Get build status returns pending for QUEUED status."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps({"status": "QUEUED"}),
                stderr="",
            )

            status = cloud_builder.get_build_status("build-123")

            assert status["status"] == "pending"

    def test_get_build_status_returns_failed_with_detail(self, cloud_builder, mock_shutil_which):
        """Get build status returns failed with status detail."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps({"status": "FAILURE", "statusDetail": "Docker build failed"}),
                stderr="",
            )

            status = cloud_builder.get_build_status("build-123")

            assert status["status"] == "failed"
            assert "Docker build failed" in status["error"]

    def test_get_build_status_maps_all_failure_states(self, cloud_builder, mock_shutil_which):
        """Get build status maps all failure states correctly."""
        failure_states = ["FAILURE", "CANCELLED", "TIMEOUT", "EXPIRED"]

        for cloud_status in failure_states:
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=0,
                    stdout=json.dumps({"status": cloud_status}),
                    stderr="",
                )

                status = cloud_builder.get_build_status("build-123")

                assert status["status"] == "failed"

    def test_get_build_status_returns_unknown_on_gcloud_error(self, cloud_builder, mock_shutil_which):
        """Get build status returns unknown on gcloud error."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="Build not found",
            )

            status = cloud_builder.get_build_status("build-123")

            assert status["status"] == "unknown"
            assert "error" in status


class TestCloudBuildImageBuilderBuild:
    """Tests for synchronous build method."""

    def test_build_waits_for_completion(self, cloud_builder, mock_shutil_which, tmp_path):
        """Build waits for completion and returns image tag."""
        context = tmp_path / "context"
        context.mkdir()
        dockerfile = context / "Dockerfile"
        dockerfile.write_text("FROM python:3.11")

        with (
            patch.object(cloud_builder, "build_async", return_value="build-123"),
            patch.object(
                cloud_builder,
                "get_build_status",
                return_value={
                    "status": "completed",
                    "image_tag": "us-docker.pkg.dev/test/repo/image:v1",
                },
            ),
        ):
            result = cloud_builder.build(context, dockerfile, "image:v1")

            assert result == "us-docker.pkg.dev/test/repo/image:v1"

    def test_build_raises_on_failure(self, cloud_builder, mock_shutil_which, tmp_path):
        """Build raises CloudBuildError on failure."""
        context = tmp_path / "context"
        context.mkdir()
        dockerfile = context / "Dockerfile"
        dockerfile.write_text("FROM python:3.11")

        with (
            patch.object(cloud_builder, "build_async", return_value="build-123"),
            patch.object(
                cloud_builder,
                "get_build_status",
                return_value={"status": "failed", "error": "Build failed"},
            ),
        ):
            with pytest.raises(CloudBuildError) as exc_info:
                cloud_builder.build(context, dockerfile, "image:v1")

            assert "failed" in str(exc_info.value).lower()


# --- ArtifactRegistryImageRegistry Fixtures ---


@pytest.fixture
def registry(mock_shutil_which):
    """Create an ArtifactRegistryImageRegistry for testing."""
    return ArtifactRegistryImageRegistry(
        project_id="test-project",
        registry_url="us-docker.pkg.dev/test-project/images",
    )


# --- ArtifactRegistryImageRegistry Tests ---


class TestArtifactRegistryImageRegistryInit:
    """Tests for ArtifactRegistryImageRegistry initialization."""

    def test_init_sets_parameters(self, mock_shutil_which):
        """Init sets project ID and registry URL."""
        registry = ArtifactRegistryImageRegistry(
            project_id="my-project",
            registry_url="europe-docker.pkg.dev/my-project/repo",
        )

        assert registry._project_id == "my-project"
        assert registry._registry_url == "europe-docker.pkg.dev/my-project/repo"
        assert registry._auth_configured is False


class TestArtifactRegistryImageRegistryConfigureAuth:
    """Tests for _configure_docker_auth method."""

    def test_configure_auth_runs_gcloud_auth(self, registry, mock_shutil_which):
        """Configure auth runs gcloud auth configure-docker."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            registry._configure_docker_auth()

            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert "gcloud" in cmd
            assert "auth" in cmd
            assert "configure-docker" in cmd
            assert "us-docker.pkg.dev" in cmd

    def test_configure_auth_caches_result(self, registry, mock_shutil_which):
        """Configure auth caches result to avoid repeated calls."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            registry._configure_docker_auth()
            registry._configure_docker_auth()

            # Should only call once
            assert mock_run.call_count == 1
            assert registry._auth_configured is True

    def test_configure_auth_raises_on_failure(self, registry, mock_shutil_which):
        """Configure auth raises ArtifactRegistryError on failure."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="Auth failed")

            with pytest.raises(ArtifactRegistryError):
                registry._configure_docker_auth()


class TestArtifactRegistryImageRegistryPush:
    """Tests for push method."""

    def test_push_tags_and_pushes_image(self, registry, mock_shutil_which):
        """Push tags local image and pushes to registry."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            result = registry.push("local-image:v1", "us-docker.pkg.dev/proj/repo/image:v1")

            assert result == "us-docker.pkg.dev/proj/repo/image:v1"
            # Should call: docker info, gcloud auth, docker tag, docker push
            assert mock_run.call_count >= 2

    def test_push_raises_on_tag_failure(self, registry, mock_shutil_which):
        """Push raises ArtifactRegistryError on tag failure."""
        with patch("subprocess.run") as mock_run:
            # docker info succeeds, auth succeeds, tag fails
            mock_run.side_effect = [
                MagicMock(returncode=0),  # docker info
                MagicMock(returncode=0),  # gcloud auth
                MagicMock(returncode=1, stderr="Tag failed"),  # docker tag
            ]

            with pytest.raises(ArtifactRegistryError) as exc_info:
                registry.push("local:v1", "registry:v1")

            assert "tag" in str(exc_info.value).lower()

    def test_push_raises_on_push_failure(self, registry, mock_shutil_which):
        """Push raises ArtifactRegistryError on push failure."""
        with patch("subprocess.run") as mock_run:
            # All succeed except push
            mock_run.side_effect = [
                MagicMock(returncode=0),  # docker info
                MagicMock(returncode=0),  # gcloud auth
                MagicMock(returncode=0),  # docker tag
                MagicMock(returncode=1, stderr="Push denied"),  # docker push
            ]

            with pytest.raises(ArtifactRegistryError) as exc_info:
                registry.push("local:v1", "registry:v1")

            assert "push" in str(exc_info.value).lower()


class TestArtifactRegistryImageRegistryPull:
    """Tests for pull method."""

    def test_pull_pulls_image(self, registry, mock_shutil_which):
        """Pull pulls image from registry."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            result = registry.pull("us-docker.pkg.dev/proj/repo/image:v1")

            assert result == "us-docker.pkg.dev/proj/repo/image:v1"

    def test_pull_raises_on_failure(self, registry, mock_shutil_which):
        """Pull raises ArtifactRegistryError on failure."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0),  # docker info
                MagicMock(returncode=0),  # gcloud auth
                MagicMock(returncode=1, stderr="Image not found"),  # docker pull
            ]

            with pytest.raises(ArtifactRegistryError) as exc_info:
                registry.pull("registry:v1")

            assert "pull" in str(exc_info.value).lower()


class TestArtifactRegistryImageRegistryExists:
    """Tests for exists method."""

    def test_exists_returns_true_when_image_exists(self, registry, mock_shutil_which):
        """Exists returns True when image exists."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            result = registry.exists("us-docker.pkg.dev/proj/repo/image:v1")

            assert result is True

    def test_exists_returns_false_when_image_missing(self, registry, mock_shutil_which):
        """Exists returns False when image doesn't exist."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1)

            result = registry.exists("us-docker.pkg.dev/proj/repo/missing:v1")

            assert result is False


class TestArtifactRegistryImageRegistryDelete:
    """Tests for delete method."""

    def test_delete_removes_image(self, registry, mock_shutil_which):
        """Delete removes image from registry."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            registry.delete("us-docker.pkg.dev/proj/repo/image:v1")

            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert "delete" in cmd
            assert "--delete-tags" in cmd

    def test_delete_is_idempotent_when_not_found(self, registry, mock_shutil_which):
        """Delete is idempotent - no error when image doesn't exist."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="NOT_FOUND")

            # Should not raise
            registry.delete("us-docker.pkg.dev/proj/repo/missing:v1")

    def test_delete_raises_on_other_failure(self, registry, mock_shutil_which):
        """Delete raises ArtifactRegistryError on non-NotFound failure."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="Permission denied")

            with pytest.raises(ArtifactRegistryError):
                registry.delete("us-docker.pkg.dev/proj/repo/image:v1")


class TestArtifactRegistryImageRegistryDockerChecks:
    """Tests for Docker availability checks."""

    def test_check_docker_raises_when_docker_not_installed(self, mock_shutil_which):
        """Check Docker raises when Docker is not installed."""
        registry = ArtifactRegistryImageRegistry(
            project_id="test",
            registry_url="us-docker.pkg.dev/test/repo",
        )

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = FileNotFoundError("docker not found")

            with pytest.raises(ArtifactRegistryError) as exc_info:
                registry._check_docker_available()

            assert "not installed" in str(exc_info.value).lower()

    def test_check_docker_raises_when_daemon_not_responding(self, mock_shutil_which):
        """Check Docker raises when daemon is not responding."""
        registry = ArtifactRegistryImageRegistry(
            project_id="test",
            registry_url="us-docker.pkg.dev/test/repo",
        )

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1)

            with pytest.raises(ArtifactRegistryError) as exc_info:
                registry._check_docker_available()

            assert "daemon" in str(exc_info.value).lower()

    def test_check_docker_raises_on_timeout(self, mock_shutil_which):
        """Check Docker raises when daemon times out."""
        registry = ArtifactRegistryImageRegistry(
            project_id="test",
            registry_url="us-docker.pkg.dev/test/repo",
        )

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired("docker", 10)

            with pytest.raises(ArtifactRegistryError) as exc_info:
                registry._check_docker_available()

            assert "timed out" in str(exc_info.value).lower()
