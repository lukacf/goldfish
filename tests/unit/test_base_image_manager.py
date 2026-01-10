"""Unit tests for BaseImageManager."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from goldfish.config import DockerConfig, GoldfishConfig
from goldfish.errors import (
    BaseImageNotFoundError,
    DockerNotAvailableError,
    RegistryNotConfiguredError,
)
from goldfish.infra.base_images.manager import BaseImageManager
from goldfish.validation import InvalidBuildIdError, InvalidImageTypeError


@pytest.fixture
def mock_config() -> GoldfishConfig:
    """Create a mock GoldfishConfig."""
    config = MagicMock(spec=GoldfishConfig)
    config.project_name = "test-project"
    config.docker = DockerConfig()
    config.gce = None
    return config


@pytest.fixture
def mock_config_with_gce() -> GoldfishConfig:
    """Create a mock GoldfishConfig with GCE configured."""
    config = MagicMock(spec=GoldfishConfig)
    config.project_name = "test-project"
    config.docker = DockerConfig()
    # Mock GCE config
    gce = MagicMock()
    gce.effective_artifact_registry = "us-docker.pkg.dev/my-project/goldfish"
    config.gce = gce
    return config


@pytest.fixture
def mock_config_with_packages() -> GoldfishConfig:
    """Create a mock GoldfishConfig with extra packages."""
    config = MagicMock(spec=GoldfishConfig)
    config.project_name = "test-project"
    config.docker = DockerConfig(
        extra_packages={
            "gpu": ["flash-attn --no-build-isolation", "triton"],
            "cpu": ["lightgbm"],
        }
    )
    config.gce = None
    return config


@pytest.fixture
def temp_project_root(tmp_path: Path) -> Path:
    """Create a temporary project root directory."""
    return tmp_path


class TestBaseImageManagerInit:
    """Tests for BaseImageManager initialization."""

    def test_initializes_with_project_root_and_config(
        self, temp_project_root: Path, mock_config: GoldfishConfig
    ) -> None:
        """Manager should initialize with project root and config."""
        manager = BaseImageManager(temp_project_root, mock_config)

        assert manager.project_root == temp_project_root
        assert manager.project_name == "test-project"
        assert manager.docker_config is not None

    def test_initializes_with_empty_builds(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """Manager should start with empty builds dict."""
        manager = BaseImageManager(temp_project_root, mock_config)

        assert manager._builds == {}


class TestDockerAvailability:
    """Tests for Docker availability checking."""

    def test_docker_not_available_raises_error(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """Should raise DockerNotAvailableError when docker info fails."""
        manager = BaseImageManager(temp_project_root, mock_config)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1)

            with pytest.raises(DockerNotAvailableError):
                manager._check_docker_available()

    def test_docker_not_installed_raises_error(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """Should raise DockerNotAvailableError when docker not found."""
        manager = BaseImageManager(temp_project_root, mock_config)

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = FileNotFoundError()

            with pytest.raises(DockerNotAvailableError) as exc_info:
                manager._check_docker_available()

            assert "not installed" in exc_info.value.message

    def test_docker_available_passes(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """Should pass when docker info succeeds."""
        manager = BaseImageManager(temp_project_root, mock_config)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            # Should not raise
            manager._check_docker_available()


class TestRegistryConfiguration:
    """Tests for artifact registry resolution."""

    def test_no_gce_raises_registry_error(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """Should raise RegistryNotConfiguredError when no GCE config."""
        manager = BaseImageManager(temp_project_root, mock_config)

        with pytest.raises(RegistryNotConfiguredError):
            manager._get_artifact_registry()

    def test_gce_with_registry_returns_url(self, temp_project_root: Path, mock_config_with_gce: GoldfishConfig) -> None:
        """Should return registry URL when GCE configured."""
        manager = BaseImageManager(temp_project_root, mock_config_with_gce)

        result = manager._get_artifact_registry()

        assert result == "us-docker.pkg.dev/my-project/goldfish"


class TestImageTagGeneration:
    """Tests for project image tag generation."""

    def test_local_tag_format(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """Local tag should be {project}-{type}:v1."""
        manager = BaseImageManager(temp_project_root, mock_config)

        gpu_tag = manager._get_project_image_tag("gpu", for_registry=False)
        cpu_tag = manager._get_project_image_tag("cpu", for_registry=False)

        assert gpu_tag == "test-project-gpu:v1"
        assert cpu_tag == "test-project-cpu:v1"

    def test_registry_tag_includes_url(self, temp_project_root: Path, mock_config_with_gce: GoldfishConfig) -> None:
        """Registry tag should include registry URL prefix."""
        manager = BaseImageManager(temp_project_root, mock_config_with_gce)

        tag = manager._get_project_image_tag("gpu", for_registry=True)

        assert tag == "us-docker.pkg.dev/my-project/goldfish/test-project-gpu:v1"


class TestExtraPackages:
    """Tests for extra package handling."""

    def test_no_packages_returns_empty(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """Should return empty list when no packages configured."""
        manager = BaseImageManager(temp_project_root, mock_config)

        assert manager._get_extra_packages("gpu") == []
        assert manager._get_extra_packages("cpu") == []

    def test_returns_configured_packages(
        self, temp_project_root: Path, mock_config_with_packages: GoldfishConfig
    ) -> None:
        """Should return configured packages for each type."""
        manager = BaseImageManager(temp_project_root, mock_config_with_packages)

        gpu_packages = manager._get_extra_packages("gpu")
        cpu_packages = manager._get_extra_packages("cpu")

        assert "flash-attn --no-build-isolation" in gpu_packages
        assert "triton" in gpu_packages
        assert "lightgbm" in cpu_packages


class TestDockerfileGeneration:
    """Tests for Dockerfile content generation."""

    def test_generates_from_directive(self, temp_project_root: Path, mock_config_with_gce: GoldfishConfig) -> None:
        """Generated Dockerfile should have FROM directive."""
        manager = BaseImageManager(temp_project_root, mock_config_with_gce)

        content = manager._generate_dockerfile_content("gpu")

        assert "FROM " in content
        assert "us-docker.pkg.dev/my-project/goldfish/goldfish-base-gpu" in content

    def test_includes_extra_packages(self, temp_project_root: Path, mock_config_with_packages: GoldfishConfig) -> None:
        """Generated Dockerfile should include pip install for extra packages."""
        manager = BaseImageManager(temp_project_root, mock_config_with_packages)

        content = manager._generate_dockerfile_content("gpu")

        assert "pip install" in content
        assert "flash-attn --no-build-isolation" in content
        assert "triton" in content

    def test_uses_custom_dockerfile_when_exists(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """Should use custom Dockerfile when it exists in project root."""
        # Create custom Dockerfile
        custom_dockerfile = temp_project_root / "Dockerfile.gpu"
        custom_dockerfile.write_text("FROM custom-base:latest\nRUN echo hello")

        manager = BaseImageManager(temp_project_root, mock_config)

        content, path = manager._get_effective_dockerfile("gpu")

        assert "custom-base" in content
        assert path == custom_dockerfile


class TestDockerfileHash:
    """Tests for Dockerfile hash computation."""

    def test_same_content_same_hash(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """Same Dockerfile content should produce same hash."""
        manager = BaseImageManager(temp_project_root, mock_config)

        hash1 = manager._compute_dockerfile_hash("gpu")
        hash2 = manager._compute_dockerfile_hash("gpu")

        assert hash1 == hash2

    def test_different_content_different_hash(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """Different Dockerfile content should produce different hash."""
        manager = BaseImageManager(temp_project_root, mock_config)

        gpu_hash = manager._compute_dockerfile_hash("gpu")
        cpu_hash = manager._compute_dockerfile_hash("cpu")

        assert gpu_hash != cpu_hash


class TestListImages:
    """Tests for list_images method."""

    def test_list_returns_both_types(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """list_images should return info for both cpu and gpu."""
        manager = BaseImageManager(temp_project_root, mock_config)

        with patch.object(manager, "_check_docker_available"):
            with patch.object(manager, "_check_local_image_exists", return_value=False):
                result = manager.list_images()

        # Now returns both base_images and project_images
        assert "base_images" in result
        assert "project_images" in result
        assert "cpu" in result["project_images"]
        assert "gpu" in result["project_images"]

    def test_list_shows_customization(self, temp_project_root: Path, mock_config_with_packages: GoldfishConfig) -> None:
        """list_images should show customization info."""
        manager = BaseImageManager(temp_project_root, mock_config_with_packages)

        with patch.object(manager, "_check_docker_available"):
            with patch.object(manager, "_check_local_image_exists", return_value=False):
                result = manager.list_images()

        gpu_info = result["project_images"]["gpu"]
        assert "customization" in gpu_info
        assert "flash-attn --no-build-isolation" in gpu_info["customization"]["extra_packages"]


class TestInspectImage:
    """Tests for inspect_image method."""

    def test_inspect_returns_dockerfile_content(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """inspect_image should return effective Dockerfile content."""
        manager = BaseImageManager(temp_project_root, mock_config)

        result = manager.inspect_image("gpu")

        assert "effective_dockerfile" in result
        assert "FROM " in result["effective_dockerfile"]

    def test_inspect_shows_config_path(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """inspect_image should show path to goldfish.yaml for editing."""
        manager = BaseImageManager(temp_project_root, mock_config)

        result = manager.inspect_image("cpu")

        assert "customization" in result
        assert "config_path" in result["customization"]
        assert "goldfish.yaml" in result["customization"]["config_path"]

    def test_inspect_invalid_type_raises(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """inspect_image should raise on invalid image type."""
        manager = BaseImageManager(temp_project_root, mock_config)

        with pytest.raises(InvalidImageTypeError):
            manager.inspect_image("invalid")


class TestCheckImages:
    """Tests for check_images method."""

    def test_check_shows_needs_rebuild_when_no_local(
        self, temp_project_root: Path, mock_config_with_packages: GoldfishConfig
    ) -> None:
        """check_images should indicate rebuild needed when no local image."""
        manager = BaseImageManager(temp_project_root, mock_config_with_packages)

        with patch.object(manager, "_check_docker_available"):
            with patch.object(manager, "_check_local_image_exists", return_value=False):
                result = manager.check_images()

        assert result["checks"]["gpu"]["needs_rebuild"] is True

    def test_check_shows_needs_push_when_local_only(
        self, temp_project_root: Path, mock_config_with_gce: GoldfishConfig
    ) -> None:
        """check_images should indicate push needed when only local exists."""
        mock_config_with_gce.docker = DockerConfig(extra_packages={"gpu": ["some-pkg"]})
        manager = BaseImageManager(temp_project_root, mock_config_with_gce)

        with patch.object(manager, "_check_docker_available"):
            with patch.object(manager, "_check_local_image_exists", return_value=True):
                with patch.object(manager, "_check_registry_image_exists", return_value=False):
                    result = manager.check_images()

        assert result["checks"]["gpu"]["needs_push"] is True


class TestBuildImage:
    """Tests for build_image method."""

    def test_build_async_returns_build_id(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """build_image with wait=False should return build_id immediately."""
        manager = BaseImageManager(temp_project_root, mock_config)

        with patch.object(manager, "_check_docker_available"):
            with patch.object(manager, "_run_build"):
                result = manager.build_image("gpu", wait=False)

        assert "build_id" in result
        assert result["build_id"].startswith("build-")
        assert result["status"] == "pending"

    def test_build_sync_blocks_until_complete(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """build_image with wait=True should block and return final status."""
        manager = BaseImageManager(temp_project_root, mock_config)

        with patch.object(manager, "_check_docker_available"):
            with patch.object(manager, "_run_build") as mock_run:
                # Simulate successful build (now takes target parameter)
                def complete_build(build_id: str, image_type: str, no_cache: bool, target: str = "project") -> None:
                    manager._builds[build_id].status = "completed"
                    manager._builds[build_id].image_tag = "test-project-gpu:v1"

                mock_run.side_effect = complete_build
                result = manager.build_image("gpu", wait=True)

        assert result["status"] == "completed"

    def test_build_invalid_type_raises(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """build_image should raise on invalid image type."""
        manager = BaseImageManager(temp_project_root, mock_config)

        with pytest.raises(InvalidImageTypeError):
            manager.build_image("invalid")


class TestPushImage:
    """Tests for push_image method."""

    def test_push_requires_local_image(self, temp_project_root: Path, mock_config_with_gce: GoldfishConfig) -> None:
        """push_image should raise when local image doesn't exist."""
        manager = BaseImageManager(temp_project_root, mock_config_with_gce)

        with patch.object(manager, "_check_docker_available"):
            with patch.object(manager, "_check_local_image_exists", return_value=False):
                with pytest.raises(BaseImageNotFoundError):
                    manager.push_image("gpu")

    def test_push_requires_registry(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """push_image should raise when registry not configured."""
        manager = BaseImageManager(temp_project_root, mock_config)

        with patch.object(manager, "_check_docker_available"):
            with patch.object(manager, "_check_local_image_exists", return_value=True):
                with pytest.raises(RegistryNotConfiguredError):
                    manager.push_image("gpu")


class TestGetBuildStatus:
    """Tests for get_build_status method."""

    def test_unknown_build_id_raises(self, temp_project_root: Path, mock_config: GoldfishConfig) -> None:
        """get_build_status should raise on unknown build ID."""
        manager = BaseImageManager(temp_project_root, mock_config)

        from goldfish.errors import GoldfishError

        with pytest.raises(GoldfishError) as exc_info:
            manager.get_build_status("build-00000000")

        assert "Unknown build ID" in str(exc_info.value)


class TestValidation:
    """Tests for validation functions."""

    def test_validate_image_type_valid(self) -> None:
        """validate_image_type should pass for valid types."""
        from goldfish.validation import validate_image_type

        # Should not raise
        validate_image_type("cpu")
        validate_image_type("gpu")

    def test_validate_image_type_invalid(self) -> None:
        """validate_image_type should raise for invalid types."""
        from goldfish.validation import validate_image_type

        with pytest.raises(InvalidImageTypeError):
            validate_image_type("tpu")

        with pytest.raises(InvalidImageTypeError):
            validate_image_type("")

    def test_validate_build_id_valid(self) -> None:
        """validate_build_id should pass for valid IDs."""
        from goldfish.validation import validate_build_id

        # Should not raise
        validate_build_id("build-abcd1234")
        validate_build_id("build-00000000")

    def test_validate_build_id_invalid(self) -> None:
        """validate_build_id should raise for invalid IDs."""
        from goldfish.validation import validate_build_id

        with pytest.raises(InvalidBuildIdError):
            validate_build_id("invalid")

        with pytest.raises(InvalidBuildIdError):
            validate_build_id("build-too-long-id")

        with pytest.raises(InvalidBuildIdError):
            validate_build_id("")
