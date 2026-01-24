"""Unit tests for BaseImageManager protocol integration.

Tests that BaseImageManager can use ImageBuilder and ImageRegistry protocols
instead of direct subprocess calls, enabling cleaner separation of concerns.
"""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, create_autospec

import pytest

from goldfish.cloud.adapters.local.image import LocalImageBuilder, LocalImageRegistry
from goldfish.cloud.protocols import ImageBuilder, ImageRegistry
from goldfish.config import DockerConfig, GoldfishConfig
from goldfish.infra.base_images.manager import BaseImageManager


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
    gce = MagicMock()
    gce.effective_artifact_registry = "us-docker.pkg.dev/my-project/goldfish"
    gce.effective_project_id = "my-project"
    config.gce = gce
    return config


@pytest.fixture
def temp_project_root(tmp_path: Path) -> Path:
    """Create a temporary project root directory."""
    return tmp_path


@pytest.fixture
def mock_image_builder() -> Any:
    """Create a mock ImageBuilder."""
    builder = create_autospec(ImageBuilder, instance=True)
    builder.build.return_value = "test-project-gpu:v1"
    builder.build_async.return_value = "build-12345678"
    builder.get_build_status.return_value = {
        "status": "completed",
        "image_tag": "test-project-gpu:v1",
        "error": None,
    }
    return builder


@pytest.fixture
def mock_image_registry() -> Any:
    """Create a mock ImageRegistry."""
    registry = create_autospec(ImageRegistry, instance=True)
    registry.exists.return_value = True
    registry.push.return_value = "us-docker.pkg.dev/my-project/goldfish/test-project-gpu:v1"
    return registry


class TestBaseImageManagerWithProtocols:
    """Tests for BaseImageManager using protocol adapters."""

    def test_manager_accepts_image_builder_parameter(
        self,
        temp_project_root: Path,
        mock_config: GoldfishConfig,
        mock_image_builder: MagicMock,
    ) -> None:
        """BaseImageManager should accept an optional ImageBuilder parameter."""
        manager = BaseImageManager(
            temp_project_root,
            mock_config,
            image_builder=mock_image_builder,
        )

        assert manager._image_builder is mock_image_builder

    def test_manager_accepts_image_registry_parameter(
        self,
        temp_project_root: Path,
        mock_config: GoldfishConfig,
        mock_image_registry: MagicMock,
    ) -> None:
        """BaseImageManager should accept an optional ImageRegistry parameter."""
        manager = BaseImageManager(
            temp_project_root,
            mock_config,
            image_registry=mock_image_registry,
        )

        assert manager._image_registry is mock_image_registry

    def test_build_uses_injected_image_builder(
        self,
        temp_project_root: Path,
        mock_config: GoldfishConfig,
        mock_image_builder: MagicMock,
    ) -> None:
        """build_image should use injected ImageBuilder instead of subprocess."""
        manager = BaseImageManager(
            temp_project_root,
            mock_config,
            image_builder=mock_image_builder,
        )

        result = manager.build_image("gpu", wait=True)

        # Should have called the ImageBuilder
        mock_image_builder.build.assert_called_once()
        call_args = mock_image_builder.build.call_args
        # Check that image_tag was passed correctly
        assert "test-project-gpu:v1" in str(call_args)

    def test_build_async_uses_injected_image_builder(
        self,
        temp_project_root: Path,
        mock_config: GoldfishConfig,
        mock_image_builder: MagicMock,
    ) -> None:
        """build_image with wait=False should use ImageBuilder.build_async."""
        manager = BaseImageManager(
            temp_project_root,
            mock_config,
            image_builder=mock_image_builder,
        )

        result = manager.build_image("gpu", wait=False)

        # Should have called build_async
        mock_image_builder.build_async.assert_called_once()
        assert "build_id" in result

    def test_push_uses_injected_image_registry(
        self,
        temp_project_root: Path,
        mock_config_with_gce: GoldfishConfig,
        mock_image_builder: MagicMock,
        mock_image_registry: MagicMock,
    ) -> None:
        """push_image should use injected ImageRegistry instead of subprocess."""
        # Mock local image exists check
        mock_image_registry.exists.return_value = True

        manager = BaseImageManager(
            temp_project_root,
            mock_config_with_gce,
            image_builder=mock_image_builder,
            image_registry=mock_image_registry,
        )

        # Mock the local image check
        manager._check_local_image_exists = MagicMock(return_value=True)

        result = manager.push_image("gpu")

        # Should have called the ImageRegistry
        mock_image_registry.push.assert_called_once()
        assert result["success"] is True

    def test_defaults_to_subprocess_when_no_builder_injected(
        self,
        temp_project_root: Path,
        mock_config: GoldfishConfig,
    ) -> None:
        """Without injected builder, should fall back to subprocess (backward compat)."""
        manager = BaseImageManager(temp_project_root, mock_config)

        # _image_builder should be None when not injected
        assert manager._image_builder is None

    def test_protocol_compliance_local_image_builder(self) -> None:
        """LocalImageBuilder should satisfy ImageBuilder protocol."""
        builder = LocalImageBuilder()
        # Protocol is runtime_checkable
        assert isinstance(builder, ImageBuilder)

    def test_protocol_compliance_local_image_registry(self) -> None:
        """LocalImageRegistry should satisfy ImageRegistry protocol."""
        registry = LocalImageRegistry()
        # Protocol is runtime_checkable
        assert isinstance(registry, ImageRegistry)
