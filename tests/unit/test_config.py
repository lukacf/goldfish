"""Unit tests for goldfish.config module."""

import os
from pathlib import Path

import pytest

from goldfish.config import DockerConfig, GoldfishConfig


def test_get_dev_repo_path_with_relative_project_root(tmp_path: Path) -> None:
    """Test that get_dev_repo_path works with relative Path('.') project root.

    Regression test for bug where Path('.').parent is '.' not the actual parent,
    causing dev_repo_path to resolve incorrectly.
    """
    # Create a project directory structure
    project_dir = tmp_path / "myproject"
    project_dir.mkdir()
    dev_dir = tmp_path / "myproject-dev"
    dev_dir.mkdir()

    # Create minimal goldfish.yaml
    config_file = project_dir / "goldfish.yaml"
    config_file.write_text("""
project_name: myproject
dev_repo_path: myproject-dev
""")

    # Change to project directory and load config with relative path
    original_cwd = os.getcwd()
    try:
        os.chdir(project_dir)
        config = GoldfishConfig.load(Path("."))

        # This should resolve to tmp_path/myproject-dev, not tmp_path/myproject/myproject-dev
        dev_repo = config.get_dev_repo_path(Path("."))
        assert dev_repo == dev_dir, f"Expected {dev_dir}, got {dev_repo}"
    finally:
        os.chdir(original_cwd)


class TestDockerConfig:
    """Tests for DockerConfig base_images and base_image_version."""

    def test_default_docker_config_has_empty_base_images(self) -> None:
        """Default DockerConfig should have empty base_images dict."""
        config = DockerConfig()
        assert config.base_images == {}
        assert config.base_image_version is None

    def test_docker_config_accepts_base_images(self) -> None:
        """DockerConfig should accept base_images dict."""
        config = DockerConfig(
            base_images={
                "gpu": "us-docker.pkg.dev/my-project/repo/custom-gpu:v1",
                "cpu": "goldfish-base-cpu",
            }
        )
        assert config.base_images["gpu"] == "us-docker.pkg.dev/my-project/repo/custom-gpu:v1"
        assert config.base_images["cpu"] == "goldfish-base-cpu"

    def test_docker_config_accepts_base_image_version(self) -> None:
        """DockerConfig should accept base_image_version override."""
        config = DockerConfig(base_image_version="v8")
        assert config.base_image_version == "v8"

    def test_goldfish_config_loads_docker_base_images(self, tmp_path: Path) -> None:
        """GoldfishConfig should load docker.base_images from YAML."""
        project_dir = tmp_path / "myproject"
        project_dir.mkdir()
        (tmp_path / "myproject-dev").mkdir()

        config_file = project_dir / "goldfish.yaml"
        config_file.write_text("""
project_name: myproject
dev_repo_path: myproject-dev
docker:
  base_images:
    gpu: "us-docker.pkg.dev/my-project/repo/custom-gpu:v2"
    cpu: "my-custom-cpu:v1"
  base_image_version: "v8"
""")

        config = GoldfishConfig.load(project_dir)
        assert config.docker.base_images["gpu"] == "us-docker.pkg.dev/my-project/repo/custom-gpu:v2"
        assert config.docker.base_images["cpu"] == "my-custom-cpu:v1"
        assert config.docker.base_image_version == "v8"

    def test_goldfish_config_docker_base_images_defaults_to_empty(self, tmp_path: Path) -> None:
        """GoldfishConfig should default to empty base_images when not specified."""
        project_dir = tmp_path / "myproject"
        project_dir.mkdir()
        (tmp_path / "myproject-dev").mkdir()

        config_file = project_dir / "goldfish.yaml"
        config_file.write_text("""
project_name: myproject
dev_repo_path: myproject-dev
""")

        config = GoldfishConfig.load(project_dir)
        assert config.docker.base_images == {}
        assert config.docker.base_image_version is None

    def test_docker_config_rejects_invalid_extra_field(self) -> None:
        """DockerConfig should reject unknown fields."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            DockerConfig(unknown_field="value")

    def test_docker_config_base_images_accepts_only_string_values(self) -> None:
        """base_images values must be strings."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            DockerConfig(base_images={"gpu": 123})  # type: ignore[dict-item]

        with pytest.raises(ValidationError):
            DockerConfig(base_images={"gpu": ["list", "not", "allowed"]})  # type: ignore[dict-item]

    def test_goldfish_config_partial_docker_section(self, tmp_path: Path) -> None:
        """Partial docker section should merge with defaults."""
        project_dir = tmp_path / "myproject"
        project_dir.mkdir()
        (tmp_path / "myproject-dev").mkdir()

        config_file = project_dir / "goldfish.yaml"
        config_file.write_text("""
project_name: myproject
dev_repo_path: myproject-dev
docker:
  base_image_version: "v8"
  # base_images not specified - should default to empty
""")

        config = GoldfishConfig.load(project_dir)
        assert config.docker.base_image_version == "v8"
        assert config.docker.base_images == {}  # Default
        assert config.docker.extra_packages == {}  # Default

    def test_goldfish_config_only_gpu_base_image(self, tmp_path: Path) -> None:
        """Setting only GPU base image should leave CPU as default."""
        project_dir = tmp_path / "myproject"
        project_dir.mkdir()
        (tmp_path / "myproject-dev").mkdir()

        config_file = project_dir / "goldfish.yaml"
        config_file.write_text("""
project_name: myproject
dev_repo_path: myproject-dev
docker:
  base_images:
    gpu: "my-custom-gpu:v1"
""")

        config = GoldfishConfig.load(project_dir)
        assert config.docker.base_images == {"gpu": "my-custom-gpu:v1"}
        assert "cpu" not in config.docker.base_images
