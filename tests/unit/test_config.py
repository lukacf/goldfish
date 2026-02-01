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


class TestS3StorageConfigValidation:
    """Tests for S3StorageConfig SSRF protection and validation."""

    def test_s3_endpoint_url_rejects_localhost(self) -> None:
        """S3 endpoint_url must reject localhost to prevent SSRF."""
        from pydantic import ValidationError

        from goldfish.config import S3StorageConfig

        with pytest.raises(ValidationError) as exc_info:
            S3StorageConfig(bucket="test", endpoint_url="http://localhost:9000")
        assert "localhost" in str(exc_info.value).lower() or "ssrf" in str(exc_info.value).lower()

    def test_s3_endpoint_url_rejects_127_0_0_1(self) -> None:
        """S3 endpoint_url must reject 127.0.0.1 to prevent SSRF."""
        from pydantic import ValidationError

        from goldfish.config import S3StorageConfig

        with pytest.raises(ValidationError) as exc_info:
            S3StorageConfig(bucket="test", endpoint_url="http://127.0.0.1:9000")
        assert "127.0.0.1" in str(exc_info.value) or "internal" in str(exc_info.value).lower()

    def test_s3_endpoint_url_rejects_internal_ip_ranges(self) -> None:
        """S3 endpoint_url must reject internal IP ranges (10.x, 172.16-31.x, 192.168.x)."""
        from pydantic import ValidationError

        from goldfish.config import S3StorageConfig

        internal_urls = [
            "http://10.0.0.1:9000",
            "http://172.16.0.1:9000",
            "http://172.31.255.255:9000",
            "http://192.168.1.1:9000",
        ]
        for url in internal_urls:
            with pytest.raises(ValidationError):
                S3StorageConfig(bucket="test", endpoint_url=url)

    def test_s3_endpoint_url_rejects_metadata_endpoint(self) -> None:
        """S3 endpoint_url must reject cloud metadata endpoints (169.254.169.254)."""
        from pydantic import ValidationError

        from goldfish.config import S3StorageConfig

        with pytest.raises(ValidationError):
            S3StorageConfig(bucket="test", endpoint_url="http://169.254.169.254/latest/meta-data/")

    def test_s3_endpoint_url_accepts_valid_public_url(self) -> None:
        """S3 endpoint_url should accept valid public URLs."""
        from goldfish.config import S3StorageConfig

        # MinIO on public domain
        config = S3StorageConfig(bucket="test", endpoint_url="https://minio.example.com:9000")
        assert config.endpoint_url == "https://minio.example.com:9000"

        # AWS S3 compatible endpoint
        config = S3StorageConfig(bucket="test", endpoint_url="https://s3.us-west-2.amazonaws.com")
        assert config.endpoint_url == "https://s3.us-west-2.amazonaws.com"

    def test_s3_endpoint_url_accepts_none(self) -> None:
        """S3 endpoint_url=None should be valid (uses default AWS endpoint)."""
        from goldfish.config import S3StorageConfig

        config = S3StorageConfig(bucket="test", endpoint_url=None)
        assert config.endpoint_url is None


class TestStorageConfigConsistency:
    """Tests for StorageConfig backend-config consistency validation."""

    def test_storage_config_gcs_requires_gcs_section(self) -> None:
        """StorageConfig with backend='gcs' must have gcs section."""
        from pydantic import ValidationError

        from goldfish.config import StorageConfig

        with pytest.raises(ValidationError) as exc_info:
            StorageConfig(backend="gcs", gcs=None)
        assert "gcs" in str(exc_info.value).lower()

    def test_storage_config_s3_requires_s3_section(self) -> None:
        """StorageConfig with backend='s3' must have s3 section."""
        from pydantic import ValidationError

        from goldfish.config import StorageConfig

        with pytest.raises(ValidationError) as exc_info:
            StorageConfig(backend="s3", s3=None)
        assert "s3" in str(exc_info.value).lower()

    def test_storage_config_azure_requires_azure_section(self) -> None:
        """StorageConfig with backend='azure' must have azure section."""
        from pydantic import ValidationError

        from goldfish.config import StorageConfig

        with pytest.raises(ValidationError) as exc_info:
            StorageConfig(backend="azure", azure=None)
        assert "azure" in str(exc_info.value).lower()

    def test_storage_config_local_does_not_require_section(self) -> None:
        """StorageConfig with backend='local' doesn't require any section."""
        from goldfish.config import StorageConfig

        config = StorageConfig(backend="local")
        assert config.backend == "local"

    def test_storage_config_gcs_with_gcs_section_valid(self) -> None:
        """StorageConfig with backend='gcs' and gcs section is valid."""
        from goldfish.config import GCSConfig, StorageConfig

        config = StorageConfig(backend="gcs", gcs=GCSConfig(bucket="my-bucket"))
        assert config.backend == "gcs"
        assert config.gcs is not None
        assert config.gcs.bucket == "my-bucket"


class TestAzureStorageConfigValidation:
    """Tests for Azure storage account name validation."""

    def test_azure_account_name_rejects_too_short(self) -> None:
        """Azure account names must be at least 3 characters."""
        from pydantic import ValidationError

        from goldfish.config import AzureStorageConfig

        with pytest.raises(ValidationError):
            AzureStorageConfig(container="test", account="ab")

    def test_azure_account_name_rejects_too_long(self) -> None:
        """Azure account names must be at most 24 characters."""
        from pydantic import ValidationError

        from goldfish.config import AzureStorageConfig

        with pytest.raises(ValidationError):
            AzureStorageConfig(container="test", account="a" * 25)

    def test_azure_account_name_rejects_invalid_chars(self) -> None:
        """Azure account names must be alphanumeric only."""
        from pydantic import ValidationError

        from goldfish.config import AzureStorageConfig

        with pytest.raises(ValidationError):
            AzureStorageConfig(container="test", account="my-account")  # hyphen not allowed

        with pytest.raises(ValidationError):
            AzureStorageConfig(container="test", account="my_account")  # underscore not allowed

    def test_azure_account_name_accepts_valid(self) -> None:
        """Azure account names that are 3-24 alphanumeric chars should be valid."""
        from goldfish.config import AzureStorageConfig

        config = AzureStorageConfig(container="test", account="mystorageaccount123")
        assert config.account == "mystorageaccount123"
