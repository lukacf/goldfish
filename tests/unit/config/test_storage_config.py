"""Unit tests for storage backend configuration.

Tests for the StorageConfig abstraction that allows configuring
multiple storage backends (GCS, S3, Azure, local) in goldfish.yaml.
"""

from pathlib import Path

import pytest
from pydantic import ValidationError

from goldfish.config import (
    AzureStorageConfig,
    GCSConfig,
    GoldfishConfig,
    S3StorageConfig,
    StorageConfig,
)


class TestS3StorageConfig:
    """Tests for S3 storage configuration model."""

    def test_s3_config_requires_bucket(self) -> None:
        """S3Config requires bucket field."""
        with pytest.raises(ValidationError):
            S3StorageConfig()  # type: ignore[call-arg]

    def test_s3_config_with_bucket_only(self) -> None:
        """S3Config can be created with just bucket."""
        config = S3StorageConfig(bucket="my-bucket")
        assert config.bucket == "my-bucket"
        assert config.region is None
        assert config.endpoint_url is None

    def test_s3_config_with_all_fields(self) -> None:
        """S3Config accepts all optional fields."""
        config = S3StorageConfig(
            bucket="my-bucket",
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            sources_prefix="data/sources/",
            artifacts_prefix="data/artifacts/",
        )
        assert config.bucket == "my-bucket"
        assert config.region == "us-east-1"
        assert config.endpoint_url == "http://localhost:9000"
        assert config.sources_prefix == "data/sources/"
        assert config.artifacts_prefix == "data/artifacts/"

    def test_s3_config_default_prefixes(self) -> None:
        """S3Config has sensible default prefixes matching GCS."""
        config = S3StorageConfig(bucket="my-bucket")
        assert config.sources_prefix == "sources/"
        assert config.artifacts_prefix == "artifacts/"
        assert config.snapshots_prefix == "snapshots/"
        assert config.datasets_prefix == "datasets/"

    def test_s3_config_rejects_unknown_fields(self) -> None:
        """S3Config rejects unknown fields."""
        with pytest.raises(ValidationError):
            S3StorageConfig(bucket="my-bucket", unknown="value")  # type: ignore[call-arg]


class TestAzureStorageConfig:
    """Tests for Azure Blob storage configuration model."""

    def test_azure_config_requires_container_and_account(self) -> None:
        """AzureConfig requires both container and account."""
        with pytest.raises(ValidationError):
            AzureStorageConfig()  # type: ignore[call-arg]

        with pytest.raises(ValidationError):
            AzureStorageConfig(container="my-container")  # type: ignore[call-arg]

        with pytest.raises(ValidationError):
            AzureStorageConfig(account="my-account")  # type: ignore[call-arg]

    def test_azure_config_with_required_fields(self) -> None:
        """AzureConfig can be created with required fields."""
        config = AzureStorageConfig(container="my-container", account="my-account")
        assert config.container == "my-container"
        assert config.account == "my-account"

    def test_azure_config_default_prefixes(self) -> None:
        """AzureConfig has sensible default prefixes matching GCS."""
        config = AzureStorageConfig(container="my-container", account="my-account")
        assert config.sources_prefix == "sources/"
        assert config.artifacts_prefix == "artifacts/"
        assert config.snapshots_prefix == "snapshots/"
        assert config.datasets_prefix == "datasets/"

    def test_azure_config_rejects_unknown_fields(self) -> None:
        """AzureConfig rejects unknown fields."""
        with pytest.raises(ValidationError):
            AzureStorageConfig(
                container="my-container",
                account="my-account",
                unknown="value",  # type: ignore[call-arg]
            )


class TestStorageConfig:
    """Tests for unified StorageConfig."""

    def test_storage_config_defaults_to_gcs(self) -> None:
        """StorageConfig defaults backend to 'gcs'."""
        config = StorageConfig()
        assert config.backend == "gcs"

    def test_storage_config_accepts_gcs_backend(self) -> None:
        """StorageConfig accepts 'gcs' backend."""
        config = StorageConfig(
            backend="gcs",
            gcs=GCSConfig(bucket="my-gcs-bucket"),
        )
        assert config.backend == "gcs"
        assert config.gcs is not None
        assert config.gcs.bucket == "my-gcs-bucket"

    def test_storage_config_accepts_s3_backend(self) -> None:
        """StorageConfig accepts 's3' backend."""
        config = StorageConfig(
            backend="s3",
            s3=S3StorageConfig(bucket="my-s3-bucket", region="us-west-2"),
        )
        assert config.backend == "s3"
        assert config.s3 is not None
        assert config.s3.bucket == "my-s3-bucket"
        assert config.s3.region == "us-west-2"

    def test_storage_config_accepts_azure_backend(self) -> None:
        """StorageConfig accepts 'azure' backend."""
        config = StorageConfig(
            backend="azure",
            azure=AzureStorageConfig(container="my-container", account="my-account"),
        )
        assert config.backend == "azure"
        assert config.azure is not None
        assert config.azure.container == "my-container"
        assert config.azure.account == "my-account"

    def test_storage_config_accepts_local_backend(self) -> None:
        """StorageConfig accepts 'local' backend."""
        config = StorageConfig(backend="local")
        assert config.backend == "local"

    def test_storage_config_rejects_invalid_backend(self) -> None:
        """StorageConfig rejects unknown backend values."""
        with pytest.raises(ValidationError):
            StorageConfig(backend="invalid")  # type: ignore[arg-type]

    def test_storage_config_rejects_unknown_fields(self) -> None:
        """StorageConfig rejects unknown fields."""
        with pytest.raises(ValidationError):
            StorageConfig(backend="gcs", unknown="value")  # type: ignore[call-arg]


class TestGoldfishConfigStorageSection:
    """Tests for storage section in GoldfishConfig."""

    def test_goldfish_config_loads_storage_section(self, tmp_path: Path) -> None:
        """GoldfishConfig loads the storage section from YAML."""
        project_dir = tmp_path / "myproject"
        project_dir.mkdir()
        (tmp_path / "myproject-dev").mkdir()

        config_file = project_dir / "goldfish.yaml"
        config_file.write_text("""
project_name: myproject
dev_repo_path: myproject-dev
storage:
  backend: s3
  s3:
    bucket: my-s3-bucket
    region: us-east-1
""")

        config = GoldfishConfig.load(project_dir)
        assert config.storage is not None
        assert config.storage.backend == "s3"
        assert config.storage.s3 is not None
        assert config.storage.s3.bucket == "my-s3-bucket"
        assert config.storage.s3.region == "us-east-1"

    def test_goldfish_config_loads_azure_storage(self, tmp_path: Path) -> None:
        """GoldfishConfig loads Azure storage config from YAML."""
        project_dir = tmp_path / "myproject"
        project_dir.mkdir()
        (tmp_path / "myproject-dev").mkdir()

        config_file = project_dir / "goldfish.yaml"
        config_file.write_text("""
project_name: myproject
dev_repo_path: myproject-dev
storage:
  backend: azure
  azure:
    container: my-container
    account: mystorageaccount
""")

        config = GoldfishConfig.load(project_dir)
        assert config.storage is not None
        assert config.storage.backend == "azure"
        assert config.storage.azure is not None
        assert config.storage.azure.container == "my-container"
        assert config.storage.azure.account == "mystorageaccount"

    def test_goldfish_config_storage_defaults_to_none(self, tmp_path: Path) -> None:
        """GoldfishConfig has storage=None when not specified."""
        project_dir = tmp_path / "myproject"
        project_dir.mkdir()
        (tmp_path / "myproject-dev").mkdir()

        config_file = project_dir / "goldfish.yaml"
        config_file.write_text("""
project_name: myproject
dev_repo_path: myproject-dev
""")

        config = GoldfishConfig.load(project_dir)
        assert config.storage is None

    def test_goldfish_config_backwards_compat_gcs_section(self, tmp_path: Path) -> None:
        """Old gcs: section still works for backwards compatibility."""
        project_dir = tmp_path / "myproject"
        project_dir.mkdir()
        (tmp_path / "myproject-dev").mkdir()

        config_file = project_dir / "goldfish.yaml"
        config_file.write_text("""
project_name: myproject
dev_repo_path: myproject-dev
gcs:
  bucket: my-legacy-bucket
""")

        config = GoldfishConfig.load(project_dir)
        # Old gcs section should still be accessible
        assert config.gcs is not None
        assert config.gcs.bucket == "my-legacy-bucket"

    def test_goldfish_config_storage_and_gcs_coexist(self, tmp_path: Path) -> None:
        """Both storage and gcs sections can coexist."""
        project_dir = tmp_path / "myproject"
        project_dir.mkdir()
        (tmp_path / "myproject-dev").mkdir()

        config_file = project_dir / "goldfish.yaml"
        config_file.write_text("""
project_name: myproject
dev_repo_path: myproject-dev
gcs:
  bucket: legacy-bucket
storage:
  backend: gcs
  gcs:
    bucket: new-bucket
""")

        config = GoldfishConfig.load(project_dir)
        # Both sections should be accessible
        assert config.gcs is not None
        assert config.gcs.bucket == "legacy-bucket"
        assert config.storage is not None
        assert config.storage.backend == "gcs"
        assert config.storage.gcs is not None
        assert config.storage.gcs.bucket == "new-bucket"

    def test_goldfish_config_storage_local_backend(self, tmp_path: Path) -> None:
        """GoldfishConfig loads local storage backend."""
        project_dir = tmp_path / "myproject"
        project_dir.mkdir()
        (tmp_path / "myproject-dev").mkdir()

        config_file = project_dir / "goldfish.yaml"
        config_file.write_text("""
project_name: myproject
dev_repo_path: myproject-dev
storage:
  backend: local
""")

        config = GoldfishConfig.load(project_dir)
        assert config.storage is not None
        assert config.storage.backend == "local"


class TestStorageConfigEffectiveBucket:
    """Tests for effective_bucket property that provides unified bucket access."""

    def test_effective_bucket_from_gcs(self) -> None:
        """effective_bucket returns GCS bucket when backend is gcs."""
        config = StorageConfig(
            backend="gcs",
            gcs=GCSConfig(bucket="gcs-bucket"),
        )
        assert config.effective_bucket == "gcs-bucket"

    def test_effective_bucket_from_s3(self) -> None:
        """effective_bucket returns S3 bucket when backend is s3."""
        config = StorageConfig(
            backend="s3",
            s3=S3StorageConfig(bucket="s3-bucket"),
        )
        assert config.effective_bucket == "s3-bucket"

    def test_effective_bucket_from_azure(self) -> None:
        """effective_bucket returns Azure container when backend is azure."""
        config = StorageConfig(
            backend="azure",
            azure=AzureStorageConfig(container="azure-container", account="myaccount"),
        )
        assert config.effective_bucket == "azure-container"

    def test_effective_bucket_local_returns_none(self) -> None:
        """effective_bucket returns None for local backend."""
        config = StorageConfig(backend="local")
        assert config.effective_bucket is None

    def test_effective_bucket_missing_config_returns_none(self) -> None:
        """effective_bucket returns None when backend config is missing."""
        config = StorageConfig(backend="gcs")  # No gcs config
        assert config.effective_bucket is None
