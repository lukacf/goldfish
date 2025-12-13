"""Tests for DatasetRegistry class."""

from unittest.mock import MagicMock, patch

import pytest

from goldfish.datasets.registry import DatasetRegistry
from goldfish.errors import GoldfishError
from goldfish.models import SourceInfo, SourceStatus


class TestRegisterDataset:
    """Test dataset registration."""

    def test_register_dataset_with_local_source(self, test_db, test_config, temp_dir):
        """register_dataset should upload local file via storage provider and register."""
        from goldfish.providers.base import StorageLocation

        registry = DatasetRegistry(db=test_db, config=test_config)

        # Create a local test file
        local_file = temp_dir / "test_data.csv"
        local_file.write_text("col1,col2\n1,2\n3,4")

        # Mock the storage provider's upload method
        mock_location = StorageLocation(
            uri="gs://test-bucket/datasets/test_data",
            size_bytes=len("col1,col2\n1,2\n3,4"),
            metadata={},
        )

        with patch.object(
            registry.storage_provider,
            "upload",
            return_value=mock_location,
        ):
            source = registry.register_dataset(
                name="test_data",
                source=f"local:{local_file}",
                description="Test dataset",
                format="csv",
            )

        # Verify source was created
        assert source.name == "test_data"
        assert source.gcs_location == "gs://test-bucket/datasets/test_data"
        assert source.description == "Test dataset"
        assert source.status == SourceStatus.AVAILABLE

    def test_register_dataset_with_gcs_source(self, test_db, test_config):
        """register_dataset should accept GCS paths directly."""
        registry = DatasetRegistry(db=test_db, config=test_config)

        source = registry.register_dataset(
            name="remote_data",
            source="gs://my-bucket/data/file.csv",
            description="Remote dataset",
            format="csv",
        )

        assert source.name == "remote_data"
        assert source.gcs_location == "gs://my-bucket/data/file.csv"
        assert source.status == SourceStatus.AVAILABLE

    def test_register_dataset_rejects_duplicate(self, test_db, test_config):
        """register_dataset should reject duplicate dataset names."""
        registry = DatasetRegistry(db=test_db, config=test_config)

        # Register first dataset
        registry.register_dataset(
            name="duplicate",
            source="gs://bucket/data.csv",
            description="First",
            format="csv",
        )

        # Try to register duplicate
        from goldfish.errors import SourceAlreadyExistsError

        with pytest.raises(SourceAlreadyExistsError, match="duplicate"):
            registry.register_dataset(
                name="duplicate",
                source="gs://bucket/data2.csv",
                description="Second",
                format="csv",
            )

    def test_register_dataset_with_metadata(self, test_db, test_config):
        """register_dataset should store metadata."""
        registry = DatasetRegistry(db=test_db, config=test_config)

        source = registry.register_dataset(
            name="data_with_meta",
            source="gs://bucket/data.csv",
            description="Test",
            format="csv",
            metadata={"rows": 1000, "columns": 5},
        )

        # Verify metadata stored (it's JSON in DB)
        assert source.name == "data_with_meta"

    def test_register_dataset_with_local_provider(self, test_db, temp_dir):
        """register_dataset should work with local provider without GCS config."""
        # Config without GCS - uses local provider by default
        from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="dev",
            workspaces_dir="workspaces",
            slots=["w1"],
            state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(backend="local", experiments_dir="experiments"),
            gcs=None,  # No GCS config
        )

        registry = DatasetRegistry(db=test_db, config=config)

        # Create a local test file
        local_file = temp_dir / "test.csv"
        local_file.write_text("test data")

        # Should succeed with local provider (no GCS needed)
        source = registry.register_dataset(
            name="test",
            source=f"local:{local_file}",
            description="Test",
            format="csv",
        )

        # Verify it was registered with file:// URI (local provider)
        assert source.name == "test"
        assert source.gcs_location.startswith("file://")
        assert source.status == SourceStatus.AVAILABLE


class TestListDatasets:
    """Test listing datasets."""

    def test_list_datasets_empty(self, test_db, test_config):
        """list_datasets should return empty list when no datasets."""
        registry = DatasetRegistry(db=test_db, config=test_config)
        datasets = registry.list_datasets()
        assert datasets == []

    def test_list_datasets_returns_all(self, test_db, test_config):
        """list_datasets should return all registered datasets."""
        registry = DatasetRegistry(db=test_db, config=test_config)

        # Register multiple datasets
        registry.register_dataset("data1", "gs://bucket/data1.csv", "First", "csv")
        registry.register_dataset("data2", "gs://bucket/data2.csv", "Second", "csv")
        registry.register_dataset("data3", "gs://bucket/data3.csv", "Third", "csv")

        datasets = registry.list_datasets()
        assert len(datasets) == 3
        assert all(isinstance(d, SourceInfo) for d in datasets)
        names = [d.name for d in datasets]
        assert "data1" in names
        assert "data2" in names
        assert "data3" in names


class TestDatasetExists:
    """Test dataset existence checking."""

    def test_dataset_exists_true(self, test_db, test_config):
        """dataset_exists should return True for registered dataset."""
        registry = DatasetRegistry(db=test_db, config=test_config)

        registry.register_dataset("existing", "gs://bucket/data.csv", "Test", "csv")

        assert registry.dataset_exists("existing") is True

    def test_dataset_exists_false(self, test_db, test_config):
        """dataset_exists should return False for non-existent dataset."""
        registry = DatasetRegistry(db=test_db, config=test_config)

        assert registry.dataset_exists("nonexistent") is False


class TestGetDataset:
    """Test getting dataset details."""

    def test_get_dataset_returns_info(self, test_db, test_config):
        """get_dataset should return SourceInfo for existing dataset."""
        registry = DatasetRegistry(db=test_db, config=test_config)

        registry.register_dataset(
            "test_data",
            "gs://bucket/data.csv",
            "Test dataset",
            "csv",
            metadata={"key": "value"},
        )

        dataset = registry.get_dataset("test_data")
        assert isinstance(dataset, SourceInfo)
        assert dataset.name == "test_data"
        assert dataset.gcs_location == "gs://bucket/data.csv"
        assert dataset.description == "Test dataset"

    def test_get_dataset_raises_on_not_found(self, test_db, test_config):
        """get_dataset should raise SourceNotFoundError for missing dataset."""
        from goldfish.errors import SourceNotFoundError

        registry = DatasetRegistry(db=test_db, config=test_config)

        with pytest.raises(SourceNotFoundError, match="nonexistent"):
            registry.get_dataset("nonexistent")
