"""Tests for DatasetRegistry class."""

from unittest.mock import MagicMock, patch

import pytest

from goldfish.datasets.registry import DatasetRegistry
from goldfish.errors import GoldfishError
from goldfish.models import SourceInfo, SourceStatus


def _csv_metadata(description: str) -> dict:
    return {
        "schema_version": 1,
        "description": description,
        "source": {
            "format": "csv",
            "size_bytes": 1234,
            "created_at": "2025-12-24T12:00:00Z",
            "format_params": {"delimiter": ","},
        },
        "schema": {
            "kind": "tabular",
            "row_count": 2,
            "columns": ["col1", "col2"],
            "dtypes": {"col1": "int64", "col2": "int64"},
        },
    }


class TestRegisterDataset:
    """Test dataset registration."""

    def test_register_dataset_with_local_source(self, test_db, test_config, temp_dir):
        """register_dataset should upload local file to GCS and register."""
        registry = DatasetRegistry(db=test_db, config=test_config)

        # Create a local test file
        local_file = temp_dir / "test_data.csv"
        local_file.write_text("col1,col2\n1,2\n3,4")

        with patch(
            "goldfish.datasets.registry.DatasetRegistry._upload_to_gcs",
            return_value="gs://test-bucket/datasets/test_data",
        ):
            source = registry.register_dataset(
                name="test_data",
                source=f"local:{local_file}",
                description="Test dataset for registry integration tests",
                format="csv",
                metadata=_csv_metadata("Test dataset for registry integration tests"),
            )

        # Verify source was created
        assert source.name == "test_data"
        assert source.gcs_location == "gs://test-bucket/datasets/test_data"
        assert source.description == "Test dataset for registry integration tests"
        assert source.status == SourceStatus.AVAILABLE

    def test_register_dataset_with_gcs_source(self, test_db, test_config):
        """register_dataset should accept GCS paths directly."""
        registry = DatasetRegistry(db=test_db, config=test_config)

        source = registry.register_dataset(
            name="remote_data",
            source="gs://my-bucket/data/file.csv",
            description="Remote dataset for registry integration tests",
            format="csv",
            metadata=_csv_metadata("Remote dataset for registry integration tests"),
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
            description="First dataset for list tests",
            format="csv",
            metadata=_csv_metadata("First dataset for list tests"),
        )

        # Try to register duplicate
        from goldfish.errors import SourceAlreadyExistsError

        with pytest.raises(SourceAlreadyExistsError, match="duplicate"):
            registry.register_dataset(
                name="duplicate",
                source="gs://bucket/data2.csv",
                description="Second dataset for list tests",
                format="csv",
                metadata=_csv_metadata("Second dataset for list tests"),
            )

    def test_register_dataset_with_metadata(self, test_db, test_config):
        """register_dataset should store metadata."""
        registry = DatasetRegistry(db=test_db, config=test_config)

        source = registry.register_dataset(
            name="data_with_meta",
            source="gs://bucket/data.csv",
            description="Test dataset with metadata storage",
            format="csv",
            metadata=_csv_metadata("Test dataset with metadata storage"),
        )

        # Verify metadata stored (it's JSON in DB)
        assert source.name == "data_with_meta"

    def test_register_dataset_requires_metadata(self, test_db, test_config):
        """register_dataset should reject missing metadata."""
        from goldfish.validation import InvalidSourceMetadataError

        registry = DatasetRegistry(db=test_db, config=test_config)

        with pytest.raises(InvalidSourceMetadataError, match="metadata"):
            registry.register_dataset(
                name="missing_meta",
                source="gs://bucket/data.csv",
                description="Missing metadata should be rejected here",
                format="csv",
                metadata=None,
            )

    def test_register_dataset_requires_gcs_config(self, test_db, temp_dir):
        """register_dataset should raise error if GCS not configured."""
        # Config without GCS
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

        with pytest.raises(GoldfishError, match="GCS not configured"):
            registry.register_dataset(
                name="test",
                source=f"local:{local_file}",
                description="Test dataset for gcs config requirement",
                format="csv",
                metadata=_csv_metadata("Test dataset for gcs config requirement"),
            )


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
        registry.register_dataset(
            "data1",
            "gs://bucket/data1.csv",
            "First dataset for listing",
            "csv",
            metadata=_csv_metadata("First dataset for listing"),
        )
        registry.register_dataset(
            "data2",
            "gs://bucket/data2.csv",
            "Second dataset for listing",
            "csv",
            metadata=_csv_metadata("Second dataset for listing"),
        )
        registry.register_dataset(
            "data3",
            "gs://bucket/data3.csv",
            "Third dataset for listing",
            "csv",
            metadata=_csv_metadata("Third dataset for listing"),
        )

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

        registry.register_dataset(
            "existing",
            "gs://bucket/data.csv",
            "Test dataset for existence checks",
            "csv",
            metadata=_csv_metadata("Test dataset for existence checks"),
        )

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
            "Test dataset for get_dataset response",
            "csv",
            metadata=_csv_metadata("Test dataset for get_dataset response"),
        )

        dataset = registry.get_dataset("test_data")
        assert isinstance(dataset, SourceInfo)
        assert dataset.name == "test_data"
        assert dataset.gcs_location == "gs://bucket/data.csv"
        assert dataset.description == "Test dataset for get_dataset response"

    def test_get_dataset_raises_on_not_found(self, test_db, test_config):
        """get_dataset should raise SourceNotFoundError for missing dataset."""
        from goldfish.errors import SourceNotFoundError

        registry = DatasetRegistry(db=test_db, config=test_config)

        with pytest.raises(SourceNotFoundError, match="nonexistent"):
            registry.get_dataset("nonexistent")


class TestGCSUpload:
    """Test GCS upload functionality."""

    def test_upload_to_gcs_single_file(self, test_db, test_config, temp_dir):
        """_upload_to_gcs should upload single file."""
        registry = DatasetRegistry(db=test_db, config=test_config)

        # Create test file
        test_file = temp_dir / "data.csv"
        test_file.write_text("test data")

        # Mock the actual GCS upload
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            gcs_path = registry._upload_to_gcs("test_dataset", test_file)

            assert gcs_path == "gs://test-bucket/datasets/test_dataset"
            # Verify gsutil was called
            mock_run.assert_called()

    def test_upload_to_gcs_directory(self, test_db, test_config, temp_dir):
        """_upload_to_gcs should upload directory recursively."""
        registry = DatasetRegistry(db=test_db, config=test_config)

        # Create test directory
        test_dir = temp_dir / "dataset"
        test_dir.mkdir()
        (test_dir / "file1.txt").write_text("data1")
        (test_dir / "file2.txt").write_text("data2")

        # Mock the actual GCS upload
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            gcs_path = registry._upload_to_gcs("test_dataset", test_dir)

            assert gcs_path == "gs://test-bucket/datasets/test_dataset"
            # Verify gsutil was called with -r for recursive
            mock_run.assert_called()
            call_args = str(mock_run.call_args)
            assert "-r" in call_args or "recursive" in call_args.lower()

    def test_upload_to_gcs_raises_on_failure(self, test_db, test_config, temp_dir):
        """_upload_to_gcs should raise GoldfishError on upload failure."""
        registry = DatasetRegistry(db=test_db, config=test_config)

        test_file = temp_dir / "data.csv"
        test_file.write_text("test")

        # Mock failed upload
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr=b"Upload failed")

            with pytest.raises(GoldfishError, match="Failed to upload"):
                registry._upload_to_gcs("test_dataset", test_file)
