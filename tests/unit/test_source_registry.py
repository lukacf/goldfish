"""Tests for SourceRegistry class.

Tests critical registry operations:
- Registering external sources
- Checking source existence
- Listing sources
- Getting source details
- Handling duplicate registrations
"""

from unittest.mock import MagicMock

import pytest

from goldfish.errors import SourceAlreadyExistsError, SourceNotFoundError
from goldfish.models import SourceInfo, SourceStatus
from goldfish.sources.registry import SourceRegistry


class TestSourceRegistryBasics:
    """Test basic registry operations."""

    def test_list_sources_empty(self):
        """list_sources should return empty list when no sources exist."""
        mock_db = MagicMock()
        mock_db.list_sources.return_value = []

        registry = SourceRegistry(db=mock_db)
        sources = registry.list_sources()

        assert sources == []
        mock_db.list_sources.assert_called_once_with(status=None)

    def test_list_sources_with_results(self):
        """list_sources should convert database dicts to SourceInfo."""
        mock_db = MagicMock()
        mock_db.list_sources.return_value = [
            {
                "id": "test_data",
                "name": "test_data",
                "description": "Test dataset",
                "created_at": "2025-12-05T10:00:00+00:00",
                "created_by": "external",
                "gcs_location": "gs://bucket/data/test.csv",
                "size_bytes": 1024,
                "status": "available",
                "metadata": None,
            }
        ]

        registry = SourceRegistry(db=mock_db)
        sources = registry.list_sources()

        assert len(sources) == 1
        assert isinstance(sources[0], SourceInfo)
        assert sources[0].name == "test_data"
        assert sources[0].description == "Test dataset"
        assert sources[0].status == SourceStatus.AVAILABLE
        assert sources[0].metadata is None
        assert sources[0].metadata_status == "missing"

    def test_list_sources_with_status_filter(self):
        """list_sources should pass status filter to database."""
        mock_db = MagicMock()
        mock_db.list_sources.return_value = []

        registry = SourceRegistry(db=mock_db)
        registry.list_sources(status="pending")

        mock_db.list_sources.assert_called_once_with(status="pending")

    def test_source_exists_true(self):
        """source_exists should return True when source exists."""
        mock_db = MagicMock()
        mock_db.source_exists.return_value = True

        registry = SourceRegistry(db=mock_db)
        assert registry.source_exists("test_data") is True

    def test_source_exists_false(self):
        """source_exists should return False when source doesn't exist."""
        mock_db = MagicMock()
        mock_db.source_exists.return_value = False

        registry = SourceRegistry(db=mock_db)
        assert registry.source_exists("nonexistent") is False


class TestGetSource:
    """Test get_source operation."""

    def test_get_source_valid(self):
        """get_source should return SourceInfo for existing source."""
        mock_db = MagicMock()
        mock_db.get_source.return_value = {
            "id": "test_data",
            "name": "test_data",
            "description": "Test dataset",
            "created_at": "2025-12-05T10:00:00+00:00",
            "created_by": "external",
            "gcs_location": "gs://bucket/data/test.csv",
            "size_bytes": None,
            "status": "available",
            "metadata": None,
        }

        registry = SourceRegistry(db=mock_db)
        source = registry.get_source("test_data")

        assert isinstance(source, SourceInfo)
        assert source.name == "test_data"
        assert source.gcs_location == "gs://bucket/data/test.csv"
        assert source.metadata is None
        assert source.metadata_status == "missing"

    def test_get_source_not_found(self):
        """get_source should raise SourceNotFoundError for missing source."""
        mock_db = MagicMock()
        mock_db.get_source.return_value = None

        registry = SourceRegistry(db=mock_db)

        with pytest.raises(SourceNotFoundError, match="Source not found: missing"):
            registry.get_source("missing")


class TestRegisterSource:
    """Test source registration."""

    def test_register_source_success(self):
        """register_source should create source and return SourceInfo."""
        mock_db = MagicMock()
        mock_db.source_exists.return_value = False
        mock_db.get_source.return_value = {
            "id": "new_data",
            "name": "new_data",
            "description": "New dataset for registry tests",
            "created_at": "2025-12-05T10:00:00+00:00",
            "created_by": "external",
            "gcs_location": "gs://bucket/data/new.csv",
            "size_bytes": 2048,
            "status": "available",
            "metadata": {
                "schema_version": 1,
                "description": "New dataset for registry tests",
                "source": {
                    "format": "csv",
                    "size_bytes": 2048,
                    "created_at": "2025-12-24T12:00:00Z",
                    "format_params": {"delimiter": ","},
                },
                "schema": {
                    "kind": "tabular",
                    "row_count": 2,
                    "columns": ["a", "b"],
                    "dtypes": {"a": "int64", "b": "int64"},
                },
            },
        }

        registry = SourceRegistry(db=mock_db)
        source = registry.register_source(
            name="new_data",
            gcs_location="gs://bucket/data/new.csv",
            description="New dataset for registry tests",
            size_bytes=2048,
            metadata={
                "schema_version": 1,
                "description": "New dataset for registry tests",
                "source": {
                    "format": "csv",
                    "size_bytes": 2048,
                    "created_at": "2025-12-24T12:00:00Z",
                    "format_params": {"delimiter": ","},
                },
                "schema": {
                    "kind": "tabular",
                    "row_count": 2,
                    "columns": ["a", "b"],
                    "dtypes": {"a": "int64", "b": "int64"},
                },
            },
        )

        # Verify create_source was called with correct params
        mock_db.create_source.assert_called_once_with(
            source_id="new_data",
            name="new_data",
            gcs_location="gs://bucket/data/new.csv",
            created_by="external",
            description="New dataset for registry tests",
            size_bytes=2048,
            metadata={
                "schema_version": 1,
                "description": "New dataset for registry tests",
                "source": {
                    "format": "csv",
                    "size_bytes": 2048,
                    "created_at": "2025-12-24T12:00:00Z",
                    "format_params": {"delimiter": ","},
                },
                "schema": {
                    "kind": "tabular",
                    "row_count": 2,
                    "columns": ["a", "b"],
                    "dtypes": {"a": "int64", "b": "int64"},
                },
            },
        )

        # Verify returned SourceInfo
        assert isinstance(source, SourceInfo)
        assert source.name == "new_data"
        assert source.metadata_status == "ok"

    def test_register_source_already_exists(self):
        """register_source should raise SourceAlreadyExistsError if source exists."""
        mock_db = MagicMock()
        mock_db.source_exists.return_value = True

        registry = SourceRegistry(db=mock_db)

        with pytest.raises(SourceAlreadyExistsError, match="Source 'duplicate' already exists"):
            registry.register_source(
                name="duplicate",
                gcs_location="gs://bucket/data/dup.csv",
                description="Duplicate dataset for registry tests",
                metadata={
                    "schema_version": 1,
                    "description": "Duplicate dataset for registry tests",
                    "source": {
                        "format": "csv",
                        "size_bytes": 123,
                        "created_at": "2025-12-24T12:00:00Z",
                        "format_params": {"delimiter": ","},
                    },
                    "schema": {
                        "kind": "tabular",
                        "row_count": 1,
                        "columns": ["a"],
                        "dtypes": {"a": "int64"},
                    },
                },
            )

        # Should not call create_source
        mock_db.create_source.assert_not_called()


class TestGetLineage:
    """Test lineage retrieval."""

    def test_get_lineage_external_source(self):
        """get_lineage should return empty lineage for external sources."""
        mock_db = MagicMock()
        mock_db.source_exists.return_value = True
        mock_db.get_lineage.return_value = []

        registry = SourceRegistry(db=mock_db)
        lineage = registry.get_lineage("external_data")

        assert lineage.source_name == "external_data"
        assert lineage.parent_sources == []
        assert lineage.job_id is None

    def test_get_lineage_promoted_artifact(self):
        """get_lineage should return parents and job for promoted artifacts."""
        mock_db = MagicMock()
        mock_db.source_exists.return_value = True
        mock_db.get_lineage.return_value = [
            {
                "source_id": "promoted_v1",
                "parent_source_id": "raw_data",
                "job_id": "job-abc123",
                "created_at": "2025-12-05T10:00:00+00:00",
            },
            {
                "source_id": "promoted_v1",
                "parent_source_id": "config_data",
                "job_id": "job-abc123",
                "created_at": "2025-12-05T10:00:00+00:00",
            },
        ]

        registry = SourceRegistry(db=mock_db)
        lineage = registry.get_lineage("promoted_v1")

        assert lineage.source_name == "promoted_v1"
        assert "raw_data" in lineage.parent_sources
        assert "config_data" in lineage.parent_sources
        assert lineage.job_id == "job-abc123"

    def test_get_lineage_source_not_found(self):
        """get_lineage should raise SourceNotFoundError for missing source."""
        mock_db = MagicMock()
        mock_db.source_exists.return_value = False

        registry = SourceRegistry(db=mock_db)

        with pytest.raises(SourceNotFoundError, match="Source not found: missing"):
            registry.get_lineage("missing")


class TestUpdateStatus:
    """Test status updates."""

    def test_update_status_not_implemented(self):
        """update_status should raise GoldfishError (not implemented)."""
        from goldfish.errors import GoldfishError

        mock_db = MagicMock()
        mock_db.source_exists.return_value = True

        registry = SourceRegistry(db=mock_db)

        with pytest.raises(GoldfishError, match="not yet implemented"):
            registry.update_status("test_data", "pending")

    def test_update_status_source_not_found(self):
        """update_status should raise SourceNotFoundError for missing source."""
        mock_db = MagicMock()
        mock_db.source_exists.return_value = False

        registry = SourceRegistry(db=mock_db)

        with pytest.raises(SourceNotFoundError, match="Source not found: missing"):
            registry.update_status("missing", "pending")
