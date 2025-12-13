"""Dataset registry for project-level data sources."""

from datetime import datetime
from pathlib import Path

from goldfish.config import GoldfishConfig
from goldfish.db.database import Database
from goldfish.errors import GoldfishError, SourceAlreadyExistsError, SourceNotFoundError
from goldfish.models import SourceInfo, SourceStatus
from goldfish.providers import get_storage_registry
from goldfish.providers.base import StorageProvider


class DatasetRegistry:
    """Manage project-level datasets (immutable data sources)."""

    def __init__(
        self,
        db: Database,
        config: GoldfishConfig,
        storage_provider: StorageProvider | None = None,
    ):
        """Initialize dataset registry.

        Args:
            db: Database instance
            config: Goldfish configuration
            storage_provider: Optional storage provider (auto-created if None)
        """
        self.db = db
        self.config = config

        # Initialize storage provider
        if storage_provider:
            self.storage_provider = storage_provider
        else:
            # Get provider from config
            provider_name = config.jobs.effective_storage_provider
            provider_config = config.get_storage_provider_config()
            registry = get_storage_registry()
            self.storage_provider = registry.get(provider_name, provider_config)

    def register_dataset(
        self,
        name: str,
        source: str,
        description: str,
        format: str,
        metadata: dict | None = None,
        size_bytes: int | None = None,
    ) -> SourceInfo:
        """Register a project-level dataset.

        Args:
            name: Dataset identifier (e.g., "eurusd_raw_v3")
            source: Local path or storage URL (e.g., "local:/path/to/data.csv" or "gs://bucket/path")
            description: Human-readable description
            format: csv, npy, directory, etc.
            metadata: Optional metadata dict
            size_bytes: Optional size in bytes

        Returns:
            SourceInfo for the registered dataset

        Raises:
            SourceAlreadyExistsError: If dataset with this name already exists
            GoldfishError: If upload fails
        """
        # Check if dataset already exists
        if self.db.source_exists(name):
            raise SourceAlreadyExistsError(f"Dataset '{name}' already exists")

        # Parse source location
        if source.startswith("local:"):
            # Upload local file/directory using storage provider
            local_path = Path(source[6:])
            if not local_path.exists():
                raise GoldfishError(f"Local source not found: {local_path}")

            # Upload to storage provider
            storage_location = self.storage_provider.upload(
                local_path=local_path,
                remote_path=name,
                metadata=metadata,
            )

            storage_uri = storage_location.uri
            size_bytes = storage_location.size_bytes or size_bytes

        elif source.startswith("gs://") or source.startswith("s3://") or source.startswith("file://"):
            # Use storage URL directly
            storage_uri = source
        else:
            raise GoldfishError(
                f"Invalid source format: {source}. Must start with 'local:', 'gs://', 's3://', or 'file://'"
            )

        # Register in database
        # Note: Still using gcs_location column name for backward compatibility
        # TODO: Rename to storage_location in database schema
        self.db.create_source(
            source_id=name,
            name=name,
            gcs_location=storage_uri,
            created_by="external",
            description=description,
            size_bytes=size_bytes,
            status="available",
            metadata=metadata,
        )

        return self.get_dataset(name)

    def list_datasets(self, status: str | None = None) -> list[SourceInfo]:
        """List all registered datasets.

        Args:
            status: Optional status filter (available, pending, failed)

        Returns:
            List of SourceInfo objects
        """
        sources = self.db.list_sources(status=status, created_by="external")
        return [
            SourceInfo(
                name=s["name"],
                description=s["description"],
                created_at=datetime.fromisoformat(s["created_at"]),
                created_by=s["created_by"],
                gcs_location=s["gcs_location"],
                size_bytes=s["size_bytes"],
                status=SourceStatus(s["status"]),
            )
            for s in sources
        ]

    def get_dataset(self, name: str) -> SourceInfo:
        """Get dataset details.

        Args:
            name: Dataset name

        Returns:
            SourceInfo object

        Raises:
            SourceNotFoundError: If dataset not found
        """
        source = self.db.get_source(name)
        if not source:
            raise SourceNotFoundError(f"Dataset not found: {name}")

        return SourceInfo(
            name=source["name"],
            description=source["description"],
            created_at=datetime.fromisoformat(source["created_at"]),
            created_by=source["created_by"],
            gcs_location=source["gcs_location"],
            size_bytes=source["size_bytes"],
            status=SourceStatus(source["status"]),
        )

    def dataset_exists(self, name: str) -> bool:
        """Check if dataset exists.

        Args:
            name: Dataset name

        Returns:
            True if dataset exists, False otherwise
        """
        return self.db.source_exists(name)

    def delete_dataset(self, name: str) -> bool:
        """Delete a dataset.

        Args:
            name: Dataset name

        Returns:
            True if deleted, False if not found
        """
        return self.db.delete_source(name)
