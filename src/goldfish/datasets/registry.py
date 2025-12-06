"""Dataset registry for project-level data sources."""

import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from goldfish.config import GoldfishConfig
from goldfish.db.database import Database
from goldfish.errors import GoldfishError, SourceAlreadyExistsError, SourceNotFoundError
from goldfish.models import SourceInfo, SourceStatus


class DatasetRegistry:
    """Manage project-level datasets (immutable data sources)."""

    def __init__(self, db: Database, config: GoldfishConfig):
        """Initialize dataset registry.

        Args:
            db: Database instance
            config: Goldfish configuration
        """
        self.db = db
        self.config = config

    def register_dataset(
        self,
        name: str,
        source: str,
        description: str,
        format: str,
        metadata: Optional[dict] = None,
        size_bytes: Optional[int] = None,
    ) -> SourceInfo:
        """Register a project-level dataset.

        Args:
            name: Dataset identifier (e.g., "eurusd_raw_v3")
            source: Local path or GCS URL (e.g., "local:/path/to/data.csv" or "gs://bucket/path")
            description: Human-readable description
            format: csv, npy, directory, etc.
            metadata: Optional metadata dict
            size_bytes: Optional size in bytes

        Returns:
            SourceInfo for the registered dataset

        Raises:
            SourceAlreadyExistsError: If dataset with this name already exists
            GoldfishError: If GCS not configured (when source is local)
        """
        # Check if dataset already exists
        if self.db.source_exists(name):
            raise SourceAlreadyExistsError(f"Dataset '{name}' already exists")

        # Parse source location
        if source.startswith("local:"):
            # Upload local file/directory to GCS
            local_path = Path(source[6:])
            if not local_path.exists():
                raise GoldfishError(f"Local source not found: {local_path}")

            gcs_location = self._upload_to_gcs(name, local_path)

            # Get size if not provided
            if size_bytes is None and local_path.is_file():
                size_bytes = local_path.stat().st_size

        elif source.startswith("gs://"):
            # Use GCS path directly
            gcs_location = source
        else:
            raise GoldfishError(
                f"Invalid source format: {source}. "
                f"Must start with 'local:' or 'gs://'"
            )

        # Register in database
        self.db.create_source(
            source_id=name,
            name=name,
            gcs_location=gcs_location,
            created_by="external",
            description=description,
            size_bytes=size_bytes,
            status="available",
            metadata=metadata,
        )

        return self.get_dataset(name)

    def _upload_to_gcs(self, name: str, local_path: Path) -> str:
        """Upload local file/directory to GCS.

        Args:
            name: Dataset name (used as GCS path)
            local_path: Local file or directory path

        Returns:
            GCS path (gs://bucket/prefix/name)

        Raises:
            GoldfishError: If GCS not configured or upload fails
        """
        if not self.config.gcs:
            raise GoldfishError(
                "GCS not configured. Cannot upload local datasets. "
                "Add GCS configuration to goldfish.yaml"
            )

        bucket = self.config.gcs.bucket
        prefix = (self.config.gcs.datasets_prefix or "datasets").rstrip("/")
        gcs_path = f"gs://{bucket}/{prefix}/{name}"

        # Build gsutil command
        if local_path.is_dir():
            # Upload directory recursively
            cmd = ["gsutil", "-m", "cp", "-r", str(local_path), gcs_path]
        else:
            # Upload single file
            cmd = ["gsutil", "cp", str(local_path), gcs_path]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                check=False,
            )

            if result.returncode != 0:
                error_msg = result.stderr.decode() if result.stderr else "Unknown error"
                raise GoldfishError(
                    f"Failed to upload dataset to GCS: {error_msg}"
                )

            return gcs_path

        except FileNotFoundError:
            raise GoldfishError(
                "gsutil command not found. Install Google Cloud SDK: "
                "https://cloud.google.com/sdk/docs/install"
            )

    def list_datasets(self, status: Optional[str] = None) -> list[SourceInfo]:
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
