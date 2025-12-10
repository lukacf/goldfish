"""Source registry - manages data sources and lineage.

Data sources in Goldfish are references to external data (typically GCS).
The registry tracks:
- What sources exist and their locations
- Lineage (which sources came from which jobs)
- Status (available, pending, failed)
"""

from datetime import datetime

from goldfish.db.database import Database
from goldfish.db.types import SourceRow
from goldfish.errors import (
    GoldfishError,
    SourceAlreadyExistsError,
    SourceNotFoundError,
)
from goldfish.models import SourceInfo, SourceLineage, SourceStatus


class SourceRegistry:
    """Registry for data sources and their lineage."""

    def __init__(self, db: Database):
        """Initialize source registry.

        Args:
            db: Database instance
        """
        self.db = db

    def list_sources(self, status: str | None = None) -> list[SourceInfo]:
        """List all sources, optionally filtered by status.

        Args:
            status: Optional status filter (available, pending, failed)

        Returns:
            List of SourceInfo objects
        """
        sources = self.db.list_sources(status=status)
        return [self._dict_to_source_info(s) for s in sources]

    def get_source(self, name: str) -> SourceInfo:
        """Get a source by name.

        Args:
            name: Source name

        Returns:
            SourceInfo

        Raises:
            SourceNotFoundError: If source doesn't exist
        """
        source = self.db.get_source(name)
        if source is None:
            raise SourceNotFoundError(f"Source not found: {name}")
        return self._dict_to_source_info(source)

    def source_exists(self, name: str) -> bool:
        """Check if a source exists.

        Args:
            name: Source name

        Returns:
            True if source exists
        """
        return self.db.source_exists(name)

    def register_source(
        self,
        name: str,
        gcs_location: str,
        description: str,
        size_bytes: int | None = None,
        metadata: dict | None = None,
    ) -> SourceInfo:
        """Register a new external data source.

        Args:
            name: Source name (e.g., "eurusd_real_ticks")
            gcs_location: GCS path (e.g., "gs://bucket/data/eurusd.csv")
            description: What this data contains
            size_bytes: Optional size in bytes
            metadata: Optional metadata dict

        Returns:
            SourceInfo for the created source

        Raises:
            SourceAlreadyExistsError: If source already exists
        """
        if self.db.source_exists(name):
            raise SourceAlreadyExistsError(f"Source '{name}' already exists")

        self.db.create_source(
            source_id=name,
            name=name,
            gcs_location=gcs_location,
            created_by="external",
            description=description,
            size_bytes=size_bytes,
            metadata=metadata,
        )

        return self.get_source(name)

    def promote_artifact(
        self,
        job_id: str,
        output_name: str,
        source_name: str,
        artifact_uri: str,
        description: str | None = None,
    ) -> tuple[SourceInfo, SourceLineage]:
        """Promote a job artifact to a reusable source.

        Creates a registry entry pointing to the artifact location
        (no data copy - just a reference). Records lineage tracking.

        Args:
            job_id: ID of the job that produced the artifact
            output_name: Name of the output in job config
            source_name: Name for the new source
            artifact_uri: Base artifact URI from job
            description: Optional description (defaults to generated)

        Returns:
            Tuple of (SourceInfo, SourceLineage)

        Raises:
            SourceAlreadyExistsError: If source already exists
        """
        if self.db.source_exists(source_name):
            raise SourceAlreadyExistsError(f"Source '{source_name}' already exists")

        # Construct GCS location for this output
        gcs_location = f"{artifact_uri.rstrip('/')}/{output_name}/"

        # Generate description if not provided
        if description is None:
            description = f"Promoted from job {job_id} output '{output_name}'"

        # Create source entry
        self.db.create_source(
            source_id=source_name,
            name=source_name,
            gcs_location=gcs_location,
            created_by=f"job:{job_id}",
            description=description,
        )

        # Record lineage from job inputs
        job_inputs = self.db.get_job_inputs(job_id)
        parent_sources = []

        for inp in job_inputs:
            parent_source_id = inp["source_id"]
            self.db.add_lineage(
                source_id=source_name,
                parent_source_id=parent_source_id,
                job_id=job_id,
            )
            parent_sources.append(parent_source_id)

        source_info = self.get_source(source_name)
        lineage = SourceLineage(
            source_name=source_name,
            parent_sources=parent_sources,
            job_id=job_id,
        )

        return source_info, lineage

    def get_lineage(self, source_name: str) -> SourceLineage:
        """Get lineage for a source.

        Args:
            source_name: Source name

        Returns:
            SourceLineage with parent sources and job info
        """
        # Verify source exists
        if not self.db.source_exists(source_name):
            raise SourceNotFoundError(f"Source not found: {source_name}")

        lineage_records = self.db.get_lineage(source_name)

        parent_sources = []
        job_id = None

        for record in lineage_records:
            parent_id = record.get("parent_source_id")
            if parent_id:
                parent_sources.append(parent_id)
            if record.get("job_id") and job_id is None:
                job_id = record["job_id"]

        return SourceLineage(
            source_name=source_name,
            parent_sources=parent_sources,
            job_id=job_id,
        )

    def update_status(self, name: str, status: str) -> SourceInfo:
        """Update source status.

        Args:
            name: Source name
            status: New status (available, pending, failed)

        Returns:
            Updated SourceInfo
        """
        if not self.db.source_exists(name):
            raise SourceNotFoundError(f"Source not found: {name}")

        # TODO: Add update_source method to database
        # For now, we don't support status updates
        raise GoldfishError("Source status updates not yet implemented")

    def _dict_to_source_info(self, source: SourceRow) -> SourceInfo:
        """Convert database source dict to SourceInfo model."""
        return SourceInfo(
            name=source["name"],
            description=source.get("description"),
            created_at=datetime.fromisoformat(source["created_at"]),
            created_by=source["created_by"],
            gcs_location=source["gcs_location"],
            size_bytes=source.get("size_bytes"),
            status=SourceStatus(source.get("status", "available")),
        )
