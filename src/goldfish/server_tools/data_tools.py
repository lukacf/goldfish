"""Goldfish MCP tools - Data Tools

Extracted from server.py for better organization.
"""

import logging
from datetime import UTC, datetime

from goldfish.errors import (
    GoldfishError,
    JobNotFoundError,
    SourceAlreadyExistsError,
    SourceNotFoundError,
    validate_reason,
)
from goldfish.models import (
    DeleteSourceResponse,
    ListSourcesResponse,
    PromoteArtifactResponse,
    RegisterDatasetResponse,
    RegisterSourceResponse,
    SourceInfo,
    SourceLineage,
    SourceStatus,
)
from goldfish.server import (
    _get_config,
    _get_dataset_registry,
    _get_db,
    _get_state_manager,
    _get_state_md,
    mcp,
)
from goldfish.utils import parse_datetime
from goldfish.validation import (
    validate_artifact_uri,
    validate_job_id,
    validate_output_name,
    validate_source_name,
)

logger = logging.getLogger("goldfish.server")


@mcp.tool()
def list_sources(
    status: str | None = None,
    created_by: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> ListSourcesResponse:
    """List available data sources with pagination and filtering.

    Args:
        status: Filter by status (available, processing, error)
        created_by: Filter by creator (external, internal, etc.)
        limit: Maximum number of sources to return (1-200, default 50)
        offset: Number of sources to skip for pagination (default 0)

    Returns:
        ListSourcesResponse with sources and pagination metadata
    """
    db = _get_db()

    # Validate limit and offset
    if limit < 1 or limit > 200:
        raise GoldfishError("limit must be between 1 and 200")
    if offset < 0:
        raise GoldfishError("offset must be >= 0")

    # Get total count matching filters
    total_count = db.count_sources(status=status, created_by=created_by)

    # Get page of sources
    sources = db.list_sources(
        status=status,
        created_by=created_by,
        limit=limit,
        offset=offset,
    )

    source_infos = [
        SourceInfo(
            name=s["name"],
            description=s.get("description"),
            created_at=parse_datetime(s["created_at"]),
            created_by=s["created_by"],
            gcs_location=s["gcs_location"],
            size_bytes=s.get("size_bytes"),
            status=SourceStatus(s["status"]),
        )
        for s in sources
    ]

    # Calculate has_more
    has_more = (offset + len(source_infos)) < total_count

    # Track which filters were applied
    filters_applied = {}
    if status:
        filters_applied["status"] = status
    if created_by:
        filters_applied["created_by"] = created_by

    return ListSourcesResponse(
        sources=source_infos,
        total_count=total_count,
        offset=offset,
        limit=limit,
        has_more=has_more,
        filters_applied=filters_applied,
    )


@mcp.tool()
def get_source(name: str) -> SourceInfo:
    """Get detailed information about a specific data source.

    Args:
        name: Name of the source to look up
    """
    db = _get_db()
    validate_source_name(name)

    source = db.get_source(name)
    if source is None:
        raise SourceNotFoundError(f"Source not found: {name}")

    return SourceInfo(
        name=source["name"],
        description=source.get("description"),
        created_at=parse_datetime(source["created_at"]),
        created_by=source["created_by"],
        gcs_location=source["gcs_location"],
        size_bytes=source.get("size_bytes"),
        status=SourceStatus(source["status"]),
    )


@mcp.tool()
def register_source(name: str, gcs_path: str, description: str, reason: str) -> RegisterSourceResponse:
    """Register an external data source.

    Args:
        name: Source name (e.g., "eurusd_real_ticks")
        gcs_path: GCS location (e.g., "gs://bucket/data/eurusd.csv")
        description: What this data contains
        reason: Why you're registering this source (min 15 chars)
    """
    logger.info("register_source() called", extra={"source": name, "gcs_path": gcs_path})

    config = _get_config()
    db = _get_db()
    state_manager = _get_state_manager()

    # Validate inputs
    validate_source_name(name)
    validate_reason(reason, config.audit.min_reason_length)

    if db.source_exists(name):
        raise SourceAlreadyExistsError(f"Source '{name}' already exists")

    try:
        db.create_source(
            source_id=name,
            name=name,
            gcs_location=gcs_path,
            created_by="external",
            description=description,
        )

        db.log_audit(
            operation="register_source",
            reason=reason,
            details={"source": name, "gcs_path": gcs_path},
        )

        state_manager.add_action(f"Registered source '{name}'")

        logger.info("register_source() succeeded", extra={"source": name})

        source = SourceInfo(
            name=name,
            description=description,
            created_at=datetime.now(),
            created_by="external",
            gcs_location=gcs_path,
        )

        state_md = _get_state_md()

        return RegisterSourceResponse(success=True, source=source, state_md=state_md)
    except Exception as e:
        logger.error("register_source() failed", extra={"source": name, "error": str(e)})
        raise


@mcp.tool()
def delete_source(source_name: str, reason: str) -> DeleteSourceResponse:
    """Delete a data source from the registry.

    WARNING: This is irreversible. Jobs that used this source
    will have broken lineage references.

    Args:
        source_name: Name of the source to delete
        reason: Why you're deleting this source (min 15 chars)
    """
    logger.info("delete_source() called", extra={"source_name": source_name})

    config = _get_config()
    db = _get_db()
    state_manager = _get_state_manager()

    validate_source_name(source_name)
    validate_reason(reason, config.audit.min_reason_length)

    # Check source exists
    if not db.source_exists(source_name):
        raise SourceNotFoundError(f"Source not found: {source_name}")

    try:
        # Delete source (and lineage)
        db.delete_source(source_name)

        # Log to audit
        db.log_audit(
            operation="delete_source",
            reason=reason,
            details={"source_name": source_name},
        )

        state_manager.add_action(f"Deleted source '{source_name}'")

        logger.info("delete_source() succeeded", extra={"source_name": source_name})

        return DeleteSourceResponse(
            success=True,
            source_name=source_name,
        )
    except Exception as e:
        logger.error("delete_source() failed", extra={"source_name": source_name, "error": str(e)})
        raise


@mcp.tool()
def get_source_lineage(source_name: str) -> SourceLineage:
    """Get lineage information for a data source.

    Shows the parent sources and creating job for a source.
    Useful for understanding data provenance.

    Args:
        source_name: Name of the source to query
    """
    db = _get_db()

    # Validate source name
    validate_source_name(source_name)

    # Check source exists
    if not db.source_exists(source_name):
        raise SourceNotFoundError(f"Source not found: {source_name}")

    # Get lineage records
    lineage_records = db.get_lineage(source_name)

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


# ============== DELETE TOOLS ==============


@mcp.tool()
def promote_artifact(job_id: str, output_name: str, source_name: str, reason: str) -> PromoteArtifactResponse:
    """Promote a job output to a reusable data source.

    This creates a registry entry pointing to the artifact location
    (no data copy - just a reference). Records lineage: the new source
    knows which job produced it and what input sources that job used.

    Args:
        job_id: ID of the completed job
        output_name: Name of the output in job config (e.g., "preprocessed")
        source_name: Name for the new source (e.g., "preprocessed_v1")
        reason: Why you're promoting this artifact (min 15 chars)
    """
    logger.info(
        "promote_artifact() called",
        extra={
            "job_id": job_id,
            "output_name": output_name,
            "source_name": source_name,
        },
    )

    config = _get_config()
    db = _get_db()
    state_manager = _get_state_manager()

    # Validate inputs
    validate_job_id(job_id)
    validate_source_name(source_name)
    validate_output_name(output_name)
    validate_reason(reason, config.audit.min_reason_length)

    # Check job exists and completed
    job = db.get_job(job_id)
    if job is None:
        raise JobNotFoundError(f"Job not found: {job_id}")

    if job["status"] != "completed":
        raise GoldfishError(f"Job {job_id} has not completed (status: {job['status']})")

    if db.source_exists(source_name):
        raise SourceAlreadyExistsError(f"Source '{source_name}' already exists")

    try:
        # Get artifact location (reference, no copy)
        artifact_uri = job.get("artifact_uri")
        if not artifact_uri:
            raise GoldfishError(f"Job {job_id} has no artifact URI")

        # Security: validate artifact URI (must be GCS, no path traversal)
        validate_artifact_uri(artifact_uri)

        # The artifact path for a specific output
        gcs_location = f"{artifact_uri.rstrip('/')}/{output_name}/"

        # Atomically create source and lineage (transaction prevents partial writes)
        with db.transaction() as conn:
            # Create source entry
            conn.execute(
                """
                INSERT INTO sources (id, name, description, created_at, created_by,
                                     gcs_location, size_bytes, status, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_name,
                    source_name,
                    f"Promoted from job {job_id} output '{output_name}'",
                    datetime.now(UTC).isoformat(),
                    f"job:{job_id}",
                    gcs_location,
                    None,  # size_bytes
                    "available",
                    None,  # metadata
                ),
            )

            # Record lineage from job inputs
            job_inputs = db.get_job_inputs(job_id)
            timestamp = datetime.now(UTC).isoformat()
            for inp in job_inputs:
                conn.execute(
                    """
                    INSERT INTO source_lineage (source_id, parent_source_id, job_id, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (source_name, inp["source_id"], job_id, timestamp),
                )

        db.log_audit(
            operation="promote_artifact",
            reason=reason,
            details={
                "job_id": job_id,
                "output_name": output_name,
                "source_name": source_name,
                "gcs_location": gcs_location,
            },
        )

        state_manager.add_action(f"Promoted '{output_name}' from job {job_id} → source '{source_name}'")

        logger.info(
            "promote_artifact() succeeded",
            extra={
                "job_id": job_id,
                "source_name": source_name,
            },
        )

        source = SourceInfo(
            name=source_name,
            description=f"Promoted from job {job_id} output '{output_name}'",
            created_at=datetime.now(),
            created_by=f"job:{job_id}",
            gcs_location=gcs_location,
        )

        lineage = SourceLineage(
            source_name=source_name,
            parent_sources=[inp["source_id"] for inp in job_inputs],
            job_id=job_id,
        )

        state_md = _get_state_md()

        return PromoteArtifactResponse(success=True, source=source, lineage=lineage, state_md=state_md)
    except Exception as e:
        logger.error(
            "promote_artifact() failed",
            extra={
                "job_id": job_id,
                "source_name": source_name,
                "error": str(e),
            },
        )
        raise


@mcp.tool()
def register_dataset(
    name: str,
    source: str,
    description: str,
    format: str,
    metadata: dict | None = None,
) -> RegisterDatasetResponse:
    """Register a project-level dataset.

    Datasets are immutable data sources shared across all workspaces.
    For local files, Goldfish uploads them to GCS automatically.

    Args:
        name: Dataset identifier (e.g., "eurusd_raw_v3")
        source: Source location:
            - "local:/path/to/file.csv" - Local file (will be uploaded to GCS)
            - "gs://bucket/path" - GCS path (used directly)
        description: Human-readable description
        format: File format (csv, npy, directory, etc.)
        metadata: Optional metadata dict (e.g., {"rows": 1000, "columns": 5})

    Returns:
        Dataset info with GCS location

    Example:
        register_dataset(
            name="eurusd_raw_v3",
            source="local:/data/eurusd.csv",
            description="EUR/USD tick data, version 3",
            format="csv"
        )
    """
    from goldfish.validation import validate_source_name

    dataset_registry = _get_dataset_registry()
    validate_source_name(name)

    try:
        dataset = dataset_registry.register_dataset(
            name=name,
            source=source,
            description=description,
            format=format,
            metadata=metadata,
        )

        return RegisterDatasetResponse(
            success=True,
            dataset=dataset,
        )
    except SourceAlreadyExistsError as e:
        raise GoldfishError(str(e)) from e


@mcp.tool()
def list_datasets(status: str | None = None) -> dict:
    """List all registered datasets.

    Args:
        status: Optional status filter (available, pending, failed)

    Returns:
        List of datasets with their info
    """
    dataset_registry = _get_dataset_registry()

    datasets = dataset_registry.list_datasets(status=status)

    return {
        "datasets": [
            {
                "name": d.name,
                "gcs_location": d.gcs_location,
                "description": d.description,
                "created_at": d.created_at.isoformat(),
                "size_bytes": d.size_bytes,
                "status": d.status.value,
            }
            for d in datasets
        ],
        "count": len(datasets),
    }


@mcp.tool()
def get_dataset(name: str) -> dict:
    """Get dataset details.

    Args:
        name: Dataset name

    Returns:
        Dataset info

    Raises:
        SourceNotFoundError: If dataset not found
    """
    from goldfish.validation import validate_source_name

    dataset_registry = _get_dataset_registry()
    validate_source_name(name)

    try:
        dataset = dataset_registry.get_dataset(name)

        return {
            "name": dataset.name,
            "gcs_location": dataset.gcs_location,
            "description": dataset.description,
            "created_at": dataset.created_at.isoformat(),
            "created_by": dataset.created_by,
            "size_bytes": dataset.size_bytes,
            "status": dataset.status.value,
        }
    except SourceNotFoundError as e:
        raise GoldfishError(str(e)) from e


# ============== CONTEXT TOOLS ==============
