"""Goldfish MCP tools - Data Tools

Extracted from server.py for better organization.
"""

import json
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
    JobStatus,
    ListSourcesResponse,
    PromoteArtifactResponse,
    RegisterDatasetResponse,
    RegisterSourceResponse,
    SourceInfo,
    SourceLineage,
    UpdateSourceMetadataResponse,
)
from goldfish.server import (
    _get_config,
    _get_dataset_registry,
    _get_db,
    _get_state_manager,
    _get_state_md,
    mcp,
)
from goldfish.sources.conversion import source_row_to_info
from goldfish.validation import (
    InvalidSourceMetadataError,
    parse_source_metadata,
    validate_artifact_uri,
    validate_job_id,
    validate_output_name,
    validate_source_metadata,
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
        created_by: Filter by creator. Use "external" for datasets (registered
                   via register_dataset), or "job:xxx" for promoted artifacts.
        limit: Maximum number of sources to return (1-200, default 50)
        offset: Number of sources to skip for pagination (default 0)

    Returns:
        ListSourcesResponse with sources and pagination metadata

    Examples:
        list_sources()                           # All sources
        list_sources(created_by="external")      # Only datasets
        list_sources(status="available")         # Only available sources
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

    source_infos = [source_row_to_info(source) for source in sources]

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

    return source_row_to_info(source)


@mcp.tool()
def update_source_metadata(
    source_name: str,
    metadata: dict,
    reason: str,
) -> UpdateSourceMetadataResponse:
    """Update metadata for an existing source.

    Args:
        source_name: Source name to update
        metadata: Required metadata dict (strict schema)
        reason: Why you're updating metadata (min 15 chars)
    """
    logger.info("update_source_metadata() called", extra={"source_name": source_name})

    config = _get_config()
    db = _get_db()
    state_manager = _get_state_manager()

    validate_source_name(source_name)
    validate_reason(reason, config.audit.min_reason_length)

    if metadata is None:
        raise InvalidSourceMetadataError("metadata is required")

    validate_source_metadata(metadata)

    source = db.get_source(source_name)
    if source is None:
        raise SourceNotFoundError(f"Source not found: {source_name}")

    description = metadata.get("description")
    size_bytes = metadata.get("source", {}).get("size_bytes")

    existing_metadata, status = parse_source_metadata(source.get("metadata"))
    if status == "ok" and existing_metadata is not None:
        existing_format = existing_metadata.get("source", {}).get("format")
        new_format = metadata.get("source", {}).get("format")
        if existing_format and new_format != existing_format:
            raise InvalidSourceMetadataError(
                f"metadata.source.format '{new_format}' does not match existing format '{existing_format}'",
                field="source.format",
            )
        existing_size = source.get("size_bytes")
        if existing_size is not None and size_bytes != existing_size:
            raise InvalidSourceMetadataError(
                f"metadata.source.size_bytes {size_bytes} does not match existing size_bytes {existing_size}",
                field="source.size_bytes",
            )

    db.update_source_metadata(
        source_id_or_name=source_name,
        metadata=metadata,
        description=description,
        size_bytes=size_bytes,
    )

    db.log_audit(
        operation="update_source_metadata",
        reason=reason,
        details={"source_name": source_name},
    )

    state_manager.add_action(f"Updated metadata for source '{source_name}'")

    source = db.get_source(source_name)
    if source is None:
        raise SourceNotFoundError(f"Source not found: {source_name}")

    source_info = source_row_to_info(source)

    state_md = _get_state_md()

    return UpdateSourceMetadataResponse(success=True, source=source_info, state_md=state_md)


@mcp.tool()
def register_source(
    name: str,
    gcs_path: str,
    description: str,
    reason: str,
    metadata: dict,
    format: str | None = None,
    size_bytes: int | None = None,
) -> RegisterSourceResponse:
    """Register an external data source.

    Args:
        name: Source name (e.g., "eurusd_real_ticks")
        gcs_path: GCS location (e.g., "gs://bucket/data/eurusd.csv")
        description: What this data contains
        reason: Why you're registering this source (min 15 chars)
        metadata: Required metadata dict (strict schema)
        format: Optional format (must match metadata.source.format)
        size_bytes: Optional size bytes (must match metadata.source.size_bytes)
    """
    logger.info("register_source() called", extra={"source": name, "gcs_path": gcs_path})

    config = _get_config()
    db = _get_db()
    state_manager = _get_state_manager()

    # Validate inputs
    validate_source_name(name)
    validate_reason(reason, config.audit.min_reason_length)

    if metadata is None:
        raise InvalidSourceMetadataError("metadata is required for new sources")

    validate_source_metadata(metadata)

    if description != metadata.get("description"):
        raise InvalidSourceMetadataError(
            f"description '{description}' does not match metadata.description '{metadata.get('description')}'",
            field="description",
        )

    metadata_format = metadata.get("source", {}).get("format")
    metadata_size = metadata.get("source", {}).get("size_bytes")

    if metadata_size is None:
        raise InvalidSourceMetadataError(
            "metadata.source.size_bytes is required for register_source",
            field="source.size_bytes",
        )

    if format is not None and format != metadata_format:
        raise InvalidSourceMetadataError(
            f"format '{format}' does not match metadata.source.format '{metadata_format}'",
            field="format",
        )
    if size_bytes is not None and metadata_size != size_bytes:
        raise InvalidSourceMetadataError(
            f"size_bytes {size_bytes} does not match metadata.source.size_bytes {metadata_size}",
            field="size_bytes",
        )

    size_bytes = metadata_size

    if db.source_exists(name):
        raise SourceAlreadyExistsError(f"Source '{name}' already exists")

    try:
        db.create_source(
            source_id=name,
            name=name,
            gcs_location=gcs_path,
            created_by="external",
            description=description,
            size_bytes=size_bytes,
            metadata=metadata,
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
            size_bytes=size_bytes,
            metadata=metadata,
            metadata_status="ok",
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
def promote_artifact(
    job_id: str,
    output_name: str,
    source_name: str,
    reason: str,
    metadata: dict,
    description: str | None = None,
    format: str | None = None,
    size_bytes: int | None = None,
) -> PromoteArtifactResponse:
    """Promote a job output to a reusable data source.

    This creates a registry entry pointing to the artifact location
    (no data copy - just a reference). Records lineage: the new source
    knows which job produced it and what input sources that job used.

    Args:
        job_id: ID of the completed job
        output_name: Name of the output in job config (e.g., "preprocessed")
        source_name: Name for the new source (e.g., "preprocessed_v1")
        reason: Why you're promoting this artifact (min 15 chars)
        metadata: Required metadata dict (strict schema)
        description: Optional description (must match metadata.description)
        format: Optional format (must match metadata.source.format)
        size_bytes: Optional size bytes (must match metadata.source.size_bytes)
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

    if metadata is None:
        raise InvalidSourceMetadataError("metadata is required for promoted artifacts")

    validate_source_metadata(metadata)

    # Check job exists and completed
    job = db.get_job(job_id)
    if job is None:
        raise JobNotFoundError(f"Job not found: {job_id}")

    if job["status"] != JobStatus.COMPLETED:
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

        metadata_description = metadata.get("description")
        metadata_format = metadata.get("source", {}).get("format")
        metadata_size = metadata.get("source", {}).get("size_bytes")

        if description is not None and description != metadata_description:
            raise InvalidSourceMetadataError(
                f"description '{description}' does not match metadata.description '{metadata_description}'",
                field="description",
            )
        if format is not None and format != metadata_format:
            raise InvalidSourceMetadataError(
                f"format '{format}' does not match metadata.source.format '{metadata_format}'",
                field="format",
            )
        if size_bytes is not None and metadata_size != size_bytes:
            raise InvalidSourceMetadataError(
                f"size_bytes {size_bytes} does not match metadata.source.size_bytes {metadata_size}",
                field="size_bytes",
            )

        description = metadata_description
        size_bytes = metadata_size

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
                    description,
                    datetime.now(UTC).isoformat(),
                    f"job:{job_id}",
                    gcs_location,
                    size_bytes,
                    "available",
                    json.dumps(metadata),
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
            description=description,
            created_at=datetime.now(),
            created_by=f"job:{job_id}",
            gcs_location=gcs_location,
            size_bytes=size_bytes,
            metadata=metadata,
            metadata_status="ok",
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
    metadata: dict,
    size_bytes: int | None = None,
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
        metadata: Required metadata dict (strict schema)
        size_bytes: Optional size bytes (must match metadata.source.size_bytes)

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

    if metadata is None:
        raise InvalidSourceMetadataError("metadata is required for new datasets")

    validate_source_metadata(metadata)

    if description != metadata.get("description"):
        raise InvalidSourceMetadataError(
            f"description '{description}' does not match metadata.description '{metadata.get('description')}'",
            field="description",
        )

    source_format = metadata.get("source", {}).get("format")
    metadata_size = metadata.get("source", {}).get("size_bytes")
    if metadata_size is None:
        raise InvalidSourceMetadataError(
            "metadata.source.size_bytes is required for register_dataset",
            field="source.size_bytes",
        )
    if format != source_format:
        raise InvalidSourceMetadataError(
            f"format '{format}' does not match metadata.source.format '{source_format}'",
            field="format",
        )

    if size_bytes is not None and metadata_size != size_bytes:
        raise InvalidSourceMetadataError(
            f"size_bytes {size_bytes} does not match metadata.source.size_bytes {metadata_size}",
            field="size_bytes",
        )

    try:
        dataset = dataset_registry.register_dataset(
            name=name,
            source=source,
            description=description,
            format=format,
            metadata=metadata,
            size_bytes=size_bytes,
        )

        return RegisterDatasetResponse(
            success=True,
            dataset=dataset,
        )
    except SourceAlreadyExistsError as e:
        raise GoldfishError(str(e)) from e


# Note: list_datasets() and get_dataset() removed.
# Use list_sources(created_by="external") and get_source() instead.
