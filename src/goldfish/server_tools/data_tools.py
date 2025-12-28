"""Goldfish MCP tools - Data Tools

Extracted from server.py for better organization.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Literal

from goldfish.errors import (
    GoldfishError,
    JobNotFoundError,
    SourceAlreadyExistsError,
    SourceNotFoundError,
    validate_reason,
)
from goldfish.models import (
    JobStatus,
    PromoteArtifactResponse,
    RegisterSourceResponse,
    SourceInfo,
    SourceLineage,
    StageRunStatus,
)
from goldfish.server_core import (
    _get_config,
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


def _truncate_for_error(value: str | None, limit: int = 120) -> str | None:
    if value is None:
        return None
    if len(value) <= limit:
        return value
    return value[:limit] + "..."


@mcp.tool()
def manage_sources(
    action: Literal["list", "get", "lineage", "update", "delete"],
    name: str | None = None,
    status: str | None = None,
    created_by: str | None = None,
    metadata: dict | None = None,
    reason: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """Unified tool for managing the data registry.

    Args:
        action: "list", "get", "lineage", "update", "delete"
        name: Source name or identifier
        status: Filter for 'list' action
        created_by: Filter for 'list' action
        metadata: New metadata for 'update' action
        reason: Why performing this action (update/delete)
        limit / offset: Pagination for 'list'
    """
    db = _get_db()
    config = _get_config()

    if action == "list":
        total = db.count_sources(status=status, created_by=created_by)
        rows = db.list_sources(status=status, created_by=created_by, limit=limit, offset=offset)
        return {"sources": [source_row_to_info(r) for r in rows], "total": total}

    if not name:
        raise GoldfishError("name is required for this action")
    validate_source_name(name)

    if action == "get":
        source = db.get_source(name)
        if not source:
            raise SourceNotFoundError(f"Source not found: {name}")
        return source_row_to_info(source).model_dump(mode="json")  # type: ignore[no-any-return]

    if action == "lineage":
        # Get lineage records
        lineage_records = db.get_lineage(name)
        return {
            "source_name": name,
            "parents": [r["parent_source_id"] for r in lineage_records if r["parent_source_id"]],
            "job_id": next((r["job_id"] for r in lineage_records if r["job_id"]), None),
        }

    if action == "update":
        if not metadata or not reason:
            raise GoldfishError("metadata and reason are required for action='update'")
        validate_reason(reason, config.audit.min_reason_length)
        validate_source_metadata(metadata)

        # Check for format mismatch against existing source
        source = db.get_source(name)
        if not source:
            raise SourceNotFoundError(f"Source not found: {name}")

        existing_metadata, status = parse_source_metadata(source.get("metadata"))
        if status == "ok" and existing_metadata is not None:
            existing_format = existing_metadata.get("source", {}).get("format")
            new_format = metadata.get("source", {}).get("format")
            if existing_format and new_format != existing_format:
                raise InvalidSourceMetadataError(
                    f"metadata.source.format '{new_format}' does not match existing format '{existing_format}'",
                    field="source.format",
                )

        db.update_source_metadata(name, metadata, metadata.get("description"))
        db.log_audit(operation="update_source", reason=reason, details={"source": name})
        return {"success": True, "source": name}

    if action == "delete":
        if not reason:
            raise GoldfishError("reason is required for action='delete'")
        validate_reason(reason, config.audit.min_reason_length)
        db.delete_source(name)
        db.log_audit(operation="delete_source", reason=reason, details={"source": name})
        return {"success": True, "deleted": name}

    raise GoldfishError(f"Unknown action: {action}")


# ============== REGISTRATION TOOLS ==============


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
            "tool description must match metadata.description exactly",
            field="description",
            details={
                "provided": _truncate_for_error(description),
                "in_metadata": _truncate_for_error(metadata.get("description")),
            },
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


# ============== ARTIFACT TOOLS ==============


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
    artifact_uri: str | None = None
    if job_id.startswith("stage-"):
        stage_run = db.get_stage_run(job_id)
        if stage_run is None:
            raise JobNotFoundError(f"Stage run not found: {job_id}")

        if stage_run["status"] != StageRunStatus.COMPLETED:
            raise GoldfishError(f"Stage run {job_id} has not completed (status: {stage_run['status']})")
        artifact_uri = stage_run.get("artifact_uri")
    else:
        job = db.get_job(job_id)
        if job is None:
            raise JobNotFoundError(f"Job not found: {job_id}")

        if job["status"] != JobStatus.COMPLETED:
            raise GoldfishError(f"Job {job_id} has not completed (status: {job['status']})")
        artifact_uri = job.get("artifact_uri")

    if db.source_exists(source_name):
        raise SourceAlreadyExistsError(f"Source '{source_name}' already exists")

    try:
        # Get artifact location (reference, no copy)
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
                "tool description must match metadata.description exactly",
                field="description",
                details={
                    "provided": _truncate_for_error(description),
                    "in_metadata": _truncate_for_error(metadata_description),
                },
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


# Note: list_datasets(), get_dataset(), and register_dataset() removed.
# Use manage_sources(), register_source(), and promote_artifact() instead.
