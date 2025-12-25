"""Helpers for converting source rows to models."""

from goldfish.db.types import SourceRow
from goldfish.models import SourceInfo, SourceStatus
from goldfish.utils import parse_datetime
from goldfish.validation import parse_source_metadata


def source_row_to_info(source: SourceRow) -> SourceInfo:
    """Convert a SourceRow dict to SourceInfo."""
    metadata, metadata_status = parse_source_metadata(source.get("metadata"))
    return SourceInfo(
        name=source["name"],
        description=source.get("description"),
        created_at=parse_datetime(source["created_at"]),
        created_by=source["created_by"],
        gcs_location=source["gcs_location"],
        size_bytes=source.get("size_bytes"),
        status=SourceStatus(source.get("status", "available")),
        metadata=metadata,
        metadata_status=metadata_status,
    )
