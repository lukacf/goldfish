"""Input validation for Goldfish - P0 Security.

All user input that flows into subprocess calls MUST be validated here.
This prevents command injection, path traversal, and other attacks.

Rules:
- Workspace/source names: alphanumeric + hyphens + underscores only
- Script paths: relative, no traversal, allowed extensions only
- Slot names: must be in whitelist

NEVER bypass these validations. If a new input type is needed,
add validation here first.
"""

import json
import os
import re
import unicodedata
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from urllib.parse import unquote

from goldfish.errors import GoldfishError


class ValidationError(GoldfishError):
    """Base class for validation errors."""

    def __init__(self, message: str, value: str, field: str, details: dict | None = None):
        self.value = value
        self.field = field
        error_details = {"field": field, "value": value}
        if details:
            error_details.update(details)
        super().__init__(message, error_details)


class InvalidWorkspaceNameError(ValidationError):
    """Workspace name is invalid."""

    def __init__(self, name: str, reason: str):
        super().__init__(
            f"Invalid workspace name '{name}': {reason}",
            value=name,
            field="workspace_name",
        )


class InvalidSourceNameError(ValidationError):
    """Source name is invalid."""

    def __init__(self, name: str, reason: str):
        super().__init__(
            f"Invalid source name '{name}': {reason}",
            value=name,
            field="source_name",
        )


class InvalidScriptPathError(ValidationError):
    """Script path is invalid."""

    def __init__(self, path: str, reason: str):
        super().__init__(
            f"Invalid script path '{path}': {reason}",
            value=path,
            field="script_path",
        )


class InvalidSlotNameError(ValidationError):
    """Slot name is invalid."""

    def __init__(self, slot: str, reason: str):
        super().__init__(
            f"Invalid slot '{slot}': {reason}",
            value=slot,
            field="slot_name",
        )


class InvalidSnapshotIdError(ValidationError):
    """Snapshot ID is invalid."""

    def __init__(self, snapshot_id: str, reason: str):
        super().__init__(
            f"Invalid snapshot ID '{snapshot_id}': {reason}",
            value=snapshot_id,
            field="snapshot_id",
        )


class InvalidVersionError(ValidationError):
    """Version identifier is invalid."""

    def __init__(self, version: str, reason: str):
        super().__init__(
            f"Invalid version '{version}': {reason}",
            value=version,
            field="version",
        )


class InvalidOutputNameError(ValidationError):
    """Output name is invalid."""

    def __init__(self, name: str, reason: str):
        super().__init__(
            f"Invalid output name '{name}': {reason}",
            value=name,
            field="output_name",
        )


class InvalidSourceMetadataError(ValidationError):
    """Source metadata is invalid."""

    def __init__(self, reason: str, field: str = "metadata", details: dict | None = None):
        super().__init__(
            f"Invalid source metadata: {reason}",
            value=str(reason),
            field=field,
            details=details,
        )


class InvalidRefNameError(ValidationError):
    """Git ref name is invalid."""

    def __init__(self, ref: str, reason: str):
        super().__init__(
            f"Invalid reference '{ref}': {reason}",
            value=ref,
            field="from_ref",
        )


class InvalidJobIdError(ValidationError):
    """Job ID is invalid."""

    def __init__(self, job_id: str, reason: str):
        super().__init__(
            f"Invalid job ID '{job_id}': {reason}",
            value=job_id,
            field="job_id",
        )


class InvalidStageRunIdError(ValidationError):
    """Stage run ID is invalid."""

    def __init__(self, run_id: str, reason: str):
        super().__init__(
            f"Invalid stage run ID '{run_id}': {reason}",
            value=run_id,
            field="run_id",
        )


class InvalidPipelineRunIdError(ValidationError):
    """Pipeline run ID is invalid."""

    def __init__(self, pipeline_id: str, reason: str):
        super().__init__(
            f"Invalid pipeline run ID '{pipeline_id}': {reason}",
            value=pipeline_id,
            field="pipeline_id",
        )


class InvalidLogPathError(ValidationError):
    """Log path is invalid."""

    def __init__(self, path: str, reason: str):
        super().__init__(
            f"Invalid log path '{path}': {reason}",
            value=path,
            field="log_uri",
        )


class InvalidArtifactUriError(ValidationError):
    """Artifact URI is invalid."""

    def __init__(self, uri: str, reason: str):
        super().__init__(
            f"Invalid artifact URI '{uri}': {reason}",
            value=uri,
            field="artifact_uri",
        )


class InvalidMetricNameError(ValidationError):
    """Metric name is invalid."""

    def __init__(self, name: str, reason: str):
        super().__init__(
            f"Invalid metric name '{name}': {reason}",
            value=name,
            field="metric_name",
        )


class InvalidMetricValueError(ValidationError):
    """Metric value is invalid."""

    def __init__(self, value: str, reason: str):
        super().__init__(
            f"Invalid metric value '{value}': {reason}",
            value=value,
            field="metric_value",
        )


class InvalidMetricStepError(ValidationError):
    """Metric step is invalid."""

    def __init__(self, value: str, reason: str):
        super().__init__(
            f"Invalid metric step '{value}': {reason}",
            value=value,
            field="metric_step",
        )


class InvalidMetricTimestampError(ValidationError):
    """Metric timestamp is invalid."""

    def __init__(self, value: str, reason: str):
        super().__init__(
            f"Invalid metric timestamp '{value}': {reason}",
            value=value,
            field="metric_timestamp",
        )


class InvalidArtifactPathError(ValidationError):
    """Artifact path is invalid."""

    def __init__(self, path: str, reason: str):
        super().__init__(
            f"Invalid artifact path '{path}': {reason}",
            value=path,
            field="artifact_path",
        )


class InvalidBatchSizeError(ValidationError):
    """Batch size is invalid."""

    def __init__(self, size: int, reason: str):
        super().__init__(
            f"Invalid batch size '{size}': {reason}",
            value=str(size),
            field="batch_size",
        )


class InvalidContainerIdError(ValidationError):
    """Container ID is invalid."""

    def __init__(self, container_id: str, reason: str):
        super().__init__(
            f"Invalid container ID '{container_id}': {reason}",
            value=container_id,
            field="container_id",
        )


class InvalidInstanceNameError(ValidationError):
    """GCE instance name is invalid."""

    def __init__(self, instance_name: str, reason: str):
        super().__init__(
            f"Invalid instance name '{instance_name}': {reason}",
            value=instance_name,
            field="instance_name",
        )


class InvalidZoneError(ValidationError):
    """GCE zone is invalid."""

    def __init__(self, zone: str, reason: str):
        super().__init__(
            f"Invalid zone '{zone}': {reason}",
            value=zone,
            field="zone",
        )


class InvalidProjectIdError(ValidationError):
    """GCP project ID is invalid."""

    def __init__(self, project_id: str, reason: str):
        super().__init__(
            f"Invalid project ID '{project_id}': {reason}",
            value=project_id,
            field="project_id",
        )


class InvalidDockerImageError(ValidationError):
    """Docker image name is invalid."""

    def __init__(self, image: str, reason: str):
        super().__init__(
            f"Invalid Docker image '{image}': {reason}",
            value=image,
            field="image",
        )


class InvalidEnvKeyError(ValidationError):
    """Environment variable key is invalid."""

    def __init__(self, key: str, reason: str):
        super().__init__(
            f"Invalid env key '{key}': {reason}",
            value=key,
            field="env_key",
        )


class InvalidEnvValueError(ValidationError):
    """Environment variable value is invalid."""

    def __init__(self, key: str, value: str, reason: str):
        super().__init__(
            f"Invalid env value for '{key}': {reason}",
            value=value,
            field="env_value",
        )


class InvalidSignalNameError(ValidationError):
    """Signal/input name is invalid."""

    def __init__(self, name: str, reason: str):
        super().__init__(
            f"Invalid signal name '{name}': {reason}",
            value=name,
            field="signal_name",
        )


# Regex patterns
# Workspace/source names: start with alphanumeric, contain alphanumeric/hyphen/underscore,
# end with alphanumeric. Length 1-64.
_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9]([a-zA-Z0-9_-]{0,62}[a-zA-Z0-9])?$")

# Script paths: relative path with allowed extensions
_SCRIPT_PATH_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_/.-]*\.(py|sh)$")

# Snapshot ID: snap-{7-8 hex chars}-{YYYYMMDD}-{HHMMSS}
_SNAPSHOT_ID_PATTERN = re.compile(r"^snap-[a-f0-9]{7,8}-\d{8}-\d{6}$")

# Version: v{digits} (e.g., v1, v2, v123)
_VERSION_PATTERN = re.compile(r"^v[0-9]+$")

# Output name: alphanumeric, hyphens, underscores only. No slashes. Max 64 chars.
_OUTPUT_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$")

# Job ID: job-{8 hex chars} (generated by launcher)
_JOB_ID_PATTERN = re.compile(r"^job-[a-f0-9]{8}$")

# Stage run ID: stage-{hex chars} (generated by stage executor)
_STAGE_RUN_ID_PATTERN = re.compile(r"^stage-[a-f0-9]+$")

# Pipeline run ID: prun-{hex chars} (generated by pipeline executor)
_PIPELINE_RUN_ID_PATTERN = re.compile(r"^prun-[a-f0-9]+$")

# Metric name: start with letter, allow alphanumeric + _-./: for hierarchical names
# Max 256 chars. Examples: "loss", "train/loss", "epoch:1/accuracy"
_METRIC_NAME_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_.\-/:]{0,255}$")

# Characters that are NEVER allowed (shell metacharacters)
_DANGEROUS_CHARS = set(";|&$`\"'\\<>*?[]{}~!\n\t\r")

# Path traversal patterns
_PATH_TRAVERSAL_PATTERNS = ["..", "//"]

# Source metadata validation limits and enums
_MAX_SOURCE_METADATA_BYTES = 1_000_000  # 1 MB
_MAX_SOURCE_SIZE_BYTES = 1_000_000_000_000_000  # 1 PB
_MAX_FEATURE_NAME_VALUES = 1_000_000
_MIN_SOURCE_DESCRIPTION_LENGTH = 20
_MAX_SOURCE_DESCRIPTION_LENGTH = 4000
_MAX_METADATA_DEPTH = 20
_MAX_METADATA_NODES = 100_000
_MAX_SHAPE_DIMS = 32
_MAX_ARRAY_COUNT = 1000
_MAX_COLUMNS = 100_000
_MAX_COLUMN_NAME_LENGTH = 256
_MAX_FEATURE_NAME_LENGTH = 1024
_MAX_FEATURE_TEMPLATE_LENGTH = 256
_MAX_FEATURE_SAMPLE_VALUES = 100
_MAX_CONTENT_TYPE_LENGTH = 256
_ALLOWED_SOURCE_FORMATS = {"npy", "npz", "csv", "file"}
_ALLOWED_CSV_DELIMITERS = {",", ";", "|", "\t", ":"}
_ALLOWED_TENSOR_ROLES = {
    "features",
    "labels",
    "embeddings",
    "weights",
    "metadata",
    "index",
    "unknown",
}


def _contains_dangerous_chars(value: str) -> str | None:
    """Check if string contains shell metacharacters.

    Returns the first dangerous character found, or None if clean.
    """
    for char in value:
        if char in _DANGEROUS_CHARS:
            return char
    return None


def _contains_path_traversal(value: str) -> bool:
    """Check if string contains path traversal attempts."""
    for pattern in _PATH_TRAVERSAL_PATTERNS:
        if pattern in value:
            return True
    return False


def validate_workspace_name(name: str) -> None:
    """Validate a workspace name.

    Workspace names must:
    - Be 1-64 characters
    - Start and end with alphanumeric characters
    - Contain only alphanumeric, hyphens, underscores
    - Not contain shell metacharacters or path components

    Args:
        name: The workspace name to validate

    Raises:
        InvalidWorkspaceNameError: If validation fails
    """
    if not name:
        raise InvalidWorkspaceNameError(name, "name cannot be empty")

    if len(name) > 64:
        raise InvalidWorkspaceNameError(name, "name cannot exceed 64 characters")

    # Check for dangerous characters first (better error message)
    dangerous = _contains_dangerous_chars(name)
    if dangerous:
        raise InvalidWorkspaceNameError(name, f"contains invalid character: '{dangerous}'")

    # Check for whitespace
    if " " in name:
        raise InvalidWorkspaceNameError(name, "name cannot contain spaces")

    # Check for path traversal
    if _contains_path_traversal(name) or "/" in name:
        raise InvalidWorkspaceNameError(name, "name cannot contain path components")

    # Check for leading/trailing special chars
    if name.startswith(("-", "_", ".")):
        raise InvalidWorkspaceNameError(name, "name must start with a letter or number")

    if name.endswith(("-", "_")):
        raise InvalidWorkspaceNameError(name, "name must end with a letter or number")

    # Final regex check
    if not _NAME_PATTERN.match(name):
        raise InvalidWorkspaceNameError(name, "name must contain only letters, numbers, hyphens, and underscores")


def validate_source_name(name: str) -> None:
    """Validate a data source name.

    Same rules as workspace names.

    Args:
        name: The source name to validate

    Raises:
        InvalidSourceNameError: If validation fails
    """
    try:
        validate_workspace_name(name)
    except InvalidWorkspaceNameError as e:
        raise InvalidSourceNameError(name, e.message.split(": ", 1)[-1]) from e


def validate_source_metadata(metadata: dict[str, Any]) -> None:
    """Validate strict source metadata schema.

    Args:
        metadata: Metadata dict to validate

    Raises:
        InvalidSourceMetadataError: If metadata fails validation
    """
    if not isinstance(metadata, dict):
        raise InvalidSourceMetadataError("metadata must be a JSON object", field="metadata")

    errors: list[InvalidSourceMetadataError] = []

    def record_error(exc: InvalidSourceMetadataError) -> None:
        errors.append(exc)

    try:
        _validate_metadata_size(metadata)
    except InvalidSourceMetadataError as exc:
        record_error(exc)

    try:
        _validate_metadata_structure_limits(metadata)
    except InvalidSourceMetadataError as exc:
        record_error(exc)

    expected_top = {"schema_version", "description", "source", "schema"}
    missing = expected_top - metadata.keys()
    extra = metadata.keys() - expected_top
    if missing:
        record_error(
            InvalidSourceMetadataError(
                f"metadata missing required fields: {', '.join(sorted(missing))}",
                field="metadata",
            )
        )
    if extra:
        record_error(
            InvalidSourceMetadataError(
                f"metadata has unexpected fields: {', '.join(sorted(extra))}",
                field="metadata",
            )
        )

    schema_version = metadata.get("schema_version")
    if isinstance(schema_version, bool) or not isinstance(schema_version, int) or schema_version != 1:
        record_error(InvalidSourceMetadataError("schema_version must be 1", field="schema_version"))

    description = metadata.get("description")
    if not isinstance(description, str):
        record_error(InvalidSourceMetadataError("description must be a string", field="description"))
    else:
        if len(description.strip()) < _MIN_SOURCE_DESCRIPTION_LENGTH:
            record_error(
                InvalidSourceMetadataError(
                    f"description must be at least {_MIN_SOURCE_DESCRIPTION_LENGTH} characters",
                    field="description",
                )
            )
        if len(description) > _MAX_SOURCE_DESCRIPTION_LENGTH:
            record_error(
                InvalidSourceMetadataError(
                    f"description must be at most {_MAX_SOURCE_DESCRIPTION_LENGTH} characters",
                    field="description",
                )
            )

    source = metadata.get("source")
    source_format: str | None = None
    if not isinstance(source, dict):
        record_error(InvalidSourceMetadataError("source must be an object", field="source"))
    else:
        try:
            source_format = _validate_source_section(source)
        except InvalidSourceMetadataError as exc:
            record_error(exc)

    schema = metadata.get("schema")
    if not isinstance(schema, dict):
        record_error(InvalidSourceMetadataError("schema must be an object", field="schema"))
    elif source_format:
        try:
            _validate_schema_section(schema, source_format)
        except InvalidSourceMetadataError as exc:
            record_error(exc)

    if errors:
        summaries = [f"{exc.field}: {exc.message}" for exc in errors]
        raise InvalidSourceMetadataError(
            f"{len(errors)} validation errors: " + "; ".join(summaries),
            field="metadata",
            details={
                "count": len(errors),
                "errors": [{"field": exc.field, "message": exc.message, "value": exc.value} for exc in errors],
            },
        )


MetadataStatus = Literal["ok", "missing", "invalid", "future"]


def parse_source_metadata(raw: str | dict[str, Any] | None) -> tuple[dict[str, Any] | None, MetadataStatus]:
    """Parse metadata from DB and return (metadata, status).

    Status values: "ok", "missing", "invalid", "future".
    """
    if raw is None:
        return None, "missing"

    if isinstance(raw, str):
        if len(raw.encode("utf-8")) > _MAX_SOURCE_METADATA_BYTES:
            return None, "invalid"
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None, "invalid"
    elif isinstance(raw, dict):
        parsed = raw
    else:
        return None, "invalid"

    if not isinstance(parsed, dict):
        return None, "invalid"

    schema_version = parsed.get("schema_version")
    if isinstance(schema_version, bool) or not isinstance(schema_version, int):
        return None, "invalid"
    if schema_version > 1:
        return parsed, "future"

    try:
        validate_source_metadata(parsed)
    except InvalidSourceMetadataError:
        return None, "invalid"

    return parsed, "ok"


def _validate_metadata_size(metadata: dict[str, Any]) -> None:
    """Ensure metadata JSON is within size limits."""
    try:
        payload = json.dumps(metadata, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise InvalidSourceMetadataError(f"metadata must be JSON-serializable: {exc}") from exc

    if len(payload) > _MAX_SOURCE_METADATA_BYTES:
        raise InvalidSourceMetadataError(
            f"metadata exceeds {_MAX_SOURCE_METADATA_BYTES} bytes",
            field="metadata",
        )


def _validate_metadata_structure_limits(metadata: dict[str, Any]) -> None:
    """Limit metadata nesting depth and node count."""
    stack: list[tuple[Any, int]] = [(metadata, 1)]
    nodes = 0

    while stack:
        node, depth = stack.pop()
        nodes += 1
        if nodes > _MAX_METADATA_NODES:
            raise InvalidSourceMetadataError(
                f"metadata exceeds {_MAX_METADATA_NODES} nodes",
                field="metadata",
            )
        if depth > _MAX_METADATA_DEPTH:
            raise InvalidSourceMetadataError(
                f"metadata nesting exceeds {_MAX_METADATA_DEPTH} levels",
                field="metadata",
            )
        if isinstance(node, dict):
            for value in node.values():
                if isinstance(value, dict | list):
                    stack.append((value, depth + 1))
        elif isinstance(node, list):
            for value in node:
                if isinstance(value, dict | list):
                    stack.append((value, depth + 1))


def _validate_exact_keys(
    obj: dict[str, Any], expected: set[str], context: str, optional: set[str] | None = None
) -> None:
    """Ensure object has exactly the expected keys.

    Args:
        obj: The object to check
        expected: Set of ALL allowed keys (required + optional)
        context: Field name for error reporting
        optional: Set of keys from 'expected' that are NOT required
    """
    optional = optional or set()
    required = expected - optional

    missing = required - obj.keys()
    if missing:
        raise InvalidSourceMetadataError(
            f"{context} missing required fields: {', '.join(sorted(missing))}",
            field=context,
        )
    extra = obj.keys() - expected
    if extra:
        raise InvalidSourceMetadataError(
            f"{context} has unexpected fields: {', '.join(sorted(extra))}",
            field=context,
        )


def _validate_source_section(source: dict[str, Any]) -> str:
    """Validate source section and return format."""
    required = {"format", "size_bytes", "created_at"}
    if source.get("format") == "csv":
        required = required | {"format_params"}
    _validate_exact_keys(source, required, "source")

    fmt = source.get("format")
    if not isinstance(fmt, str):
        raise InvalidSourceMetadataError("source.format must be a string", field="source.format")
    if fmt == "directory":
        raise InvalidSourceMetadataError("directory format is not allowed", field="source.format")
    if fmt not in _ALLOWED_SOURCE_FORMATS:
        raise InvalidSourceMetadataError(f"unsupported source format '{fmt}'", field="source.format")

    size_bytes = source.get("size_bytes")
    if size_bytes is not None:
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes <= 0:
            raise InvalidSourceMetadataError("source.size_bytes must be a positive integer", field="source.size_bytes")
        if size_bytes > _MAX_SOURCE_SIZE_BYTES:
            raise InvalidSourceMetadataError(
                f"source.size_bytes exceeds {_MAX_SOURCE_SIZE_BYTES}",
                field="source.size_bytes",
            )

    created_at = source.get("created_at")
    _validate_utc_isoformat(created_at, field="source.created_at")

    if fmt == "csv":
        format_params = source.get("format_params")
        if not isinstance(format_params, dict):
            raise InvalidSourceMetadataError("source.format_params must be an object", field="source.format_params")
        _validate_exact_keys(format_params, {"delimiter"}, "source.format_params")
        delimiter = format_params.get("delimiter")
        if not isinstance(delimiter, str) or len(delimiter) != 1:
            raise InvalidSourceMetadataError("delimiter must be a single character", field="source.format_params")
        if delimiter not in _ALLOWED_CSV_DELIMITERS:
            raise InvalidSourceMetadataError(
                f"delimiter must be one of {sorted(_ALLOWED_CSV_DELIMITERS)}",
                field="source.format_params",
            )

    return fmt


def _validate_schema_section(schema: dict[str, Any], source_format: str) -> None:
    """Validate schema section based on source format."""
    kind = schema.get("kind")
    if not isinstance(kind, str):
        raise InvalidSourceMetadataError("schema.kind must be a string", field="schema.kind")

    if source_format in {"npy", "npz"}:
        if kind != "tensor":
            raise InvalidSourceMetadataError("schema.kind must be 'tensor' for npy/npz", field="schema.kind")
        _validate_tensor_schema(schema, source_format)
    elif source_format == "csv":
        if kind != "tabular":
            raise InvalidSourceMetadataError("schema.kind must be 'tabular' for csv", field="schema.kind")
        _validate_tabular_schema(schema)
    elif source_format == "file":
        if kind != "file":
            raise InvalidSourceMetadataError("schema.kind must be 'file' for file format", field="schema.kind")
        _validate_file_schema(schema)
    else:
        raise InvalidSourceMetadataError(f"unsupported schema format '{source_format}'", field="schema.kind")


def _validate_tensor_schema(schema: dict[str, Any], source_format: str) -> None:
    _validate_exact_keys(schema, {"kind", "arrays", "primary_array"}, "schema")

    arrays = schema.get("arrays")
    if not isinstance(arrays, dict) or not arrays:
        raise InvalidSourceMetadataError("schema.arrays must be a non-empty object", field="schema.arrays")

    if source_format == "npy" and len(arrays) != 1:
        raise InvalidSourceMetadataError("npy metadata must define exactly one array", field="schema.arrays")
    if len(arrays) > _MAX_ARRAY_COUNT:
        raise InvalidSourceMetadataError(
            f"arrays exceeds {_MAX_ARRAY_COUNT} entries",
            field="schema.arrays",
        )

    primary_array = schema.get("primary_array")
    if not isinstance(primary_array, str) or primary_array not in arrays:
        raise InvalidSourceMetadataError("primary_array must reference an array key", field="schema.primary_array")

    for name, array in arrays.items():
        if not isinstance(name, str) or not name:
            raise InvalidSourceMetadataError("array names must be strings", field="schema.arrays")
        if len(name) > _MAX_COLUMN_NAME_LENGTH:
            raise InvalidSourceMetadataError(
                f"array name exceeds {_MAX_COLUMN_NAME_LENGTH} characters",
                field=f"schema.arrays.{name}",
            )
        if not isinstance(array, dict):
            raise InvalidSourceMetadataError("array definition must be an object", field=f"schema.arrays.{name}")

        _validate_exact_keys(array, {"role", "shape", "dtype", "feature_names"}, f"schema.arrays.{name}")

        role = array.get("role")
        if not isinstance(role, str) or role not in _ALLOWED_TENSOR_ROLES:
            raise InvalidSourceMetadataError(
                f"invalid role '{role}'",
                field=f"schema.arrays.{name}.role",
            )

        shape = array.get("shape")
        if not isinstance(shape, list):
            raise InvalidSourceMetadataError(
                "shape must be a list of non-negative integers",
                field=f"schema.arrays.{name}.shape",
            )
        if len(shape) > _MAX_SHAPE_DIMS:
            raise InvalidSourceMetadataError(
                f"shape exceeds {_MAX_SHAPE_DIMS} dimensions",
                field=f"schema.arrays.{name}.shape",
            )
        if any(isinstance(dim, bool) or not isinstance(dim, int) or dim < 0 for dim in shape):
            raise InvalidSourceMetadataError(
                "shape must be a list of non-negative integers",
                field=f"schema.arrays.{name}.shape",
            )

        dtype = array.get("dtype")
        if not isinstance(dtype, str) or not dtype:
            raise InvalidSourceMetadataError(
                "dtype must be a non-empty string",
                field=f"schema.arrays.{name}.dtype",
            )

        feature_names = array.get("feature_names")
        _validate_feature_names(feature_names, shape, name)


def _validate_feature_names(feature_names: Any, shape: list[int], array_name: str) -> None:
    if not isinstance(feature_names, dict):
        raise InvalidSourceMetadataError(
            "feature_names must be an object",
            field=f"schema.arrays.{array_name}.feature_names",
        )

    kind = feature_names.get("kind")
    if kind not in {"list", "pattern", "none", "sequence"}:
        raise InvalidSourceMetadataError(
            "feature_names.kind must be list, pattern, none, or sequence",
            field=f"schema.arrays.{array_name}.feature_names.kind",
        )

    if kind == "list":
        _validate_exact_keys(feature_names, {"kind", "values"}, f"schema.arrays.{array_name}.feature_names")
        values = feature_names.get("values")
        if not isinstance(values, list) or any(not isinstance(v, str) for v in values):
            raise InvalidSourceMetadataError(
                "feature_names.values must be a list of strings",
                field=f"schema.arrays.{array_name}.feature_names.values",
            )
        if len(values) > _MAX_FEATURE_NAME_VALUES:
            raise InvalidSourceMetadataError(
                f"feature_names.values exceeds {_MAX_FEATURE_NAME_VALUES} entries",
                field=f"schema.arrays.{array_name}.feature_names.values",
            )
        if any(len(v) > _MAX_FEATURE_NAME_LENGTH for v in values):
            raise InvalidSourceMetadataError(
                f"feature_names.values entries must be <= {_MAX_FEATURE_NAME_LENGTH} chars",
                field=f"schema.arrays.{array_name}.feature_names.values",
            )
        _validate_feature_count(values_count=len(values), shape=shape, array_name=array_name)
        return

    if kind == "pattern":
        _validate_exact_keys(
            feature_names,
            {"kind", "template", "start", "count", "sample"},
            f"schema.arrays.{array_name}.feature_names",
        )
        template = feature_names.get("template")
        start = feature_names.get("start")
        count = feature_names.get("count")
        sample = feature_names.get("sample")
        if not isinstance(template, str) or "{i}" not in template:
            raise InvalidSourceMetadataError(
                "feature_names.template must include '{i}'",
                field=f"schema.arrays.{array_name}.feature_names.template",
            )
        if len(template) > _MAX_FEATURE_TEMPLATE_LENGTH:
            raise InvalidSourceMetadataError(
                f"feature_names.template must be <= {_MAX_FEATURE_TEMPLATE_LENGTH} chars",
                field=f"schema.arrays.{array_name}.feature_names.template",
            )
        if isinstance(start, bool) or not isinstance(start, int):
            raise InvalidSourceMetadataError(
                "feature_names.start must be an integer",
                field=f"schema.arrays.{array_name}.feature_names.start",
            )
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise InvalidSourceMetadataError(
                "feature_names.count must be a non-negative integer",
                field=f"schema.arrays.{array_name}.feature_names.count",
            )
        if not isinstance(sample, list) or any(not isinstance(v, str) for v in sample):
            raise InvalidSourceMetadataError(
                "feature_names.sample must be a list of strings",
                field=f"schema.arrays.{array_name}.feature_names.sample",
            )
        if len(sample) > _MAX_FEATURE_SAMPLE_VALUES:
            raise InvalidSourceMetadataError(
                f"feature_names.sample must be <= {_MAX_FEATURE_SAMPLE_VALUES} entries",
                field=f"schema.arrays.{array_name}.feature_names.sample",
            )
        if any(len(v) > _MAX_FEATURE_NAME_LENGTH for v in sample):
            raise InvalidSourceMetadataError(
                f"feature_names.sample entries must be <= {_MAX_FEATURE_NAME_LENGTH} chars",
                field=f"schema.arrays.{array_name}.feature_names.sample",
            )
        _validate_feature_count(values_count=count, shape=shape, array_name=array_name)
        return

    if kind == "none":
        _validate_exact_keys(feature_names, {"kind", "reason"}, f"schema.arrays.{array_name}.feature_names")
        reason = feature_names.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise InvalidSourceMetadataError(
                "feature_names.reason must be a non-empty string",
                field=f"schema.arrays.{array_name}.feature_names.reason",
            )
        if len(reason) > _MAX_SOURCE_DESCRIPTION_LENGTH:
            raise InvalidSourceMetadataError(
                f"feature_names.reason must be <= {_MAX_SOURCE_DESCRIPTION_LENGTH} chars",
                field=f"schema.arrays.{array_name}.feature_names.reason",
            )
        if len(shape) != 0:
            raise InvalidSourceMetadataError(
                "feature_names.kind 'none' only allowed for scalar shapes. Use 'sequence' for non-scalar unnamed data.",
                field=f"schema.arrays.{array_name}.feature_names.kind",
            )
        return

    if kind == "sequence":
        # 'sequence' is for unnamed data (e.g. time-series, raw signals)
        # It allows optional metadata about the sequence.
        _validate_exact_keys(
            feature_names,
            {"kind", "reason", "interval", "unit", "start_value"},
            f"schema.arrays.{array_name}.feature_names",
            optional={"interval", "unit", "start_value"},
        )
        reason = feature_names.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise InvalidSourceMetadataError(
                "feature_names.reason must be a non-empty string",
                field=f"schema.arrays.{array_name}.feature_names.reason",
            )

        # Optional time-series metadata
        interval = feature_names.get("interval")
        if interval is not None and (not isinstance(interval, int | float) or interval <= 0):
            raise InvalidSourceMetadataError(
                "feature_names.interval must be a positive number",
                field=f"schema.arrays.{array_name}.feature_names.interval",
            )

        unit = feature_names.get("unit")
        if unit is not None and (not isinstance(unit, str) or not unit.strip()):
            raise InvalidSourceMetadataError(
                "feature_names.unit must be a non-empty string",
                field=f"schema.arrays.{array_name}.feature_names.unit",
            )

        if len(shape) == 0:
            raise InvalidSourceMetadataError(
                "feature_names.kind 'sequence' only allowed for non-scalar shapes",
                field=f"schema.arrays.{array_name}.feature_names.kind",
            )
        return


def _validate_feature_count(values_count: int, shape: list[int], array_name: str) -> None:
    if len(shape) == 0:
        if values_count != 0:
            raise InvalidSourceMetadataError(
                "feature_names must be empty for scalar shapes",
                field=f"schema.arrays.{array_name}.feature_names",
            )
        return

    last_dim = shape[-1]
    if values_count != last_dim:
        raise InvalidSourceMetadataError(
            f"feature_names count ({values_count}) must equal last dimension ({last_dim})",
            field=f"schema.arrays.{array_name}.feature_names",
        )


def _validate_tabular_schema(schema: dict[str, Any]) -> None:
    _validate_exact_keys(schema, {"kind", "row_count", "columns", "dtypes"}, "schema")

    row_count = schema.get("row_count")
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
        raise InvalidSourceMetadataError("row_count must be a non-negative integer", field="schema.row_count")

    columns = schema.get("columns")
    if not isinstance(columns, list) or any(not isinstance(col, str) for col in columns):
        raise InvalidSourceMetadataError("columns must be a list of strings", field="schema.columns")
    if len(columns) > _MAX_COLUMNS:
        raise InvalidSourceMetadataError(
            f"columns exceeds {_MAX_COLUMNS} entries",
            field="schema.columns",
        )
    if any(len(col) > _MAX_COLUMN_NAME_LENGTH for col in columns):
        raise InvalidSourceMetadataError(
            f"column names must be <= {_MAX_COLUMN_NAME_LENGTH} chars",
            field="schema.columns",
        )

    if len(columns) != len(set(columns)):
        raise InvalidSourceMetadataError("columns must be unique", field="schema.columns")

    dtypes = schema.get("dtypes")
    if not isinstance(dtypes, dict) or any(not isinstance(k, str) for k in dtypes.keys()):
        raise InvalidSourceMetadataError("dtypes must be an object with string keys", field="schema.dtypes")

    if set(dtypes.keys()) != set(columns):
        raise InvalidSourceMetadataError("dtypes keys must match columns", field="schema.dtypes")

    if any(not isinstance(v, str) or not v for v in dtypes.values()):
        raise InvalidSourceMetadataError("dtypes values must be non-empty strings", field="schema.dtypes")


def _validate_file_schema(schema: dict[str, Any]) -> None:
    _validate_exact_keys(schema, {"kind", "content_type"}, "schema")
    content_type = schema.get("content_type")
    if not isinstance(content_type, str) or not content_type.strip():
        raise InvalidSourceMetadataError("content_type must be a non-empty string", field="schema.content_type")
    if len(content_type) > _MAX_CONTENT_TYPE_LENGTH:
        raise InvalidSourceMetadataError(
            f"content_type must be <= {_MAX_CONTENT_TYPE_LENGTH} chars",
            field="schema.content_type",
        )


def _validate_utc_isoformat(value: Any, field: str) -> None:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise InvalidSourceMetadataError("created_at must be ISO-8601 UTC with Z suffix", field=field)
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidSourceMetadataError("created_at must be ISO-8601 UTC with Z suffix", field=field) from exc


def validate_script_path(path: str) -> None:
    """Validate a script path.

    Script paths must:
    - Be relative (no leading /)
    - Not contain path traversal (..)
    - End with .py or .sh
    - Not contain shell metacharacters

    Args:
        path: The script path to validate

    Raises:
        InvalidScriptPathError: If validation fails
    """
    if not path:
        raise InvalidScriptPathError(path, "path cannot be empty")

    # Check for dangerous characters
    dangerous = _contains_dangerous_chars(path)
    if dangerous:
        raise InvalidScriptPathError(path, f"contains invalid character: '{dangerous}'")

    # Check for whitespace
    if " " in path:
        raise InvalidScriptPathError(path, "path cannot contain spaces")

    # Check for absolute paths
    if path.startswith("/"):
        raise InvalidScriptPathError(path, "must be a relative path")

    # Check for path traversal
    if _contains_path_traversal(path):
        raise InvalidScriptPathError(path, "cannot contain path traversal")

    # Check extension
    if not (path.endswith(".py") or path.endswith(".sh")):
        raise InvalidScriptPathError(path, "must end with .py or .sh")

    # Final pattern check
    if not _SCRIPT_PATH_PATTERN.match(path):
        raise InvalidScriptPathError(path, "contains invalid characters")


def validate_slot_name(slot: str, valid_slots: list[str]) -> None:
    """Validate a slot name against whitelist.

    Args:
        slot: The slot name to validate
        valid_slots: List of valid slot names

    Raises:
        InvalidSlotNameError: If validation fails
    """
    if not slot:
        raise InvalidSlotNameError(slot, "slot cannot be empty")

    # Check for dangerous characters (defense in depth)
    dangerous = _contains_dangerous_chars(slot)
    if dangerous:
        raise InvalidSlotNameError(slot, f"contains invalid character: '{dangerous}'")

    # Check for path traversal
    if _contains_path_traversal(slot) or "/" in slot:
        raise InvalidSlotNameError(slot, "cannot contain path components")

    # Whitelist check
    if slot not in valid_slots:
        raise InvalidSlotNameError(slot, f"must be one of: {', '.join(valid_slots)}")


def validate_ref_name(ref: str) -> None:
    """Validate a git ref name (for from_ref parameter).

    This is used when creating branches from a reference.

    Args:
        ref: The reference name (e.g., "main", "HEAD")

    Raises:
        InvalidWorkspaceNameError: If validation fails
    """
    # Allow common refs
    allowed_refs = {"main", "master", "HEAD"}
    if ref in allowed_refs:
        return

    # Otherwise validate as workspace name
    validate_workspace_name(ref)


def validate_snapshot_id(snapshot_id: str) -> None:
    """Validate a snapshot ID.

    Snapshot IDs must match format: snap-{7-8 hex chars}-{YYYYMMDD}-{HHMMSS}
    This prevents git command injection via rollback().

    Args:
        snapshot_id: The snapshot ID to validate (e.g., "snap-a1b2c3d-20251205-143000")

    Raises:
        InvalidSnapshotIdError: If validation fails
    """
    if not snapshot_id:
        raise InvalidSnapshotIdError(snapshot_id, "snapshot ID cannot be empty")

    # Check for dangerous characters first (better error message)
    dangerous = _contains_dangerous_chars(snapshot_id)
    if dangerous:
        raise InvalidSnapshotIdError(snapshot_id, f"contains invalid character: '{dangerous}'")

    # Check for path traversal
    if _contains_path_traversal(snapshot_id) or "/" in snapshot_id:
        raise InvalidSnapshotIdError(snapshot_id, "cannot contain path components")

    # Must match exact format
    if not _SNAPSHOT_ID_PATTERN.match(snapshot_id):
        raise InvalidSnapshotIdError(
            snapshot_id, "must match format snap-{hex}-{YYYYMMDD}-{HHMMSS} (e.g., snap-a1b2c3d-20251205-143000)"
        )


def validate_version(version: str) -> None:
    """Validate a version identifier.

    Versions must match format: v{digits} (e.g., v1, v2, v123)
    This prevents command injection via rollback().

    Args:
        version: The version to validate (e.g., "v1", "v12")

    Raises:
        InvalidVersionError: If validation fails
    """
    if not version:
        raise InvalidVersionError(version, "version cannot be empty")

    # Check for dangerous characters first (better error message)
    dangerous = _contains_dangerous_chars(version)
    if dangerous:
        raise InvalidVersionError(version, f"contains invalid character: '{dangerous}'")

    # Check for path traversal
    if _contains_path_traversal(version) or "/" in version:
        raise InvalidVersionError(version, "cannot contain path components")

    # Must match exact format
    if not _VERSION_PATTERN.match(version):
        raise InvalidVersionError(version, "must match format v{number} (e.g., v1, v2)")


def validate_output_name(name: str) -> None:
    """Validate an output name for promote_artifact.

    Output names must:
    - Be 1-64 characters
    - Contain only alphanumeric, hyphens, underscores
    - NOT contain slashes or path components (prevents path traversal)

    Args:
        name: The output name to validate (e.g., "model", "checkpoint_v1")

    Raises:
        InvalidOutputNameError: If validation fails
    """
    if not name:
        raise InvalidOutputNameError(name, "output name cannot be empty")

    if len(name) > 64:
        raise InvalidOutputNameError(name, "output name cannot exceed 64 characters")

    # Check for dangerous characters
    dangerous = _contains_dangerous_chars(name)
    if dangerous:
        raise InvalidOutputNameError(name, f"contains invalid character: '{dangerous}'")

    # Check for path components (slashes, backslashes, traversal)
    if "/" in name or "\\" in name:
        raise InvalidOutputNameError(name, "cannot contain path separators")

    if _contains_path_traversal(name):
        raise InvalidOutputNameError(name, "cannot contain path traversal")

    # Must match pattern
    if not _OUTPUT_NAME_PATTERN.match(name):
        raise InvalidOutputNameError(name, "must contain only letters, numbers, hyphens, and underscores")


def validate_from_ref(ref: str) -> None:
    """Validate a from_ref parameter for create_workspace.

    Accepts:
    - Whitelisted refs: main, master, HEAD
    - Valid workspace names (for branching from another workspace)

    Rejects:
    - Remote refs (refs/remotes/...)
    - Dangerous patterns (injection attempts)

    Args:
        ref: The reference name

    Raises:
        InvalidRefNameError: If validation fails
    """
    if not ref:
        raise InvalidRefNameError(ref, "reference cannot be empty")

    # Check for dangerous characters
    dangerous = _contains_dangerous_chars(ref)
    if dangerous:
        raise InvalidRefNameError(ref, f"contains invalid character: '{dangerous}'")

    # Check for path traversal
    if _contains_path_traversal(ref):
        raise InvalidRefNameError(ref, "cannot contain path traversal")

    # Reject remote refs
    if ref.startswith("refs/remotes/"):
        raise InvalidRefNameError(ref, "remote references not allowed")

    # If it passed the security checks above (no dangerous chars, no path traversal,
    # no remote refs), accept it. It could be a workspace name, a branch like
    # goldfish/baseline, a tag, or a SHA. The workspace manager will verify it
    # actually resolves before branching.


def validate_job_id(job_id: str) -> None:
    """Validate a job ID or stage run ID.

    Accepts:
    - Legacy Job IDs: job-{8 hex chars}
    - Stage Run IDs: stage-{hex chars}

    This prevents database/command injection.

    Args:
        job_id: The ID to validate

    Raises:
        InvalidJobIdError: If validation fails
    """
    if not job_id:
        raise InvalidJobIdError(job_id, "job ID cannot be empty")

    # Check for dangerous characters first (better error message)
    dangerous = _contains_dangerous_chars(job_id)
    if dangerous:
        raise InvalidJobIdError(job_id, f"contains invalid character: '{dangerous}'")

    # Check for path traversal
    if _contains_path_traversal(job_id) or "/" in job_id:
        raise InvalidJobIdError(job_id, "cannot contain path components")

    # Check against allowed patterns
    if not (_JOB_ID_PATTERN.match(job_id) or _STAGE_RUN_ID_PATTERN.match(job_id)):
        raise InvalidJobIdError(job_id, "must match format job-{8 hex chars} or stage-{hex chars}")


def validate_stage_run_id(run_id: str) -> None:
    """Validate a stage run ID.

    Stage run IDs must match format: stage-{hex chars}
    This prevents database/command injection via API endpoints.

    Args:
        run_id: The stage run ID to validate (e.g., "stage-abc123def456")

    Raises:
        InvalidStageRunIdError: If validation fails
    """
    if not run_id:
        raise InvalidStageRunIdError(run_id, "stage run ID cannot be empty")

    # Check for dangerous characters first
    dangerous = _contains_dangerous_chars(run_id)
    if dangerous:
        raise InvalidStageRunIdError(run_id, f"contains invalid character: '{dangerous}'")

    # Check for path traversal
    if _contains_path_traversal(run_id) or "/" in run_id:
        raise InvalidStageRunIdError(run_id, "cannot contain path components")

    # Must match exact format
    if not _STAGE_RUN_ID_PATTERN.match(run_id):
        raise InvalidStageRunIdError(run_id, "must match format stage-{hex} (e.g., stage-abc123)")


def validate_pipeline_run_id(pipeline_id: str) -> None:
    """Validate a pipeline run ID.

    Pipeline run IDs must match format: prun-{hex chars}
    This prevents database/command injection via API endpoints.

    Args:
        pipeline_id: The pipeline run ID to validate (e.g., "prun-abc123def456")

    Raises:
        InvalidPipelineRunIdError: If validation fails
    """
    if not pipeline_id:
        raise InvalidPipelineRunIdError(pipeline_id, "pipeline run ID cannot be empty")

    # Check for dangerous characters first
    dangerous = _contains_dangerous_chars(pipeline_id)
    if dangerous:
        raise InvalidPipelineRunIdError(pipeline_id, f"contains invalid character: '{dangerous}'")

    # Check for path traversal
    if _contains_path_traversal(pipeline_id) or "/" in pipeline_id:
        raise InvalidPipelineRunIdError(pipeline_id, "cannot contain path components")

    # Must match exact format
    if not _PIPELINE_RUN_ID_PATTERN.match(pipeline_id):
        raise InvalidPipelineRunIdError(pipeline_id, "must match format prun-{hex} (e.g., prun-abc123)")


def validate_log_path(log_uri: str, project_root: "Path") -> "Path":
    """Validate a log_uri and return the resolved path.

    Log URIs must:
    - Be file:// URIs or absolute paths
    - Resolve to a path within the project_root
    - Not contain path traversal patterns

    SECURITY: This function uses os.path.realpath() for atomic symlink resolution,
    avoiding TOCTOU (time-of-check to time-of-use) race conditions. The returned
    resolved path MUST be used for all subsequent file operations.

    Args:
        log_uri: The log URI to validate (e.g., "file:///path/to/log" or "/path/to/log")
        project_root: The project root directory (all log paths must be within this)

    Returns:
        Resolved Path object (fully resolved, with all symlinks expanded)

    Raises:
        InvalidLogPathError: If validation fails
    """
    import os
    from pathlib import Path

    if not log_uri:
        raise InvalidLogPathError(log_uri, "log URI cannot be empty")

    # Parse the URI first
    if log_uri.startswith("file://"):
        path_str = log_uri[7:]  # Remove "file://"
    else:
        path_str = log_uri

    # Check for path traversal in the raw path (defense in depth)
    if ".." in path_str:
        raise InvalidLogPathError(log_uri, "cannot contain path traversal")

    # Must be absolute
    if not path_str.startswith("/"):
        raise InvalidLogPathError(log_uri, "must be an absolute path")

    # SECURITY: Use os.path.realpath() for atomic symlink resolution
    # This resolves ALL symlinks in the path atomically, avoiding TOCTOU races
    # that could occur with separate is_symlink() + resolve() calls
    try:
        resolved_str = os.path.realpath(path_str)
        log_path = Path(resolved_str)
    except (OSError, RuntimeError) as e:
        raise InvalidLogPathError(log_uri, f"invalid path: {e}") from e

    # Resolve project root the same way for consistent comparison
    try:
        project_resolved = Path(os.path.realpath(str(project_root)))
    except (OSError, RuntimeError) as e:
        raise InvalidLogPathError(log_uri, f"invalid project root: {e}") from e

    # SECURITY: Check that fully resolved path is within project_root
    # This is the primary security check - symlinks are already resolved
    try:
        log_path.relative_to(project_resolved)
    except ValueError as e:
        raise InvalidLogPathError(log_uri, f"path must be within project directory (resolved to: {log_path})") from e

    return log_path


def validate_artifact_uri(artifact_uri: str) -> None:
    """Validate an artifact URI from job output.

    Artifact URIs must:
    - Use a valid StorageURI (e.g., scheme://bucket/path or file://absolute/path)
    - Not contain path traversal patterns
    - Not reference unexpected buckets via traversal (e.g., bucket/..)

    Args:
        artifact_uri: The artifact URI to validate (e.g., "s3://bucket/path/")

    Raises:
        InvalidArtifactUriError: If validation fails
    """
    if not artifact_uri:
        raise InvalidArtifactUriError(artifact_uri, "artifact URI cannot be empty")

    from goldfish.cloud.contracts import StorageURI

    try:
        StorageURI.parse(artifact_uri)
    except ValueError as e:
        raise InvalidArtifactUriError(artifact_uri, str(e)) from e

    # StorageURI.parse() enforces scheme presence and blocks path traversal.


# ============== CONFIG FIELD SUGGESTIONS ==============


def _levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein distance between two strings.

    Used for suggesting similar field names on typos.
    """
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # Cost is 0 if characters match, 1 otherwise
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def suggest_similar_field(unknown_field: str, valid_fields: list[str], max_distance: int = 3) -> str | None:
    """Suggest a similar field name for a typo.

    Args:
        unknown_field: The unknown/invalid field name
        valid_fields: List of valid field names
        max_distance: Maximum Levenshtein distance to consider a match

    Returns:
        The most similar valid field name, or None if no good match
    """
    if not valid_fields:
        return None

    best_match = None
    best_distance = max_distance + 1

    for valid_field in valid_fields:
        distance = _levenshtein_distance(unknown_field.lower(), valid_field.lower())
        if distance < best_distance:
            best_distance = distance
            best_match = valid_field

    # Only return if within threshold
    if best_distance <= max_distance:
        return best_match
    return None


def format_unknown_field_error(
    unknown_field: str,
    valid_fields: list[str],
    suggested_field: str | None = None,
    context: str = "",
) -> str:
    """Format a helpful error message for unknown field.

    Args:
        unknown_field: The unknown field name
        valid_fields: List of valid field names
        suggested_field: Pre-computed suggestion (or None to auto-suggest)
        context: Additional context (e.g., "in gce section")

    Returns:
        Formatted error message with suggestion and valid fields
    """
    if suggested_field is None:
        suggested_field = suggest_similar_field(unknown_field, valid_fields)

    parts = [f"Unknown field '{unknown_field}'"]

    if context:
        parts[0] += f" {context}"

    if suggested_field:
        parts.append(f"Did you mean '{suggested_field}'?")

    # List some valid fields (not all if there are many)
    if valid_fields:
        if len(valid_fields) <= 8:
            fields_str = ", ".join(sorted(valid_fields))
        else:
            fields_str = ", ".join(sorted(valid_fields)[:8]) + ", ..."
        parts.append(f"Valid fields: {fields_str}")

    return " ".join(parts)


# ============== METRICS VALIDATION ==============


def validate_metric_name(name: str) -> None:
    """Validate a metric name.

    Metric names must:
    - Be 1-256 characters
    - Start with a letter
    - Contain only alphanumeric, underscore, hyphen, dot, colon, slash
    - Not contain shell metacharacters or path traversal

    Args:
        name: The metric name to validate

    Raises:
        InvalidMetricNameError: If validation fails
    """
    if not name:
        raise InvalidMetricNameError(name, "name cannot be empty")

    if len(name) > 256:
        raise InvalidMetricNameError(name, "name cannot exceed 256 characters")

    # Check for dangerous characters first (better error message)
    dangerous = _contains_dangerous_chars(name)
    if dangerous:
        raise InvalidMetricNameError(name, f"contains invalid character: '{dangerous}'")

    # Check for whitespace
    if " " in name:
        raise InvalidMetricNameError(name, "name cannot contain spaces")

    # Check for null bytes
    if "\x00" in name:
        raise InvalidMetricNameError(name, "name cannot contain null bytes")

    # Check for path traversal
    if _contains_path_traversal(name):
        raise InvalidMetricNameError(name, "cannot contain path traversal")

    # Must start with a letter
    if not name[0].isalpha():
        raise InvalidMetricNameError(name, "name must start with a letter")

    # Final regex check
    if not _METRIC_NAME_PATTERN.match(name):
        raise InvalidMetricNameError(
            name, "must contain only letters, numbers, underscores, hyphens, dots, colons, and slashes"
        )


def validate_metric_value(value: float) -> None:
    """Validate a metric value is finite (not NaN or Infinity).

    Args:
        value: The metric value to validate

    Raises:
        InvalidMetricValueError: If value is NaN or infinite
    """
    import math

    if math.isnan(value):
        raise InvalidMetricValueError(str(value), "value cannot be NaN")

    if math.isinf(value):
        raise InvalidMetricValueError(str(value), "value must be finite (not Infinity)")


# Allow minor clock skew but prevent obviously corrupt timestamps.
_DEFAULT_MAX_METRIC_FUTURE_DRIFT = timedelta(days=1)
_MAX_METRIC_PAST_DRIFT = timedelta(days=3650)  # 10 years


def _get_metric_future_drift() -> timedelta:
    """Get max allowed future drift for metrics timestamps."""
    override = os.environ.get("GOLDFISH_METRICS_MAX_FUTURE_DRIFT_SECONDS")
    if override:
        try:
            seconds = int(override)
            if seconds < 0:
                raise ValueError("negative drift")
            return timedelta(seconds=seconds)
        except ValueError:
            # Fall back to default if env var is invalid
            return _DEFAULT_MAX_METRIC_FUTURE_DRIFT
    return _DEFAULT_MAX_METRIC_FUTURE_DRIFT


def validate_metric_timestamp(timestamp: str) -> str:
    """Validate and normalize metric timestamp (ISO 8601, UTC).

    Returns normalized ISO 8601 string in UTC.
    """
    if not timestamp:
        raise InvalidMetricTimestampError(timestamp, "timestamp cannot be empty")

    try:
        ts = timestamp.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
    except Exception as exc:
        raise InvalidMetricTimestampError(timestamp, "timestamp must be ISO 8601") from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)

    dt_utc = dt.astimezone(UTC)
    now = datetime.now(UTC)

    max_future_drift = _get_metric_future_drift()
    if dt_utc > now + max_future_drift:
        raise InvalidMetricTimestampError(timestamp, "timestamp is too far in the future")
    if dt_utc < now - _MAX_METRIC_PAST_DRIFT:
        raise InvalidMetricTimestampError(timestamp, "timestamp is too far in the past")

    return dt_utc.isoformat()


def validate_metric_step(step: int | None) -> None:
    """Validate metric step value (non-negative integer)."""
    if step is None:
        return
    if not isinstance(step, int):
        raise InvalidMetricStepError(str(step), "step must be an integer")
    if step < 0:
        raise InvalidMetricStepError(str(step), "step must be >= 0")


def validate_artifact_path(path: str) -> None:
    """Validate an artifact path.

    Artifact paths must:
    - Be non-empty
    - Be relative (no leading /)
    - Not contain path traversal (..)
    - Not contain shell metacharacters
    - Not contain null bytes or control characters

    Args:
        path: The artifact path to validate

    Raises:
        InvalidArtifactPathError: If validation fails
    """
    if not path:
        raise InvalidArtifactPathError(path, "path cannot be empty")

    # Check for null bytes (can truncate paths in C-based systems)
    if "\x00" in path:
        raise InvalidArtifactPathError(path, "cannot contain null bytes")

    # Normalize unicode to catch dot variants and normalize separators
    normalized = unicodedata.normalize("NFKC", path)
    decoded = unquote(normalized)

    # Reject Windows-style backslashes (including encoded)
    if "\\" in normalized or "\\" in decoded:
        raise InvalidArtifactPathError(path, "backslashes are not allowed in paths")

    # Check for dangerous characters (raw + decoded)
    dangerous = _contains_dangerous_chars(normalized)
    if dangerous:
        raise InvalidArtifactPathError(path, f"contains invalid character: '{dangerous}'")
    dangerous_decoded = _contains_dangerous_chars(decoded)
    if dangerous_decoded:
        raise InvalidArtifactPathError(path, f"contains invalid character: '{dangerous_decoded}'")

    # Reject paths that are only whitespace or start/end with whitespace
    if normalized != normalized.strip():
        raise InvalidArtifactPathError(path, "cannot have leading or trailing whitespace")

    # Check for absolute paths (Unix)
    if normalized.startswith("/") or decoded.startswith("/"):
        raise InvalidArtifactPathError(path, "must be a relative path (cannot start with /)")

    # Check for absolute paths (Windows)
    if len(decoded) >= 2 and decoded[1] == ":" and decoded[0].isalpha():
        raise InvalidArtifactPathError(path, "must be a relative path (cannot be absolute Windows path)")

    # Check for path traversal (literal or decoded)
    for candidate in (normalized, decoded):
        if ".." in candidate:
            raise InvalidArtifactPathError(path, "cannot contain path traversal (..)")

        # Split on forward slashes and reject dot segments
        parts = candidate.replace("\\", "/").split("/")
        for part in parts:
            if part in ("..", "."):
                raise InvalidArtifactPathError(path, "cannot contain path traversal")


def validate_batch_size(count: int, max_size: int = 10000) -> None:
    """Validate batch size is within limits.

    Args:
        count: The batch size to validate
        max_size: Maximum allowed batch size (default: 10000)

    Raises:
        InvalidBatchSizeError: If batch size is invalid
    """
    if count <= 0:
        raise InvalidBatchSizeError(count, "batch size must be positive")

    if count > max_size:
        raise InvalidBatchSizeError(count, f"batch size cannot exceed {max_size}")


# ============== DOCKER IMAGE VALIDATION ==============


class InvalidImageTypeError(ValidationError):
    """Docker image type is invalid."""

    def __init__(self, image_type: str, reason: str):
        super().__init__(
            f"Invalid image type '{image_type}': {reason}",
            value=image_type,
            field="image_type",
        )


class InvalidBuildIdError(ValidationError):
    """Build ID is invalid."""

    def __init__(self, build_id: str, reason: str):
        super().__init__(
            f"Invalid build ID '{build_id}': {reason}",
            value=build_id,
            field="build_id",
        )


# Build ID pattern: build-{8 hex chars}
_BUILD_ID_PATTERN = re.compile(r"^build-[a-f0-9]{8}$")

# Valid image types
_VALID_IMAGE_TYPES = {"cpu", "gpu"}


def validate_image_type(image_type: str) -> None:
    """Validate a Docker base image type.

    Args:
        image_type: The image type to validate ("cpu" or "gpu")

    Raises:
        InvalidImageTypeError: If validation fails
    """
    if not image_type:
        raise InvalidImageTypeError(image_type, "image type cannot be empty")

    if image_type not in _VALID_IMAGE_TYPES:
        raise InvalidImageTypeError(image_type, f"must be one of: {', '.join(sorted(_VALID_IMAGE_TYPES))}")


def validate_build_id(build_id: str) -> None:
    """Validate a Docker image build operation ID.

    Build IDs must match format: build-{8 hex chars}

    Args:
        build_id: The build ID to validate

    Raises:
        InvalidBuildIdError: If validation fails
    """
    if not build_id:
        raise InvalidBuildIdError(build_id, "build ID cannot be empty")

    # Check for dangerous characters first
    dangerous = _contains_dangerous_chars(build_id)
    if dangerous:
        raise InvalidBuildIdError(build_id, f"contains invalid character: '{dangerous}'")

    # Must match exact format
    if not _BUILD_ID_PATTERN.match(build_id):
        raise InvalidBuildIdError(build_id, "must match format build-{8 hex chars}")


# Container ID pattern: 12-64 hex chars or name pattern (alphanumeric + hyphens/underscores/dots)
# Docker container IDs are 64 hex chars, short form is 12 chars
_CONTAINER_ID_PATTERN = re.compile(r"^([a-f0-9]{12,64}|[a-zA-Z0-9][a-zA-Z0-9._-]{0,127})$")

# GCE instance name: lowercase alphanumeric + hyphens, max 63 chars
# Must start with lowercase letter, end with alphanumeric
_INSTANCE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9-]{0,61}[a-z0-9]?$")

# GCE zone pattern: region-zone (e.g., us-central1-a)
_ZONE_PATTERN = re.compile(r"^[a-z]+-[a-z]+[0-9]+-[a-z]$")

# GCP project ID: lowercase alphanumeric + hyphens, 6-30 chars
_PROJECT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")


def validate_container_id(container_id: str) -> None:
    """Validate a Docker container ID.

    Container IDs can be:
    - Full hex hash (64 chars)
    - Short hex hash (12 chars)
    - Container name (alphanumeric with hyphens, underscores, dots)

    This prevents command injection via subprocess calls.

    Args:
        container_id: The container ID to validate

    Raises:
        InvalidContainerIdError: If validation fails
    """
    if not container_id:
        raise InvalidContainerIdError(container_id, "container ID cannot be empty")

    if len(container_id) > 128:
        raise InvalidContainerIdError(container_id, "container ID too long (max 128 chars)")

    # Check for dangerous characters first
    dangerous = _contains_dangerous_chars(container_id)
    if dangerous:
        raise InvalidContainerIdError(container_id, f"contains invalid character: '{dangerous}'")

    # Must match allowed pattern
    if not _CONTAINER_ID_PATTERN.match(container_id):
        raise InvalidContainerIdError(
            container_id,
            "must be hex hash (12-64 chars) or valid name (alphanumeric with .-_)",
        )


def validate_instance_name(instance_name: str) -> None:
    """Validate a GCE instance name.

    GCE instance names must:
    - Start with lowercase letter
    - Contain only lowercase alphanumeric and hyphens
    - Be at most 63 characters
    - End with alphanumeric

    This prevents command injection via gcloud subprocess calls.

    Args:
        instance_name: The instance name to validate

    Raises:
        InvalidInstanceNameError: If validation fails
    """
    if not instance_name:
        raise InvalidInstanceNameError(instance_name, "instance name cannot be empty")

    if len(instance_name) > 63:
        raise InvalidInstanceNameError(instance_name, "instance name too long (max 63 chars)")

    # Check for dangerous characters first
    dangerous = _contains_dangerous_chars(instance_name)
    if dangerous:
        raise InvalidInstanceNameError(instance_name, f"contains invalid character: '{dangerous}'")

    # GCE requires lowercase
    if instance_name != instance_name.lower():
        raise InvalidInstanceNameError(instance_name, "must be lowercase (GCE requirement)")

    # Must match GCE naming pattern
    if not _INSTANCE_NAME_PATTERN.match(instance_name):
        raise InvalidInstanceNameError(
            instance_name,
            "must start with lowercase letter, contain only lowercase alphanumeric and hyphens",
        )


def validate_zone(zone: str) -> None:
    """Validate a GCE zone name.

    GCE zones must match pattern: region-zone (e.g., us-central1-a)

    This prevents command injection via gcloud subprocess calls.

    Args:
        zone: The zone to validate

    Raises:
        InvalidZoneError: If validation fails
    """
    if not zone:
        raise InvalidZoneError(zone, "zone cannot be empty")

    # Check for dangerous characters first
    dangerous = _contains_dangerous_chars(zone)
    if dangerous:
        raise InvalidZoneError(zone, f"contains invalid character: '{dangerous}'")

    # Must match zone pattern
    if not _ZONE_PATTERN.match(zone):
        raise InvalidZoneError(zone, "must match format region-zone (e.g., us-central1-a)")


def validate_project_id(project_id: str) -> None:
    """Validate a GCP project ID.

    GCP project IDs must:
    - Be 6-30 characters
    - Start with lowercase letter
    - Contain only lowercase alphanumeric and hyphens
    - End with alphanumeric

    This prevents command injection via gcloud subprocess calls.

    Args:
        project_id: The project ID to validate

    Raises:
        InvalidProjectIdError: If validation fails
    """
    if not project_id:
        raise InvalidProjectIdError(project_id, "project ID cannot be empty")

    # Check for dangerous characters first
    dangerous = _contains_dangerous_chars(project_id)
    if dangerous:
        raise InvalidProjectIdError(project_id, f"contains invalid character: '{dangerous}'")

    # Must match project ID pattern
    if not _PROJECT_ID_PATTERN.match(project_id):
        raise InvalidProjectIdError(
            project_id,
            "must be 6-30 chars, start with lowercase letter, contain only lowercase alphanumeric and hyphens",
        )


# Docker image pattern: [registry/]name[:tag]
# Allows: alpine, alpine:latest, gcr.io/project/image:v1, localhost:5000/image
# Registry: optional, alphanumeric with dots, hyphens, colons (for ports)
# Name: alphanumeric with dots, hyphens, underscores, slashes
# Tag: optional, alphanumeric with dots, hyphens, underscores
_DOCKER_IMAGE_PATTERN = re.compile(r"^[a-zA-Z0-9]([a-zA-Z0-9._/:@-]{0,253}[a-zA-Z0-9])?$")

# Environment variable key pattern: POSIX-compliant
# Must start with letter or underscore, contain only alphanumeric and underscore
_ENV_KEY_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,255}$")

# Signal/input name pattern: same as output name
# Alphanumeric, hyphens, underscores. Max 64 chars.
_SIGNAL_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$")


def validate_docker_image(image: str) -> None:
    """Validate a Docker image name.

    Docker image names can include:
    - Simple names: alpine, python
    - Names with tags: alpine:latest, python:3.12
    - Registry paths: gcr.io/project/image:tag
    - Digest references: image@sha256:...

    This prevents command injection via Docker subprocess calls.

    Args:
        image: The Docker image name to validate

    Raises:
        InvalidDockerImageError: If validation fails
    """
    if not image:
        raise InvalidDockerImageError(image, "image name cannot be empty")

    if len(image) > 255:
        raise InvalidDockerImageError(image, "image name too long (max 255 chars)")

    # Check for dangerous characters first
    dangerous = _contains_dangerous_chars(image)
    if dangerous:
        raise InvalidDockerImageError(image, f"contains invalid character: '{dangerous}'")

    # Must match Docker image pattern
    if not _DOCKER_IMAGE_PATTERN.match(image):
        raise InvalidDockerImageError(
            image,
            "must be valid Docker image format (e.g., alpine, alpine:latest, gcr.io/project/image:tag)",
        )


def validate_env_key(key: str) -> None:
    """Validate an environment variable key.

    Environment variable keys must:
    - Start with letter or underscore
    - Contain only alphanumeric and underscore
    - Be at most 256 characters

    This prevents command injection via Docker -e arguments.

    Args:
        key: The environment variable key to validate

    Raises:
        InvalidEnvKeyError: If validation fails
    """
    if not key:
        raise InvalidEnvKeyError(key, "env key cannot be empty")

    if len(key) > 256:
        raise InvalidEnvKeyError(key, "env key too long (max 256 chars)")

    # Check for dangerous characters first
    dangerous = _contains_dangerous_chars(key)
    if dangerous:
        raise InvalidEnvKeyError(key, f"contains invalid character: '{dangerous}'")

    # Must match POSIX env key pattern
    if not _ENV_KEY_PATTERN.match(key):
        raise InvalidEnvKeyError(
            key,
            "must start with letter/underscore, contain only alphanumeric and underscore",
        )


def validate_env_value(key: str, value: str) -> None:
    """Validate an environment variable value.

    Environment variable values must not contain:
    - Control characters that can break argument parsing
    - Newlines (could break -e argument parsing)

    Args:
        key: The environment variable key (for error messages)
        value: The environment variable value to validate

    Raises:
        InvalidEnvValueError: If validation fails
    """
    if len(value) > 32768:
        raise InvalidEnvValueError(key, value[:50] + "...", "env value too long (max 32KB)")

    # Disallow control characters that can break CLI argument parsing.
    # Note: JSON produced by json.dumps() uses escape sequences (e.g., "\\n"), not literal newlines.
    for char in ("\n", "\r", "\t", "\x00"):
        if char in value:
            raise InvalidEnvValueError(key, value[:50], f"contains invalid character: '{char}'")

    # Internal Goldfish env vars can safely contain JSON payloads, which include characters like
    # braces and quotes. These values are passed as subprocess args (not via a shell).
    if key.startswith("GOLDFISH_"):
        return

    # Check for dangerous characters
    dangerous = _contains_dangerous_chars(value)
    if dangerous:
        raise InvalidEnvValueError(key, value[:50], f"contains invalid character: '{dangerous}'")


def validate_signal_name(name: str) -> None:
    """Validate a signal/input name.

    Signal names must:
    - Start with alphanumeric
    - Contain only alphanumeric, hyphens, underscores
    - Be at most 64 characters

    This prevents path injection via mount paths.

    Args:
        name: The signal name to validate

    Raises:
        InvalidSignalNameError: If validation fails
    """
    if not name:
        raise InvalidSignalNameError(name, "signal name cannot be empty")

    if len(name) > 64:
        raise InvalidSignalNameError(name, "signal name too long (max 64 chars)")

    # Check for dangerous characters first
    dangerous = _contains_dangerous_chars(name)
    if dangerous:
        raise InvalidSignalNameError(name, f"contains invalid character: '{dangerous}'")

    # Check for path components
    if "/" in name or "\\" in name:
        raise InvalidSignalNameError(name, "cannot contain path separators")

    # Must match signal name pattern
    if not _SIGNAL_NAME_PATTERN.match(name):
        raise InvalidSignalNameError(
            name,
            "must start with alphanumeric, contain only alphanumeric, hyphens, and underscores",
        )
