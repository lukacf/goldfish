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

import re
from pathlib import Path

from goldfish.errors import GoldfishError


class ValidationError(GoldfishError):
    """Base class for validation errors."""

    def __init__(self, message: str, value: str, field: str):
        self.value = value
        self.field = field
        super().__init__(message)


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

# Characters that are NEVER allowed (shell metacharacters)
_DANGEROUS_CHARS = set(";|&$`\"'\\<>*?[]{}~!\n\t\r")

# Path traversal patterns
_PATH_TRAVERSAL_PATTERNS = ["..", "//"]


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

    # Allow common refs
    allowed_refs = {"main", "master", "HEAD"}
    if ref in allowed_refs:
        return

    # Otherwise validate as workspace name (can branch from another workspace)
    try:
        validate_workspace_name(ref)
    except InvalidWorkspaceNameError as e:
        # Re-raise with proper error type
        reason = e.message.split(": ", 1)[-1] if ": " in e.message else str(e)
        raise InvalidRefNameError(ref, reason) from e


def validate_job_id(job_id: str) -> None:
    """Validate a job ID.

    Job IDs must match format: job-{8 hex chars}
    This prevents database/command injection via job_status(), get_job_logs(), etc.

    Args:
        job_id: The job ID to validate (e.g., "job-a1b2c3d4")

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

    # Must match exact format
    if not _JOB_ID_PATTERN.match(job_id):
        raise InvalidJobIdError(job_id, "must match format job-{8 hex chars} (e.g., job-a1b2c3d4)")


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
    - Not contain path traversal patterns or symlinks

    Args:
        log_uri: The log URI to validate (e.g., "file:///path/to/log" or "/path/to/log")
        project_root: The project root directory (all log paths must be within this)

    Returns:
        Resolved Path object

    Raises:
        InvalidLogPathError: If validation fails
    """
    from pathlib import Path

    if not log_uri:
        raise InvalidLogPathError(log_uri, "log URI cannot be empty")

    # Parse the URI first
    if log_uri.startswith("file://"):
        path_str = log_uri[7:]  # Remove "file://"
    else:
        path_str = log_uri

    # Check for path traversal in the path portion (after parsing)
    if ".." in path_str:
        raise InvalidLogPathError(log_uri, "cannot contain path traversal")

    # Must be absolute
    if not path_str.startswith("/"):
        raise InvalidLogPathError(log_uri, "must be an absolute path")

    # Create path object
    log_path = Path(path_str)

    # SECURITY: Check for symlinks before resolving
    if log_path.exists() and log_path.is_symlink():
        raise InvalidLogPathError(log_uri, "log path cannot be a symlink (security risk)")

    # Resolve path
    try:
        log_path = log_path.resolve(strict=False)  # strict=False allows non-existent paths
    except (OSError, RuntimeError) as e:
        raise InvalidLogPathError(log_uri, f"invalid path: {e}") from e

    project_resolved = project_root.resolve()

    # Check that resolved path is within project_root
    try:
        log_path.relative_to(project_resolved)
    except ValueError as e:
        raise InvalidLogPathError(log_uri, f"path must be within project directory (got: {log_path})") from e

    # Verify no intermediate symlinks in the path hierarchy
    for parent in log_path.parents:
        if parent == project_resolved or parent == project_resolved.parent:
            break
        if parent.exists() and parent.is_symlink():
            raise InvalidLogPathError(log_uri, f"path contains symlink: {parent}")

    return log_path


def validate_artifact_uri(artifact_uri: str) -> None:
    """Validate an artifact URI from job output.

    Artifact URIs must:
    - Start with gs:// (Google Cloud Storage only)
    - Not contain path traversal patterns
    - Not reference unexpected buckets via traversal

    Args:
        artifact_uri: The artifact URI to validate (e.g., "gs://bucket/path/")

    Raises:
        InvalidArtifactUriError: If validation fails
    """
    if not artifact_uri:
        raise InvalidArtifactUriError(artifact_uri, "artifact URI cannot be empty")

    # Must be GCS URI
    if not artifact_uri.startswith("gs://"):
        raise InvalidArtifactUriError(artifact_uri, "artifact URI must start with gs://")

    # Check for path traversal (just ".." - gs:// contains // which is fine)
    if ".." in artifact_uri:
        raise InvalidArtifactUriError(artifact_uri, "cannot contain path traversal")


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
