"""Error types and git error translation.

All errors shown to Claude should be git-agnostic.
"""


class GoldfishError(Exception):
    """Base error for all Goldfish operations."""

    def __init__(self, message: str, details: dict | None = None):
        self.message = message
        self.details = details or {}
        super().__init__(message)


class WorkspaceNotFoundError(GoldfishError):
    """Workspace does not exist."""

    pass


class WorkspaceAlreadyExistsError(GoldfishError):
    """Workspace with this name already exists."""

    pass


class SlotNotEmptyError(GoldfishError):
    """Slot already has a mounted workspace."""

    pass


class SlotEmptyError(GoldfishError):
    """Operation requires a mounted workspace but slot is empty."""

    pass


class InvalidSlotError(GoldfishError):
    """Invalid slot name."""

    pass


class SyncError(GoldfishError):
    """Failed to sync workspace to remote."""

    pass


class ReasonTooShortError(GoldfishError):
    """Reason parameter is too short."""

    def __init__(self, reason: str, min_length: int = 15):
        message = f"Reason must be at least {min_length} characters. Got: '{reason}' ({len(reason)} chars)"
        super().__init__(message, {"reason": reason, "min_length": min_length})


class SourceNotFoundError(GoldfishError):
    """Data source does not exist."""

    pass


class SourceAlreadyExistsError(GoldfishError):
    """Data source with this name already exists."""

    pass


class JobNotFoundError(GoldfishError):
    """Job does not exist."""

    pass


class InvalidSnapshotIdError(GoldfishError):
    """Snapshot ID has invalid format or contains dangerous characters."""

    def __init__(self, snapshot_id: str, reason: str = "invalid format"):
        message = f"Invalid snapshot ID '{snapshot_id}': {reason}"
        super().__init__(message, {"snapshot_id": snapshot_id, "reason": reason})


class InvalidSourceNameError(GoldfishError):
    """Source name has invalid format or contains dangerous characters."""

    def __init__(self, name: str, reason: str = "invalid format"):
        message = f"Invalid source name '{name}': {reason}"
        super().__init__(message, {"name": name, "reason": reason})


class ProjectNotInitializedError(GoldfishError):
    """Project has not been initialized with Goldfish."""

    pass


class DatabaseError(GoldfishError):
    """Database operation failed."""

    def __init__(self, message: str, operation: str | None = None, *, path: str | None = None):
        details = {}
        if path:
            details["path"] = path
        if operation:
            details["operation"] = operation
        super().__init__(message, details)


class ConfigParamNotFoundError(GoldfishError):
    """Config parameter referenced in schema but not found in merged config."""

    def __init__(self, param: str, available: list[str] | None = None):
        message = f"Config parameter '{param}' not found in stage config"
        details = {"param": param, "available": available or []}
        super().__init__(message, details)


# Cloud abstraction layer errors


class StorageError(GoldfishError):
    """Storage operation failed."""

    def __init__(self, message: str, uri: str | None = None):
        details = {}
        if uri:
            details["uri"] = uri
        super().__init__(message, details)


class NotFoundError(GoldfishError):
    """Requested object not found in storage."""

    def __init__(self, uri: str):
        message = f"Object not found: {uri}"
        super().__init__(message, {"uri": uri})


class CapacityError(GoldfishError):
    """No capacity available (zone exhausted, quota hit, etc.)."""

    def __init__(self, message: str, zones_tried: list[str] | None = None):
        details = {}
        if zones_tried:
            details["zones_tried"] = zones_tried
        super().__init__(message, details)


class LaunchError(GoldfishError):
    """Failed to launch compute run."""

    def __init__(self, message: str, stage_run_id: str | None = None, cause: str | None = None):
        details = {}
        if stage_run_id:
            details["stage_run_id"] = stage_run_id
        if cause:
            details["cause"] = cause
        super().__init__(message, details)


class MetadataSizeLimitError(GoldfishError):
    """Metadata signal exceeds size limit (256KB for GCP compatibility)."""

    MAX_SIZE_BYTES = 262144  # 256KB

    def __init__(self, actual_size: int, key: str | None = None):
        message = f"Metadata exceeds 256KB limit: {actual_size} bytes"
        details: dict[str, int | str] = {
            "actual_size": actual_size,
            "max_size": self.MAX_SIZE_BYTES,
        }
        if key:
            details["key"] = key
        super().__init__(message, details)


# Docker image management errors


class DockerNotAvailableError(GoldfishError):
    """Docker is not installed or daemon is not running."""

    def __init__(self, reason: str = "Docker daemon not responding"):
        message = f"Docker not available: {reason}"
        details = {
            "hint": "Install Docker Desktop or ensure Docker daemon is running",
            "check_command": "docker info",
        }
        super().__init__(message, details)


class RegistryNotConfiguredError(GoldfishError):
    """Artifact Registry not configured in goldfish.yaml."""

    def __init__(self):
        message = "Artifact Registry not configured"
        details = {
            "hint": "Configure gce.project_id in goldfish.yaml (registry auto-generates from project_id)",
            "example": "gce:\n  project_id: my-project",
        }
        super().__init__(message, details)


class BaseImageBuildError(GoldfishError):
    """Docker base image build failed."""

    def __init__(self, image_type: str, reason: str, logs_tail: str | None = None):
        message = f"Failed to build {image_type} image: {reason}"
        details = {"image_type": image_type, "reason": reason}
        if logs_tail:
            details["logs_tail"] = logs_tail
        super().__init__(message, details)


class BaseImageNotFoundError(GoldfishError):
    """Local base image does not exist."""

    def __init__(self, image_tag: str):
        message = f"Local image not found: {image_tag}"
        details = {
            "image_tag": image_tag,
            "hint": "Build the image first with manage_base_images(action='build', image_type='...')",
        }
        super().__init__(message, details)


# Cloud Build errors


class CloudBuildError(GoldfishError):
    """Cloud Build operation failed."""

    def __init__(self, message: str, cloud_build_id: str | None = None, logs_uri: str | None = None):
        details: dict[str, str] = {}
        if cloud_build_id:
            details["cloud_build_id"] = cloud_build_id
        if logs_uri:
            details["logs_uri"] = logs_uri
        super().__init__(f"Cloud Build failed: {message}", details)


class CloudBuildNotConfiguredError(GoldfishError):
    """Cloud Build requires GCE configuration."""

    def __init__(self):
        message = "Cloud Build requires GCE project configuration"
        details = {
            "hint": "Configure gce.project_id in goldfish.yaml to use Cloud Build",
            "example": "gce:\n  project_id: my-gcp-project",
        }
        super().__init__(message, details)


# Git error translation - never let Claude see git terminology
GIT_ERROR_TRANSLATIONS = {
    "already exists": "already exists",
    "does not exist": "not found",
    "not found": "not found",
    "not an empty directory": "slot is not empty - hibernate the current workspace first",
    "fatal: not a git repository": "project not initialized - run goldfish init first",
    "merge conflict": "there are conflicting changes that need manual resolution",
    "permission denied": "permission denied - check file permissions",
    "cannot lock ref": "workspace is locked - another operation may be in progress",
}

# Terms to sanitize from error messages
GIT_TERM_REPLACEMENTS = {
    "branch": "workspace",
    "branches": "workspaces",
    "worktree": "slot",
    "worktrees": "slots",
    "commit": "checkpoint",
    "commits": "checkpoints",
    "repository": "project",
    "repo": "project",
    "git": "version control",
    "HEAD": "current state",
    "ref": "reference",
    "refs": "references",
}


def translate_git_error(error_message: str) -> str:
    """Translate git errors to user-friendly, git-agnostic messages."""
    lower = error_message.lower()

    # Check for known error patterns
    for pattern, translation in GIT_ERROR_TRANSLATIONS.items():
        if pattern in lower:
            return translation

    # Sanitize git terminology
    result = error_message
    for old, new in GIT_TERM_REPLACEMENTS.items():
        # Case-insensitive replacement
        import re

        result = re.sub(re.escape(old), new, result, flags=re.IGNORECASE)

    return result


def validate_reason(reason: str, min_length: int = 15) -> None:
    """Validate reason parameter length."""
    if len(reason) < min_length:
        raise ReasonTooShortError(reason, min_length)
