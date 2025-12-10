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

    def __init__(self, message: str, path: str | None = None, operation: str | None = None):
        details = {}
        if path:
            details["path"] = path
        if operation:
            details["operation"] = operation
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
