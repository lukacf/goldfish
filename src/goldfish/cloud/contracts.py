"""Contract types for cloud abstraction layer.

These types define the data structures used across all cloud backends.
They are derived from RCT (Representation Contract Tests) observations
of real GCP behavior in Phase 0.

Design principles:
1. Provider-agnostic: No GCP/AWS/Azure specifics leak into these types
2. Validated by RCT: Each type corresponds to observed real behavior
3. Capability-aware: Optional features are explicit, not assumed
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass  # Reserved for future type imports

# Validators for from_dict security (must be runtime imports, not TYPE_CHECKING)
from goldfish.validation import (
    validate_container_id,  # noqa: E402
    validate_stage_run_id,  # noqa: E402
)


class StorageURI:
    """Provider-agnostic storage URI.

    Abstracts storage URIs (cloud + local) into a common interface.
    Based on RCT-GCS-1 observations: URIs are (scheme, bucket, path) tuples.

    Examples:
        StorageURI("gs", "my-bucket", "path/to/file.txt")
        StorageURI("s3", "my-bucket", "path/to/file.txt")
        file:///local/path/file.txt -> StorageURI("file", "", "/local/path/file.txt")
    """

    def __init__(self, scheme: str, bucket: str, path: str) -> None:
        """Initialize a StorageURI.

        Args:
            scheme: URI scheme (gs, s3, file, etc.)
            bucket: Bucket/container name (empty for file://)
            path: Object path within bucket
        """
        self.scheme = scheme
        self.bucket = bucket
        # For file:// scheme, preserve absolute path; for cloud, normalize
        if scheme == "file":
            self.path = path  # Keep as-is (should be absolute like /tmp/x)
        else:
            self.path = path.lstrip("/")  # Normalize: no leading slash for cloud

    @classmethod
    def parse(cls, uri: str) -> StorageURI:
        """Parse a URI string into StorageURI.

        Args:
            uri: Full URI string (e.g., "s3://bucket/path" or "file:///tmp/x")

        Returns:
            Parsed StorageURI

        Raises:
            ValueError: If URI format is invalid or contains path traversal
        """
        if "://" not in uri:
            raise ValueError(f"Invalid URI format (missing scheme): {uri}")

        scheme, rest = uri.split("://", 1)

        if scheme == "file":
            # For file://, preserve absolute path (e.g., file:///tmp/x -> /tmp/x)
            path = rest if rest.startswith("/") else "/" + rest
            # Validate no path traversal
            if ".." in path:
                raise ValueError(f"Path traversal not allowed in URI: {uri}")
            return cls(scheme, "", path)

        if "/" in rest:
            bucket, path = rest.split("/", 1)
        else:
            bucket, path = rest, ""

        # Validate no path traversal in cloud URIs
        if ".." in path or ".." in bucket:
            raise ValueError(f"Path traversal not allowed in URI: {uri}")

        # Validate cloud URIs have non-empty bucket
        if scheme in ("gs", "s3") and not bucket:
            raise ValueError(f"Cloud URI must have bucket (got empty bucket in {uri})")

        return cls(scheme, bucket, path)

    def __str__(self) -> str:
        """Convert back to URI string."""
        if self.scheme == "file":
            # file:// + absolute path (e.g., file:///tmp/x)
            return f"file://{self.path}"
        return f"{self.scheme}://{self.bucket}/{self.path}"

    def __repr__(self) -> str:
        return f"StorageURI({self.scheme!r}, {self.bucket!r}, {self.path!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StorageURI):
            return NotImplemented
        return (self.scheme, self.bucket, self.path) == (other.scheme, other.bucket, other.path)

    def __hash__(self) -> int:
        return hash((self.scheme, self.bucket, self.path))

    def join(self, *parts: str) -> StorageURI:
        """Join path components to this URI.

        Args:
            *parts: Path components to append

        Returns:
            New StorageURI with appended path
        """
        new_path = "/".join([self.path.rstrip("/")] + [p.strip("/") for p in parts])
        return StorageURI(self.scheme, self.bucket, new_path)


class RunStatus(Enum):
    """Normalized run status across all backends.

    Based on RCT-GCE-2 observations of GCE status values:
    - PROVISIONING, STAGING -> PREPARING
    - RUNNING -> RUNNING
    - STOPPING -> RUNNING (still alive)
    - TERMINATED -> COMPLETED/FAILED/TERMINATED (based on exit code)

    This enum is provider-agnostic. Backend adapters map their native
    statuses to these values.
    """

    # Initial states
    PENDING = "pending"  # Not yet started
    PREPARING = "preparing"  # Resources being allocated

    # Active states
    RUNNING = "running"  # Container/instance is executing

    # Unknown state (API errors, connection issues)
    UNKNOWN = "unknown"  # Status cannot be determined

    # Terminal states (success)
    COMPLETED = "completed"  # Exit code 0

    # Terminal states (failure)
    FAILED = "failed"  # Non-zero exit code (not preemption/OOM)
    TERMINATED = "terminated"  # Killed externally (preemption, OOM, timeout)
    CANCELED = "canceled"  # User-initiated cancellation

    def is_terminal(self) -> bool:
        """Return True if this is a terminal (final) status."""
        return self in (
            RunStatus.COMPLETED,
            RunStatus.FAILED,
            RunStatus.TERMINATED,
            RunStatus.CANCELED,
        )

    def is_success(self) -> bool:
        """Return True if this represents successful completion."""
        return self == RunStatus.COMPLETED


@dataclass
class BackendStatus:
    """Detailed status information from a backend.

    Combines the normalized RunStatus with backend-specific details.
    Based on RCT-GCE-2 and RCT-EXIT-1 observations.
    """

    status: RunStatus
    exit_code: int | None = None  # None if not yet terminated
    termination_cause: str | None = None  # "preemption", "oom", "timeout", "user"
    message: str | None = None  # Human-readable status message
    started_at: str | None = None  # ISO timestamp
    finished_at: str | None = None  # ISO timestamp

    @classmethod
    def from_exit_code(cls, exit_code: int, termination_cause: str | None = None) -> BackendStatus:
        """Create BackendStatus from exit code.

        Based on RCT-EXIT-1 semantics:
        - 0 -> COMPLETED
        - 1-127 -> FAILED
        - 137 (128+9) -> TERMINATED (SIGKILL/OOM)
        - 143 (128+15) -> TERMINATED (SIGTERM/preemption)

        Args:
            exit_code: Process exit code
            termination_cause: Optional cause (overrides inference from exit code)

        Returns:
            BackendStatus with appropriate status
        """
        if exit_code == 0:
            return cls(status=RunStatus.COMPLETED, exit_code=exit_code)

        # Signal-based termination (128 + signal number)
        if exit_code == 137:  # SIGKILL
            cause = termination_cause or "oom"
            return cls(status=RunStatus.TERMINATED, exit_code=exit_code, termination_cause=cause)

        if exit_code == 143:  # SIGTERM
            cause = termination_cause or "preemption"
            return cls(status=RunStatus.TERMINATED, exit_code=exit_code, termination_cause=cause)

        # Regular failure
        return cls(status=RunStatus.FAILED, exit_code=exit_code)


@dataclass
class BackendCapabilities:
    """Capabilities advertised by a backend.

    Used for capability negotiation - callers can check what features
    are available before using them.

    Based on LOCAL_PARITY_SPEC.md design.
    """

    supports_gpu: bool = False
    supports_spot: bool = False  # Spot/preemptible instances
    supports_preemption: bool = False  # Can gracefully handle preemption (SIGTERM)
    supports_preemption_detection: bool = False  # Can detect when preemption occurs
    supports_live_logs: bool = False  # Real-time log streaming
    supports_metrics: bool = False  # Metrics collection during run
    max_run_duration_hours: int | None = None  # None = unlimited

    # Sync behavior - used by callers to adjust timeouts and messaging
    ack_timeout_seconds: float = 1.0  # Default ACK timeout for sync operations
    ack_timeout_running_seconds: float = 1.0  # ACK timeout when already running
    has_launch_delay: bool = False  # Whether backend has delay between launch and running
    logs_unavailable_message: str = "Logs not available"  # Message when logs can't be fetched
    timeout_becomes_pending: bool = False  # ACK timeout means "sync pending", not failure
    status_message_for_preparing: str = "Preparing..."  # Message for PREPARING status
    zone_resolution_method: str = "config"  # "config" = use config zones, "handle" = use handle.zone


@dataclass
class RunSpec:
    """Specification for launching a run.

    Contains everything needed to launch a stage on any backend.
    Provider-specific details are handled by the adapter.
    """

    # Identity
    stage_run_id: str  # Unique identifier (e.g., "stage-abc123")
    workspace_name: str
    stage_name: str

    # Container
    image: str  # Docker image to run
    command: list[str] | None = None  # Override entrypoint
    env: dict[str, str] = field(default_factory=dict)

    # Resources
    profile: str = "cpu-small"  # Resource profile name
    machine_type: str | None = None  # Machine type from profile (e.g., "a3-highgpu-1g")
    gpu_count: int = 0
    gpu_type: str | None = None  # GPU type (e.g., "nvidia-tesla-t4", "nvidia-h100-80gb")
    memory_gb: float = 4.0
    cpu_count: float = 2.0

    # Storage
    inputs: dict[str, StorageURI] = field(default_factory=dict)  # signal_name -> uri
    output_uri: StorageURI | None = None  # Where to write outputs

    # Options
    spot: bool = False  # Request spot/preemptible
    timeout_seconds: int | None = None  # Max run duration


@dataclass
class RunHandle:
    """Handle to a running or completed run.

    Returned by RunBackend.launch(), used for status checks and termination.
    The handle is opaque to callers - implementation details are backend-specific.
    """

    # Universal fields
    stage_run_id: str
    backend_type: str  # "local", "gce" (GCP), "aws" - matches backend adapter name

    # Backend-specific handle (opaque to callers)
    # For GCP: instance name + zone
    # For local: container ID
    # For AWS: instance ID + region
    backend_handle: str

    # Optional metadata
    created_at: str | None = None  # ISO timestamp
    zone: str | None = None  # For multi-zone backends

    def to_dict(self) -> dict[str, str | None]:
        """Serialize handle for storage."""
        return {
            "stage_run_id": self.stage_run_id,
            "backend_type": self.backend_type,
            "backend_handle": self.backend_handle,
            "created_at": self.created_at,
            "zone": self.zone,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str | None]) -> RunHandle:
        """Deserialize handle from storage.

        Security: Validates all fields before use to prevent injection
        when handle data comes from untrusted sources (DB, API, etc.).
        """
        # Validate backend_handle is not None (would become "None" string via str())
        raw_handle = data.get("backend_handle")
        if raw_handle is None:
            raise ValueError("backend_handle cannot be None in RunHandle.from_dict")

        stage_run_id = str(data["stage_run_id"])
        backend_type = str(data["backend_type"])
        backend_handle = str(raw_handle)

        # Validate to prevent injection attacks via deserialized data
        validate_stage_run_id(stage_run_id)

        # Validate backend_handle without branching on backend_type.
        # Backends may validate more strictly at adapter boundaries.
        validate_container_id(backend_handle)

        return cls(
            stage_run_id=stage_run_id,
            backend_type=backend_type,
            backend_handle=backend_handle,
            created_at=data.get("created_at"),
            zone=data.get("zone"),
        )
