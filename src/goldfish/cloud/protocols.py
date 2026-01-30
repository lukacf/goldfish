"""Protocol definitions for cloud abstraction layer.

These protocols define the interfaces that all cloud backends must implement.
They are the contracts between Goldfish core and provider-specific adapters.

Design principles:
1. Protocol (structural typing): Adapters don't need to inherit, just implement
2. Minimal interface: Only methods that ALL backends can reasonably provide
3. Capability-aware: Optional features use capability flags, not NotImplementedError
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pathlib import Path

    from goldfish.cloud.contracts import (
        BackendCapabilities,
        BackendStatus,
        RunHandle,
        RunSpec,
        StorageURI,
    )
    from goldfish.infra.metadata.base import MetadataSignal


@runtime_checkable
class ObjectStorage(Protocol):
    """Protocol for object storage operations.

    Abstracts blob storage (GCS, S3, local filesystem).
    Based on RCT-GCS-1, RCT-GCS-2 observations.

    All methods work with StorageURI for provider-agnostic addressing.
    """

    def put(self, uri: StorageURI, data: bytes) -> None:
        """Write bytes to storage.

        Args:
            uri: Target location
            data: Bytes to write

        Raises:
            StorageError: If write fails
        """
        ...

    def get(self, uri: StorageURI) -> bytes:
        """Read bytes from storage.

        Args:
            uri: Source location

        Returns:
            File contents as bytes

        Raises:
            NotFoundError: If object doesn't exist
            StorageError: If read fails
        """
        ...

    def exists(self, uri: StorageURI) -> bool:
        """Check if object exists.

        Args:
            uri: Location to check

        Returns:
            True if object exists
        """
        ...

    def list_prefix(self, prefix: StorageURI) -> list[StorageURI]:
        """List objects with given prefix.

        Based on RCT-GCS-2: Returns all objects matching prefix.

        Args:
            prefix: URI prefix to match

        Returns:
            List of matching URIs (may be empty)
        """
        ...

    def delete(self, uri: StorageURI) -> None:
        """Delete object.

        No-op if object doesn't exist (idempotent).

        Args:
            uri: Object to delete
        """
        ...

    def get_local_path(self, uri: StorageURI) -> Path | None:
        """Get local filesystem path for URI if available.

        For local storage: returns the actual path.
        For cloud storage with mount: returns mount path.
        For cloud storage without mount: returns None.

        Args:
            uri: Storage URI

        Returns:
            Local path if available, None otherwise
        """
        ...

    def download_to_file(self, uri: StorageURI, destination: Path) -> bool:
        """Download object to a local file.

        Convenience method for downloading large files without loading into memory.

        Args:
            uri: Source location
            destination: Local path to write to

        Returns:
            True if download succeeded, False if object doesn't exist
        """
        ...

    def get_size(self, uri: StorageURI) -> int | None:
        """Get size of object in bytes.

        Args:
            uri: Location to check

        Returns:
            Size in bytes, or None if object doesn't exist
        """
        ...


@runtime_checkable
class RunBackend(Protocol):
    """Protocol for compute backend operations.

    Abstracts compute resources (GCE instances, local Docker, ECS tasks).
    Based on RCT-GCE-1, RCT-GCE-2 observations.
    """

    @property
    def capabilities(self) -> BackendCapabilities:
        """Return capabilities of this backend.

        Callers should check capabilities before using optional features.
        """
        ...

    def launch(self, spec: RunSpec) -> RunHandle:
        """Launch a new run.

        Args:
            spec: Run specification

        Returns:
            Handle to the launched run

        Raises:
            CapacityError: If no capacity available (e.g., zone exhausted)
            LaunchError: If launch fails for other reasons
        """
        ...

    def get_status(self, handle: RunHandle) -> BackendStatus:
        """Get current status of a run.

        Based on RCT-GCE-2: Status values are mapped to normalized RunStatus.

        Args:
            handle: Run handle from launch()

        Returns:
            Current status with details

        Raises:
            NotFoundError: If run no longer exists
        """
        ...

    def get_logs(self, handle: RunHandle, tail: int = 200, since: str | None = None) -> str:
        """Get logs from a run.

        Args:
            handle: Run handle
            tail: Number of lines from end (0 for all)
            since: Only return logs after this timestamp (ISO format or duration)

        Returns:
            Log content as string
        """
        ...

    def terminate(self, handle: RunHandle) -> None:
        """Terminate a running run.

        Sends termination signal. Run may not stop immediately.
        Idempotent: no error if already terminated.

        Args:
            handle: Run handle
        """
        ...

    def cleanup(self, handle: RunHandle) -> None:
        """Clean up resources for a terminated run.

        Should be called after run completes to free resources.
        Idempotent.

        Args:
            handle: Run handle
        """
        ...

    def get_zone(self, handle: RunHandle) -> str | None:
        """Get the zone where a run is executing.

        Args:
            handle: Run handle

        Returns:
            Zone string if known, None otherwise.
        """
        ...

    def get_output_dir(self, handle: RunHandle) -> Path | None:
        """Get the local output directory for a run.

        For local backends: returns the temp directory where outputs are written.
        For cloud backends: returns None (outputs go to cloud storage).

        Args:
            handle: Run handle

        Returns:
            Path to output directory, or None if not applicable.
        """
        ...


@runtime_checkable
class SignalBus(Protocol):
    """Protocol for control plane signaling.

    Abstracts metadata-based communication (GCP metadata server, local JSON).
    Based on RCT-META-1, RCT-META-2 observations.

    Signals flow bidirectionally:
    - Server -> Instance: commands (sync, stop)
    - Instance -> Server: acks, heartbeats
    """

    def set_signal(self, key: str, signal: MetadataSignal, target: str | None = None) -> None:
        """Set a signal for a target.

        Args:
            key: Signal key (e.g., "goldfish")
            signal: Signal payload
            target: Target identifier (instance name, container ID)
        """
        ...

    def get_signal(self, key: str, target: str | None = None) -> MetadataSignal | None:
        """Get current signal for a target.

        Args:
            key: Signal key
            target: Target identifier

        Returns:
            Current signal or None if not set
        """
        ...

    def clear_signal(self, key: str, target: str | None = None) -> None:
        """Clear a signal.

        Args:
            key: Signal key
            target: Target identifier
        """
        ...

    def set_ack(self, key: str, request_id: str, target: str | None = None) -> None:
        """Set acknowledgment for a signal.

        Args:
            key: Signal key (ack stored at "{key}-ack")
            request_id: Request ID being acknowledged
            target: Target identifier
        """
        ...

    def get_ack(self, key: str, target: str | None = None) -> str | None:
        """Get acknowledgment for a signal.

        Args:
            key: Signal key
            target: Target identifier

        Returns:
            Acknowledged request ID or None
        """
        ...


@runtime_checkable
class InstanceIdentity(Protocol):
    """Protocol for instance identity discovery.

    Abstracts how a running instance discovers its own identity.
    On GCP: uses metadata server (http://metadata.google.internal)
    On local: returns configured values or infers from environment

    This is primarily used by code running INSIDE containers/instances
    to identify themselves to the control plane.
    """

    def get_project_id(self) -> str | None:
        """Get the project ID this instance belongs to.

        Returns:
            Project ID string, or None if not available
        """
        ...

    def get_instance_name(self) -> str | None:
        """Get this instance's name.

        Returns:
            Instance name string, or None if not available
        """
        ...

    def get_zone(self) -> str | None:
        """Get the zone this instance is running in.

        Returns:
            Zone string (e.g., "us-central1-a"), or None if not available
        """
        ...

    def get_instance_id(self) -> str | None:
        """Get unique instance identifier.

        On GCP: numeric instance ID
        On local: container ID or generated UUID

        Returns:
            Instance ID string, or None if not available
        """
        ...

    def is_preemptible(self) -> bool:
        """Check if this instance is preemptible/spot.

        Returns:
            True if instance is preemptible, False otherwise
        """
        ...


@runtime_checkable
class ImageBuilder(Protocol):
    """Protocol for building container images.

    Abstracts image building (local Docker, Cloud Build, etc.).
    The build process is the same conceptually, but execution differs.
    """

    def build(
        self,
        context_path: Path,
        dockerfile_path: Path,
        image_tag: str,
        build_args: dict[str, str] | None = None,
        no_cache: bool = False,
    ) -> str:
        """Build a container image.

        Args:
            context_path: Path to build context directory
            dockerfile_path: Path to Dockerfile
            image_tag: Tag for the built image
            build_args: Optional build arguments
            no_cache: If True, disable layer caching

        Returns:
            The image tag/URI of the built image

        Raises:
            BuildError: If build fails
        """
        ...

    def build_async(
        self,
        context_path: Path,
        dockerfile_path: Path,
        image_tag: str,
        build_args: dict[str, str] | None = None,
        no_cache: bool = False,
    ) -> str:
        """Start an async image build.

        Args:
            context_path: Path to build context directory
            dockerfile_path: Path to Dockerfile
            image_tag: Tag for the built image
            build_args: Optional build arguments
            no_cache: If True, disable layer caching

        Returns:
            Build ID for status polling

        Raises:
            BuildError: If build submission fails
        """
        ...

    def get_build_status(self, build_id: str) -> dict[str, str | None]:
        """Get status of an async build.

        Args:
            build_id: Build ID from build_async()

        Returns:
            Dict with 'status' (pending/building/completed/failed),
            'image_tag' (if completed), 'error' (if failed)
        """
        ...


@runtime_checkable
class ImageRegistry(Protocol):
    """Protocol for container image registry operations.

    Abstracts registry operations (Artifact Registry, ECR, Docker Hub, etc.).
    """

    def push(self, local_tag: str, registry_tag: str) -> str:
        """Push a local image to the registry.

        Args:
            local_tag: Local image tag
            registry_tag: Full registry tag (e.g., "us-docker.pkg.dev/proj/repo/image:tag")

        Returns:
            The pushed registry tag

        Raises:
            RegistryError: If push fails
        """
        ...

    def pull(self, registry_tag: str) -> str:
        """Pull an image from the registry.

        Args:
            registry_tag: Full registry tag

        Returns:
            The local tag of the pulled image

        Raises:
            RegistryError: If pull fails
        """
        ...

    def exists(self, registry_tag: str) -> bool:
        """Check if an image exists in the registry.

        Args:
            registry_tag: Full registry tag

        Returns:
            True if image exists
        """
        ...

    def delete(self, registry_tag: str) -> None:
        """Delete an image from the registry.

        Args:
            registry_tag: Full registry tag

        Raises:
            RegistryError: If delete fails
        """
        ...
