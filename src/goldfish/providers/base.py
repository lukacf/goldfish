"""Base provider interfaces for execution and storage abstraction."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class ExecutionResult:
    """Result from launching a stage execution."""

    instance_id: str
    """Unique identifier for the running instance (container ID, VM name, etc.)"""

    metadata: dict[str, Any]
    """Provider-specific metadata (zones, URLs, etc.)"""

    hyperlink: str | None = None
    """Optional provider-specific hyperlink (e.g., GCE console URL)"""


@dataclass
class ExecutionStatus:
    """Status of a running execution."""

    state: str
    """One of: running, succeeded, failed, unknown"""

    exit_code: int | None = None
    """Exit code if completed"""

    message: str | None = None
    """Status message or error details"""

    metadata: dict[str, Any] | None = None
    """Provider-specific status metadata"""


@dataclass
class StorageLocation:
    """Reference to data in storage."""

    uri: str
    """Storage URI (e.g., gs://bucket/path, s3://bucket/path, file:///path)"""

    size_bytes: int | None = None
    """Size in bytes if known"""

    metadata: dict[str, Any] | None = None
    """Provider-specific metadata"""

    hyperlink: str | None = None
    """Optional provider-specific hyperlink for viewing"""


@dataclass
class VolumeInfo:
    """Information about a provisioned volume."""

    volume_id: str
    """Provider-specific volume identifier"""

    region: str | None = None
    """Region where volume is provisioned"""

    size_gb: int | None = None
    """Volume size in GB"""

    metadata: dict[str, Any] | None = None
    """Provider-specific metadata"""


class ExecutionProvider(ABC):
    """Abstract interface for execution backends (GCE, local Docker, etc.).

    Execution providers handle:
    - Building container images
    - Launching stage executions
    - Monitoring execution status
    - Streaming logs
    - Canceling running executions

    Optional advanced features:
    - Provisioning region-matched volumes
    - Mounting volumes for runtime I/O
    - Generating provider hyperlinks
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize provider with configuration.

        Args:
            config: Provider-specific configuration dict
        """
        self.config = config

    @abstractmethod
    def build_image(
        self,
        image_tag: str,
        dockerfile_path: Path,
        context_path: Path,
        base_image: str | None = None,
    ) -> str:
        """Build a container image.

        Args:
            image_tag: Tag for the built image
            dockerfile_path: Path to Dockerfile
            context_path: Build context directory
            base_image: Optional base image override

        Returns:
            Final image tag (may differ from input if pushed to registry)

        Raises:
            GoldfishError: If build fails
        """

    @abstractmethod
    def launch_stage(
        self,
        image_tag: str,
        stage_run_id: str,
        entrypoint_script: str,
        stage_config: dict[str, Any],
        work_dir: Path,
        inputs_dir: Path | None = None,
        outputs_dir: Path | None = None,
        machine_type: str | None = None,
        gpu_type: str | None = None,
        gpu_count: int = 0,
        profile_hints: dict[str, Any] | None = None,
    ) -> ExecutionResult:
        """Launch a stage execution.

        Args:
            image_tag: Container image to run
            stage_run_id: Unique stage run identifier
            entrypoint_script: Shell script to execute
            stage_config: Stage configuration dict
            work_dir: Working directory for execution artifacts
            inputs_dir: Directory containing input data
            outputs_dir: Directory for output data
            machine_type: Machine type hint
            gpu_type: GPU type hint
            gpu_count: Number of GPUs
            profile_hints: Additional profile hints for provider

        Returns:
            ExecutionResult with instance ID and metadata

        Raises:
            GoldfishError: If launch fails
        """

    @abstractmethod
    def get_status(self, instance_id: str) -> ExecutionStatus:
        """Get execution status.

        Args:
            instance_id: Instance identifier from launch_stage

        Returns:
            ExecutionStatus with current state

        Raises:
            GoldfishError: If status check fails
        """

    @abstractmethod
    def get_logs(self, instance_id: str, tail: int | None = None) -> str:
        """Get execution logs.

        Args:
            instance_id: Instance identifier from launch_stage
            tail: Optional number of lines to return from end

        Returns:
            Log output as string

        Raises:
            GoldfishError: If log retrieval fails
        """

    @abstractmethod
    def cancel(self, instance_id: str) -> bool:
        """Cancel a running execution.

        Args:
            instance_id: Instance identifier from launch_stage

        Returns:
            True if cancelled, False if already stopped

        Raises:
            GoldfishError: If cancellation fails
        """

    # Optional advanced features

    def supports_volumes(self) -> bool:
        """Check if provider supports volume provisioning.

        Returns:
            True if provision_volume and mount_volume are supported
        """
        return False

    def provision_volume(
        self,
        volume_id: str,
        size_gb: int,
        region: str | None = None,
    ) -> VolumeInfo:
        """Provision a persistent volume (optional).

        Args:
            volume_id: Identifier for the volume
            size_gb: Size in gigabytes
            region: Target region for volume

        Returns:
            VolumeInfo with volume details

        Raises:
            NotImplementedError: If provider doesn't support volumes
            GoldfishError: If provisioning fails
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support volume provisioning")

    def mount_volume(
        self,
        instance_id: str,
        volume_id: str,
        mount_path: str = "/mnt/data",
    ) -> bool:
        """Mount a volume to a running instance (optional).

        Args:
            instance_id: Instance to mount to
            volume_id: Volume to mount
            mount_path: Path inside instance

        Returns:
            True if mounted successfully

        Raises:
            NotImplementedError: If provider doesn't support volumes
            GoldfishError: If mounting fails
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support volume mounting")


class StorageProvider(ABC):
    """Abstract interface for storage backends (GCS, S3, local filesystem, etc.).

    Storage providers handle:
    - Uploading data to storage
    - Downloading data from storage
    - Checking existence
    - Generating presigned URLs

    Optional advanced features:
    - Snapshotting data
    - Generating hyperlinks for viewing
    - Storing opaque handles/metadata
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize provider with configuration.

        Args:
            config: Provider-specific configuration dict
        """
        self.config = config

    @abstractmethod
    def upload(
        self,
        local_path: Path,
        remote_path: str,
        metadata: dict[str, Any] | None = None,
    ) -> StorageLocation:
        """Upload local file or directory to storage.

        Args:
            local_path: Local file or directory path
            remote_path: Remote path identifier (provider-specific)
            metadata: Optional metadata to attach

        Returns:
            StorageLocation with URI and metadata

        Raises:
            GoldfishError: If upload fails
        """

    @abstractmethod
    def download(
        self,
        remote_path: str,
        local_path: Path,
    ) -> Path:
        """Download from storage to local filesystem.

        Args:
            remote_path: Remote path identifier
            local_path: Local destination path

        Returns:
            Path to downloaded file/directory

        Raises:
            GoldfishError: If download fails
        """

    @abstractmethod
    def exists(self, remote_path: str) -> bool:
        """Check if path exists in storage.

        Args:
            remote_path: Remote path identifier

        Returns:
            True if exists

        Raises:
            GoldfishError: If check fails
        """

    @abstractmethod
    def get_size(self, remote_path: str) -> int | None:
        """Get size of stored object.

        Args:
            remote_path: Remote path identifier

        Returns:
            Size in bytes, or None if not available

        Raises:
            GoldfishError: If check fails
        """

    def presign(self, remote_path: str, expiration_seconds: int = 3600) -> str | None:
        """Generate presigned URL for temporary access (optional).

        Args:
            remote_path: Remote path identifier
            expiration_seconds: URL validity duration

        Returns:
            Presigned URL, or None if not supported

        Raises:
            GoldfishError: If URL generation fails
        """
        return None

    def get_hyperlink(self, remote_path: str) -> str | None:
        """Get provider-specific hyperlink for viewing (optional).

        Args:
            remote_path: Remote path identifier

        Returns:
            Hyperlink URL (e.g., GCS console URL), or None if not supported
        """
        return None

    def snapshot(
        self,
        remote_path: str,
        snapshot_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> StorageLocation | None:
        """Create a snapshot of stored data (optional).

        Args:
            remote_path: Remote path to snapshot
            snapshot_id: Identifier for snapshot
            metadata: Optional metadata

        Returns:
            StorageLocation for snapshot, or None if not supported

        Raises:
            GoldfishError: If snapshot fails
        """
        return None

    def store_handle(
        self,
        remote_path: str,
        handle: str,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Store opaque handle/reference for future lookup (optional).

        Args:
            remote_path: Remote path
            handle: Opaque handle string
            metadata: Optional metadata

        Returns:
            True if stored, False if not supported
        """
        return False

    def retrieve_handle(self, remote_path: str) -> str | None:
        """Retrieve stored handle (optional).

        Args:
            remote_path: Remote path

        Returns:
            Handle string, or None if not found or not supported
        """
        return None
