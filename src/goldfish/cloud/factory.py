"""Factory for creating cloud adapters based on configuration.

Provides a unified way to instantiate the appropriate cloud adapters
(storage, compute, signaling, identity, image) based on the configured backend type.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from goldfish.cloud.adapters.local.identity import LocalIdentity, LocalIdentityConfig
from goldfish.cloud.adapters.local.image import LocalImageBuilder, LocalImageRegistry
from goldfish.cloud.adapters.local.run_backend import (
    LOCAL_DEFAULT_CAPABILITIES,
    LocalRunBackend,
)
from goldfish.cloud.adapters.local.storage import LocalObjectStorage
from goldfish.cloud.contracts import BackendCapabilities

# NOTE: goldfish.infra is NOT available in container images - imports must be lazy

if TYPE_CHECKING:
    pass  # Path moved to runtime import

    from goldfish.cloud.protocols import (
        ImageBuilder,
        ImageRegistry,
        InstanceIdentity,
        ObjectStorage,
        RunBackend,
        SignalBus,
    )
    from goldfish.config import GoldfishConfig
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)

BackendType = Literal["local", "gce"]


class AdapterFactory:
    """Factory for creating cloud adapters.

    Creates the appropriate adapter implementations based on backend type.
    Supports 'local' (Docker-based) and 'gce' (Google Compute Engine) backends.
    """

    def __init__(self, config: GoldfishConfig) -> None:
        """Initialize factory with configuration.

        Args:
            config: Goldfish configuration with backend settings.
        """
        self._config = config
        self._backend_type: BackendType = config.jobs.backend  # type: ignore[assignment]

    @property
    def backend_type(self) -> BackendType:
        """Return the configured backend type."""
        return self._backend_type

    def create_storage(self, root: Path | None = None) -> ObjectStorage:
        """Create an ObjectStorage adapter.

        Storage backend selection priority:
        1. If storage: section exists, use storage.backend
        2. Otherwise, fall back to jobs.backend for backwards compatibility

        Args:
            root: Root directory for local storage (ignored for cloud storage).

        Returns:
            ObjectStorage implementation for the configured backend.
        """
        # Check new storage: section first
        storage_config = self._config.storage
        if storage_config is not None:
            return self._create_storage_from_storage_config(storage_config, root)

        # Fall back to legacy behavior based on jobs.backend
        return self._create_storage_legacy(root)

    def _create_storage_from_storage_config(self, storage_config: Any, root: Path | None = None) -> ObjectStorage:
        """Create storage adapter from the new storage: config section."""
        backend = storage_config.backend

        if backend == "local":
            return self._create_local_storage(root)

        if backend == "gcs":
            from goldfish.cloud.adapters.gcp.storage import GCSStorage

            # Use project from gce config if available
            gce_config = self._config.gce
            project = gce_config.project or gce_config.project_id if gce_config else None
            return GCSStorage(project=project)

        if backend == "s3":
            raise NotImplementedError(
                "S3 storage adapter not yet implemented. " "Use storage.backend='gcs' or 'local' for now."
            )

        if backend == "azure":
            raise NotImplementedError(
                "Azure storage adapter not yet implemented. " "Use storage.backend='gcs' or 'local' for now."
            )

        raise ValueError(f"Unknown storage backend: {backend}")

    def _create_storage_legacy(self, root: Path | None = None) -> ObjectStorage:
        """Create storage adapter using legacy jobs.backend selection."""
        if self._backend_type == "local":
            return self._create_local_storage(root)

        if self._backend_type == "gce":
            from goldfish.cloud.adapters.gcp.storage import GCSStorage

            gce_config = self._config.gce
            project = gce_config.project or gce_config.project_id if gce_config else None
            return GCSStorage(project=project)

        raise ValueError(f"Unknown backend type: {self._backend_type}")

    def _create_local_storage(self, root: Path | None = None) -> LocalObjectStorage:
        """Create LocalObjectStorage with appropriate configuration."""
        from goldfish.config import LocalStorageConfig

        # Use config values if available, or defaults
        local_config = self._config.local
        if local_config and local_config.storage:
            local_storage_config = local_config.storage
            config = LocalStorageConfig(
                consistency_delay_ms=local_storage_config.consistency_delay_ms,
                size_limit_mb=local_storage_config.size_limit_mb,
            )
            storage_root = root or Path(local_storage_config.root)
        else:
            config = LocalStorageConfig(
                consistency_delay_ms=0,
                size_limit_mb=None,
            )
            storage_root = root or Path(".local_gcs")
        return LocalObjectStorage(root=storage_root, config=config)

    def create_run_backend(self) -> RunBackend:
        """Create a RunBackend adapter.

        Returns:
            RunBackend implementation for the configured backend.
        """
        if self._backend_type == "local":
            from goldfish.config import LocalComputeConfig

            # Build config from Goldfish settings
            local_config = self._config.local
            if local_config and local_config.compute:
                compute_config = local_config.compute
                config = LocalComputeConfig(
                    docker_socket=compute_config.docker_socket,
                    simulate_preemption_after_seconds=compute_config.simulate_preemption_after_seconds,
                    preemption_grace_period_seconds=compute_config.preemption_grace_period_seconds,
                    zone_availability=compute_config.zone_availability,
                )
            else:
                config = LocalComputeConfig(
                    docker_socket="/var/run/docker.sock",
                    simulate_preemption_after_seconds=None,
                    preemption_grace_period_seconds=30,
                    zone_availability={"local-zone-1": True},
                )
            # Inject storage for GCS input downloads
            # We know this is a local backend, so create_storage() returns LocalObjectStorage
            storage = self.create_storage()
            assert isinstance(storage, LocalObjectStorage), "Local backend requires LocalObjectStorage"
            return LocalRunBackend(storage=storage, config=config)

        elif self._backend_type == "gce":
            from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

            gce_config = self._config.gce
            gcs_config = self._config.gcs
            project = gce_config.project or gce_config.project_id if gce_config else None
            bucket = gcs_config.bucket if gcs_config else None

            return GCERunBackend(
                project_id=project,
                zones=gce_config.zones if gce_config else None,
                bucket=bucket,
                gpu_preference=gce_config.gpu_preference if gce_config else None,
                service_account=gce_config.service_account if gce_config else None,
            )

        else:
            raise ValueError(f"Unknown backend type: {self._backend_type}")

    def create_signal_bus(self, metadata_path: Path | None = None) -> SignalBus:
        """Create a SignalBus adapter.

        Args:
            metadata_path: Path to metadata file for local signal bus (ignored for GCP).

        Returns:
            SignalBus implementation for the configured backend.
        """
        if self._backend_type == "local":
            from goldfish.config import LocalSignalingConfig
            from goldfish.infra.metadata.local import LocalMetadataBus

            local_config = self._config.local
            if local_config and local_config.signaling:
                signaling_config = local_config.signaling
                config = LocalSignalingConfig(
                    size_limit_bytes=signaling_config.size_limit_bytes,
                    latency_ms=signaling_config.latency_ms,
                )
                path = metadata_path or Path(signaling_config.metadata_file)
            else:
                config = LocalSignalingConfig(
                    size_limit_bytes=256 * 1024,  # 256KB like GCP
                    latency_ms=0,
                )
                path = metadata_path or Path(".local_metadata.json")
            return LocalMetadataBus(metadata_path=path, config=config)

        elif self._backend_type == "gce":
            from goldfish.cloud.adapters.gcp.signal_bus import GCPSignalBus

            return GCPSignalBus()

        else:
            raise ValueError(f"Unknown backend type: {self._backend_type}")

    def create_identity(self) -> InstanceIdentity:
        """Create an InstanceIdentity adapter.

        Returns:
            InstanceIdentity implementation for the configured backend.
        """
        if self._backend_type == "local":
            # For local backend, identity comes from environment or defaults
            # LocalIdentity will read from env vars like GOLDFISH_PROJECT_ID
            identity_config = LocalIdentityConfig()
            return LocalIdentity(config=identity_config)

        elif self._backend_type == "gce":
            from goldfish.cloud.adapters.gcp.identity import GCPIdentity

            return GCPIdentity()

        else:
            raise ValueError(f"Unknown backend type: {self._backend_type}")

    def create_image_builder(self, db: Database | None = None) -> ImageBuilder:
        """Create an ImageBuilder adapter.

        Args:
            db: Optional database for persistent build tracking.

        Returns:
            ImageBuilder implementation for the configured backend.
        """
        if self._backend_type == "local":
            return LocalImageBuilder()

        elif self._backend_type == "gce":
            from goldfish.cloud.adapters.gcp.image import CloudBuildImageBuilder

            gce_config = self._config.gce
            if not gce_config:
                raise ValueError("GCE configuration required for CloudBuildImageBuilder")

            project_id = gce_config.project or gce_config.project_id
            registry_url = gce_config.effective_artifact_registry
            if not registry_url:
                raise ValueError("artifact_registry configuration required for CloudBuildImageBuilder")

            cloud_build = self._config.docker.cloud_build if self._config.docker else None

            return CloudBuildImageBuilder(
                project_id=project_id or "",
                registry_url=registry_url,
                machine_type=cloud_build.machine_type if cloud_build else "E2_HIGHCPU_32",
                timeout_minutes=cloud_build.timeout_minutes if cloud_build else 30,
                disk_size_gb=cloud_build.disk_size_gb if cloud_build else 100,
                db=db,
            )

        else:
            raise ValueError(f"Unknown backend type: {self._backend_type}")

    def create_image_registry(self) -> ImageRegistry:
        """Create an ImageRegistry adapter.

        Returns:
            ImageRegistry implementation for the configured backend.
        """
        if self._backend_type == "local":
            return LocalImageRegistry()

        elif self._backend_type == "gce":
            from goldfish.cloud.adapters.gcp.image import ArtifactRegistryImageRegistry

            gce_config = self._config.gce
            if not gce_config:
                raise ValueError("GCE configuration required for ArtifactRegistryImageRegistry")

            project_id = gce_config.project or gce_config.project_id
            registry_url = gce_config.effective_artifact_registry
            if not registry_url:
                raise ValueError("artifact_registry configuration required for ArtifactRegistryImageRegistry")

            return ArtifactRegistryImageRegistry(
                project_id=project_id or "",
                registry_url=registry_url,
            )

        else:
            raise ValueError(f"Unknown backend type: {self._backend_type}")


def get_adapter_factory(config: GoldfishConfig) -> AdapterFactory:
    """Get an AdapterFactory instance for the given configuration.

    This is the main entry point for creating adapters.

    Args:
        config: Goldfish configuration.

    Returns:
        AdapterFactory configured for the specified backend.

    Example:
        factory = get_adapter_factory(config)
        storage = factory.create_storage()
        backend = factory.create_run_backend()
        signal_bus = factory.create_signal_bus()
        identity = factory.create_identity()
        image_builder = factory.create_image_builder()
        image_registry = factory.create_image_registry()
    """
    return AdapterFactory(config)


def create_backend_for_cleanup(backend_type: str, *, project_id: str | None = None) -> RunBackend:
    """Create a RunBackend for cleanup operations (terminate/cleanup).

    This factory function creates a minimal RunBackend based on stored backend_type.
    Used by cancel operations where we only need terminate() capability.

    Unlike AdapterFactory.create_run_backend() which uses config, this creates
    a backend just for termination based on the stored backend_type in the database.

    Args:
        backend_type: Backend type string ("local" or "gce").
        project_id: Optional GCP project ID (used for GCE status/cleanup helpers).

    Returns:
        RunBackend implementation that can terminate runs.

    Raises:
        ValueError: If backend_type is unknown.
    """
    if backend_type == "local":
        return LocalRunBackend()

    elif backend_type == "gce":
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

        # For cleanup, GCERunBackend needs minimal config
        # It will use gcloud CLI which handles auth/project from environment
        return GCERunBackend(project_id=project_id)

    else:
        raise ValueError(f"Unknown backend type for cleanup: {backend_type}")


def validate_backend_handle(backend_type: str, backend_handle: str) -> None:
    """Validate a backend_handle string for a given backend type.

    This helper keeps backend-specific validation behind the cloud/factory.py
    boundary so core modules don't need to import adapter packages directly.

    Args:
        backend_type: Backend type string ("local" or "gce").
        backend_handle: Container ID or instance name.

    Raises:
        InvalidContainerIdError: If backend_handle is invalid for local backend.
        InvalidInstanceNameError: If backend_handle is invalid for GCE backend.
        ValueError: If backend_type is unknown.
    """
    from goldfish.validation import validate_container_id, validate_instance_name

    if backend_type == "local":
        validate_container_id(backend_handle)
        return

    if backend_type == "gce":
        validate_instance_name(backend_handle)
        return

    raise ValueError(f"Unknown backend type for handle validation: {backend_type}")


def get_capabilities_for_backend(backend_type: str) -> BackendCapabilities:
    """Get default capabilities for a backend type.

    This is used when we need capabilities based on a stored backend_type
    string (e.g., from a database row) without access to the actual backend
    instance.

    The default capabilities are defined in each adapter module to keep
    provider-specific values with their respective implementations.

    Args:
        backend_type: Backend type string ("local" or "gce")

    Returns:
        Default BackendCapabilities for that backend type.
        Returns local defaults for unknown backend types.
    """
    if backend_type == "gce":
        from goldfish.cloud.adapters.gcp.run_backend import GCE_DEFAULT_CAPABILITIES

        return GCE_DEFAULT_CAPABILITIES
    # Default to local capabilities for unknown types
    return LOCAL_DEFAULT_CAPABILITIES


def validate_compute_profile(config: GoldfishConfig, profile_name: str) -> tuple[bool, str | None]:
    """Validate a stage compute.profile value for the configured backend.

    This keeps backend-specific validation logic behind the cloud/factory.py
    boundary so Core modules don't import adapters directly.
    """
    if config.jobs.backend != "gce":
        return True, None

    from goldfish.cloud.adapters.gcp.profiles import (
        ProfileNotFoundError,
        ProfileResolver,
        ProfileValidationError,
    )

    gce_config = config.gce
    resolver = ProfileResolver(
        profile_overrides=gce_config.effective_profile_overrides if gce_config else None,
        global_zones=gce_config.zones if gce_config else None,
    )
    try:
        resolver.resolve(profile_name)
    except (ProfileNotFoundError, ProfileValidationError) as e:
        return False, str(e)
    except Exception as e:
        return False, f"Failed to validate profile '{profile_name}': {e}"

    return True, None


def backend_requires_compute_profile(config: GoldfishConfig) -> bool:
    """Return True when the configured backend requires compute.profile."""
    return config.jobs.backend == "gce"


def create_storage_from_env() -> ObjectStorage:
    """Create an ObjectStorage adapter based on environment variables.

    This supports runtime contexts where a full GoldfishConfig is not available,
    while still keeping adapter imports behind the cloud/factory.py boundary.
    """
    import os

    backend = os.environ.get("GOLDFISH_STORAGE_BACKEND", "gcs").lower()

    if backend in ("gcs", "gce"):
        try:
            from goldfish.cloud.adapters.gcp.storage import GCSStorage
        except ImportError as e:
            raise RuntimeError(
                "google-cloud-storage not installed. "
                "Add it to your requirements.txt or set GOLDFISH_STORAGE_BACKEND=local"
            ) from e

        return GCSStorage(project=None)

    if backend == "local":
        from goldfish.config import LocalStorageConfig

        root = Path(os.environ.get("GOLDFISH_LOCAL_STORAGE_ROOT", "/tmp/goldfish_storage"))
        config = LocalStorageConfig(consistency_delay_ms=0, size_limit_mb=None)
        return LocalObjectStorage(root=root, config=config)

    raise RuntimeError(f"Unknown storage backend: {backend}. Supported: gcs, local")


def resolve_compute_profile(config: GoldfishConfig, profile_name: str) -> dict[str, Any]:
    """Resolve a compute profile behind the cloud/factory.py boundary."""
    from goldfish.cloud.adapters.gcp.profiles import ProfileResolver

    gce_config = config.gce
    resolver = ProfileResolver(
        profile_overrides=gce_config.effective_profile_overrides if gce_config else None,
        global_zones=gce_config.zones if gce_config else None,
    )
    return resolver.resolve(profile_name)


def resolve_profile_base_image(
    profile: dict[str, Any],
    artifact_registry: str | None,
    version: str | None,
) -> str:
    """Resolve a profile base image behind the cloud/factory.py boundary."""
    from goldfish.cloud.adapters.gcp.profiles import resolve_base_image

    return resolve_base_image(profile, artifact_registry, version)
