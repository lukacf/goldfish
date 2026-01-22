"""Factory for creating cloud adapters based on configuration.

Provides a unified way to instantiate the appropriate cloud adapters
(storage, compute, signaling, identity, image) based on the configured backend type.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from goldfish.cloud.adapters.local.identity import LocalIdentity, LocalIdentityConfig
from goldfish.cloud.adapters.local.image import LocalImageBuilder, LocalImageRegistry
from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
from goldfish.cloud.adapters.local.storage import LocalObjectStorage
from goldfish.infra.metadata.local import LocalMetadataBus

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

        Args:
            root: Root directory for local storage (ignored for GCS).

        Returns:
            ObjectStorage implementation for the configured backend.
        """
        if self._backend_type == "local":
            from goldfish.config import LocalStorageConfig

            # Use config values if available, or defaults
            local_config = self._config.local
            if local_config and local_config.storage:
                storage_config = local_config.storage
                config = LocalStorageConfig(
                    consistency_delay_ms=storage_config.consistency_delay_ms,
                    size_limit_mb=storage_config.size_limit_mb,
                )
                storage_root = root or Path(storage_config.root)
            else:
                config = LocalStorageConfig(
                    consistency_delay_ms=0,
                    size_limit_mb=None,
                )
                storage_root = root or Path(".local_gcs")
            return LocalObjectStorage(root=storage_root, config=config)

        elif self._backend_type == "gce":
            from goldfish.cloud.adapters.gcp.storage import GCSStorage

            gce_config = self._config.gce
            project = gce_config.project or gce_config.project_id if gce_config else None
            return GCSStorage(project=project)

        else:
            raise ValueError(f"Unknown backend type: {self._backend_type}")

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
            return LocalRunBackend(config=config)

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
