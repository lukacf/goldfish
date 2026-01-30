"""GCP adapter implementations for cloud abstraction layer.

Wraps existing GCP code (GCELauncher, GCPMetadataBus) to implement
the cloud protocols defined in goldfish.cloud.protocols.

These adapters enable switching between local and GCP backends
via configuration without changing core Goldfish code.

Note: Some imports are lazy to avoid pulling in server-side dependencies
(like state_machine) when only storage adapters are needed in containers.
"""

# These are safe for container runtime (no state_machine dependency)
from goldfish.cloud.adapters.gcp.storage import GCSStorage

__all__ = [
    "GCERunBackend",
    "GCSStorage",
    "GCPSignalBus",
    "GCPIdentity",
    "GCPIdentityError",
    "CloudBuildImageBuilder",
    "ArtifactRegistryImageRegistry",
    "ArtifactRegistryError",
]


def __getattr__(name: str):
    """Lazy imports for server-side components that have heavy dependencies."""
    if name == "GCERunBackend":
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

        return GCERunBackend
    if name == "GCPSignalBus":
        from goldfish.cloud.adapters.gcp.signal_bus import GCPSignalBus

        return GCPSignalBus
    if name in ("GCPIdentity", "GCPIdentityError"):
        from goldfish.cloud.adapters.gcp.identity import GCPIdentity, GCPIdentityError

        return GCPIdentity if name == "GCPIdentity" else GCPIdentityError
    if name in ("CloudBuildImageBuilder", "ArtifactRegistryImageRegistry", "ArtifactRegistryError"):
        from goldfish.cloud.adapters.gcp.image import (
            ArtifactRegistryError,
            ArtifactRegistryImageRegistry,
            CloudBuildImageBuilder,
        )

        if name == "CloudBuildImageBuilder":
            return CloudBuildImageBuilder
        if name == "ArtifactRegistryImageRegistry":
            return ArtifactRegistryImageRegistry
        return ArtifactRegistryError
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
