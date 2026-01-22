"""GCP adapter implementations for cloud abstraction layer.

Wraps existing GCP code (GCELauncher, GCPMetadataBus) to implement
the cloud protocols defined in goldfish.cloud.protocols.

These adapters enable switching between local and GCP backends
via configuration without changing core Goldfish code.
"""

from goldfish.cloud.adapters.gcp.identity import GCPIdentity, GCPIdentityError
from goldfish.cloud.adapters.gcp.image import (
    ArtifactRegistryError,
    ArtifactRegistryImageRegistry,
    CloudBuildImageBuilder,
)
from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend
from goldfish.cloud.adapters.gcp.signal_bus import GCPSignalBus
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
