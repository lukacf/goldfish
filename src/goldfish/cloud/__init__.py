"""Cloud abstraction layer for Goldfish.

This module provides cloud-agnostic abstractions for:
- ObjectStorage: Blob storage operations (GCS, local filesystem)
- RunBackend: Compute operations (GCE, local Docker)
- SignalBus: Control plane messaging (GCP metadata, local JSON)
- InstanceIdentity: Instance metadata (GCP metadata server, local config)
- ImageBuilder: Container image building (Docker, Cloud Build)
- ImageRegistry: Container image registry (Artifact Registry, local)

The abstraction layer enables:
1. Local development with full feature parity
2. Future support for AWS, Azure, Kubernetes backends
3. Testing without real cloud resources
"""

from goldfish.cloud.contracts import (
    BackendCapabilities,
    BackendStatus,
    RunHandle,
    RunSpec,
    RunStatus,
    StorageURI,
)
from goldfish.cloud.protocols import (
    ImageBuilder,
    ImageRegistry,
    InstanceIdentity,
    ObjectStorage,
    RunBackend,
    SignalBus,
)

__all__ = [
    # Contracts
    "StorageURI",
    "RunSpec",
    "RunHandle",
    "RunStatus",
    "BackendStatus",
    "BackendCapabilities",
    # Protocols
    "ObjectStorage",
    "RunBackend",
    "SignalBus",
    "InstanceIdentity",
    "ImageBuilder",
    "ImageRegistry",
]
