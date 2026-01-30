"""Local adapter implementations.

These adapters provide local filesystem and Docker-based implementations
of the cloud abstraction protocols for development and testing.
"""

from goldfish.cloud.adapters.local.identity import LocalIdentity, LocalIdentityConfig
from goldfish.cloud.adapters.local.image import (
    ImageBuildError,
    ImageRegistryError,
    LocalImageBuilder,
    LocalImageRegistry,
)
from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
from goldfish.cloud.adapters.local.storage import LocalObjectStorage

__all__ = [
    "LocalObjectStorage",
    "LocalRunBackend",
    "LocalIdentity",
    "LocalIdentityConfig",
    "LocalImageBuilder",
    "LocalImageRegistry",
    "ImageBuildError",
    "ImageRegistryError",
]
