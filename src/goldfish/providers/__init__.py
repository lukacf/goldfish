"""Provider infrastructure for pluggable execution and storage backends."""

from goldfish.providers.base import ExecutionProvider, StorageProvider
from goldfish.providers.builtin import register_builtin_providers
from goldfish.providers.registry import (
    ExecutionProviderRegistry,
    StorageProviderRegistry,
    get_execution_registry,
    get_storage_registry,
)

# Auto-register built-in providers
register_builtin_providers()

__all__ = [
    "ExecutionProvider",
    "StorageProvider",
    "ExecutionProviderRegistry",
    "StorageProviderRegistry",
    "get_execution_registry",
    "get_storage_registry",
    "register_builtin_providers",
]
