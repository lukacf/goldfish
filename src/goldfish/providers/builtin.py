"""Built-in provider registration.

This module registers all built-in execution and storage providers.
Import this module to ensure providers are registered.
"""

from goldfish.providers.gce_provider import GCEExecutionProvider
from goldfish.providers.gcs_provider import GCSStorageProvider
from goldfish.providers.local_provider import LocalExecutionProvider, LocalStorageProvider
from goldfish.providers.registry import get_execution_registry, get_storage_registry


def register_builtin_providers() -> None:
    """Register all built-in execution and storage providers.

    This should be called once at module initialization.
    """
    execution_registry = get_execution_registry()
    storage_registry = get_storage_registry()

    # Register execution providers
    if not execution_registry.has_provider("gce"):
        execution_registry.register("gce", GCEExecutionProvider)

    if not execution_registry.has_provider("local"):
        execution_registry.register("local", LocalExecutionProvider)

    # Register storage providers
    if not storage_registry.has_provider("gcs"):
        storage_registry.register("gcs", GCSStorageProvider)

    if not storage_registry.has_provider("local"):
        storage_registry.register("local", LocalStorageProvider)


# Auto-register on import
register_builtin_providers()
