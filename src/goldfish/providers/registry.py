"""Provider registries for managing execution and storage providers."""

from __future__ import annotations

from typing import Any, Type

from goldfish.errors import GoldfishError
from goldfish.providers.base import ExecutionProvider, StorageProvider


class ExecutionProviderRegistry:
    """Registry for execution providers.

    Maps provider names to ExecutionProvider classes and manages
    provider instantiation with configuration validation.
    """

    def __init__(self) -> None:
        """Initialize empty registry."""
        self._providers: dict[str, Type[ExecutionProvider]] = {}

    def register(
        self,
        name: str,
        provider_class: Type[ExecutionProvider],
    ) -> None:
        """Register an execution provider.

        Args:
            name: Provider name (e.g., "gce", "local", "kubernetes")
            provider_class: ExecutionProvider subclass

        Raises:
            GoldfishError: If provider name already registered
        """
        if name in self._providers:
            raise GoldfishError(f"Execution provider '{name}' already registered")

        if not issubclass(provider_class, ExecutionProvider):
            raise GoldfishError(f"Provider class must inherit from ExecutionProvider: {provider_class}")

        self._providers[name] = provider_class

    def get(self, name: str, config: dict[str, Any] | None = None) -> ExecutionProvider:
        """Get and instantiate an execution provider.

        Args:
            name: Provider name
            config: Provider-specific configuration

        Returns:
            Instantiated ExecutionProvider

        Raises:
            GoldfishError: If provider not found or instantiation fails
        """
        if name not in self._providers:
            available = ", ".join(sorted(self._providers.keys()))
            raise GoldfishError(
                f"Execution provider '{name}' not found. Available providers: {available or 'none'}"
            )

        provider_class = self._providers[name]
        try:
            return provider_class(config or {})
        except Exception as e:
            raise GoldfishError(f"Failed to initialize execution provider '{name}': {e}") from e

    def list_providers(self) -> list[str]:
        """List all registered provider names.

        Returns:
            Sorted list of provider names
        """
        return sorted(self._providers.keys())

    def has_provider(self, name: str) -> bool:
        """Check if provider is registered.

        Args:
            name: Provider name

        Returns:
            True if registered
        """
        return name in self._providers


class StorageProviderRegistry:
    """Registry for storage providers.

    Maps provider names to StorageProvider classes and manages
    provider instantiation with configuration validation.
    """

    def __init__(self) -> None:
        """Initialize empty registry."""
        self._providers: dict[str, Type[StorageProvider]] = {}

    def register(
        self,
        name: str,
        provider_class: Type[StorageProvider],
    ) -> None:
        """Register a storage provider.

        Args:
            name: Provider name (e.g., "gcs", "s3", "local")
            provider_class: StorageProvider subclass

        Raises:
            GoldfishError: If provider name already registered
        """
        if name in self._providers:
            raise GoldfishError(f"Storage provider '{name}' already registered")

        if not issubclass(provider_class, StorageProvider):
            raise GoldfishError(f"Provider class must inherit from StorageProvider: {provider_class}")

        self._providers[name] = provider_class

    def get(self, name: str, config: dict[str, Any] | None = None) -> StorageProvider:
        """Get and instantiate a storage provider.

        Args:
            name: Provider name
            config: Provider-specific configuration

        Returns:
            Instantiated StorageProvider

        Raises:
            GoldfishError: If provider not found or instantiation fails
        """
        if name not in self._providers:
            available = ", ".join(sorted(self._providers.keys()))
            raise GoldfishError(
                f"Storage provider '{name}' not found. Available providers: {available or 'none'}"
            )

        provider_class = self._providers[name]
        try:
            return provider_class(config or {})
        except Exception as e:
            raise GoldfishError(f"Failed to initialize storage provider '{name}': {e}") from e

    def list_providers(self) -> list[str]:
        """List all registered provider names.

        Returns:
            Sorted list of provider names
        """
        return sorted(self._providers.keys())

    def has_provider(self, name: str) -> bool:
        """Check if provider is registered.

        Args:
            name: Provider name

        Returns:
            True if registered
        """
        return name in self._providers


# Global registries
_execution_registry = ExecutionProviderRegistry()
_storage_registry = StorageProviderRegistry()


def get_execution_registry() -> ExecutionProviderRegistry:
    """Get the global execution provider registry.

    Returns:
        Global ExecutionProviderRegistry instance
    """
    return _execution_registry


def get_storage_registry() -> StorageProviderRegistry:
    """Get the global storage provider registry.

    Returns:
        Global StorageProviderRegistry instance
    """
    return _storage_registry
