"""Metrics backend registry.

Backends register themselves here and can be looked up by name (e.g., "wandb").
This follows the plugin pattern used elsewhere in Goldfish.
"""

from __future__ import annotations

import logging

from goldfish.metrics.backends.base import MetricsBackend

logger = logging.getLogger(__name__)


class MetricsBackendRegistry:
    """Registry for metrics backends.

    Backends register themselves by name (e.g., "wandb", "mlflow") and can be
    instantiated by configuration.
    """

    def __init__(self) -> None:
        self._backends: dict[str, type[MetricsBackend]] = {}

    def register(self, backend_class: type[MetricsBackend]) -> None:
        """Register a backend class.

        Args:
            backend_class: Backend class implementing MetricsBackend ABC
        """
        name = backend_class.name()
        if name in self._backends:
            logger.warning(f"Backend '{name}' already registered, overwriting")
        self._backends[name] = backend_class
        logger.debug(f"Registered metrics backend: {name}")

    def get(self, name: str) -> type[MetricsBackend] | None:
        """Get a backend class by name.

        Args:
            name: Backend name (e.g., "wandb")

        Returns:
            Backend class, or None if not found
        """
        return self._backends.get(name)

    def list_backends(self) -> list[str]:
        """List all registered backend names.

        Returns:
            List of backend names
        """
        return list(self._backends.keys())

    def list_available(self) -> list[str]:
        """List backends that are currently available.

        A backend is available if its package is installed and configured.

        Returns:
            List of available backend names
        """
        available = []
        for name, backend_class in self._backends.items():
            if backend_class.is_available():
                available.append(name)
        return available


# Global registry instance
_registry = MetricsBackendRegistry()


def get_registry() -> MetricsBackendRegistry:
    """Get the global metrics backend registry.

    Returns:
        Global MetricsBackendRegistry instance
    """
    return _registry


def register_backend(backend_class: type[MetricsBackend]) -> None:
    """Register a backend with the global registry.

    Args:
        backend_class: Backend class implementing MetricsBackend ABC
    """
    _registry.register(backend_class)


# Auto-register built-in backends
# Import and register WandBBackend if wandb is available
try:
    from goldfish.metrics.backends.wandb import WandBBackend

    if WandBBackend.is_available():
        register_backend(WandBBackend)
        logger.debug("Auto-registered WandBBackend")
except ImportError:
    # wandb package not installed, skip registration
    pass


__all__ = [
    "MetricsBackend",
    "MetricsBackendRegistry",
    "get_registry",
    "register_backend",
]
