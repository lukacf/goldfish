"""Unit tests for AdapterFactory.

Tests that the factory correctly creates and wires up adapters.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
from goldfish.cloud.adapters.local.storage import LocalObjectStorage
from goldfish.cloud.factory import AdapterFactory
from goldfish.config import GoldfishConfig, JobsConfig


@pytest.fixture
def local_config() -> GoldfishConfig:
    """Create a minimal config with local backend."""
    return GoldfishConfig(
        project_name="test-project",
        dev_repo_path="test-dev",
        jobs=JobsConfig(backend="local"),
    )


class TestAdapterFactoryCreateRunBackend:
    """Tests for create_run_backend method."""

    def test_factory_injects_storage_into_local_backend(self, local_config: GoldfishConfig, tmp_path: Path) -> None:
        """Factory should inject storage adapter into LocalRunBackend.

        Without storage injection, LocalRunBackend cannot download GCS-backed
        inputs for local execution. This test verifies that the factory
        properly creates a storage adapter and passes it to LocalRunBackend.
        """
        factory = AdapterFactory(local_config)

        # Create the run backend
        backend = factory.create_run_backend()

        # Verify it's a LocalRunBackend
        assert isinstance(backend, LocalRunBackend)

        # Verify storage was injected (this is the key assertion)
        assert backend._storage is not None, (
            "LocalRunBackend._storage should not be None when created by factory. "
            "Without storage, GCS inputs cannot be downloaded for local execution."
        )

        # Verify it's the right type
        assert isinstance(backend._storage, LocalObjectStorage)

    def test_factory_creates_local_run_backend_for_local_config(self, local_config: GoldfishConfig) -> None:
        """Factory creates LocalRunBackend when backend is 'local'."""
        factory = AdapterFactory(local_config)

        backend = factory.create_run_backend()

        assert isinstance(backend, LocalRunBackend)

    def test_factory_backend_type_property(self, local_config: GoldfishConfig) -> None:
        """Factory exposes backend_type property."""
        factory = AdapterFactory(local_config)

        assert factory.backend_type == "local"


class TestAdapterFactoryCreateSignalBus:
    """Tests for create_signal_bus method."""

    def test_factory_creates_local_signal_bus_for_local_config(
        self, local_config: GoldfishConfig, tmp_path: Path
    ) -> None:
        """Factory creates LocalMetadataBus when backend is 'local'."""
        from goldfish.infra.metadata.local import LocalMetadataBus

        factory = AdapterFactory(local_config)
        metadata_path = tmp_path / ".metadata_bus.json"

        signal_bus = factory.create_signal_bus(metadata_path=metadata_path)

        assert isinstance(signal_bus, LocalMetadataBus)

    def test_signal_bus_implements_protocol(self, local_config: GoldfishConfig, tmp_path: Path) -> None:
        """Signal bus returned by factory implements SignalBus protocol."""
        from goldfish.cloud.protocols import SignalBus

        factory = AdapterFactory(local_config)
        metadata_path = tmp_path / ".metadata_bus.json"

        signal_bus = factory.create_signal_bus(metadata_path=metadata_path)

        assert isinstance(signal_bus, SignalBus)

    def test_signal_bus_also_implements_metadata_bus_protocol(
        self, local_config: GoldfishConfig, tmp_path: Path
    ) -> None:
        """Signal bus should also satisfy MetadataBus protocol for backwards compatibility.

        ServerContext.metadata_bus expects MetadataBus type, so the factory's
        SignalBus must be usable directly without additional wrapping.
        """
        from goldfish.infra.metadata.base import MetadataBus

        factory = AdapterFactory(local_config)
        metadata_path = tmp_path / ".metadata_bus.json"

        signal_bus = factory.create_signal_bus(metadata_path=metadata_path)

        # The returned signal_bus should be directly usable as a MetadataBus
        assert isinstance(signal_bus, MetadataBus)
