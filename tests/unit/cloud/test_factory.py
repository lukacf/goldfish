"""Unit tests for AdapterFactory.

Tests that the factory correctly creates and wires up adapters.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
from goldfish.cloud.adapters.local.storage import LocalObjectStorage
from goldfish.cloud.factory import AdapterFactory
from goldfish.config import (
    AzureStorageConfig,
    GCEConfig,
    GCSConfig,
    GoldfishConfig,
    JobsConfig,
    S3StorageConfig,
    StorageConfig,
)


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


class TestAdapterFactoryGCERunBackend:
    """Tests for GCE backend creation.

    Tests that the factory correctly creates GCERunBackend with proper config.
    Resource building is tested in run_backend tests since it's now internal
    to GCERunBackend (proper abstraction - factory doesn't know about profiles).
    """

    @pytest.fixture
    def gce_config(self) -> GoldfishConfig:
        """Create a config with GCE backend."""
        return GoldfishConfig(
            project_name="test-project",
            dev_repo_path="test-dev",
            jobs=JobsConfig(backend="gce"),
            gce=GCEConfig(
                project="test-gcp-project",
                zones=["us-central1-a", "us-central1-b"],
            ),
            gcs=GCSConfig(bucket="test-bucket"),
        )

    def test_factory_creates_gce_run_backend_for_gce_config(self, gce_config: GoldfishConfig) -> None:
        """Factory creates GCERunBackend when backend is 'gce'."""
        with patch("goldfish.cloud.adapters.gcp.gce_launcher.GCELauncher"):
            factory = AdapterFactory(gce_config)
            backend = factory.create_run_backend()

            from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

            assert isinstance(backend, GCERunBackend)

    def test_factory_passes_zones_to_gce_backend(self, gce_config: GoldfishConfig) -> None:
        """Factory passes zones from config to GCERunBackend.

        Zones are used for:
        1. Multi-zone capacity search
        2. Global zones override for profiles (via GCERunBackend internal logic)
        """
        with patch("goldfish.cloud.adapters.gcp.gce_launcher.GCELauncher") as mock_launcher_class:
            factory = AdapterFactory(gce_config)
            factory.create_run_backend()

            mock_launcher_class.assert_called_once()
            call_kwargs = mock_launcher_class.call_args.kwargs
            assert call_kwargs["zones"] == ["us-central1-a", "us-central1-b"]

    def test_factory_passes_project_to_gce_backend(self, gce_config: GoldfishConfig) -> None:
        """Factory passes project from config to GCERunBackend."""
        with patch("goldfish.cloud.adapters.gcp.gce_launcher.GCELauncher") as mock_launcher_class:
            factory = AdapterFactory(gce_config)
            factory.create_run_backend()

            call_kwargs = mock_launcher_class.call_args.kwargs
            assert call_kwargs["project_id"] == "test-gcp-project"


class TestGetCapabilitiesForBackend:
    """Tests for get_capabilities_for_backend function.

    This function is used when we need capabilities based on a stored backend_type
    string (e.g., from a database row) without access to the actual backend instance.
    """

    def test_returns_local_capabilities_for_local_backend(self) -> None:
        """get_capabilities_for_backend returns local defaults for 'local' backend."""
        from goldfish.cloud.adapters.local.run_backend import LOCAL_DEFAULT_CAPABILITIES
        from goldfish.cloud.contracts import BackendCapabilities
        from goldfish.cloud.factory import get_capabilities_for_backend

        caps = get_capabilities_for_backend("local")

        assert isinstance(caps, BackendCapabilities)
        assert caps == LOCAL_DEFAULT_CAPABILITIES
        # Verify key local-specific values
        assert caps.has_launch_delay is False
        assert caps.timeout_becomes_pending is False
        assert caps.zone_resolution_method == "config"

    def test_returns_gce_capabilities_for_gce_backend(self) -> None:
        """get_capabilities_for_backend returns GCE defaults for 'gce' backend."""
        from goldfish.cloud.adapters.gcp.run_backend import GCE_DEFAULT_CAPABILITIES
        from goldfish.cloud.contracts import BackendCapabilities
        from goldfish.cloud.factory import get_capabilities_for_backend

        caps = get_capabilities_for_backend("gce")

        assert isinstance(caps, BackendCapabilities)
        assert caps == GCE_DEFAULT_CAPABILITIES
        # Verify key GCE-specific values
        assert caps.has_launch_delay is True
        assert caps.timeout_becomes_pending is True
        assert caps.zone_resolution_method == "handle"

    def test_returns_local_capabilities_for_unknown_backend(self) -> None:
        """get_capabilities_for_backend returns local defaults for unknown backends."""
        from goldfish.cloud.adapters.local.run_backend import LOCAL_DEFAULT_CAPABILITIES
        from goldfish.cloud.factory import get_capabilities_for_backend

        caps = get_capabilities_for_backend("unknown_backend")

        assert caps == LOCAL_DEFAULT_CAPABILITIES

    def test_gce_capabilities_use_provider_agnostic_strings(self) -> None:
        """GCE capabilities should not contain GCP-specific terminology.

        This is a regression test for the abstraction violation fix.
        User-facing strings should be provider-agnostic.
        """
        from goldfish.cloud.factory import get_capabilities_for_backend

        caps = get_capabilities_for_backend("gce")

        # Should NOT contain "GCE" or "GCP" in user-facing messages
        assert "GCE" not in caps.logs_unavailable_message
        assert "GCP" not in caps.logs_unavailable_message
        assert "GCE" not in caps.status_message_for_preparing
        assert "GCP" not in caps.status_message_for_preparing

    def test_local_capabilities_match_adapter_defaults(self) -> None:
        """Local capabilities from factory should match adapter's static defaults.

        This ensures consistency between the factory function and the adapter module.
        """
        from goldfish.cloud.adapters.local.run_backend import LOCAL_DEFAULT_CAPABILITIES
        from goldfish.cloud.factory import get_capabilities_for_backend

        caps = get_capabilities_for_backend("local")

        assert caps == LOCAL_DEFAULT_CAPABILITIES

    def test_gce_capabilities_match_adapter_defaults(self) -> None:
        """GCE capabilities from factory should match adapter's static defaults.

        This ensures consistency between the factory function and the adapter module.
        """
        from goldfish.cloud.adapters.gcp.run_backend import GCE_DEFAULT_CAPABILITIES
        from goldfish.cloud.factory import get_capabilities_for_backend

        caps = get_capabilities_for_backend("gce")

        assert caps == GCE_DEFAULT_CAPABILITIES


class TestAdapterFactoryStorageBackendSelection:
    """Tests for storage backend selection based on storage config.

    The factory should read from the new storage: section in config
    to determine which storage backend to create.
    """

    def test_factory_uses_storage_backend_for_s3(self) -> None:
        """Factory reads storage.backend='s3' and returns appropriate storage adapter.

        When storage.backend is 's3', factory should prepare to create an S3 adapter.
        Since S3 adapter is not yet implemented, this test verifies config is read.
        """
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="test-dev",
            jobs=JobsConfig(backend="local"),
            storage=StorageConfig(
                backend="s3",
                s3=S3StorageConfig(bucket="my-s3-bucket", region="us-west-2"),
            ),
        )

        factory = AdapterFactory(config)

        # Verify the storage config is accessible
        assert config.storage is not None
        assert config.storage.backend == "s3"
        assert config.storage.s3 is not None
        assert config.storage.s3.bucket == "my-s3-bucket"
        # Factory should be created successfully
        assert factory.backend_type == "local"

    def test_factory_uses_legacy_gcs_when_no_storage_section(self) -> None:
        """Factory falls back to legacy gcs: section when storage: is not present.

        For backwards compatibility, if no storage: section exists but gcs: does,
        the factory should still work with GCS storage.
        """
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="test-dev",
            jobs=JobsConfig(backend="gce"),
            gcs=GCSConfig(bucket="legacy-bucket"),
            gce=GCEConfig(project="test-project", zones=["us-central1-a"]),
        )

        factory = AdapterFactory(config)

        # Factory should be able to access legacy GCS config
        assert config.gcs is not None
        assert config.gcs.bucket == "legacy-bucket"
        assert config.storage is None  # No new storage section
        assert factory.backend_type == "gce"

    def test_factory_storage_local_backend_creates_local_storage(self) -> None:
        """When storage.backend='local', factory creates LocalObjectStorage.

        This verifies that the local storage backend selection works correctly
        through the new storage: config section.
        """
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="test-dev",
            jobs=JobsConfig(backend="local"),
            storage=StorageConfig(backend="local"),
        )

        factory = AdapterFactory(config)
        storage = factory.create_storage()

        assert isinstance(storage, LocalObjectStorage)

    def test_factory_storage_gcs_backend_with_storage_section(self) -> None:
        """When storage.backend='gcs' with storage.gcs config, factory uses it.

        Verifies that GCS config can be specified in either the new storage:
        section or the legacy gcs: section.
        """
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="test-dev",
            jobs=JobsConfig(backend="local"),
            storage=StorageConfig(
                backend="gcs",
                gcs=GCSConfig(bucket="new-style-bucket"),
            ),
        )

        # Config should be valid and accessible
        assert config.storage is not None
        assert config.storage.backend == "gcs"
        assert config.storage.gcs is not None
        assert config.storage.gcs.bucket == "new-style-bucket"

        factory = AdapterFactory(config)
        assert factory is not None

    def test_factory_raises_not_implemented_for_s3_storage(self) -> None:
        """Factory raises NotImplementedError when trying to create S3 storage.

        S3 adapter is not yet implemented. This test ensures a clear error message
        is provided rather than a cryptic failure.
        """
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="test-dev",
            jobs=JobsConfig(backend="local"),
            storage=StorageConfig(
                backend="s3",
                s3=S3StorageConfig(bucket="my-s3-bucket"),
            ),
        )

        factory = AdapterFactory(config)

        with pytest.raises(NotImplementedError, match="S3 storage adapter not yet implemented"):
            factory.create_storage()

    def test_factory_raises_not_implemented_for_azure_storage(self) -> None:
        """Factory raises NotImplementedError when trying to create Azure storage.

        Azure adapter is not yet implemented. This test ensures a clear error message
        is provided rather than a cryptic failure.
        """
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="test-dev",
            jobs=JobsConfig(backend="local"),
            storage=StorageConfig(
                backend="azure",
                azure=AzureStorageConfig(container="my-container", account="myaccount"),
            ),
        )

        factory = AdapterFactory(config)

        with pytest.raises(NotImplementedError, match="Azure storage adapter not yet implemented"):
            factory.create_storage()
