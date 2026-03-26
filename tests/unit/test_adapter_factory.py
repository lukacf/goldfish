"""Unit tests for goldfish.cloud.factory module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestAdapterFactoryBackendValidation:
    """Tests for AdapterFactory runtime backend validation."""

    def test_factory_rejects_invalid_backend_type(self) -> None:
        """AdapterFactory should reject unknown backend types at construction time."""
        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import GoldfishConfig, JobsConfig
        from goldfish.errors import GoldfishError

        # Create a config with completely unknown backend
        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.jobs = MagicMock(spec=JobsConfig)
        mock_config.jobs.backend = "unknown_backend"

        with pytest.raises((ValueError, GoldfishError)) as exc_info:
            AdapterFactory(mock_config)
        assert "unknown_backend" in str(exc_info.value) or "backend" in str(exc_info.value).lower()

    def test_factory_rejects_kubernetes_backend_with_clear_message(self) -> None:
        """AdapterFactory should reject kubernetes with a helpful 'not yet implemented' message."""
        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import GoldfishConfig, JobsConfig

        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.jobs = MagicMock(spec=JobsConfig)
        mock_config.jobs.backend = "kubernetes"

        with pytest.raises(NotImplementedError) as exc_info:
            AdapterFactory(mock_config)
        assert "kubernetes" in str(exc_info.value).lower()
        assert "not yet implemented" in str(exc_info.value).lower() or "coming soon" in str(exc_info.value).lower()

    def test_factory_accepts_local_backend(self) -> None:
        """AdapterFactory should accept 'local' backend type."""
        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import GoldfishConfig, JobsConfig, LocalConfig

        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.jobs = MagicMock(spec=JobsConfig)
        mock_config.jobs.backend = "local"
        mock_config.local = MagicMock(spec=LocalConfig)
        mock_config.storage = None

        factory = AdapterFactory(mock_config)
        assert factory.backend_type == "local"

    def test_factory_accepts_gce_backend(self) -> None:
        """AdapterFactory should accept 'gce' backend type."""
        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import GCEConfig, GoldfishConfig, JobsConfig

        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.jobs = MagicMock(spec=JobsConfig)
        mock_config.jobs.backend = "gce"
        mock_config.gce = MagicMock(spec=GCEConfig)
        mock_config.storage = None

        factory = AdapterFactory(mock_config)
        assert factory.backend_type == "gce"


class TestOperatorPrecedenceFix:
    """Tests for operator precedence bugs in factory.py.

    The bug: `project = gce_config.project or gce_config.project_id if gce_config else None`
    Due to operator precedence, this is parsed as:
        `project = gce_config.project or (gce_config.project_id if gce_config else None)`

    This means when gce_config.project is None but gce_config exists and has project_id,
    it returns project_id. BUT when gce_config.project is None and gce_config is None,
    it should return None but the precedence causes issues.

    The fix: `project = (gce_config.project or gce_config.project_id) if gce_config else None`
    """

    def test_create_storage_uses_project_id_when_project_is_none(self) -> None:
        """When gce_config.project is None, should use gce_config.project_id."""
        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig, StorageConfig

        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.jobs = MagicMock(spec=JobsConfig)
        mock_config.jobs.backend = "gce"
        mock_config.gce = MagicMock(spec=GCEConfig)
        mock_config.gce.project = None
        mock_config.gce.project_id = "my-project-id"
        mock_config.storage = MagicMock(spec=StorageConfig)
        mock_config.storage.backend = "gcs"
        mock_config.storage.gcs = MagicMock(spec=GCSConfig)
        mock_config.local = None

        factory = AdapterFactory(mock_config)

        with patch("goldfish.cloud.adapters.gcp.storage.GCSStorage") as mock_gcs:
            mock_gcs.return_value = MagicMock()
            factory.create_storage()
            # Verify project_id was passed
            mock_gcs.assert_called_once_with(project="my-project-id")

    def test_create_storage_uses_project_when_both_set(self) -> None:
        """When both project and project_id are set, should prefer project."""
        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig, StorageConfig

        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.jobs = MagicMock(spec=JobsConfig)
        mock_config.jobs.backend = "gce"
        mock_config.gce = MagicMock(spec=GCEConfig)
        mock_config.gce.project = "preferred-project"
        mock_config.gce.project_id = "fallback-project"
        mock_config.storage = MagicMock(spec=StorageConfig)
        mock_config.storage.backend = "gcs"
        mock_config.storage.gcs = MagicMock(spec=GCSConfig)
        mock_config.local = None

        factory = AdapterFactory(mock_config)

        with patch("goldfish.cloud.adapters.gcp.storage.GCSStorage") as mock_gcs:
            mock_gcs.return_value = MagicMock()
            factory.create_storage()
            mock_gcs.assert_called_once_with(project="preferred-project")

    def test_create_storage_returns_none_project_when_no_gce_config(self) -> None:
        """When gce_config is None, project should be None (not cause error).

        This tests the operator precedence bug:
        Current buggy code: project = gce_config.project or gce_config.project_id if gce_config else None
        Parsed as: project = gce_config.project or (gce_config.project_id if gce_config else None)

        When gce_config is None, evaluating gce_config.project raises AttributeError.

        Fixed code: project = (gce_config.project or gce_config.project_id) if gce_config else None
        """
        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import GCSConfig, GoldfishConfig, JobsConfig, StorageConfig

        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.jobs = MagicMock(spec=JobsConfig)
        mock_config.jobs.backend = "gce"
        mock_config.gce = None  # No GCE config - this triggers the bug
        mock_config.storage = MagicMock(spec=StorageConfig)
        mock_config.storage.backend = "gcs"
        mock_config.storage.gcs = MagicMock(spec=GCSConfig)
        mock_config.local = None

        factory = AdapterFactory(mock_config)

        with patch("goldfish.cloud.adapters.gcp.storage.GCSStorage") as mock_gcs:
            mock_gcs.return_value = MagicMock()
            # This should NOT raise AttributeError
            factory.create_storage()
            mock_gcs.assert_called_once_with(project=None)

    def test_create_run_backend_uses_project_id_when_project_is_none(self) -> None:
        """create_run_backend should use project_id when project is None."""
        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig

        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.jobs = MagicMock(spec=JobsConfig)
        mock_config.jobs.backend = "gce"
        mock_config.gce = MagicMock(spec=GCEConfig)
        mock_config.gce.project = None
        mock_config.gce.project_id = "my-project-id"
        mock_config.gce.zones = ["us-central1-a"]
        mock_config.gce.gpu_preference = ["h100"]
        mock_config.gce.service_account = None
        mock_config.gce.search_timeout_sec = 900
        mock_config.gce.initial_backoff_sec = 10
        mock_config.gce.backoff_multiplier = 1.5
        mock_config.gce.max_attempts = 150
        mock_config.gcs = MagicMock(spec=GCSConfig)
        mock_config.gcs.bucket = "my-bucket"
        mock_config.storage = None
        mock_config.local = None

        factory = AdapterFactory(mock_config)

        with patch("goldfish.cloud.adapters.gcp.run_backend.GCERunBackend") as mock_backend:
            mock_backend.return_value = MagicMock()
            factory.create_run_backend()
            call_kwargs = mock_backend.call_args[1]
            assert call_kwargs["project_id"] == "my-project-id"


class TestStorageConfigTypeAnnotation:
    """Tests for proper type annotation usage in factory methods."""

    def test_create_storage_from_storage_config_accepts_storage_config(self) -> None:
        """_create_storage_from_storage_config should accept StorageConfig type."""
        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import GoldfishConfig, JobsConfig, LocalConfig, StorageConfig

        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.jobs = MagicMock(spec=JobsConfig)
        mock_config.jobs.backend = "local"
        mock_config.storage = None
        mock_config.local = MagicMock(spec=LocalConfig)
        mock_config.local.storage = None

        factory = AdapterFactory(mock_config)

        # Create a real StorageConfig
        storage_config = StorageConfig(backend="local")
        storage = factory._create_storage_from_storage_config(storage_config)
        assert storage is not None


class TestGCSProjectWarning:
    """Tests for GCS project None warning."""

    def test_create_storage_warns_when_gce_config_has_no_project(self, caplog: pytest.LogCaptureFixture) -> None:
        """Should warn when gce_config exists but both project and project_id are None."""
        import logging

        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig, StorageConfig

        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.jobs = MagicMock(spec=JobsConfig)
        mock_config.jobs.backend = "gce"
        mock_config.gce = MagicMock(spec=GCEConfig)
        mock_config.gce.project = None
        mock_config.gce.project_id = None
        mock_config.storage = MagicMock(spec=StorageConfig)
        mock_config.storage.backend = "gcs"
        mock_config.storage.gcs = MagicMock(spec=GCSConfig)
        mock_config.local = None

        factory = AdapterFactory(mock_config)

        with caplog.at_level(logging.WARNING):
            with patch("goldfish.cloud.adapters.gcp.storage.GCSStorage") as mock_gcs:
                mock_gcs.return_value = MagicMock()
                factory.create_storage()

        # Should have logged a warning about missing project
        assert any("project" in record.message.lower() for record in caplog.records)

    def test_create_storage_no_warning_when_project_is_set(self, caplog: pytest.LogCaptureFixture) -> None:
        """Should NOT warn when gce_config has a valid project_id."""
        import logging

        from goldfish.cloud.factory import AdapterFactory
        from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig, StorageConfig

        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.jobs = MagicMock(spec=JobsConfig)
        mock_config.jobs.backend = "gce"
        mock_config.gce = MagicMock(spec=GCEConfig)
        mock_config.gce.project = None
        mock_config.gce.project_id = "my-project"
        mock_config.storage = MagicMock(spec=StorageConfig)
        mock_config.storage.backend = "gcs"
        mock_config.storage.gcs = MagicMock(spec=GCSConfig)
        mock_config.local = None

        factory = AdapterFactory(mock_config)

        with caplog.at_level(logging.WARNING):
            with patch("goldfish.cloud.adapters.gcp.storage.GCSStorage") as mock_gcs:
                mock_gcs.return_value = MagicMock()
                factory.create_storage()

        # Should NOT have logged a warning about missing project
        assert not any("project" in record.message.lower() for record in caplog.records)


class TestCreateWarmPoolManager:
    def test_skips_manager_for_local_backend_even_if_warm_pool_enabled(self) -> None:
        from goldfish.cloud.factory import create_warm_pool_manager
        from goldfish.config import GCEConfig, GoldfishConfig, JobsConfig, WarmPoolConfig

        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.jobs = MagicMock(spec=JobsConfig)
        mock_config.jobs.backend = "local"
        mock_config.gce = MagicMock(spec=GCEConfig)
        mock_config.gce.warm_pool = WarmPoolConfig(enabled=True, max_instances=2)
        mock_config.gce.project_id = "test-project"
        mock_config.gce.project = None
        mock_config.gcs = None

        mgr = create_warm_pool_manager(MagicMock(), mock_config)
        assert mgr is None
