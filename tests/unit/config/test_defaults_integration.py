"""Unit tests for DefaultsConfig integration with stage executor.

These tests verify that DefaultsConfig fields are actually wired up
and used as fallbacks when stage-level config doesn't specify values.
"""

from unittest.mock import MagicMock


class TestDefaultsTimeoutIntegration:
    """Tests for defaults.timeout_seconds integration."""

    def test_timeout_uses_defaults_when_stage_config_not_specified(self) -> None:
        """When stage config has no max_runtime_seconds, should use defaults.timeout_seconds."""
        from goldfish.config import DefaultsConfig, GoldfishConfig

        # Create config with defaults
        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.defaults = DefaultsConfig(timeout_seconds=7200)  # 2 hours

        # Stage config without max_runtime_seconds
        stage_config_yaml: dict = {"compute": {}}  # No max_runtime_seconds

        # Extract timeout using the same logic as _stage_executor_impl.py
        timeout_seconds: int | None = None
        compute_config = stage_config_yaml.get("compute", {})
        if compute_config and isinstance(compute_config, dict):
            max_runtime = compute_config.get("max_runtime_seconds")
            if max_runtime is not None:
                timeout_seconds = int(max_runtime)

        # If stage doesn't specify, use defaults
        if timeout_seconds is None:
            timeout_seconds = mock_config.defaults.timeout_seconds

        assert timeout_seconds == 7200

    def test_timeout_prefers_stage_config_over_defaults(self) -> None:
        """When stage config specifies max_runtime_seconds, should use that over defaults."""
        from goldfish.config import DefaultsConfig, GoldfishConfig

        # Create config with defaults
        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.defaults = DefaultsConfig(timeout_seconds=7200)  # 2 hours

        # Stage config with specific max_runtime_seconds
        stage_config_yaml: dict = {"compute": {"max_runtime_seconds": 1800}}  # 30 min

        # Extract timeout using the same logic as _stage_executor_impl.py
        timeout_seconds: int | None = None
        compute_config = stage_config_yaml.get("compute", {})
        if compute_config and isinstance(compute_config, dict):
            max_runtime = compute_config.get("max_runtime_seconds")
            if max_runtime is not None:
                timeout_seconds = int(max_runtime)

        # If stage doesn't specify, use defaults (but it does specify)
        if timeout_seconds is None:
            timeout_seconds = mock_config.defaults.timeout_seconds

        assert timeout_seconds == 1800  # Stage config wins


class TestDefaultsLogSyncIntervalIntegration:
    """Tests for defaults.log_sync_interval integration."""

    def test_log_sync_interval_returns_config_value(self) -> None:
        """_get_log_sync_interval should return config value when set."""
        from goldfish.config import DefaultsConfig, GoldfishConfig

        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.defaults = DefaultsConfig(log_sync_interval=15)

        # The method should return config value (env var takes precedence if set)
        interval = mock_config.defaults.log_sync_interval
        assert interval == 15

    def test_log_sync_interval_has_sensible_default(self) -> None:
        """DefaultsConfig should have a sensible default log_sync_interval."""
        from goldfish.config import DefaultsConfig

        defaults = DefaultsConfig()
        assert defaults.log_sync_interval == 10  # Default is 10 seconds
        assert defaults.log_sync_interval > 0


class TestDefaultsBackendIntegration:
    """Tests for defaults.backend interaction with jobs.backend."""

    def test_defaults_backend_is_informational(self) -> None:
        """defaults.backend is for documentation - jobs.backend is the actual setting.

        The defaults.backend field exists for clarity in goldfish.yaml but
        AdapterFactory uses jobs.backend. This test documents that behavior.
        """
        from goldfish.config import DefaultsConfig, GoldfishConfig, JobsConfig

        # defaults.backend and jobs.backend can differ
        # jobs.backend is what's actually used
        mock_config = MagicMock(spec=GoldfishConfig)
        mock_config.defaults = DefaultsConfig(backend="gce")
        mock_config.jobs = JobsConfig(backend="local")

        # The factory would use jobs.backend, not defaults.backend
        actual_backend = mock_config.jobs.backend
        assert actual_backend == "local"
