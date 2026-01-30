"""Unit tests for DefaultsConfig global defaults configuration.

Tests the defaults: section in goldfish.yaml for global stage execution defaults.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from goldfish.config import DefaultsConfig, GoldfishConfig


class TestDefaultsConfigModel:
    """Tests for DefaultsConfig Pydantic model."""

    def test_defaults_config_with_all_fields(self) -> None:
        """DefaultsConfig accepts all three configuration fields."""
        defaults = DefaultsConfig(
            timeout_seconds=7200,
            log_sync_interval=15,
            backend="gce",
        )
        assert defaults.timeout_seconds == 7200
        assert defaults.log_sync_interval == 15
        assert defaults.backend == "gce"

    def test_defaults_config_with_default_values(self) -> None:
        """DefaultsConfig uses sensible defaults when no values provided."""
        defaults = DefaultsConfig()
        assert defaults.timeout_seconds == 3600  # 1 hour default
        assert defaults.log_sync_interval == 10  # 10 seconds default
        assert defaults.backend == "local"  # Safe default

    def test_defaults_config_timeout_must_be_positive(self) -> None:
        """DefaultsConfig rejects non-positive timeout values."""
        with pytest.raises(ValidationError) as exc_info:
            DefaultsConfig(timeout_seconds=0)
        assert "timeout_seconds" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            DefaultsConfig(timeout_seconds=-1)
        assert "timeout_seconds" in str(exc_info.value)

    def test_defaults_config_log_sync_interval_must_be_positive(self) -> None:
        """DefaultsConfig rejects non-positive log_sync_interval values."""
        with pytest.raises(ValidationError) as exc_info:
            DefaultsConfig(log_sync_interval=0)
        assert "log_sync_interval" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            DefaultsConfig(log_sync_interval=-1)
        assert "log_sync_interval" in str(exc_info.value)

    def test_defaults_config_backend_validates_allowed_values(self) -> None:
        """DefaultsConfig only accepts valid backend values."""
        # Valid backends
        for backend in ("local", "gce", "kubernetes"):
            defaults = DefaultsConfig(backend=backend)  # type: ignore[arg-type]
            assert defaults.backend == backend

        # Invalid backend
        with pytest.raises(ValidationError) as exc_info:
            DefaultsConfig(backend="invalid")  # type: ignore[arg-type]
        assert "backend" in str(exc_info.value)

    def test_defaults_config_rejects_unknown_fields(self) -> None:
        """DefaultsConfig follows extra=forbid pattern."""
        with pytest.raises(ValidationError) as exc_info:
            DefaultsConfig(unknown_field="value")  # type: ignore[call-arg]
        assert "extra" in str(exc_info.value).lower() or "unknown_field" in str(exc_info.value)


class TestGoldfishConfigWithDefaults:
    """Tests for defaults integration in GoldfishConfig."""

    def test_goldfish_config_accepts_defaults_section(self) -> None:
        """GoldfishConfig accepts a defaults section."""
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            defaults=DefaultsConfig(
                timeout_seconds=7200,
                log_sync_interval=15,
                backend="gce",
            ),
        )
        assert config.defaults is not None
        assert config.defaults.timeout_seconds == 7200
        assert config.defaults.log_sync_interval == 15
        assert config.defaults.backend == "gce"

    def test_goldfish_config_defaults_is_optional(self) -> None:
        """GoldfishConfig works without defaults section (backwards compatible)."""
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
        )
        # defaults should have a default value for backwards compatibility
        assert config.defaults is not None
        assert config.defaults.timeout_seconds == 3600
        assert config.defaults.log_sync_interval == 10
        assert config.defaults.backend == "local"


class TestDefaultsConfigLoading:
    """Tests for loading defaults from goldfish.yaml."""

    def test_load_config_with_defaults_section(self, tmp_path) -> None:
        """GoldfishConfig.load() parses defaults section from YAML."""
        config_content = """
project_name: test-project
dev_repo_path: ../test-dev
defaults:
  timeout_seconds: 7200
  log_sync_interval: 15
  backend: gce
"""
        config_path = tmp_path / "goldfish.yaml"
        config_path.write_text(config_content)

        config = GoldfishConfig.load(tmp_path)

        assert config.defaults is not None
        assert config.defaults.timeout_seconds == 7200
        assert config.defaults.log_sync_interval == 15
        assert config.defaults.backend == "gce"

    def test_load_config_without_defaults_section(self, tmp_path) -> None:
        """GoldfishConfig.load() works without defaults section (backwards compatible)."""
        config_content = """
project_name: test-project
dev_repo_path: ../test-dev
"""
        config_path = tmp_path / "goldfish.yaml"
        config_path.write_text(config_content)

        config = GoldfishConfig.load(tmp_path)

        # Should get default DefaultsConfig
        assert config.defaults is not None
        assert config.defaults.timeout_seconds == 3600
        assert config.defaults.log_sync_interval == 10
        assert config.defaults.backend == "local"

    def test_load_config_with_partial_defaults(self, tmp_path) -> None:
        """GoldfishConfig.load() handles partial defaults (uses defaults for unspecified)."""
        config_content = """
project_name: test-project
dev_repo_path: ../test-dev
defaults:
  timeout_seconds: 5400
"""
        config_path = tmp_path / "goldfish.yaml"
        config_path.write_text(config_content)

        config = GoldfishConfig.load(tmp_path)

        assert config.defaults is not None
        assert config.defaults.timeout_seconds == 5400  # Specified
        assert config.defaults.log_sync_interval == 10  # Default
        assert config.defaults.backend == "local"  # Default

    def test_load_config_with_invalid_defaults_backend(self, tmp_path) -> None:
        """GoldfishConfig.load() rejects invalid backend in defaults."""
        config_content = """
project_name: test-project
dev_repo_path: ../test-dev
defaults:
  backend: invalid_backend
"""
        config_path = tmp_path / "goldfish.yaml"
        config_path.write_text(config_content)

        from goldfish.errors import GoldfishError

        with pytest.raises(GoldfishError) as exc_info:
            GoldfishConfig.load(tmp_path)
        assert "defaults" in str(exc_info.value).lower() or "backend" in str(exc_info.value).lower()
