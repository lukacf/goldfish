"""Logging settings - environment-based for cross-project consistency."""

import os
from dataclasses import dataclass, field
from functools import lru_cache


@dataclass
class DeveloperModeSettings:
    """Developer mode settings for log querying."""

    enabled: bool = True  # Enable by default for Goldfish


@dataclass
class LoggingSettings:
    """Logging configuration from environment variables."""

    level: str = "INFO"
    victoria_logs_enabled: bool = False
    victoria_logs_url: str = "http://localhost:9428"
    loki_app_tag: str = "goldfish"
    project_path: str | None = None
    developer_mode: DeveloperModeSettings = field(default_factory=DeveloperModeSettings)


@dataclass
class DevSettings:
    """Development/CI settings."""

    ci_e2e: bool = False


@dataclass
class Settings:
    """Global settings container."""

    logging: LoggingSettings
    dev: DevSettings


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get settings from environment variables.

    Environment variables:
        GOLDFISH_LOG_LEVEL: Logging level (default: INFO)
        GOLDFISH_VICTORIA_LOGS_ENABLED: Enable VictoriaLogs (default: true)
        GOLDFISH_VICTORIA_LOGS_URL: VictoriaLogs URL (default: http://localhost:9428)
        GOLDFISH_CI_E2E: Running in E2E test mode (default: false)
    """
    log_level = os.environ.get("GOLDFISH_LOG_LEVEL", "INFO")
    victoria_enabled = os.environ.get("GOLDFISH_VICTORIA_LOGS_ENABLED", "false").lower() == "true"
    victoria_url = os.environ.get("GOLDFISH_VICTORIA_LOGS_URL", "http://localhost:9428")
    ci_e2e = os.environ.get("CI_E2E", "0") == "1"
    project_path = os.environ.get("GOLDFISH_PROJECT_PATH")

    return Settings(
        logging=LoggingSettings(
            level=log_level,
            victoria_logs_enabled=victoria_enabled,
            victoria_logs_url=victoria_url,
            project_path=project_path,
            developer_mode=DeveloperModeSettings(enabled=True),
        ),
        dev=DevSettings(ci_e2e=ci_e2e),
    )


def clear_settings_cache() -> None:
    """Clear the settings cache (useful for testing)."""
    get_settings.cache_clear()
