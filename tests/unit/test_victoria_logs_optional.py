"""Tests that VictoriaLogs is truly optional — goldfish must never crash without it.

Bug: victoria_logs_enabled defaults to True, causing goldfish to fail when
VictoriaLogs is not running. VictoriaLogs should default to disabled.
"""

from __future__ import annotations

import os
from unittest.mock import patch


def test_victoria_logs_disabled_by_default() -> None:
    """VictoriaLogs should be disabled by default so goldfish works without it."""
    from goldfish.logging.settings import clear_settings_cache, get_settings

    clear_settings_cache()
    # No env vars set — should default to disabled
    with patch.dict(os.environ, {}, clear=True):
        clear_settings_cache()
        settings = get_settings()
        assert settings.logging.victoria_logs_enabled is False
    clear_settings_cache()


def test_victoria_logs_enabled_when_explicitly_set() -> None:
    """VictoriaLogs can be enabled via environment variable."""
    from goldfish.logging.settings import clear_settings_cache, get_settings

    clear_settings_cache()
    with patch.dict(os.environ, {"GOLDFISH_VICTORIA_LOGS_ENABLED": "true"}, clear=True):
        clear_settings_cache()
        settings = get_settings()
        assert settings.logging.victoria_logs_enabled is True
    clear_settings_cache()


def test_search_logs_when_disabled_returns_helpful_message() -> None:
    """search_goldfish_logs tool should return a clear message when VictoriaLogs is disabled."""
    from goldfish.logging.settings import clear_settings_cache

    clear_settings_cache()
    with patch.dict(os.environ, {"GOLDFISH_VICTORIA_LOGS_ENABLED": "false"}, clear=True):
        clear_settings_cache()
        from goldfish.logging.service import search_logs_sync

        result = search_logs_sync("_time:5m error")
        assert "disabled" in result.lower() or "not enabled" in result.lower()
    clear_settings_cache()


def test_setup_logging_when_disabled_does_not_crash() -> None:
    """setup_logging should work fine when VictoriaLogs is disabled."""
    import logging

    from goldfish.logging.settings import clear_settings_cache
    from goldfish.logging.setup import setup_logging, shutdown_logging

    app_logger = logging.getLogger("goldfish")
    orig_handlers = list(app_logger.handlers)
    orig_propagate = app_logger.propagate
    orig_level = app_logger.level

    clear_settings_cache()
    try:
        with patch.dict(os.environ, {"GOLDFISH_VICTORIA_LOGS_ENABLED": "false"}, clear=True):
            clear_settings_cache()
            # Reset logging state
            import goldfish.logging.setup as setup_mod

            setup_mod._LOGGING_INITIALIZED = False

            setup_logging(component="test")

            # Should have a stderr handler, not a Loki handler
            assert len(app_logger.handlers) >= 1
            assert any(isinstance(h, logging.StreamHandler) for h in app_logger.handlers)

            shutdown_logging()
    finally:
        # Restore logger state to avoid polluting other tests
        app_logger.handlers = orig_handlers
        app_logger.propagate = orig_propagate
        app_logger.setLevel(orig_level)
        clear_settings_cache()
