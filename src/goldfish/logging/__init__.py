"""Goldfish centralized logging system.

Logs to VictoriaLogs for cross-instance visibility and debugging.
"""

from .service import LOGSQL_GUIDE, search_logs, search_logs_sync
from .settings import clear_settings_cache, get_settings
from .setup import get_instance_id, setup_logging, shutdown_logging

__all__ = [
    "setup_logging",
    "shutdown_logging",
    "get_instance_id",
    "get_settings",
    "clear_settings_cache",
    "search_logs",
    "search_logs_sync",
    "LOGSQL_GUIDE",
]
