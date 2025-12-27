"""Goldfish MCP tools - Logging Tools

Provides tools for searching centralized logs via VictoriaLogs.
"""

import logging

from goldfish.logging.service import LOGSQL_GUIDE, search_logs_sync
from goldfish.server_core import mcp

logger = logging.getLogger("goldfish.server")


@mcp.tool()
def search_goldfish_logs(query: str | None = None, show_guide: bool = False) -> dict:
    """Search centralized Goldfish logs using LogsQL.

    Args:
        query: LogsQL query string. Must include a time filter like _time:5m.
        show_guide: If True, returns the LogsQL query guide instead of searching.

    Returns:
        dict with 'results' or 'guide'.
    """
    if show_guide:
        return {"guide": LOGSQL_GUIDE}

    if not query or not query.strip():
        return {
            "error": "Missing required parameter: query. Use show_guide=True for help.",
            "help": "Example: _time:30m {app='goldfish'} error",
        }

    try:
        result = search_logs_sync(query)
        return {"results": result}
    except Exception as e:
        logger.exception("Log search failed: %s", e)
        return {
            "error": str(e),
            "help": "Ensure VictoriaLogs is running.",
        }
