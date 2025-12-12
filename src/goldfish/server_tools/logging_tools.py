"""Goldfish MCP tools - Logging Tools

Provides tools for searching centralized logs via VictoriaLogs.
"""

import logging

from goldfish.logging.service import LOGSQL_GUIDE, search_logs_sync
from goldfish.server import mcp

logger = logging.getLogger("goldfish.server")


@mcp.tool()
def search_goldfish_logs(query: str) -> dict:
    """Search centralized Goldfish logs using LogsQL.

    All Goldfish instances (server, worker, stages) log to a central
    VictoriaLogs instance. Use this to debug issues, trace execution,
    and understand what's happening across the system.

    Args:
        query: LogsQL query string. Must include a time filter like _time:5m.
               Example: '_time:30m {app="goldfish"} error'

    Returns:
        dict with 'results' containing formatted log output, or 'error' on failure.

    Quick Reference:
    - Time filter: _time:5m, _time:1h, _time:24h
    - Label filter: {app="goldfish"} {component="worker"}
    - Text search: error, "connection refused"
    - Field match: status:=500, level:ERROR
    - Pipes: | head 20, | sort by (_time desc), | stats count() by (component)

    Examples:
        # Last 20 errors from any Goldfish component
        _time:30m {app="goldfish"} error | head 20

        # Worker logs from last hour
        _time:1h {app="goldfish"} {component="worker"}

        # Stage execution logs
        _time:2h {component="stage"} | sort by (_time desc)

        # Error rate by component
        _time:1h {app="goldfish"} error | stats count() by (component)
    """
    if not query or not query.strip():
        return {
            "error": "Missing required parameter: query",
            "help": LOGSQL_GUIDE,
        }

    try:
        result = search_logs_sync(query)
        return {"results": result}
    except Exception as e:
        logger.exception("Log search failed: %s", e)
        return {
            "error": str(e),
            "help": "Ensure VictoriaLogs is running and GOLDFISH_VICTORIA_LOGS_URL is set correctly.",
        }


@mcp.tool()
def get_logsql_guide() -> dict:
    """Get LogsQL query language reference.

    Returns a quick reference guide for writing LogsQL queries
    to search Goldfish logs.

    Returns:
        dict with 'guide' containing the LogsQL reference.
    """
    return {"guide": LOGSQL_GUIDE}
