"""Goldfish MCP tools - Web Visualization Tools

Tools for managing the web visualization server.
"""

import logging

from goldfish.server import _get_project_root, mcp

logger = logging.getLogger("goldfish.server")


@mcp.tool()
def start_web_server(port: int = 8080, open_browser: bool = True) -> dict:
    """Start the Goldfish web visualization server.

    Launches a beautiful web interface for exploring provenance data,
    workspaces, runs, and lineage graphs.

    Args:
        port: Port to listen on (default: 8080)
        open_browser: Whether to open browser automatically (default: True)

    Returns:
        dict with status and URL to access the server
    """
    from goldfish.web_server import is_web_server_running, spawn_web_server

    project_root = _get_project_root()

    # Check if already running
    running, pid, existing_port = is_web_server_running(project_root)
    if running:
        return {
            "status": "already_running",
            "message": f"Web server already running (pid={pid})",
            "url": f"http://127.0.0.1:{existing_port}",
            "port": existing_port,
            "pid": pid,
        }

    # Start the server
    try:
        pid = spawn_web_server(project_root, port=port, open_browser=open_browser)

        return {
            "status": "started",
            "message": f"Web server started (pid={pid})",
            "url": f"http://127.0.0.1:{port}",
            "port": port,
            "pid": pid,
        }
    except Exception as e:
        logger.exception("Failed to start web server: %s", e)
        return {"status": "error", "message": f"Failed to start web server: {e}"}


@mcp.tool()
def get_web_server_status() -> dict:
    """Check the status of the web visualization server.

    Returns:
        dict with running status, PID, port, and URL if running
    """
    from goldfish.web_server import is_web_server_running

    project_root = _get_project_root()

    running, pid, port = is_web_server_running(project_root)

    if running:
        return {
            "status": "running",
            "pid": pid,
            "port": port,
            "url": f"http://127.0.0.1:{port}",
            "message": f"Web server is running (pid={pid}, port={port})",
        }
    else:
        return {
            "status": "stopped",
            "pid": None,
            "port": None,
            "url": None,
            "message": "Web server is not running. Use start_web_server to launch it.",
        }


@mcp.tool()
def stop_web_server() -> dict:
    """Stop the web visualization server.

    Returns:
        dict with status of the stop operation
    """
    from goldfish.web_server import is_web_server_running, stop_web_server

    project_root = _get_project_root()

    # Check if running
    running, pid, _ = is_web_server_running(project_root)
    if not running:
        return {"status": "not_running", "message": "Web server is not running"}

    # Stop the server
    try:
        success = stop_web_server(project_root, timeout=10.0)
        if success:
            return {
                "status": "stopped",
                "message": f"Web server stopped (was pid={pid})",
                "pid": pid,
            }
        else:
            return {
                "status": "timeout",
                "message": "Web server did not stop within timeout",
                "pid": pid,
            }
    except Exception as e:
        logger.exception("Failed to stop web server: %s", e)
        return {"status": "error", "message": f"Failed to stop web server: {e}"}
