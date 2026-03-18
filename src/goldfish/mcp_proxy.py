"""Goldfish MCP Proxy - Thin client that forwards to daemon.

This is what Claude actually connects to. It:
1. Checks if daemon is running, spawns if needed
2. Forwards all tool calls to daemon via Unix socket
3. Handles reconnection if daemon restarts
4. Checks version compatibility and restarts daemon if needed
"""

import functools
import inspect
import json
import logging
import socket
import time
from pathlib import Path
from typing import Any

from fastmcp import FastMCP

from goldfish.daemon import (
    DAEMON_PROTOCOL_VERSION,
    get_socket_path,
    is_daemon_running,
    spawn_daemon,
    stop_daemon,
)
from goldfish.errors import GoldfishError, ProjectNotInitializedError
from goldfish.logging import setup_logging

logger = logging.getLogger("goldfish.proxy")

# Global state for the proxy
_daemon_socket_path: Path | None = None
_project_root: Path | None = None

# Connection settings
SOCKET_CONNECT_TIMEOUT = 5.0
SOCKET_READ_TIMEOUT = 300.0  # 5 minutes for long-running tools
MAX_RECONNECT_ATTEMPTS = 3


def _get_version() -> str:
    """Get goldfish package version."""
    try:
        from importlib.metadata import version

        return version("goldfish")
    except Exception:
        return "unknown"


class DaemonConnection:
    """Manages connection to the daemon via Unix socket."""

    def __init__(self, socket_path: Path, project_root: Path, force_restart: bool = False):
        logger.debug("Creating DaemonConnection to %s", socket_path)
        self.socket_path = socket_path
        self.project_root = project_root
        self._force_restart = force_restart
        self._ensure_daemon()

    def _ensure_daemon(self) -> None:
        """Ensure daemon is running with compatible version, spawn if needed."""
        running, pid = is_daemon_running(self.project_root)
        if running:
            logger.debug("Daemon already running (pid=%d)", pid)

            # Force restart if requested (useful for development)
            if self._force_restart:
                logger.info("Force restart requested, restarting daemon...")
                self._restart_daemon()
                return

            # Check version compatibility
            try:
                health = self._health_check()
                daemon_version = health.get("version", "unknown")
                daemon_protocol = health.get("protocol_version", "0.0")
                proxy_version = _get_version()

                if daemon_protocol != DAEMON_PROTOCOL_VERSION:
                    logger.warning(
                        "Daemon protocol mismatch (daemon=%s, proxy=%s), restarting...",
                        daemon_protocol,
                        DAEMON_PROTOCOL_VERSION,
                    )
                    self._restart_daemon()
                elif daemon_version != proxy_version and proxy_version != "unknown":
                    logger.warning(
                        "Daemon version mismatch (daemon=%s, proxy=%s), restarting...",
                        daemon_version,
                        proxy_version,
                    )
                    self._restart_daemon()
                else:
                    return
            except Exception as e:
                logger.warning("Health check failed, restarting daemon: %s", e)
                self._restart_daemon()
                return

        logger.info("Daemon not running, spawning...")
        spawn_daemon(self.project_root)

        # Wait for daemon to be ready
        for _ in range(20):  # 10 seconds max
            if self.socket_path.exists():
                try:
                    self._health_check()
                    logger.info("Daemon started successfully")
                    return
                except Exception:
                    pass
            time.sleep(0.5)

        raise GoldfishError("Failed to start daemon")

    def _restart_daemon(self) -> None:
        """Stop and restart the daemon."""
        logger.info("Restarting daemon...")
        stop_daemon(self.project_root, timeout=10.0)
        time.sleep(0.5)
        spawn_daemon(self.project_root)

        # Wait for new daemon to be ready
        for _ in range(20):
            if self.socket_path.exists():
                try:
                    self._health_check()
                    logger.info("Daemon restarted successfully")
                    return
                except Exception:
                    pass
            time.sleep(0.5)

        raise GoldfishError("Failed to restart daemon")

    def _health_check(self) -> dict[str, Any]:
        """Check daemon health."""
        logger.debug("Performing health check")
        result: dict[str, Any] = self._request("GET", "/health")
        return result

    def _request(self, method: str, path: str, body: dict | None = None) -> Any:
        """Make HTTP request to daemon over Unix socket."""
        logger.debug("Making %s request to %s", method, path)
        start_time = time.time()

        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(SOCKET_CONNECT_TIMEOUT)

        try:
            logger.debug("Connecting to daemon socket: %s", self.socket_path)
            sock.connect(str(self.socket_path))
            sock.settimeout(SOCKET_READ_TIMEOUT)

            # Build HTTP request
            if body:
                body_bytes = json.dumps(body).encode()
                request = (
                    f"{method} {path} HTTP/1.1\r\n"
                    f"Host: localhost\r\n"
                    f"Content-Type: application/json\r\n"
                    f"Content-Length: {len(body_bytes)}\r\n"
                    f"Connection: close\r\n"
                    f"\r\n"
                ).encode() + body_bytes
            else:
                request = (
                    f"{method} {path} HTTP/1.1\r\n" f"Host: localhost\r\n" f"Connection: close\r\n" f"\r\n"
                ).encode()

            sock.sendall(request)

            # Read response
            response = b""
            while True:
                try:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    response += chunk

                    # Check if we have complete response
                    if b"\r\n\r\n" in response:
                        header_end = response.index(b"\r\n\r\n")
                        headers = response[:header_end].decode()

                        # Parse content length
                        content_length = 0
                        for line in headers.split("\r\n"):
                            if line.lower().startswith("content-length:"):
                                content_length = int(line.split(":")[1].strip())
                                break

                        body_start = header_end + 4
                        if len(response) >= body_start + content_length:
                            break
                except TimeoutError as e:
                    raise GoldfishError("Timeout waiting for daemon response") from e

            # Parse response
            if b"\r\n\r\n" not in response:
                raise GoldfishError("Invalid response from daemon")

            header_end = response.index(b"\r\n\r\n")
            headers = response[:header_end].decode()
            body_bytes_resp = response[header_end + 4 :]

            # Check status
            status_line = headers.split("\r\n")[0]
            status_code = int(status_line.split()[1])

            if body_bytes_resp:
                result = json.loads(body_bytes_resp)
            else:
                result = {}

            if status_code >= 400:
                error_msg = result.get("error", f"HTTP {status_code}")
                logger.debug("Request failed with status %d: %s", status_code, error_msg)
                raise GoldfishError(error_msg)

            elapsed = time.time() - start_time
            logger.debug("Request completed in %.3fs (status=%d)", elapsed, status_code)
            return result

        finally:
            sock.close()

    def call_tool(self, tool_name: str, params: dict) -> Any:
        """Call a tool on the daemon with reconnection support."""
        logger.debug("Calling tool via proxy: %s", tool_name)

        # Errors that indicate connection issues (should trigger reconnect)
        connection_errors = (
            ConnectionRefusedError,
            ConnectionResetError,
            BrokenPipeError,
            FileNotFoundError,
            TimeoutError,
        )

        last_error: Exception | None = None
        for attempt in range(MAX_RECONNECT_ATTEMPTS):
            try:
                result = self._request("POST", "/tool", {"tool": tool_name, "params": params})
                logger.debug("Tool %s completed successfully", tool_name)
                return result.get("result")
            except connection_errors as e:
                last_error = e
                if attempt < MAX_RECONNECT_ATTEMPTS - 1:
                    logger.warning(
                        "Daemon connection lost (attempt %d/%d), reconnecting: %s",
                        attempt + 1,
                        MAX_RECONNECT_ATTEMPTS,
                        e,
                    )
                    time.sleep(0.5 * (attempt + 1))  # Backoff
                    self._ensure_daemon()
            except OSError as e:
                # Check if it's a connection-related error
                if e.errno in (
                    111,  # Connection refused
                    104,  # Connection reset
                    32,  # Broken pipe
                ):
                    last_error = e
                    if attempt < MAX_RECONNECT_ATTEMPTS - 1:
                        logger.warning(
                            "Daemon connection error (attempt %d/%d), reconnecting: %s",
                            attempt + 1,
                            MAX_RECONNECT_ATTEMPTS,
                            e,
                        )
                        time.sleep(0.5 * (attempt + 1))
                        self._ensure_daemon()
                else:
                    raise

        # All attempts failed
        logger.error(
            "Failed to call tool %s after %d attempts: %s",
            tool_name,
            MAX_RECONNECT_ATTEMPTS,
            last_error,
        )
        raise GoldfishError(f"Failed to connect to daemon after {MAX_RECONNECT_ATTEMPTS} attempts: {last_error}")

    def get_tools(self) -> list[str]:
        """Get list of available tools."""
        logger.debug("Getting list of available tools from daemon")
        result = self._request("GET", "/tools")
        tools: list[str] = result.get("tools", [])
        logger.debug("Daemon has %d tools available", len(tools))
        return tools


# Create FastMCP server
mcp = FastMCP("goldfish")


def _get_connection() -> DaemonConnection:
    """Get or create daemon connection."""
    global _daemon_socket_path, _project_root
    if not _daemon_socket_path or not _project_root:
        raise GoldfishError("Proxy not initialized")
    # Don't force restart on subsequent connections, only on initial startup
    return DaemonConnection(_daemon_socket_path, _project_root, force_restart=False)


def _register_proxy_tools() -> None:
    """Register all tools as proxies to daemon."""
    # Import server which imports tools at module load time.
    # Do NOT import goldfish.server_tools first - causes circular import.
    # For each tool in the original MCP, create a proxy version
    import asyncio

    from goldfish.server import mcp as original_mcp

    tools = asyncio.run(original_mcp._list_tools())
    for tool in tools:
        tool_name = tool.name
        original_fn = tool.fn

        # Create a proxy function that forwards to daemon
        def make_proxy(name: str, orig_fn: Any) -> Any:
            @functools.wraps(orig_fn)
            def forwarding_fn(**kwargs: Any) -> Any:
                conn = _get_connection()
                return conn.call_tool(name, kwargs)

            # Preserve the original signature for MCP schema generation
            forwarding_fn.__signature__ = inspect.signature(orig_fn)  # type: ignore[attr-defined]
            return forwarding_fn

        wrapped_fn = make_proxy(tool_name, original_fn)

        # Register with our MCP
        mcp.tool()(wrapped_fn)

    logger.info("Registered %d proxy tools", len(tools))


def run_proxy(project_root: Path, force_restart: bool = False) -> None:
    """Run the MCP proxy server.

    Args:
        project_root: Path to the project root directory.
        force_restart: If True, restart the daemon even if it's already running.
            Useful during development to pick up code changes.
    """
    global _daemon_socket_path, _project_root

    setup_logging(component="proxy")
    logger.info("Starting Goldfish MCP proxy for %s", project_root)
    if force_restart:
        logger.info("Force restart mode enabled")

    try:
        # Validate project is initialized (this will raise if not)
        from goldfish.config import GoldfishConfig

        GoldfishConfig.load(project_root)

        # Get socket path from daemon module (uses ~/.goldfish/sockets/<hash>/)
        _daemon_socket_path = get_socket_path(project_root)
        _project_root = project_root
        logger.debug("Using socket path: %s", _daemon_socket_path)

        # Ensure daemon is running (restart if force_restart=True)
        conn = DaemonConnection(_daemon_socket_path, project_root, force_restart=force_restart)
        health = conn._health_check()
        logger.info(
            "Connected to daemon (pid=%s, version=%s)",
            health.get("pid"),
            health.get("version"),
        )

        # Register proxy tools
        _register_proxy_tools()

        # Run MCP server
        mcp.run(transport="stdio")

    except ProjectNotInitializedError:
        # Project not initialized - run in "uninitialized" mode
        # Just expose initialize_project tool
        logger.info("Project not initialized, running in setup mode")
        _project_root = project_root

        @mcp.tool()
        def initialize_project(project_name: str, project_root: str, from_existing: str | None = None) -> dict:
            """Initialize a new Goldfish project."""
            from goldfish.init import init_from_existing, init_project

            project_path = Path(project_root).resolve()

            if from_existing:
                source_path = Path(from_existing)
                init_from_existing(project_path, source_path)
                message = f"Initialized '{project_name}' from {from_existing}"
            else:
                init_project(project_name, project_path)
                message = f"Initialized '{project_name}'"

            return {
                "success": True,
                "message": message,
                "project_path": str(project_path),
                "next_step": "Restart the MCP server to load the project",
            }

        mcp.run(transport="stdio")
