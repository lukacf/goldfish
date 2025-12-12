"""Goldfish Web Server - Provenance visualization interface.

This is a singleton web server that:
- Provides a beautiful UI for exploring provenance
- Follows the same Unix socket singleton pattern as the daemon
- Serves HTTP on localhost for the visualization interface
- Reads from the same database as the daemon (read-only)
"""

from __future__ import annotations

import fcntl
import http.server
import json
import logging
import os
import signal
import socketserver
import sys
import threading
import time
import webbrowser
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from goldfish.config import GoldfishConfig
from goldfish.db.database import Database
from goldfish.errors import ProjectNotInitializedError

logger = logging.getLogger("goldfish.web")

# Web server version
WEB_SERVER_VERSION = "1.0"

# Default port for web UI
DEFAULT_WEB_PORT = 8080


def _get_socket_dir(project_root: Path) -> Path:
    """Get the directory for web server socket/pid/lock files."""
    import hashlib

    path_hash = hashlib.sha256(str(project_root.resolve()).encode()).hexdigest()[:12]
    socket_dir = Path.home() / ".goldfish" / "web-sockets" / path_hash
    socket_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(socket_dir, 0o700)
    return socket_dir


def get_web_pid_file(project_root: Path) -> Path:
    """Get the PID file path for the web server."""
    return _get_socket_dir(project_root) / "web.pid"


def get_web_lock_file(project_root: Path) -> Path:
    """Get the lock file path for the web server."""
    return _get_socket_dir(project_root) / "web.lock"


def get_web_port_file(project_root: Path) -> Path:
    """Get the port file path for the web server."""
    return _get_socket_dir(project_root) / "web.port"


def is_web_server_running(project_root: Path) -> tuple[bool, int | None, int | None]:
    """Check if web server is running for a project.

    Returns:
        Tuple of (is_running, pid, port)
    """
    try:
        pid_file = get_web_pid_file(project_root)
        port_file = get_web_port_file(project_root)

        if not pid_file.exists():
            return False, None, None

        pid = int(pid_file.read_text().strip())

        # Check if process is alive
        try:
            os.kill(pid, 0)
        except (ProcessLookupError, PermissionError):
            return False, None, None

        # Get port
        port = None
        if port_file.exists():
            port = int(port_file.read_text().strip())

        return True, pid, port

    except (ValueError, ProjectNotInitializedError):
        return False, None, None


def stop_web_server(project_root: Path, timeout: float = 10.0) -> bool:
    """Stop the web server and wait for it to exit.

    Args:
        project_root: Project root directory
        timeout: Maximum seconds to wait for server to exit

    Returns:
        True if server stopped successfully, False if timeout
    """
    logger.debug("Stopping web server for project: %s", project_root)

    running, pid, _ = is_web_server_running(project_root)
    if not running or pid is None:
        logger.debug("Web server not running, nothing to stop")
        return True

    logger.debug("Sending SIGTERM to web server pid=%d", pid)
    os.kill(pid, signal.SIGTERM)

    # Wait for exit
    start = time.time()
    while time.time() - start < timeout:
        try:
            os.kill(pid, 0)
            time.sleep(0.1)
        except ProcessLookupError:
            elapsed = time.time() - start
            logger.debug("Web server stopped after %.2fs", elapsed)
            return True

    logger.warning("Web server did not stop within %.1fs timeout", timeout)
    return False


class ProvenanceRequestHandler(http.server.BaseHTTPRequestHandler):
    """Handle HTTP requests for the provenance UI."""

    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        """Custom logging to avoid stderr spam."""
        logger.debug("Request: %s", format % args)

    def _send_json(self, data: Any, status: int = 200) -> None:
        """Send JSON response."""
        payload = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(payload)

    def _send_html(self, html: str, status: int = 200) -> None:
        """Send HTML response."""
        payload = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_error(self, status: int, message: str) -> None:
        """Send error response."""
        self._send_json({"error": message}, status)

    def do_GET(self) -> None:
        """Handle GET requests."""
        parsed = urlparse(self.path)
        path = parsed.path
        query_params = parse_qs(parsed.query)

        server = self.server  # type: ignore[attr-defined]
        db: Database = server.db  # type: ignore[attr-defined]

        try:
            # Serve the UI
            if path == "/" or path == "/index.html":
                self._send_html(get_ui_html())
                return

            # API endpoints
            elif path == "/api/health":
                self._send_json(
                    {
                        "status": "healthy",
                        "version": WEB_SERVER_VERSION,
                        "pid": os.getpid(),
                        "project": str(server.project_root),  # type: ignore[attr-defined]
                    }
                )

            elif path == "/api/workspaces":
                workspaces = self._get_workspaces(db)
                self._send_json({"workspaces": workspaces})

            elif path.startswith("/api/workspace/"):
                workspace_name = path.split("/")[-1]
                details = self._get_workspace_details(db, workspace_name)
                self._send_json(details)

            elif path == "/api/runs":
                limit = int(query_params.get("limit", ["100"])[0])
                runs = self._get_stage_runs(db, limit)
                self._send_json({"runs": runs})

            elif path.startswith("/api/run/"):
                run_id = path.split("/")[-1]
                details = self._get_run_details(db, run_id)
                self._send_json(details)

            elif path == "/api/pipelines":
                limit = int(query_params.get("limit", ["100"])[0])
                pipelines = self._get_pipeline_runs(db, limit)
                self._send_json({"pipelines": pipelines})

            elif path.startswith("/api/pipeline/"):
                pipeline_id = path.split("/")[-1]
                details = self._get_pipeline_details(db, pipeline_id)
                self._send_json(details)

            elif path == "/api/graph":
                workspace = query_params.get("workspace", [None])[0]
                graph = self._get_provenance_graph(db, workspace)
                self._send_json(graph)

            else:
                self._send_error(404, "Not found")

        except Exception as e:
            logger.exception("Request error: %s", e)
            self._send_error(500, str(e))

    def _get_workspaces(self, db: Database) -> list[dict]:
        """Get list of all workspaces."""
        with db._conn() as conn:
            rows = conn.execute(
                """
                SELECT wl.workspace_name, wl.description, wl.created_at,
                       wl.parent_workspace, wl.parent_version,
                       COUNT(DISTINCT wv.version) as version_count,
                       MAX(wv.created_at) as last_version_at,
                       wm.status as mount_status
                FROM workspace_lineage wl
                LEFT JOIN workspace_versions wv ON wl.workspace_name = wv.workspace_name
                LEFT JOIN workspace_mounts wm ON wl.workspace_name = wm.workspace_name
                GROUP BY wl.workspace_name
                ORDER BY wl.created_at DESC
                """
            ).fetchall()

        return [
            {
                "name": r["workspace_name"],
                "description": r["description"],
                "created_at": r["created_at"],
                "parent_workspace": r["parent_workspace"],
                "parent_version": r["parent_version"],
                "version_count": r["version_count"],
                "last_version_at": r["last_version_at"],
                "mount_status": r["mount_status"],
            }
            for r in rows
        ]

    def _get_workspace_details(self, db: Database, workspace_name: str) -> dict:
        """Get detailed information about a workspace."""
        with db._conn() as conn:
            # Get workspace info
            workspace = conn.execute(
                """
                SELECT * FROM workspace_lineage
                WHERE workspace_name = ?
                """,
                (workspace_name,),
            ).fetchone()

            if not workspace:
                raise ValueError(f"Workspace '{workspace_name}' not found")

            # Get versions
            versions = conn.execute(
                """
                SELECT * FROM workspace_versions
                WHERE workspace_name = ?
                ORDER BY created_at DESC
                """,
                (workspace_name,),
            ).fetchall()

            # Get recent runs
            runs = conn.execute(
                """
                SELECT * FROM stage_runs
                WHERE workspace_name = ?
                ORDER BY started_at DESC
                LIMIT 50
                """,
                (workspace_name,),
            ).fetchall()

            return {
                "workspace": dict(workspace),
                "versions": [dict(v) for v in versions],
                "recent_runs": [dict(r) for r in runs],
            }

    def _get_stage_runs(self, db: Database, limit: int = 100) -> list[dict]:
        """Get list of stage runs."""
        with db._conn() as conn:
            rows = conn.execute(
                """
                SELECT sr.*, sv.version_num as stage_version_num
                FROM stage_runs sr
                LEFT JOIN stage_versions sv ON sr.stage_version_id = sv.id
                ORDER BY sr.started_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [dict(r) for r in rows]

    def _get_run_details(self, db: Database, run_id: str) -> dict:
        """Get detailed information about a stage run."""
        with db._conn() as conn:
            # Get run info
            run = conn.execute(
                """
                SELECT sr.*, sv.version_num as stage_version_num,
                       sv.config_hash
                FROM stage_runs sr
                LEFT JOIN stage_versions sv ON sr.stage_version_id = sv.id
                WHERE sr.id = ?
                """,
                (run_id,),
            ).fetchone()

            if not run:
                raise ValueError(f"Run '{run_id}' not found")

            # Get signal lineage (inputs and outputs)
            signals = conn.execute(
                """
                SELECT * FROM signal_lineage
                WHERE stage_run_id = ? OR consumed_by = ?
                """,
                (run_id, run_id),
            ).fetchall()

            return {
                "run": dict(run),
                "signals": [dict(s) for s in signals],
            }

    def _get_pipeline_runs(self, db: Database, limit: int = 100) -> list[dict]:
        """Get list of pipeline runs."""
        with db._conn() as conn:
            rows = conn.execute(
                """
                SELECT pr.*,
                       COUNT(DISTINCT psq.stage_name) as total_stages,
                       SUM(CASE WHEN psq.status = 'completed' THEN 1 ELSE 0 END) as completed_stages
                FROM pipeline_runs pr
                LEFT JOIN pipeline_stage_queue psq ON pr.id = psq.pipeline_run_id
                GROUP BY pr.id
                ORDER BY pr.started_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [dict(r) for r in rows]

    def _get_pipeline_details(self, db: Database, pipeline_id: str) -> dict:
        """Get detailed information about a pipeline run."""
        with db._conn() as conn:
            # Get pipeline info
            pipeline = conn.execute(
                """
                SELECT * FROM pipeline_runs
                WHERE id = ?
                """,
                (pipeline_id,),
            ).fetchone()

            if not pipeline:
                raise ValueError(f"Pipeline '{pipeline_id}' not found")

            # Get stage queue
            stages = conn.execute(
                """
                SELECT psq.*, sr.started_at as stage_started_at,
                       sr.completed_at as stage_completed_at
                FROM pipeline_stage_queue psq
                LEFT JOIN stage_runs sr ON psq.stage_run_id = sr.id
                WHERE psq.pipeline_run_id = ?
                ORDER BY psq.id
                """,
                (pipeline_id,),
            ).fetchall()

            return {
                "pipeline": dict(pipeline),
                "stages": [dict(s) for s in stages],
            }

    def _get_provenance_graph(self, db: Database, workspace: str | None = None) -> dict:
        """Get full provenance graph for visualization."""
        with db._conn() as conn:
            # Build nodes (stage runs)
            if workspace:
                runs = conn.execute(
                    """
                    SELECT sr.id, sr.workspace_name, sr.stage_name, sr.status,
                           sr.started_at, sr.completed_at, sr.pipeline_name,
                           sv.version_num as stage_version
                    FROM stage_runs sr
                    LEFT JOIN stage_versions sv ON sr.stage_version_id = sv.id
                    WHERE sr.workspace_name = ?
                    ORDER BY sr.started_at
                    """,
                    (workspace,),
                ).fetchall()
            else:
                runs = conn.execute(
                    """
                    SELECT sr.id, sr.workspace_name, sr.stage_name, sr.status,
                           sr.started_at, sr.completed_at, sr.pipeline_name,
                           sv.version_num as stage_version
                    FROM stage_runs sr
                    LEFT JOIN stage_versions sv ON sr.stage_version_id = sv.id
                    ORDER BY sr.started_at DESC
                    LIMIT 500
                    """
                ).fetchall()

            nodes = [
                {
                    "id": r["id"],
                    "type": "stage_run",
                    "workspace": r["workspace_name"],
                    "stage": r["stage_name"],
                    "status": r["status"],
                    "started_at": r["started_at"],
                    "completed_at": r["completed_at"],
                    "pipeline": r["pipeline_name"],
                    "stage_version": r["stage_version"],
                }
                for r in runs
            ]

            # Build edges (signal lineage)
            edges = []
            for run in runs:
                signals = conn.execute(
                    """
                    SELECT signal_name, signal_type, source_stage_run_id
                    FROM signal_lineage
                    WHERE stage_run_id = ? AND source_stage_run_id IS NOT NULL
                    """,
                    (run["id"],),
                ).fetchall()

                for sig in signals:
                    edges.append(
                        {
                            "source": sig["source_stage_run_id"],
                            "target": run["id"],
                            "signal": sig["signal_name"],
                            "type": sig["signal_type"],
                        }
                    )

            return {"nodes": nodes, "edges": edges}


class ThreadedHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Threaded HTTP server."""

    allow_reuse_address = True
    daemon_threads = True


class GoldfishWebServer:
    """The Goldfish web visualization server."""

    def __init__(self, project_root: Path, port: int = DEFAULT_WEB_PORT):
        self.project_root = project_root.resolve()
        self.port = port
        self.start_time = time.time()
        self.shutdown_event = threading.Event()
        self.http_server: ThreadedHTTPServer | None = None

        # Paths
        self.config: GoldfishConfig | None = None
        self.dev_repo_path: Path | None = None
        self.pid_file: Path | None = None
        self.lock_file: Path | None = None
        self.port_file: Path | None = None
        self.db: Database | None = None

    def initialize(self) -> None:
        """Initialize the web server - load config, connect to DB."""
        logger.info("Initializing web server for project: %s", self.project_root)

        # Load config
        self.config = GoldfishConfig.load(self.project_root)
        self.dev_repo_path = self.config.get_dev_repo_path(self.project_root)

        # Set up paths
        self.pid_file = get_web_pid_file(self.project_root)
        self.lock_file = get_web_lock_file(self.project_root)
        self.port_file = get_web_port_file(self.project_root)

        # Connect to database (read-only)
        db_path = self.dev_repo_path / self.config.db_path
        if not db_path.exists():
            raise RuntimeError(f"Database not found: {db_path}")

        self.db = Database(db_path)

        logger.info("Web server initialized successfully")

    def write_pid_file(self) -> None:
        """Write PID file atomically."""
        if self.pid_file:
            temp_file = self.pid_file.with_suffix(".tmp")
            temp_file.write_text(str(os.getpid()))
            temp_file.rename(self.pid_file)

    def write_port_file(self) -> None:
        """Write port file atomically."""
        if self.port_file:
            temp_file = self.port_file.with_suffix(".tmp")
            temp_file.write_text(str(self.port))
            temp_file.rename(self.port_file)

    def start_http_server(self) -> None:
        """Start the HTTP server."""
        logger.info("Starting HTTP server on port %d", self.port)

        self.http_server = ThreadedHTTPServer(("127.0.0.1", self.port), ProvenanceRequestHandler)
        self.http_server.project_root = self.project_root  # type: ignore[attr-defined]
        self.http_server.db = self.db  # type: ignore[attr-defined]

        logger.info("Web server listening on http://127.0.0.1:%d", self.port)

    def run(self) -> None:
        """Run the web server main loop."""
        self.write_pid_file()
        self.write_port_file()
        self.start_http_server()

        # Set up signal handlers
        def handle_shutdown(signum: int, frame: Any) -> None:
            logger.info("Received signal %d, shutting down...", signum)
            threading.Thread(target=self.shutdown, daemon=True).start()

        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)

        logger.info("Web server running (pid=%d, port=%d)", os.getpid(), self.port)

        # Serve forever
        try:
            if self.http_server:
                self.http_server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            if not self.shutdown_event.is_set():
                self.shutdown()

    def shutdown(self) -> None:
        """Gracefully shutdown the web server."""
        if self.shutdown_event.is_set():
            return

        logger.info("Shutting down web server...")
        self.shutdown_event.set()

        if self.http_server:
            self.http_server.shutdown()
            self.http_server.server_close()

        # Clean up files
        if self.pid_file and self.pid_file.exists():
            self.pid_file.unlink(missing_ok=True)
        if self.port_file and self.port_file.exists():
            self.port_file.unlink(missing_ok=True)

        logger.info("Web server stopped")


def get_ui_html() -> str:
    """Get the HTML for the provenance UI."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Goldfish Provenance</title>
    <style>
        /* Dieter Rams inspired design - less but better */
        :root {
            --goldfish-orange: #FF6B35;
            --goldfish-orange-light: #FF8C5A;
            --goldfish-orange-dark: #E85A24;
            --bg-primary: #FAFAFA;
            --bg-secondary: #FFFFFF;
            --text-primary: #1A1A1A;
            --text-secondary: #6B6B6B;
            --border-color: #E0E0E0;
            --shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
            --shadow-hover: 0 2px 8px rgba(0, 0, 0, 0.12);
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
        }

        /* Header */
        header {
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border-color);
            padding: 1rem 2rem;
            position: sticky;
            top: 0;
            z-index: 100;
            box-shadow: var(--shadow);
        }

        .header-content {
            max-width: 1400px;
            margin: 0 auto;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .logo {
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }

        .logo-icon {
            width: 32px;
            height: 32px;
            background: var(--goldfish-orange);
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: 600;
            font-size: 1.2rem;
        }

        .logo h1 {
            font-size: 1.5rem;
            font-weight: 400;
            color: var(--text-primary);
        }

        .logo-accent {
            color: var(--goldfish-orange);
            font-weight: 500;
        }

        nav {
            display: flex;
            gap: 1.5rem;
        }

        nav button {
            background: none;
            border: none;
            color: var(--text-secondary);
            cursor: pointer;
            padding: 0.5rem 1rem;
            border-radius: 4px;
            transition: all 0.2s;
            font-size: 0.95rem;
        }

        nav button:hover {
            color: var(--text-primary);
            background: var(--bg-primary);
        }

        nav button.active {
            color: var(--goldfish-orange);
            font-weight: 500;
        }

        /* Main content */
        main {
            max-width: 1400px;
            margin: 2rem auto;
            padding: 0 2rem;
        }

        /* Cards */
        .card {
            background: var(--bg-secondary);
            border-radius: 8px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            box-shadow: var(--shadow);
            transition: box-shadow 0.2s;
        }

        .card:hover {
            box-shadow: var(--shadow-hover);
        }

        .card-title {
            font-size: 1.1rem;
            font-weight: 500;
            margin-bottom: 1rem;
            color: var(--text-primary);
        }

        /* Grid layouts */
        .workspace-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 1.5rem;
        }

        .workspace-card {
            background: var(--bg-secondary);
            border-radius: 8px;
            padding: 1.5rem;
            box-shadow: var(--shadow);
            transition: all 0.2s;
            cursor: pointer;
            border: 2px solid transparent;
        }

        .workspace-card:hover {
            box-shadow: var(--shadow-hover);
            border-color: var(--goldfish-orange);
            transform: translateY(-2px);
        }

        .workspace-card h3 {
            font-size: 1.2rem;
            font-weight: 500;
            margin-bottom: 0.5rem;
            color: var(--text-primary);
        }

        .workspace-card p {
            color: var(--text-secondary);
            font-size: 0.9rem;
            margin-bottom: 0.5rem;
        }

        .workspace-meta {
            display: flex;
            gap: 1rem;
            margin-top: 1rem;
            font-size: 0.85rem;
            color: var(--text-secondary);
        }

        .meta-item {
            display: flex;
            align-items: center;
            gap: 0.25rem;
        }

        /* Status badges */
        .status {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .status-completed { background: #E8F5E9; color: #2E7D32; }
        .status-running { background: #FFF3E0; color: #E65100; }
        .status-pending { background: #E3F2FD; color: #1565C0; }
        .status-failed { background: #FFEBEE; color: #C62828; }
        .status-active { background: #FFE8DC; color: var(--goldfish-orange-dark); }

        /* Graph container */
        #graph-container {
            background: var(--bg-secondary);
            border-radius: 8px;
            min-height: 600px;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: var(--shadow);
            overflow: hidden;
        }

        #graph {
            width: 100%;
            height: 600px;
        }

        /* Loading state */
        .loading {
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 3rem;
            color: var(--text-secondary);
        }

        .spinner {
            border: 3px solid var(--border-color);
            border-top: 3px solid var(--goldfish-orange);
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin-right: 1rem;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        /* Tabs */
        .tabs {
            display: flex;
            gap: 0;
            border-bottom: 2px solid var(--border-color);
            margin-bottom: 2rem;
        }

        .tab {
            padding: 1rem 1.5rem;
            background: none;
            border: none;
            color: var(--text-secondary);
            cursor: pointer;
            font-size: 1rem;
            border-bottom: 2px solid transparent;
            margin-bottom: -2px;
            transition: all 0.2s;
        }

        .tab:hover {
            color: var(--text-primary);
        }

        .tab.active {
            color: var(--goldfish-orange);
            border-bottom-color: var(--goldfish-orange);
            font-weight: 500;
        }

        .tab-content {
            display: none;
        }

        .tab-content.active {
            display: block;
        }

        /* Timeline */
        .timeline {
            position: relative;
            padding-left: 2rem;
        }

        .timeline::before {
            content: '';
            position: absolute;
            left: 0;
            top: 0;
            bottom: 0;
            width: 2px;
            background: var(--border-color);
        }

        .timeline-item {
            position: relative;
            padding-bottom: 2rem;
        }

        .timeline-item::before {
            content: '';
            position: absolute;
            left: -2.5rem;
            top: 0.5rem;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: var(--goldfish-orange);
            border: 3px solid var(--bg-primary);
        }

        .timeline-content {
            background: var(--bg-secondary);
            padding: 1rem;
            border-radius: 6px;
            box-shadow: var(--shadow);
        }

        .timeline-time {
            color: var(--text-secondary);
            font-size: 0.85rem;
            margin-bottom: 0.5rem;
        }

        /* Empty state */
        .empty-state {
            text-align: center;
            padding: 4rem 2rem;
            color: var(--text-secondary);
        }

        .empty-state-icon {
            font-size: 4rem;
            margin-bottom: 1rem;
            opacity: 0.3;
        }

        /* Utility */
        .hidden {
            display: none !important;
        }
    </style>
</head>
<body>
    <header>
        <div class="header-content">
            <div class="logo">
                <div class="logo-icon">🐠</div>
                <h1><span class="logo-accent">Goldfish</span> Provenance</h1>
            </div>
            <nav>
                <button class="active" onclick="showView('workspaces')">Workspaces</button>
                <button onclick="showView('runs')">Runs</button>
                <button onclick="showView('graph')">Graph</button>
            </nav>
        </div>
    </header>

    <main>
        <!-- Workspaces View -->
        <div id="view-workspaces">
            <h2 style="margin-bottom: 1.5rem; font-weight: 400;">Workspaces</h2>
            <div id="workspaces-container" class="workspace-grid">
                <div class="loading">
                    <div class="spinner"></div>
                    <span>Loading workspaces...</span>
                </div>
            </div>
        </div>

        <!-- Runs View -->
        <div id="view-runs" class="hidden">
            <h2 style="margin-bottom: 1.5rem; font-weight: 400;">Recent Runs</h2>
            <div class="tabs">
                <button class="tab active" onclick="showRunsTab('stage')">Stage Runs</button>
                <button class="tab" onclick="showRunsTab('pipeline')">Pipeline Runs</button>
            </div>
            <div id="tab-stage-runs" class="tab-content active">
                <div id="stage-runs-container">
                    <div class="loading">
                        <div class="spinner"></div>
                        <span>Loading stage runs...</span>
                    </div>
                </div>
            </div>
            <div id="tab-pipeline-runs" class="tab-content">
                <div id="pipeline-runs-container">
                    <div class="loading">
                        <div class="spinner"></div>
                        <span>Loading pipeline runs...</span>
                    </div>
                </div>
            </div>
        </div>

        <!-- Graph View -->
        <div id="view-graph" class="hidden">
            <h2 style="margin-bottom: 1.5rem; font-weight: 400;">Provenance Graph</h2>
            <div id="graph-container">
                <div class="loading">
                    <div class="spinner"></div>
                    <span>Loading provenance graph...</span>
                </div>
            </div>
        </div>
    </main>

    <script>
        // State
        let currentView = 'workspaces';
        let currentRunsTab = 'stage';
        let data = {
            workspaces: [],
            stageRuns: [],
            pipelineRuns: [],
            graph: null
        };

        // View switching
        function showView(view) {
            currentView = view;

            // Update nav
            document.querySelectorAll('nav button').forEach(btn => {
                btn.classList.remove('active');
            });
            event.target.classList.add('active');

            // Show/hide views
            document.getElementById('view-workspaces').classList.toggle('hidden', view !== 'workspaces');
            document.getElementById('view-runs').classList.toggle('hidden', view !== 'runs');
            document.getElementById('view-graph').classList.toggle('hidden', view !== 'graph');

            // Load data if needed
            if (view === 'workspaces' && data.workspaces.length === 0) {
                loadWorkspaces();
            } else if (view === 'runs') {
                if (currentRunsTab === 'stage' && data.stageRuns.length === 0) {
                    loadStageRuns();
                } else if (currentRunsTab === 'pipeline' && data.pipelineRuns.length === 0) {
                    loadPipelineRuns();
                }
            } else if (view === 'graph' && !data.graph) {
                loadGraph();
            }
        }

        function showRunsTab(tab) {
            currentRunsTab = tab;

            // Update tabs
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            event.target.classList.add('active');

            // Show/hide content
            document.getElementById('tab-stage-runs').classList.toggle('hidden', tab !== 'stage');
            document.getElementById('tab-pipeline-runs').classList.toggle('hidden', tab !== 'pipeline');

            // Load data if needed
            if (tab === 'stage' && data.stageRuns.length === 0) {
                loadStageRuns();
            } else if (tab === 'pipeline' && data.pipelineRuns.length === 0) {
                loadPipelineRuns();
            }
        }

        // API calls
        async function loadWorkspaces() {
            try {
                const response = await fetch('/api/workspaces');
                const result = await response.json();
                data.workspaces = result.workspaces;
                renderWorkspaces();
            } catch (error) {
                console.error('Failed to load workspaces:', error);
                document.getElementById('workspaces-container').innerHTML =
                    '<div class="empty-state"><div class="empty-state-icon">⚠️</div><p>Failed to load workspaces</p></div>';
            }
        }

        async function loadStageRuns() {
            try {
                const response = await fetch('/api/runs?limit=100');
                const result = await response.json();
                data.stageRuns = result.runs;
                renderStageRuns();
            } catch (error) {
                console.error('Failed to load stage runs:', error);
                document.getElementById('stage-runs-container').innerHTML =
                    '<div class="empty-state"><div class="empty-state-icon">⚠️</div><p>Failed to load runs</p></div>';
            }
        }

        async function loadPipelineRuns() {
            try {
                const response = await fetch('/api/pipelines?limit=100');
                const result = await response.json();
                data.pipelineRuns = result.pipelines;
                renderPipelineRuns();
            } catch (error) {
                console.error('Failed to load pipeline runs:', error);
                document.getElementById('pipeline-runs-container').innerHTML =
                    '<div class="empty-state"><div class="empty-state-icon">⚠️</div><p>Failed to load pipelines</p></div>';
            }
        }

        async function loadGraph() {
            try {
                const response = await fetch('/api/graph');
                data.graph = await response.json();
                renderGraph();
            } catch (error) {
                console.error('Failed to load graph:', error);
                document.getElementById('graph-container').innerHTML =
                    '<div class="empty-state"><div class="empty-state-icon">⚠️</div><p>Failed to load graph</p></div>';
            }
        }

        // Rendering
        function renderWorkspaces() {
            const container = document.getElementById('workspaces-container');

            if (data.workspaces.length === 0) {
                container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📁</div><p>No workspaces found</p></div>';
                return;
            }

            container.innerHTML = data.workspaces.map(ws => `
                <div class="workspace-card">
                    <h3>${ws.name}</h3>
                    ${ws.description ? `<p>${ws.description}</p>` : ''}
                    ${ws.mount_status ? `<span class="status status-${ws.mount_status}">${ws.mount_status}</span>` : ''}
                    <div class="workspace-meta">
                        <div class="meta-item">📦 ${ws.version_count} versions</div>
                        ${ws.parent_workspace ? `<div class="meta-item">🔀 from ${ws.parent_workspace}</div>` : ''}
                    </div>
                </div>
            `).join('');
        }

        function renderStageRuns() {
            const container = document.getElementById('stage-runs-container');

            if (data.stageRuns.length === 0) {
                container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🎯</div><p>No stage runs found</p></div>';
                return;
            }

            container.innerHTML = '<div class="timeline">' + data.stageRuns.map(run => `
                <div class="timeline-item">
                    <div class="timeline-content">
                        <div class="timeline-time">${new Date(run.started_at).toLocaleString()}</div>
                        <strong>${run.workspace_name}</strong> / ${run.stage_name}
                        <span class="status status-${run.status}">${run.status}</span>
                        ${run.pipeline_name ? `<div style="margin-top: 0.5rem; color: var(--text-secondary); font-size: 0.9rem;">Pipeline: ${run.pipeline_name}</div>` : ''}
                    </div>
                </div>
            `).join('') + '</div>';
        }

        function renderPipelineRuns() {
            const container = document.getElementById('pipeline-runs-container');

            if (data.pipelineRuns.length === 0) {
                container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🔄</div><p>No pipeline runs found</p></div>';
                return;
            }

            container.innerHTML = '<div class="timeline">' + data.pipelineRuns.map(run => `
                <div class="timeline-item">
                    <div class="timeline-content">
                        <div class="timeline-time">${new Date(run.started_at).toLocaleString()}</div>
                        <strong>${run.workspace_name}</strong> / ${run.pipeline_name || 'pipeline'}
                        <span class="status status-${run.status}">${run.status}</span>
                        <div style="margin-top: 0.5rem; color: var(--text-secondary); font-size: 0.9rem;">
                            ${run.completed_stages || 0} / ${run.total_stages || 0} stages completed
                        </div>
                    </div>
                </div>
            `).join('') + '</div>';
        }

        function renderGraph() {
            const container = document.getElementById('graph-container');

            if (!data.graph || data.graph.nodes.length === 0) {
                container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🕸️</div><p>No provenance data to visualize</p></div>';
                return;
            }

            // Simple text-based graph for now (could be enhanced with D3.js later)
            const nodes = data.graph.nodes.length;
            const edges = data.graph.edges.length;

            container.innerHTML = `
                <div class="card" style="width: 100%; margin: 0;">
                    <h3 style="margin-bottom: 1rem;">Provenance Graph</h3>
                    <p style="color: var(--text-secondary); margin-bottom: 1rem;">
                        ${nodes} stage runs connected by ${edges} data dependencies
                    </p>
                    <div id="graph"></div>
                    <p style="margin-top: 1rem; color: var(--text-secondary); font-size: 0.9rem;">
                        Interactive graph visualization coming soon. This will show the full data lineage across all stages.
                    </p>
                </div>
            `;
        }

        // Initialize
        loadWorkspaces();
    </script>
</body>
</html>"""


def spawn_web_server(project_root: Path, port: int = DEFAULT_WEB_PORT, open_browser: bool = True) -> int:
    """Spawn the web server as a detached background process.

    Args:
        project_root: Project root directory
        port: Port to listen on
        open_browser: Whether to open browser after starting

    Returns:
        PID of spawned server
    """
    import subprocess

    logger.debug("Spawning web server for project: %s", project_root)

    cmd = [sys.executable, "-m", "goldfish", "web", "--project", str(project_root), "--port", str(port)]

    if not open_browser:
        cmd.append("--no-browser")

    logger.debug("Web server command: %s", " ".join(cmd))

    # Spawn detached
    proc = subprocess.Popen(
        cmd,
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    logger.debug("Web server process spawned with pid=%d", proc.pid)
    time.sleep(0.5)

    return proc.pid


def run_web_server(project_root: Path, port: int = DEFAULT_WEB_PORT, open_browser: bool = True) -> None:
    """Entry point for running the web server."""
    # Acquire lock immediately
    lock_file = get_web_lock_file(project_root)
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = os.open(str(lock_file), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        # Another web server is running
        os.close(lock_fd)
        logger.warning("Web server already running")
        sys.exit(0)

    from goldfish.logging import setup_logging

    setup_logging(component="web")

    logger.info("Starting Goldfish web server for %s", project_root)

    server = GoldfishWebServer(project_root, port)

    try:
        server.initialize()

        # Open browser if requested
        if open_browser:
            url = f"http://127.0.0.1:{port}"
            logger.info("Opening browser: %s", url)
            threading.Timer(1.0, lambda: webbrowser.open(url)).start()

        server.run()
    except ProjectNotInitializedError as e:
        logger.error("Project not initialized: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.exception("Web server failed: %s", e)
        sys.exit(1)
    finally:
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
            except OSError:
                pass
