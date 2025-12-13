"""Goldfish Global Web Server - Multi-project provenance visualization.

This is a GLOBAL singleton web server that:
- Runs ONE instance for the entire system (not per-project)
- Discovers and serves ALL Goldfish projects
- Auto-starts in background when needed
- Serves projects at /project/<name>/ routes
- Is for HUMAN use only (no MCP exposure)

The daemon can optionally notify/start this server, but it's independent.
"""

from __future__ import annotations

import fcntl
import hashlib
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
from goldfish.validation import (
    validate_pipeline_run_id,
    validate_stage_run_id,
    validate_workspace_name,
)

logger = logging.getLogger("goldfish.web")

# Web server version
WEB_SERVER_VERSION = "1.0"

# Global web server port (single instance for all projects)
DEFAULT_WEB_PORT = 7342  # "FISH" on phone keypad

# Constants
SOCKET_DIR_HASH_LENGTH = 12


def _get_global_web_dir() -> Path:
    """Get the directory for global web server files.

    Returns single directory for the one global web server instance.
    """
    web_dir = Path.home() / ".goldfish" / "web"
    web_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(web_dir, 0o700)
    return web_dir


def get_web_pid_file() -> Path:
    """Get the PID file path for the global web server."""
    return _get_global_web_dir() / "web.pid"


def get_web_lock_file() -> Path:
    """Get the lock file path for the global web server."""
    return _get_global_web_dir() / "web.lock"


def get_web_port_file() -> Path:
    """Get the port file path for the global web server."""
    return _get_global_web_dir() / "web.port"


def is_web_server_running() -> tuple[bool, int | None, int | None]:
    """Check if the global web server is running.

    Returns:
        Tuple of (is_running, pid, port)
    """
    try:
        pid_file = get_web_pid_file()
        port_file = get_web_port_file()

        if not pid_file.exists():
            return False, None, None

        pid = int(pid_file.read_text().strip())

        # Check if process is alive
        try:
            os.kill(pid, 0)
        except (ProcessLookupError, PermissionError):
            # Clean up stale PID file
            pid_file.unlink(missing_ok=True)
            port_file.unlink(missing_ok=True)
            return False, None, None

        # Get port
        port = None
        if port_file.exists():
            port = int(port_file.read_text().strip())

        return True, pid, port

    except (ValueError, OSError):
        return False, None, None


def stop_web_server(timeout: float = 10.0) -> bool:
    """Stop the global web server and wait for it to exit.

    Args:
        timeout: Maximum seconds to wait for server to exit

    Returns:
        True if server stopped successfully, False if timeout
    """
    logger.debug("Stopping global web server")

    running, pid, _ = is_web_server_running()
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
            # Clean up files
            get_web_pid_file().unlink(missing_ok=True)
            get_web_port_file().unlink(missing_ok=True)
            return True

    logger.warning("Web server did not stop within %.1fs timeout", timeout)
    return False


class ProjectInfo:
    """Information about a discovered Goldfish project."""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.config = GoldfishConfig.load(project_root)
        self.dev_repo_path = self.config.get_dev_repo_path(project_root)
        self.db_path = self.dev_repo_path / self.config.db_path
        self.name = project_root.name
        # Use hash for URL-safe routing
        self.url_id = hashlib.sha256(str(project_root.resolve()).encode()).hexdigest()[:SOCKET_DIR_HASH_LENGTH]

    def get_db(self) -> Database:
        """Get database connection for this project."""
        return Database(self.db_path)


def discover_projects() -> list[ProjectInfo]:
    """Discover all Goldfish projects by scanning daemon PID files.

    Returns list of active projects with running daemons.
    """
    projects: list[ProjectInfo] = []

    # Scan daemon socket directory
    daemon_sockets_dir = Path.home() / ".goldfish" / "sockets"
    if not daemon_sockets_dir.exists():
        return projects

    for project_dir in daemon_sockets_dir.iterdir():
        if not project_dir.is_dir():
            continue

        pid_file = project_dir / "daemon.pid"
        if not pid_file.exists():
            continue

        try:
            # Read PID and check if alive
            pid = int(pid_file.read_text().strip())
            try:
                os.kill(pid, 0)
            except (ProcessLookupError, PermissionError):
                continue  # Daemon not running

            # Try to find project root from socket path
            # The daemon stores project info, we can infer from daemon socket path
            # For now, we'll need to add a project_root file to daemon directory
            project_root_file = project_dir / "project_root"
            if not project_root_file.exists():
                continue

            project_root = Path(project_root_file.read_text().strip())
            if not project_root.exists():
                continue

            # Load project
            project = ProjectInfo(project_root)
            projects.append(project)
            logger.debug("Discovered project: %s at %s", project.name, project.project_root)

        except (ValueError, OSError, ProjectNotInitializedError) as e:
            logger.debug("Failed to load project from %s: %s", project_dir, e)
            continue

    return projects


class ProvenanceRequestHandler(http.server.BaseHTTPRequestHandler):
    """Handle HTTP requests for multi-project provenance UI."""

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

    def _send_error_json(self, status: int, message: str) -> None:
        """Send error response."""
        self._send_json({"error": message}, status)

    def _get_project_by_id(self, project_id: str) -> ProjectInfo | None:
        """Get project by URL ID."""
        server = self.server  # type: ignore[attr-defined]
        projects: list[ProjectInfo] = server.projects  # type: ignore[attr-defined]

        for project in projects:
            if project.url_id == project_id:
                return project
        return None

    def do_GET(self) -> None:
        """Handle GET requests."""
        parsed = urlparse(self.path)
        path = parsed.path
        query_params = parse_qs(parsed.query)

        try:
            # Root - show project list
            if path == "/" or path == "/index.html":
                self._send_html(get_index_html())
                return

            # API: List all projects
            elif path == "/api/projects":
                server = self.server  # type: ignore[attr-defined]
                projects: list[ProjectInfo] = server.projects  # type: ignore[attr-defined]

                self._send_json({
                    "projects": [
                        {
                            "id": p.url_id,
                            "name": p.name,
                            "root": str(p.project_root),
                        }
                        for p in projects
                    ]
                })
                return

            # Health check
            elif path == "/api/health":
                self._send_json({
                    "status": "healthy",
                    "version": WEB_SERVER_VERSION,
                    "pid": os.getpid(),
                    "projects_count": len(self.server.projects),  # type: ignore[attr-defined]
                })
                return

            # Project-specific routes: /project/<id>/...
            elif path.startswith("/project/"):
                parts = path.split("/")
                if len(parts) < 3:
                    self._send_error_json(400, "Invalid project path")
                    return

                project_id = parts[2]
                project = self._get_project_by_id(project_id)
                if not project:
                    self._send_error_json(404, f"Project '{project_id}' not found")
                    return

                # Serve project UI
                if len(parts) == 3 or (len(parts) == 4 and parts[3] == ""):
                    self._send_html(get_project_html(project))
                    return

                # Project API routes
                elif len(parts) >= 4 and parts[3] == "api":
                    self._handle_project_api(project, parts[4:], query_params)
                    return

            self._send_error_json(404, "Not found")

        except Exception as e:
            logger.exception("Request error: %s", e)
            self._send_error_json(500, "Internal server error")

    def _handle_project_api(self, project: ProjectInfo, path_parts: list[str], query_params: dict) -> None:
        """Handle API requests for a specific project."""
        if not path_parts:
            self._send_error_json(400, "Invalid API path")
            return

        endpoint = path_parts[0]
        db = project.get_db()

        try:
            if endpoint == "workspaces":
                workspaces = self._get_workspaces(db)
                self._send_json({"workspaces": workspaces})

            elif endpoint == "workspace" and len(path_parts) > 1:
                workspace_name = path_parts[1]
                validate_workspace_name(workspace_name)  # Security: validate input
                details = self._get_workspace_details(db, workspace_name)
                self._send_json(details)

            elif endpoint == "runs":
                limit = int(query_params.get("limit", ["100"])[0])
                runs = self._get_stage_runs(db, limit)
                self._send_json({"runs": runs})

            elif endpoint == "run" and len(path_parts) > 1:
                run_id = path_parts[1]
                validate_stage_run_id(run_id)  # Security: validate input
                details = self._get_run_details(db, run_id)
                self._send_json(details)

            elif endpoint == "pipelines":
                limit = int(query_params.get("limit", ["100"])[0])
                pipelines = self._get_pipeline_runs(db, limit)
                self._send_json({"pipelines": pipelines})

            elif endpoint == "pipeline" and len(path_parts) > 1:
                pipeline_id = path_parts[1]
                validate_pipeline_run_id(pipeline_id)  # Security: validate input
                details = self._get_pipeline_details(db, pipeline_id)
                self._send_json(details)

            elif endpoint == "graph":
                workspace = query_params.get("workspace", [None])[0]
                if workspace:
                    validate_workspace_name(workspace)  # Security: validate input
                graph = self._get_provenance_graph(db, workspace)
                self._send_json(graph)

            else:
                self._send_error_json(404, "API endpoint not found")

        finally:
            # Close database connection
            pass  # Database class handles this via context managers

    def _get_workspaces(self, db: Database) -> list[dict[str, Any]]:
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

    def _get_workspace_details(self, db: Database, workspace_name: str) -> dict[str, Any]:
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

    def _get_stage_runs(self, db: Database, limit: int = 100) -> list[dict[str, Any]]:
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

    def _get_run_details(self, db: Database, run_id: str) -> dict[str, Any]:
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

    def _get_pipeline_runs(self, db: Database, limit: int = 100) -> list[dict[str, Any]]:
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

    def _get_pipeline_details(self, db: Database, pipeline_id: str) -> dict[str, Any]:
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

    def _get_provenance_graph(self, db: Database, workspace: str | None = None) -> dict[str, Any]:
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
    """The global Goldfish web visualization server."""

    def __init__(self, port: int = DEFAULT_WEB_PORT):
        self.port = port
        self.start_time = time.time()
        self.shutdown_event = threading.Event()
        self.http_server: ThreadedHTTPServer | None = None
        self.projects: list[ProjectInfo] = []

        # Paths
        self.pid_file = get_web_pid_file()
        self.lock_file = get_web_lock_file()
        self.port_file = get_web_port_file()

    def initialize(self) -> None:
        """Initialize the web server - discover projects."""
        logger.info("Initializing global web server")

        # Discover all Goldfish projects
        self.projects = discover_projects()
        logger.info("Discovered %d Goldfish projects", len(self.projects))

    def write_pid_file(self) -> None:
        """Write PID file atomically."""
        temp_file = self.pid_file.with_suffix(".tmp")
        temp_file.write_text(str(os.getpid()))
        temp_file.rename(self.pid_file)

    def write_port_file(self) -> None:
        """Write port file atomically."""
        temp_file = self.port_file.with_suffix(".tmp")
        temp_file.write_text(str(self.port))
        temp_file.rename(self.port_file)

    def start_http_server(self) -> None:
        """Start the HTTP server with port retry logic."""
        logger.info("Starting HTTP server")

        # Try to bind to port with retry
        max_retries = 10
        for attempt in range(max_retries):
            try:
                port = self.port + attempt
                self.http_server = ThreadedHTTPServer(("127.0.0.1", port), ProvenanceRequestHandler)
                self.http_server.projects = self.projects  # type: ignore[attr-defined]
                self.port = port  # Update to actual port
                logger.info("Web server listening on http://127.0.0.1:%d", port)
                return
            except OSError as e:
                if attempt == max_retries - 1:
                    raise RuntimeError(f"Failed to bind to any port in range {self.port}-{self.port + max_retries}") from e
                logger.debug("Port %d in use, trying next port", port)
                continue

    def run(self) -> None:
        """Run the web server main loop."""
        self.write_pid_file()
        self.start_http_server()
        self.write_port_file()  # Write after we know the actual port

        # Set up signal handlers
        def handle_shutdown(signum: int, frame: Any) -> None:
            logger.info("Received signal %d, shutting down...", signum)
            threading.Thread(target=self.shutdown, daemon=True).start()

        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)

        logger.info("Web server running (pid=%d, port=%d, projects=%d)", os.getpid(), self.port, len(self.projects))

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
        self.pid_file.unlink(missing_ok=True)
        self.port_file.unlink(missing_ok=True)

        logger.info("Web server stopped")


def get_index_html() -> str:
    """Get the HTML for the project list page."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Goldfish Projects</title>
    <style>
        /* Dieter Rams inspired design - less but better */
        :root {
            --goldfish-orange: #FF6B35;
            --bg-primary: #FAFAFA;
            --bg-secondary: #FFFFFF;
            --text-primary: #1A1A1A;
            --text-secondary: #6B6B6B;
            --border-color: #E0E0E0;
            --shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
            --shadow-hover: 0 2px 8px rgba(0, 0, 0, 0.12);
        }

        * { margin: 0; padding: 0; box-sizing: border-box; }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
        }

        header {
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border-color);
            padding: 2rem;
            text-align: center;
        }

        .logo {
            font-size: 3rem;
            margin-bottom: 0.5rem;
        }

        h1 {
            font-size: 2rem;
            font-weight: 400;
        }

        .accent { color: var(--goldfish-orange); font-weight: 500; }

        main {
            max-width: 1200px;
            margin: 3rem auto;
            padding: 0 2rem;
        }

        .project-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 2rem;
        }

        .project-card {
            background: var(--bg-secondary);
            border-radius: 8px;
            padding: 2rem;
            box-shadow: var(--shadow);
            transition: all 0.2s;
            cursor: pointer;
            border: 2px solid transparent;
        }

        .project-card:hover {
            box-shadow: var(--shadow-hover);
            border-color: var(--goldfish-orange);
            transform: translateY(-2px);
        }

        .project-card h2 {
            font-size: 1.5rem;
            font-weight: 500;
            margin-bottom: 0.5rem;
        }

        .project-card p {
            color: var(--text-secondary);
            font-size: 0.9rem;
        }

        .loading {
            text-align: center;
            padding: 3rem;
            color: var(--text-secondary);
        }

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
    </style>
</head>
<body>
    <header>
        <div class="logo">🐠</div>
        <h1><span class="accent">Goldfish</span> Projects</h1>
        <p style="color: var(--text-secondary); margin-top: 0.5rem;">Select a project to explore its provenance</p>
    </header>

    <main>
        <div id="projects-container">
            <div class="loading">Loading projects...</div>
        </div>
    </main>

    <script>
        async function loadProjects() {
            try {
                const response = await fetch('/api/projects');
                const data = await response.json();

                const container = document.getElementById('projects-container');

                if (data.projects.length === 0) {
                    container.innerHTML = `
                        <div class="empty-state">
                            <div class="empty-state-icon">📁</div>
                            <p>No active Goldfish projects found</p>
                            <p style="margin-top: 1rem; font-size: 0.9rem;">Start a daemon to see projects here</p>
                        </div>
                    `;
                    return;
                }

                container.innerHTML = '<div class="project-grid">' + data.projects.map(project => `
                    <div class="project-card" onclick="window.location.href='/project/${project.id}/'">
                        <h2>${project.name}</h2>
                        <p>${project.root}</p>
                    </div>
                `).join('') + '</div>';

            } catch (error) {
                console.error('Failed to load projects:', error);
                document.getElementById('projects-container').innerHTML = `
                    <div class="empty-state">
                        <div class="empty-state-icon">⚠️</div>
                        <p>Failed to load projects</p>
                    </div>
                `;
            }
        }

        loadProjects();

        // Refresh project list every 10 seconds
        setInterval(loadProjects, 10000);
    </script>
</body>
</html>"""


def get_project_html(project: ProjectInfo) -> str:
    """Get the HTML for a specific project's provenance UI."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{project.name} - Goldfish Provenance</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        /* Dieter Rams inspired design - less but better */
        :root {{
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
        }}

        * {{ margin: 0; padding: 0; box-sizing: border-box; }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
        }}

        /* Header */
        header {{
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border-color);
            padding: 1rem 2rem;
            position: sticky;
            top: 0;
            z-index: 100;
            box-shadow: var(--shadow);
        }}

        .header-content {{
            max-width: 1400px;
            margin: 0 auto;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }}

        .logo {{
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }}

        .logo-icon {{
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
        }}

        .logo h1 {{
            font-size: 1.5rem;
            font-weight: 400;
            color: var(--text-primary);
        }}

        .logo-accent {{ color: var(--goldfish-orange); font-weight: 500; }}

        .breadcrumb {{
            color: var(--text-secondary);
            font-size: 0.9rem;
            margin-top: 0.25rem;
        }}

        .breadcrumb a {{
            color: var(--goldfish-orange);
            text-decoration: none;
        }}

        .breadcrumb a:hover {{ text-decoration: underline; }}

        nav {{
            display: flex;
            gap: 1.5rem;
        }}

        nav button {{
            background: none;
            border: none;
            color: var(--text-secondary);
            cursor: pointer;
            padding: 0.5rem 1rem;
            border-radius: 4px;
            transition: all 0.2s;
            font-size: 0.95rem;
        }}

        nav button:hover {{
            color: var(--text-primary);
            background: var(--bg-primary);
        }}

        nav button.active {{
            color: var(--goldfish-orange);
            font-weight: 500;
        }}

        /* Main content */
        main {{
            max-width: 1400px;
            margin: 2rem auto;
            padding: 0 2rem;
        }}

        /* Cards */
        .card {{
            background: var(--bg-secondary);
            border-radius: 8px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            box-shadow: var(--shadow);
            transition: box-shadow 0.2s;
        }}

        .card:hover {{ box-shadow: var(--shadow-hover); }}

        .card-title {{
            font-size: 1.1rem;
            font-weight: 500;
            margin-bottom: 1rem;
            color: var(--text-primary);
        }}

        /* Grid layouts */
        .workspace-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 1.5rem;
        }}

        .workspace-card {{
            background: var(--bg-secondary);
            border-radius: 8px;
            padding: 1.5rem;
            box-shadow: var(--shadow);
            transition: all 0.2s;
            cursor: pointer;
            border: 2px solid transparent;
        }}

        .workspace-card:hover {{
            box-shadow: var(--shadow-hover);
            border-color: var(--goldfish-orange);
            transform: translateY(-2px);
        }}

        .workspace-card h3 {{
            font-size: 1.2rem;
            font-weight: 500;
            margin-bottom: 0.5rem;
            color: var(--text-primary);
        }}

        .workspace-card p {{
            color: var(--text-secondary);
            font-size: 0.9rem;
            margin-bottom: 0.5rem;
        }}

        .workspace-meta {{
            display: flex;
            gap: 1rem;
            margin-top: 1rem;
            font-size: 0.85rem;
            color: var(--text-secondary);
        }}

        .meta-item {{
            display: flex;
            align-items: center;
            gap: 0.25rem;
        }}

        /* Status badges */
        .status {{
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        .status-completed {{ background: #E8F5E9; color: #2E7D32; }}
        .status-running {{ background: #FFF3E0; color: #E65100; }}
        .status-pending {{ background: #E3F2FD; color: #1565C0; }}
        .status-failed {{ background: #FFEBEE; color: #C62828; }}
        .status-active {{ background: #FFE8DC; color: var(--goldfish-orange-dark); }}

        /* Graph container */
        #graph-container {{
            background: var(--bg-secondary);
            border-radius: 8px;
            min-height: 600px;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: var(--shadow);
            overflow: hidden;
        }}

        #graph {{ width: 100%; height: 600px; }}

        /* Loading state */
        .loading {{
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 3rem;
            color: var(--text-secondary);
        }}

        .spinner {{
            border: 3px solid var(--border-color);
            border-top: 3px solid var(--goldfish-orange);
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin-right: 1rem;
        }}

        @keyframes spin {{
            0% {{ transform: rotate(0deg); }}
            100% {{ transform: rotate(360deg); }}
        }}

        /* Tabs */
        .tabs {{
            display: flex;
            gap: 0;
            border-bottom: 2px solid var(--border-color);
            margin-bottom: 2rem;
        }}

        .tab {{
            padding: 1rem 1.5rem;
            background: none;
            border: none;
            color: var(--text-secondary);
            cursor: pointer;
            font-size: 1rem;
            border-bottom: 2px solid transparent;
            margin-bottom: -2px;
            transition: all 0.2s;
        }}

        .tab:hover {{ color: var(--text-primary); }}

        .tab.active {{
            color: var(--goldfish-orange);
            border-bottom-color: var(--goldfish-orange);
            font-weight: 500;
        }}

        .tab-content {{ display: none; }}
        .tab-content.active {{ display: block; }}

        /* Modal */
        .modal {{
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0, 0, 0, 0.5);
            z-index: 1000;
            align-items: center;
            justify-content: center;
            padding: 2rem;
        }}

        .modal.active {{ display: flex; }}

        .modal-content {{
            background: var(--bg-secondary);
            border-radius: 8px;
            max-width: 800px;
            width: 100%;
            max-height: 80vh;
            overflow-y: auto;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.2);
            position: relative;
        }}

        .modal-header {{
            padding: 1.5rem;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            align-items: center;
            justify-content: space-between;
            position: sticky;
            top: 0;
            background: var(--bg-secondary);
            z-index: 1;
        }}

        .modal-title {{
            font-size: 1.3rem;
            font-weight: 500;
            color: var(--text-primary);
        }}

        .modal-close {{
            background: none;
            border: none;
            font-size: 1.5rem;
            color: var(--text-secondary);
            cursor: pointer;
            padding: 0.25rem 0.5rem;
            line-height: 1;
            border-radius: 4px;
            transition: all 0.2s;
        }}

        .modal-close:hover {{
            background: var(--bg-primary);
            color: var(--text-primary);
        }}

        .modal-body {{
            padding: 1.5rem;
        }}

        .detail-section {{
            margin-bottom: 2rem;
        }}

        .detail-section:last-child {{ margin-bottom: 0; }}

        .detail-section-title {{
            font-size: 1.1rem;
            font-weight: 500;
            color: var(--goldfish-orange);
            margin-bottom: 1rem;
        }}

        .detail-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
        }}

        .detail-item {{
            padding: 0.75rem;
            background: var(--bg-primary);
            border-radius: 4px;
        }}

        .detail-label {{
            font-size: 0.85rem;
            color: var(--text-secondary);
            margin-bottom: 0.25rem;
        }}

        .detail-value {{
            font-size: 1rem;
            color: var(--text-primary);
            font-weight: 500;
        }}

        .detail-list {{
            list-style: none;
        }}

        .detail-list li {{
            padding: 0.75rem;
            border-bottom: 1px solid var(--border-color);
        }}

        .detail-list li:last-child {{ border-bottom: none; }}

        /* Graph */
        #graph-svg {{
            width: 100%;
            height: 600px;
            background: var(--bg-secondary);
            border-radius: 8px;
            box-shadow: var(--shadow);
        }}

        .graph-node {{
            cursor: pointer;
            transition: all 0.2s;
        }}

        .graph-node:hover {{
            stroke: var(--goldfish-orange);
            stroke-width: 3px;
        }}

        .graph-node-completed {{ fill: #4CAF50; }}
        .graph-node-running {{ fill: var(--goldfish-orange); }}
        .graph-node-failed {{ fill: #F44336; }}
        .graph-node-pending {{ fill: var(--text-secondary); }}

        .graph-link {{
            stroke: var(--border-color);
            stroke-opacity: 0.6;
            stroke-width: 2px;
        }}

        .graph-link-hover {{
            stroke: var(--goldfish-orange);
            stroke-opacity: 1;
            stroke-width: 3px;
        }}

        .graph-label {{
            font-size: 10px;
            fill: var(--text-primary);
            pointer-events: none;
        }}

        .graph-controls {{
            margin-bottom: 1rem;
            display: flex;
            gap: 0.5rem;
        }}

        .graph-control-btn {{
            padding: 0.5rem 1rem;
            background: var(--bg-secondary);
            border: 2px solid var(--border-color);
            border-radius: 6px;
            cursor: pointer;
            font-size: 0.9rem;
            transition: all 0.2s;
        }}

        .graph-control-btn:hover {{
            border-color: var(--goldfish-orange);
            color: var(--goldfish-orange);
        }}

        /* Timeline */
        .timeline {{
            position: relative;
            padding-left: 2rem;
        }}

        .timeline::before {{
            content: '';
            position: absolute;
            left: 0;
            top: 0;
            bottom: 0;
            width: 2px;
            background: var(--border-color);
        }}

        .timeline-item {{
            position: relative;
            padding-bottom: 2rem;
        }}

        .timeline-item::before {{
            content: '';
            position: absolute;
            left: -2.5rem;
            top: 0.5rem;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: var(--goldfish-orange);
            border: 3px solid var(--bg-primary);
        }}

        .timeline-content {{
            background: var(--bg-secondary);
            padding: 1rem;
            border-radius: 6px;
            box-shadow: var(--shadow);
        }}

        .timeline-time {{
            color: var(--text-secondary);
            font-size: 0.85rem;
            margin-bottom: 0.5rem;
        }}

        /* Search/Filter */
        .search-box {{
            margin-bottom: 1.5rem;
            position: relative;
        }}

        .search-input {{
            width: 100%;
            padding: 0.75rem 1rem 0.75rem 2.5rem;
            border: 2px solid var(--border-color);
            border-radius: 6px;
            font-size: 1rem;
            background: var(--bg-secondary);
            transition: all 0.2s;
        }}

        .search-input:focus {{
            outline: none;
            border-color: var(--goldfish-orange);
            box-shadow: 0 0 0 3px rgba(255, 107, 53, 0.1);
        }}

        .search-icon {{
            position: absolute;
            left: 0.75rem;
            top: 50%;
            transform: translateY(-50%);
            color: var(--text-secondary);
            pointer-events: none;
        }}

        .filter-tags {{
            display: flex;
            gap: 0.5rem;
            margin-bottom: 1rem;
            flex-wrap: wrap;
        }}

        .filter-tag {{
            padding: 0.5rem 1rem;
            background: var(--bg-secondary);
            border: 2px solid var(--border-color);
            border-radius: 20px;
            cursor: pointer;
            font-size: 0.9rem;
            transition: all 0.2s;
        }}

        .filter-tag:hover {{
            border-color: var(--goldfish-orange-light);
        }}

        .filter-tag.active {{
            background: var(--goldfish-orange);
            color: white;
            border-color: var(--goldfish-orange);
        }}

        /* Empty state */
        .empty-state {{
            text-align: center;
            padding: 4rem 2rem;
            color: var(--text-secondary);
        }}

        .empty-state-icon {{
            font-size: 4rem;
            margin-bottom: 1rem;
            opacity: 0.3;
        }}

        .hidden {{ display: none !important; }}
    </style>
</head>
<body>
    <header>
        <div class="header-content">
            <div class="logo">
                <div class="logo-icon">🐠</div>
                <div>
                    <h1><span class="logo-accent">Goldfish</span> Provenance</h1>
                    <div class="breadcrumb">
                        <a href="/">All Projects</a> › {project.name}
                    </div>
                </div>
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
            <div class="search-box">
                <span class="search-icon">🔍</span>
                <input type="text" id="workspace-search" class="search-input" placeholder="Search workspaces by name or description..." oninput="filterWorkspaces()">
            </div>
            <div class="filter-tags" id="workspace-filters">
                <div class="filter-tag active" data-filter="all" onclick="setWorkspaceFilter('all')">All</div>
                <div class="filter-tag" data-filter="mounted" onclick="setWorkspaceFilter('mounted')">Mounted</div>
                <div class="filter-tag" data-filter="hibernating" onclick="setWorkspaceFilter('hibernating')">Hibernating</div>
            </div>
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
            <div class="search-box">
                <span class="search-icon">🔍</span>
                <input type="text" id="runs-search" class="search-input" placeholder="Search runs by workspace, stage, or pipeline..." oninput="filterRuns()">
            </div>
            <div class="filter-tags" id="run-filters">
                <div class="filter-tag active" data-filter="all" onclick="setRunFilter('all')">All</div>
                <div class="filter-tag" data-filter="running" onclick="setRunFilter('running')">Running</div>
                <div class="filter-tag" data-filter="completed" onclick="setRunFilter('completed')">Completed</div>
                <div class="filter-tag" data-filter="failed" onclick="setRunFilter('failed')">Failed</div>
            </div>
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
            <div class="graph-controls">
                <button class="graph-control-btn" onclick="resetGraphZoom()">Reset Zoom</button>
                <button class="graph-control-btn" onclick="centerGraph()">Center</button>
                <select id="workspace-filter-graph" class="graph-control-btn" onchange="loadGraph()">
                    <option value="">All Workspaces</option>
                </select>
            </div>
            <div id="graph-container">
                <svg id="graph-svg"></svg>
            </div>
        </div>
    </main>

    <!-- Detail Modal -->
    <div id="detail-modal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2 class="modal-title" id="modal-title">Details</h2>
                <button class="modal-close" onclick="closeModal()">&times;</button>
            </div>
            <div class="modal-body" id="modal-body">
                <div class="loading">
                    <div class="spinner"></div>
                    <span>Loading...</span>
                </div>
            </div>
        </div>
    </div>

    <script>
        const PROJECT_ID = '{project.url_id}';
        const API_BASE = '/project/' + PROJECT_ID + '/api';

        // State
        let currentView = 'workspaces';
        let currentRunsTab = 'stage';
        let workspaceFilter = 'all';
        let runFilter = 'all';
        let data = {{
            workspaces: [],
            stageRuns: [],
            pipelineRuns: [],
            graph: null,
            filteredWorkspaces: [],
            filteredRuns: []
        }};

        // View switching
        function showView(view) {{
            currentView = view;

            // Update nav
            document.querySelectorAll('nav button').forEach(btn => {{
                btn.classList.remove('active');
            }});
            event.target.classList.add('active');

            // Show/hide views
            document.getElementById('view-workspaces').classList.toggle('hidden', view !== 'workspaces');
            document.getElementById('view-runs').classList.toggle('hidden', view !== 'runs');
            document.getElementById('view-graph').classList.toggle('hidden', view !== 'graph');

            // Load data if needed
            if (view === 'workspaces' && data.workspaces.length === 0) {{
                loadWorkspaces();
            }} else if (view === 'runs') {{
                if (currentRunsTab === 'stage' && data.stageRuns.length === 0) {{
                    loadStageRuns();
                }} else if (currentRunsTab === 'pipeline' && data.pipelineRuns.length === 0) {{
                    loadPipelineRuns();
                }}
            }} else if (view === 'graph' && !data.graph) {{
                loadGraph();
            }}
        }}

        function showRunsTab(tab) {{
            currentRunsTab = tab;

            // Update tabs
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            event.target.classList.add('active');

            // Show/hide content
            document.getElementById('tab-stage-runs').classList.toggle('active', tab === 'stage');
            document.getElementById('tab-pipeline-runs').classList.toggle('active', tab === 'pipeline');

            // Load data if needed
            if (tab === 'stage' && data.stageRuns.length === 0) {{
                loadStageRuns();
            }} else if (tab === 'pipeline' && data.pipelineRuns.length === 0) {{
                loadPipelineRuns();
            }}
        }}

        // Filter functions
        function setWorkspaceFilter(filter) {{
            workspaceFilter = filter;

            // Update filter tags
            document.querySelectorAll('#workspace-filters .filter-tag').forEach(tag => {{
                tag.classList.toggle('active', tag.dataset.filter === filter);
            }});

            filterWorkspaces();
        }}

        function filterWorkspaces() {{
            const searchTerm = document.getElementById('workspace-search').value.toLowerCase();

            data.filteredWorkspaces = data.workspaces.filter(ws => {{
                // Filter by search term
                const matchesSearch = !searchTerm ||
                    ws.name.toLowerCase().includes(searchTerm) ||
                    (ws.description && ws.description.toLowerCase().includes(searchTerm));

                // Filter by status
                const matchesFilter = workspaceFilter === 'all' ||
                    (workspaceFilter === 'mounted' && ws.mount_status === 'mounted') ||
                    (workspaceFilter === 'hibernating' && ws.mount_status === 'hibernating');

                return matchesSearch && matchesFilter;
            }});

            renderWorkspaces();
        }}

        function setRunFilter(filter) {{
            runFilter = filter;

            // Update filter tags
            document.querySelectorAll('#run-filters .filter-tag').forEach(tag => {{
                tag.classList.toggle('active', tag.dataset.filter === filter);
            }});

            filterRuns();
        }}

        function filterRuns() {{
            const searchTerm = document.getElementById('runs-search').value.toLowerCase();

            data.filteredRuns = data.stageRuns.filter(run => {{
                // Filter by search term
                const matchesSearch = !searchTerm ||
                    run.workspace_name.toLowerCase().includes(searchTerm) ||
                    run.stage_name.toLowerCase().includes(searchTerm) ||
                    (run.pipeline_name && run.pipeline_name.toLowerCase().includes(searchTerm));

                // Filter by status
                const matchesFilter = runFilter === 'all' ||
                    (runFilter === 'running' && run.status === 'running') ||
                    (runFilter === 'completed' && run.status === 'completed') ||
                    (runFilter === 'failed' && run.status === 'failed');

                return matchesSearch && matchesFilter;
            }});

            renderStageRuns();
        }}

        // API calls
        async function loadWorkspaces() {{
            try {{
                const response = await fetch(API_BASE + '/workspaces');
                const result = await response.json();
                data.workspaces = result.workspaces;
                data.filteredWorkspaces = data.workspaces;
                filterWorkspaces();
            }} catch (error) {{
                console.error('Failed to load workspaces:', error);
                document.getElementById('workspaces-container').innerHTML =
                    '<div class="empty-state"><div class="empty-state-icon">⚠️</div><p>Failed to load workspaces</p></div>';
            }}
        }}

        async function loadStageRuns() {{
            try {{
                const response = await fetch(API_BASE + '/runs?limit=100');
                const result = await response.json();
                data.stageRuns = result.runs;
                data.filteredRuns = data.stageRuns;
                filterRuns();
            }} catch (error) {{
                console.error('Failed to load stage runs:', error);
                document.getElementById('stage-runs-container').innerHTML =
                    '<div class="empty-state"><div class="empty-state-icon">⚠️</div><p>Failed to load runs</p></div>';
            }}
        }}

        async function loadPipelineRuns() {{
            try {{
                const response = await fetch(API_BASE + '/pipelines?limit=100');
                const result = await response.json();
                data.pipelineRuns = result.pipelines;
                renderPipelineRuns();
            }} catch (error) {{
                console.error('Failed to load pipeline runs:', error);
                document.getElementById('pipeline-runs-container').innerHTML =
                    '<div class="empty-state"><div class="empty-state-icon">⚠️</div><p>Failed to load pipelines</p></div>';
            }}
        }}

        async function loadGraph() {{
            try {{
                const workspace = document.getElementById('workspace-filter-graph')?.value || '';
                const url = workspace ? `${{API_BASE}}/graph?workspace=${{encodeURIComponent(workspace)}}` : `${{API_BASE}}/graph`;
                const response = await fetch(url);
                data.graph = await response.json();
                renderGraph();
            }} catch (error) {{
                console.error('Failed to load graph:', error);
                const svg = document.getElementById('graph-svg');
                if (svg) {{
                    svg.innerHTML = '<text x="50%" y="50%" text-anchor="middle" fill="var(--text-secondary)">Failed to load graph</text>';
                }}
            }}
        }}

        // Modal functions
        function openModal() {{
            document.getElementById('detail-modal').classList.add('active');
            document.body.style.overflow = 'hidden';
        }}

        function closeModal() {{
            document.getElementById('detail-modal').classList.remove('active');
            document.body.style.overflow = '';
        }}

        async function showWorkspaceDetails(workspaceName) {{
            openModal();
            document.getElementById('modal-title').textContent = workspaceName;
            document.getElementById('modal-body').innerHTML = '<div class="loading"><div class="spinner"></div><span>Loading...</span></div>';

            try {{
                const response = await fetch(API_BASE + '/workspace/' + encodeURIComponent(workspaceName));
                const details = await response.json();

                const workspace = details.workspace;
                const versions = details.versions || [];
                const runs = details.recent_runs || [];

                document.getElementById('modal-body').innerHTML = `
                    <div class="detail-section">
                        <div class="detail-section-title">Workspace Information</div>
                        <div class="detail-grid">
                            <div class="detail-item">
                                <div class="detail-label">Name</div>
                                <div class="detail-value">${{workspace.workspace_name}}</div>
                            </div>
                            <div class="detail-item">
                                <div class="detail-label">Created</div>
                                <div class="detail-value">${{new Date(workspace.created_at).toLocaleString()}}</div>
                            </div>
                            ${{workspace.parent_workspace ? `
                            <div class="detail-item">
                                <div class="detail-label">Parent</div>
                                <div class="detail-value">${{workspace.parent_workspace}}</div>
                            </div>
                            ` : ''}}
                            ${{workspace.mount_status ? `
                            <div class="detail-item">
                                <div class="detail-label">Mount Status</div>
                                <div class="detail-value">${{workspace.mount_status}}</div>
                            </div>
                            ` : ''}}
                        </div>
                        ${{workspace.description ? `
                        <div style="margin-top: 1rem;">
                            <div class="detail-label">Description</div>
                            <p style="margin-top: 0.5rem;">${{workspace.description}}</p>
                        </div>
                        ` : ''}}
                    </div>

                    <div class="detail-section">
                        <div class="detail-section-title">Versions (${{versions.length}})</div>
                        ${{versions.length > 0 ? `
                        <ul class="detail-list">
                            ${{versions.slice(0, 10).map(v => `
                            <li>
                                <strong>${{v.version}}</strong>
                                <div style="font-size: 0.9rem; color: var(--text-secondary); margin-top: 0.25rem;">
                                    Created: ${{new Date(v.created_at).toLocaleString()}} •
                                    By: ${{v.created_by}}
                                </div>
                            </li>
                            `).join('')}}
                            ${{versions.length > 10 ? `<li style="color: var(--text-secondary); font-style: italic;">+ ${{versions.length - 10}} more versions</li>` : ''}}
                        </ul>
                        ` : '<p style="color: var(--text-secondary);">No versions yet</p>'}}
                    </div>

                    <div class="detail-section">
                        <div class="detail-section-title">Recent Runs (${{runs.length}})</div>
                        ${{runs.length > 0 ? `
                        <ul class="detail-list">
                            ${{runs.slice(0, 10).map(r => `
                            <li style="cursor: pointer;" onclick="showRunDetails('${{r.id}}')">
                                <strong>${{r.stage_name}}</strong>
                                <span class="status status-${{r.status}}">${{r.status}}</span>
                                <div style="font-size: 0.9rem; color: var(--text-secondary); margin-top: 0.25rem;">
                                    Started: ${{new Date(r.started_at).toLocaleString()}}
                                    ${{r.pipeline_name ? ` • Pipeline: ${{r.pipeline_name}}` : ''}}
                                </div>
                            </li>
                            `).join('')}}
                            ${{runs.length > 10 ? `<li style="color: var(--text-secondary); font-style: italic;">+ ${{runs.length - 10}} more runs</li>` : ''}}
                        </ul>
                        ` : '<p style="color: var(--text-secondary);">No runs yet</p>'}}
                    </div>
                `;
            }} catch (error) {{
                console.error('Failed to load workspace details:', error);
                document.getElementById('modal-body').innerHTML =
                    '<div class="empty-state"><div class="empty-state-icon">⚠️</div><p>Failed to load details</p></div>';
            }}
        }}

        async function showRunDetails(runId) {{
            openModal();
            document.getElementById('modal-title').textContent = 'Run ' + runId;
            document.getElementById('modal-body').innerHTML = '<div class="loading"><div class="spinner"></div><span>Loading...</span></div>';

            try {{
                const response = await fetch(API_BASE + '/run/' + encodeURIComponent(runId));
                const details = await response.json();

                const run = details.run;
                const signals = details.signals || [];

                const inputs = signals.filter(s => s.consumed_by === runId);
                const outputs = signals.filter(s => s.stage_run_id === runId);

                document.getElementById('modal-body').innerHTML = `
                    <div class="detail-section">
                        <div class="detail-section-title">Run Information</div>
                        <div class="detail-grid">
                            <div class="detail-item">
                                <div class="detail-label">Run ID</div>
                                <div class="detail-value">${{run.id}}</div>
                            </div>
                            <div class="detail-item">
                                <div class="detail-label">Workspace</div>
                                <div class="detail-value">${{run.workspace_name}}</div>
                            </div>
                            <div class="detail-item">
                                <div class="detail-label">Stage</div>
                                <div class="detail-value">${{run.stage_name}}</div>
                            </div>
                            <div class="detail-item">
                                <div class="detail-label">Status</div>
                                <div class="detail-value"><span class="status status-${{run.status}}">${{run.status}}</span></div>
                            </div>
                            <div class="detail-item">
                                <div class="detail-label">Started</div>
                                <div class="detail-value">${{new Date(run.started_at).toLocaleString()}}</div>
                            </div>
                            ${{run.completed_at ? `
                            <div class="detail-item">
                                <div class="detail-label">Completed</div>
                                <div class="detail-value">${{new Date(run.completed_at).toLocaleString()}}</div>
                            </div>
                            ` : ''}}
                            ${{run.pipeline_name ? `
                            <div class="detail-item">
                                <div class="detail-label">Pipeline</div>
                                <div class="detail-value">${{run.pipeline_name}}</div>
                            </div>
                            ` : ''}}
                            ${{run.backend_type ? `
                            <div class="detail-item">
                                <div class="detail-label">Backend</div>
                                <div class="detail-value">${{run.backend_type}}</div>
                            </div>
                            ` : ''}}
                        </div>
                    </div>

                    <div class="detail-section">
                        <div class="detail-section-title">Input Signals (${{inputs.length}})</div>
                        ${{inputs.length > 0 ? `
                        <ul class="detail-list">
                            ${{inputs.map(s => `
                            <li>
                                <strong>${{s.signal_name}}</strong>
                                <span style="color: var(--text-secondary);">(${{s.signal_type}})</span>
                                ${{s.storage_location ? `
                                <div style="font-size: 0.9rem; color: var(--text-secondary); margin-top: 0.25rem; word-break: break-all;">
                                    ${{s.storage_location}}
                                </div>
                                ` : ''}}
                            </li>
                            `).join('')}}
                        </ul>
                        ` : '<p style="color: var(--text-secondary);">No input signals</p>'}}
                    </div>

                    <div class="detail-section">
                        <div class="detail-section-title">Output Signals (${{outputs.length}})</div>
                        ${{outputs.length > 0 ? `
                        <ul class="detail-list">
                            ${{outputs.map(s => `
                            <li>
                                <strong>${{s.signal_name}}</strong>
                                <span style="color: var(--text-secondary);">(${{s.signal_type}})</span>
                                ${{s.storage_location ? `
                                <div style="font-size: 0.9rem; color: var(--text-secondary); margin-top: 0.25rem; word-break: break-all;">
                                    ${{s.storage_location}}
                                </div>
                                ` : ''}}
                            </li>
                            `).join('')}}
                        </ul>
                        ` : '<p style="color: var(--text-secondary);">No output signals</p>'}}
                    </div>
                `;
            }} catch (error) {{
                console.error('Failed to load run details:', error);
                document.getElementById('modal-body').innerHTML =
                    '<div class="empty-state"><div class="empty-state-icon">⚠️</div><p>Failed to load details</p></div>';
            }}
        }}

        // Close modal on background click
        document.getElementById('detail-modal').addEventListener('click', function(e) {{
            if (e.target.id === 'detail-modal') {{
                closeModal();
            }}
        }});

        // Close modal on Escape key
        document.addEventListener('keydown', function(e) {{
            if (e.key === 'Escape') {{
                closeModal();
            }}
        }});

        // Rendering
        function renderWorkspaces() {{
            const container = document.getElementById('workspaces-container');

            if (data.filteredWorkspaces.length === 0) {{
                const message = data.workspaces.length === 0 ? 'No workspaces found' : 'No workspaces match the current filters';
                container.innerHTML = `<div class="empty-state"><div class="empty-state-icon">📁</div><p>${{message}}</p></div>`;
                return;
            }}

            container.innerHTML = data.filteredWorkspaces.map(ws => `
                <div class="workspace-card" style="cursor: pointer;" onclick="showWorkspaceDetails('${{ws.name}}')">
                    <h3>${{ws.name}}</h3>
                    ${{ws.description ? `<p>${{ws.description}}</p>` : ''}}
                    ${{ws.mount_status ? `<span class="status status-${{ws.mount_status}}">${{ws.mount_status}}</span>` : ''}}
                    <div class="workspace-meta">
                        <div class="meta-item">📦 ${{ws.version_count}} versions</div>
                        ${{ws.parent_workspace ? `<div class="meta-item">🔀 from ${{ws.parent_workspace}}</div>` : ''}}
                    </div>
                </div>
            `).join('');
        }}

        function renderStageRuns() {{
            const container = document.getElementById('stage-runs-container');

            if (data.filteredRuns.length === 0) {{
                const message = data.stageRuns.length === 0 ? 'No stage runs found' : 'No runs match the current filters';
                container.innerHTML = `<div class="empty-state"><div class="empty-state-icon">🎯</div><p>${{message}}</p></div>`;
                return;
            }}

            container.innerHTML = '<div class="timeline">' + data.filteredRuns.map(run => `
                <div class="timeline-item">
                    <div class="timeline-content" style="cursor: pointer;" onclick="showRunDetails('${{run.id}}')">
                        <div class="timeline-time">${{new Date(run.started_at).toLocaleString()}}</div>
                        <strong>${{run.workspace_name}}</strong> / ${{run.stage_name}}
                        <span class="status status-${{run.status}}">${{run.status}}</span>
                        ${{run.pipeline_name ? `<div style="margin-top: 0.5rem; color: var(--text-secondary); font-size: 0.9rem;">Pipeline: ${{run.pipeline_name}}</div>` : ''}}
                    </div>
                </div>
            `).join('') + '</div>';
        }}

        function renderPipelineRuns() {{
            const container = document.getElementById('pipeline-runs-container');

            if (data.pipelineRuns.length === 0) {{
                container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🔄</div><p>No pipeline runs found</p></div>';
                return;
            }}

            container.innerHTML = '<div class="timeline">' + data.pipelineRuns.map(run => `
                <div class="timeline-item">
                    <div class="timeline-content">
                        <div class="timeline-time">${{new Date(run.started_at).toLocaleString()}}</div>
                        <strong>${{run.workspace_name}}</strong> / ${{run.pipeline_name || 'pipeline'}}
                        <span class="status status-${{run.status}}">${{run.status}}</span>
                        <div style="margin-top: 0.5rem; color: var(--text-secondary); font-size: 0.9rem;">
                            ${{run.completed_stages || 0}} / ${{run.total_stages || 0}} stages completed
                        </div>
                    </div>
                </div>
            `).join('') + '</div>';
        }}

        // D3.js graph state
        let graphSimulation = null;
        let graphZoom = null;

        function renderGraph() {{
            if (!data.graph || data.graph.nodes.length === 0) {{
                document.getElementById('graph-svg').innerHTML = '';
                return;
            }}

            // Clear existing graph
            d3.select('#graph-svg').selectAll('*').remove();

            const svg = d3.select('#graph-svg');
            const container = svg.node().getBoundingClientRect();
            const width = container.width;
            const height = container.height;

            // Create zoom behavior
            graphZoom = d3.zoom()
                .scaleExtent([0.1, 4])
                .on('zoom', (event) => {{
                    g.attr('transform', event.transform);
                }});

            svg.call(graphZoom);

            // Main group for zoom/pan
            const g = svg.append('g');

            // Create copy of data for simulation
            const nodes = data.graph.nodes.map(d => ({{...d}}));
            const links = data.graph.edges.map(d => ({{
                source: d.source,
                target: d.target,
                signal: d.signal,
                type: d.type
            }}));

            // Create force simulation
            graphSimulation = d3.forceSimulation(nodes)
                .force('link', d3.forceLink(links).id(d => d.id).distance(100))
                .force('charge', d3.forceManyBody().strength(-300))
                .force('center', d3.forceCenter(width / 2, height / 2))
                .force('collision', d3.forceCollide().radius(30));

            // Create arrow markers for directed edges
            svg.append('defs').append('marker')
                .attr('id', 'arrowhead')
                .attr('viewBox', '-0 -5 10 10')
                .attr('refX', 25)
                .attr('refY', 0)
                .attr('orient', 'auto')
                .attr('markerWidth', 8)
                .attr('markerHeight', 8)
                .append('svg:path')
                .attr('d', 'M 0,-5 L 10 ,0 L 0,5')
                .attr('fill', '#E0E0E0')
                .style('stroke', 'none');

            // Create links
            const link = g.append('g')
                .selectAll('line')
                .data(links)
                .join('line')
                .attr('class', 'graph-link')
                .attr('marker-end', 'url(#arrowhead)')
                .on('mouseover', function(event, d) {{
                    d3.select(this).classed('graph-link-hover', true);

                    // Show tooltip
                    const tooltip = g.append('text')
                        .attr('class', 'link-tooltip')
                        .attr('x', (d.source.x + d.target.x) / 2)
                        .attr('y', (d.source.y + d.target.y) / 2)
                        .attr('text-anchor', 'middle')
                        .attr('fill', 'var(--goldfish-orange)')
                        .attr('font-weight', 'bold')
                        .text(`${{d.signal}} (${{d.type}})`);
                }})
                .on('mouseout', function() {{
                    d3.select(this).classed('graph-link-hover', false);
                    g.selectAll('.link-tooltip').remove();
                }});

            // Create nodes
            const node = g.append('g')
                .selectAll('circle')
                .data(nodes)
                .join('circle')
                .attr('class', d => `graph-node graph-node-${{d.status}}`)
                .attr('r', 12)
                .attr('stroke', '#fff')
                .attr('stroke-width', 2)
                .on('click', (event, d) => {{
                    event.stopPropagation();
                    showRunDetails(d.id);
                }})
                .call(d3.drag()
                    .on('start', dragStarted)
                    .on('drag', dragged)
                    .on('end', dragEnded));

            // Create labels
            const label = g.append('g')
                .selectAll('text')
                .data(nodes)
                .join('text')
                .attr('class', 'graph-label')
                .attr('text-anchor', 'middle')
                .attr('dy', -18)
                .text(d => `${{d.workspace}}/${{d.stage}}`);

            // Update positions on each tick
            graphSimulation.on('tick', () => {{
                link
                    .attr('x1', d => d.source.x)
                    .attr('y1', d => d.source.y)
                    .attr('x2', d => d.target.x)
                    .attr('y2', d => d.target.y);

                node
                    .attr('cx', d => d.x)
                    .attr('cy', d => d.y);

                label
                    .attr('x', d => d.x)
                    .attr('y', d => d.y);
            }});

            function dragStarted(event, d) {{
                if (!event.active) graphSimulation.alphaTarget(0.3).restart();
                d.fx = d.x;
                d.fy = d.y;
            }}

            function dragged(event, d) {{
                d.fx = event.x;
                d.fy = event.y;
            }}

            function dragEnded(event, d) {{
                if (!event.active) graphSimulation.alphaTarget(0);
                d.fx = null;
                d.fy = null;
            }}

            // Populate workspace filter dropdown
            const workspaces = [...new Set(nodes.map(n => n.workspace))].sort();
            const select = document.getElementById('workspace-filter-graph');
            const currentValue = select.value;
            select.innerHTML = '<option value="">All Workspaces</option>' +
                workspaces.map(ws => `<option value="${{ws}}">${{ws}}</option>`).join('');
            select.value = currentValue;
        }}

        function resetGraphZoom() {{
            if (graphZoom) {{
                d3.select('#graph-svg')
                    .transition()
                    .duration(750)
                    .call(graphZoom.transform, d3.zoomIdentity);
            }}
        }}

        function centerGraph() {{
            if (graphSimulation) {{
                const svg = d3.select('#graph-svg');
                const container = svg.node().getBoundingClientRect();
                graphSimulation.force('center', d3.forceCenter(container.width / 2, container.height / 2));
                graphSimulation.alpha(0.3).restart();
            }}
        }}

        // Initialize
        loadWorkspaces();

        // Auto-refresh every 30 seconds
        setInterval(() => {{
            if (currentView === 'workspaces') loadWorkspaces();
            else if (currentView === 'runs' && currentRunsTab === 'stage') loadStageRuns();
            else if (currentView === 'runs' && currentRunsTab === 'pipeline') loadPipelineRuns();
            else if (currentView === 'graph') loadGraph();
        }}, 30000);
    </script>
</body>
</html>"""


def spawn_web_server(port: int = DEFAULT_WEB_PORT, open_browser: bool = True) -> int:
    """Spawn the global web server as a detached background process.

    Args:
        port: Port to listen on
        open_browser: Whether to open browser after starting

    Returns:
        PID of spawned server
    """
    import subprocess

    logger.debug("Spawning global web server")

    cmd = [sys.executable, "-m", "goldfish", "web", "--port", str(port)]

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


def run_web_server(port: int = DEFAULT_WEB_PORT, open_browser: bool = True) -> None:
    """Entry point for running the global web server."""
    # Acquire lock immediately
    lock_file = get_web_lock_file()
    lock_fd = os.open(str(lock_file), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        # Web server already running
        os.close(lock_fd)
        logger.warning("Global web server already running")
        sys.exit(0)

    from goldfish.logging import setup_logging

    setup_logging(component="web")

    logger.info("Starting Goldfish global web server")

    server = GoldfishWebServer(port)

    try:
        server.initialize()

        # Open browser if requested
        if open_browser:
            url = f"http://127.0.0.1:{server.port}"
            logger.info("Opening browser: %s", url)
            threading.Timer(1.0, lambda: webbrowser.open(url)).start()

        server.run()
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
