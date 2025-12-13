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
from goldfish.validation import validate_workspace_name

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
                # TODO: Add validation for run_id format
                details = self._get_run_details(db, run_id)
                self._send_json(details)

            elif endpoint == "pipelines":
                limit = int(query_params.get("limit", ["100"])[0])
                pipelines = self._get_pipeline_runs(db, limit)
                self._send_json({"pipelines": pipelines})

            elif endpoint == "pipeline" and len(path_parts) > 1:
                pipeline_id = path_parts[1]
                # TODO: Add validation for pipeline_id format
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
    # This would be the full UI from before, but scoped to this project
    # For now, placeholder
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>{project.name} - Goldfish Provenance</title>
</head>
<body>
    <h1>Project: {project.name}</h1>
    <p>Full UI coming soon...</p>
    <p><a href="/">← Back to projects</a></p>
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
