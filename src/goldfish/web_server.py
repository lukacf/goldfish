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
from goldfish.errors import GoldfishError, ProjectNotInitializedError
from goldfish.validation import (
    ValidationError,
    validate_pipeline_run_id,
    validate_stage_run_id,
    validate_workspace_name,
)

# Web template and static file paths
WEB_DIR = Path(__file__).parent / "web"
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"

# MIME types for static files
MIME_TYPES = {
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".html": "text/html; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
}

logger = logging.getLogger("goldfish.web")

# Web server version
WEB_SERVER_VERSION = "1.0"
API_VERSION = "v1"

# Global web server port (single instance for all projects)
DEFAULT_WEB_PORT = 7342  # "FISH" on phone keypad

# Constants
SOCKET_DIR_HASH_LENGTH = 8  # Shorter hash for URL readability
DEFAULT_API_LIMIT = 100
MAX_API_LIMIT = 1000
MAX_GRAPH_NODES = 200  # D3 force simulation struggles with more
GRAPH_CACHE_TTL = 30  # Cache graph queries for 30 seconds


class GraphCache:
    """Simple time-based cache for graph queries.

    Thread-safe with RLock protection.
    """

    def __init__(self, ttl: int = GRAPH_CACHE_TTL):
        self._cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._lock = threading.RLock()
        self._ttl = ttl

    def get(self, key: str) -> dict[str, Any] | None:
        """Get cached value if not expired."""
        with self._lock:
            if key in self._cache:
                timestamp, value = self._cache[key]
                if time.time() - timestamp < self._ttl:
                    return value
                # Expired - remove it
                del self._cache[key]
            return None

    def set(self, key: str, value: dict[str, Any]) -> None:
        """Store value in cache."""
        with self._lock:
            self._cache[key] = (time.time(), value)
            # Clean up old entries (simple LRU-like behavior)
            if len(self._cache) > 100:
                # Remove oldest entries
                sorted_keys = sorted(self._cache.keys(), key=lambda k: self._cache[k][0])
                for old_key in sorted_keys[:50]:
                    del self._cache[old_key]

    def clear(self) -> None:
        """Clear all cached values."""
        with self._lock:
            self._cache.clear()


# Global cache instance for graph queries
_graph_cache = GraphCache()


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
        # Human-readable URL: name-shorthash (e.g., my-project-a7b2c3d4)
        short_hash = hashlib.sha256(str(project_root.resolve()).encode()).hexdigest()[:SOCKET_DIR_HASH_LENGTH]
        # Slugify name: lowercase, replace spaces/underscores with hyphens
        slug = self.name.lower().replace("_", "-").replace(" ", "-")
        self.url_id = f"{slug}-{short_hash}"

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
        # No CORS header - this is a localhost-only server for security
        # Adding Access-Control-Allow-Origin: * would allow malicious websites to access local data
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

    def _send_static_file(self, file_path: Path) -> bool:
        """Send a static file. Returns True if file was sent, False if not found."""
        # Security: ensure path doesn't escape STATIC_DIR
        try:
            resolved = file_path.resolve()
            if not resolved.is_relative_to(STATIC_DIR.resolve()):
                logger.warning("Path traversal attempt: %s", file_path)
                return False
            if resolved.is_symlink():
                logger.warning("Symlink detected: %s", file_path)
                return False
        except (ValueError, OSError):
            return False

        if not resolved.is_file():
            return False

        # Get MIME type
        suffix = resolved.suffix.lower()
        content_type = MIME_TYPES.get(suffix, "application/octet-stream")

        try:
            content = resolved.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(content)))
            # Cache static assets for 1 hour
            self.send_header("Cache-Control", "public, max-age=3600")
            self.end_headers()
            self.wfile.write(content)
            return True
        except OSError as e:
            logger.error("Error reading static file %s: %s", file_path, e)
            return False

    def _parse_limit(self, query_params: dict, default: int = DEFAULT_API_LIMIT) -> int:
        """Parse and validate limit query parameter."""
        try:
            limit = int(query_params.get("limit", [str(default)])[0])
            return max(1, min(limit, MAX_API_LIMIT))  # Clamp to [1, MAX_API_LIMIT]
        except (ValueError, IndexError):
            return default

    def _parse_offset(self, query_params: dict) -> int:
        """Parse and validate offset query parameter."""
        try:
            offset = int(query_params.get("offset", ["0"])[0])
            return max(0, offset)
        except (ValueError, IndexError):
            return 0

    def _get_project_by_id(self, project_id: str) -> ProjectInfo | None:
        """Get project by URL ID (thread-safe)."""
        server = self.server  # type: ignore[attr-defined]
        projects_lock: threading.RLock = server.projects_lock  # type: ignore[attr-defined]

        with projects_lock:
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

            # API: List all projects (versioned, thread-safe)
            # Re-discovers projects on each call to pick up new/stopped daemons
            elif path == f"/api/{API_VERSION}/projects" or path == "/api/projects":
                server = self.server  # type: ignore[attr-defined]
                projects_lock: threading.RLock = server.projects_lock  # type: ignore[attr-defined]

                # Refresh project list to pick up new daemons
                with projects_lock:
                    server.projects = discover_projects()  # type: ignore[attr-defined]
                    projects: list[ProjectInfo] = server.projects  # type: ignore[attr-defined]
                    project_list = [
                        {
                            "id": p.url_id,
                            "name": p.name,
                            "root": str(p.project_root),
                        }
                        for p in projects
                    ]

                self._send_json({"projects": project_list})
                return

            # Health check (versioned, thread-safe)
            elif path == f"/api/{API_VERSION}/health" or path == "/api/health":
                server = self.server  # type: ignore[attr-defined]
                lock = server.projects_lock  # type: ignore[attr-defined]
                with lock:
                    projects_count = len(server.projects)  # type: ignore[attr-defined]

                self._send_json(
                    {
                        "status": "healthy",
                        "version": WEB_SERVER_VERSION,
                        "api_version": API_VERSION,
                        "pid": os.getpid(),
                        "projects_count": projects_count,
                    }
                )
                return

            # Static files: /static/...
            elif path.startswith("/static/"):
                # Remove /static/ prefix and construct file path
                relative_path = path[8:]  # len("/static/") == 8
                file_path = STATIC_DIR / relative_path
                if self._send_static_file(file_path):
                    return
                self._send_error_json(404, "Static file not found")
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

                # Project API routes (versioned and legacy)
                # Supports both /project/<id>/api/v1/<endpoint> and /project/<id>/api/<endpoint>
                elif len(parts) >= 4 and parts[3] == "api":
                    api_parts = parts[4:]
                    # Skip version prefix if present (for backward compatibility)
                    if api_parts and api_parts[0] == API_VERSION:
                        api_parts = api_parts[1:]
                    self._handle_project_api(project, api_parts, query_params)
                    return

            self._send_error_json(404, "Not found")

        except ValidationError as e:
            # Client provided invalid input - 400 Bad Request
            logger.warning("Validation error: %s", e)
            self._send_error_json(400, str(e))
        except ValueError as e:
            # Resource not found - 404
            logger.warning("Not found: %s", e)
            self._send_error_json(404, str(e))
        except GoldfishError as e:
            # Known Goldfish error - usually 4xx
            logger.warning("Goldfish error: %s", e)
            self._send_error_json(400, str(e))
        except Exception as e:
            # Unknown error - 500 Internal Server Error
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
                self._send_json(
                    {
                        "data": workspaces,
                        "pagination": {"total": len(workspaces)},
                    }
                )

            elif endpoint == "workspace" and len(path_parts) > 1:
                workspace_name = path_parts[1]
                validate_workspace_name(workspace_name)  # Security: validate input
                details = self._get_workspace_details(db, workspace_name)
                self._send_json({"data": details})

            elif endpoint == "runs":
                limit = self._parse_limit(query_params)
                offset = self._parse_offset(query_params)
                runs, total = self._get_stage_runs(db, limit, offset)
                self._send_json(
                    {
                        "data": runs,
                        "pagination": {
                            "limit": limit,
                            "offset": offset,
                            "total": total,
                            "has_more": offset + len(runs) < total,
                        },
                    }
                )

            elif endpoint == "run" and len(path_parts) > 1:
                run_id = path_parts[1]
                validate_stage_run_id(run_id)  # Security: validate input
                details = self._get_run_details(db, run_id)
                self._send_json({"data": details})

            elif endpoint == "pipelines":
                limit = self._parse_limit(query_params)
                offset = self._parse_offset(query_params)
                pipelines, total = self._get_pipeline_runs(db, limit, offset)
                self._send_json(
                    {
                        "data": pipelines,
                        "pagination": {
                            "limit": limit,
                            "offset": offset,
                            "total": total,
                            "has_more": offset + len(pipelines) < total,
                        },
                    }
                )

            elif endpoint == "pipeline" and len(path_parts) > 1:
                pipeline_id = path_parts[1]
                validate_pipeline_run_id(pipeline_id)  # Security: validate input
                details = self._get_pipeline_details(db, pipeline_id)
                self._send_json({"data": details})

            elif endpoint == "graph" or endpoint == "lineage":
                # Version lineage graph (git-style DAG)
                lineage = self._get_version_lineage(db)
                self._send_json({"data": lineage})

            elif endpoint == "stages":
                # Stage versions with run counts
                stages = self._get_stage_versions(db)
                self._send_json({"data": stages})

            else:
                self._send_error_json(404, "API endpoint not found")

        finally:
            # Note: Database connections are handled by context managers in each method
            pass

    def _get_workspaces(self, db: Database) -> list[dict[str, Any]]:
        """Get list of all workspaces with version counts (excluding pruned)."""
        with db._conn() as conn:
            rows = conn.execute(
                """
                SELECT wl.workspace_name, wl.description, wl.created_at,
                       wl.parent_workspace, wl.parent_version,
                       COUNT(DISTINCT CASE WHEN wv.pruned_at IS NULL THEN wv.version END) as version_count,
                       COUNT(DISTINCT CASE WHEN wv.pruned_at IS NOT NULL THEN wv.version END) as pruned_count,
                       MAX(CASE WHEN wv.pruned_at IS NULL THEN wv.created_at END) as last_version_at,
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
                "pruned_count": r["pruned_count"],
                "last_version_at": r["last_version_at"],
                "mount_status": r["mount_status"],
            }
            for r in rows
        ]

    def _get_workspace_details(self, db: Database, workspace_name: str) -> dict[str, Any]:
        """Get detailed information about a workspace (excluding pruned versions)."""
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

            # Get versions (excluding pruned) with their tags
            versions = conn.execute(
                """
                SELECT wv.*, wvt.tag_name
                FROM workspace_versions wv
                LEFT JOIN workspace_version_tags wvt
                    ON wv.workspace_name = wvt.workspace_name AND wv.version = wvt.version
                WHERE wv.workspace_name = ? AND wv.pruned_at IS NULL
                ORDER BY wv.created_at DESC
                """,
                (workspace_name,),
            ).fetchall()

            # Get pruned count
            pruned_count = conn.execute(
                """
                SELECT COUNT(*) as count FROM workspace_versions
                WHERE workspace_name = ? AND pruned_at IS NOT NULL
                """,
                (workspace_name,),
            ).fetchone()["count"]

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
                "pruned_count": pruned_count,
                "recent_runs": [dict(r) for r in runs],
            }

    def _get_stage_runs(self, db: Database, limit: int = 100, offset: int = 0) -> tuple[list[dict[str, Any]], int]:
        """Get list of stage runs with pagination.

        Returns:
            Tuple of (runs list, total count)
        """
        with db._conn() as conn:
            # Get total count
            total = conn.execute("SELECT COUNT(*) FROM stage_runs").fetchone()[0]

            # Get paginated runs
            rows = conn.execute(
                """
                SELECT sr.*, sv.version_num as stage_version_num
                FROM stage_runs sr
                LEFT JOIN stage_versions sv ON sr.stage_version_id = sv.id
                ORDER BY sr.started_at DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()

        return [dict(r) for r in rows], total

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

    def _get_pipeline_runs(self, db: Database, limit: int = 100, offset: int = 0) -> tuple[list[dict[str, Any]], int]:
        """Get list of pipeline runs with pagination.

        Returns:
            Tuple of (pipelines list, total count)
        """
        with db._conn() as conn:
            # Get total count
            total = conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]

            # Get paginated pipelines
            rows = conn.execute(
                """
                SELECT pr.*,
                       COUNT(DISTINCT psq.stage_name) as total_stages,
                       SUM(CASE WHEN psq.status = 'completed' THEN 1 ELSE 0 END) as completed_stages
                FROM pipeline_runs pr
                LEFT JOIN pipeline_stage_queue psq ON pr.id = psq.pipeline_run_id
                GROUP BY pr.id
                ORDER BY pr.started_at DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()

        return [dict(r) for r in rows], total

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

    def _get_version_lineage(self, db: Database) -> dict[str, Any]:
        """Get workspace/version lineage for git-style DAG visualization.

        Excludes pruned versions by default and includes tags.

        Returns:
            {
                "workspaces": [
                    {
                        "name": "baseline",
                        "parent": null,
                        "parent_version": null,
                        "created_at": "...",
                        "pruned_count": 5,
                        "versions": [
                            {"version": "v1", "git_sha": "abc123", "created_at": "...", "created_by": "run", "tag_name": null},
                            {"version": "v2", "git_sha": "def456", "created_at": "...", "created_by": "checkpoint", "tag_name": "baseline-working"},
                        ]
                    },
                    ...
                ]
            }
        """
        with db._conn() as conn:
            # Get all workspaces with lineage info and pruned counts
            workspaces = conn.execute(
                """
                SELECT wl.workspace_name, wl.parent_workspace, wl.parent_version,
                       wl.created_at, wl.description,
                       COUNT(CASE WHEN wv.pruned_at IS NOT NULL THEN 1 END) as pruned_count
                FROM workspace_lineage wl
                LEFT JOIN workspace_versions wv ON wl.workspace_name = wv.workspace_name
                GROUP BY wl.workspace_name
                ORDER BY wl.created_at ASC
                """
            ).fetchall()

            # Get all non-pruned versions with tags, grouped by workspace
            versions = conn.execute(
                """
                SELECT wv.workspace_name, wv.version, wv.git_sha, wv.created_at,
                       wv.created_by, wv.description, wvt.tag_name
                FROM workspace_versions wv
                LEFT JOIN workspace_version_tags wvt
                    ON wv.workspace_name = wvt.workspace_name AND wv.version = wvt.version
                WHERE wv.pruned_at IS NULL
                ORDER BY wv.workspace_name, wv.created_at ASC
                """
            ).fetchall()

            # Build versions lookup by workspace
            versions_by_workspace: dict[str, list[dict[str, Any]]] = {}
            for v in versions:
                ws = v["workspace_name"]
                if ws not in versions_by_workspace:
                    versions_by_workspace[ws] = []
                versions_by_workspace[ws].append(
                    {
                        "version": v["version"],
                        "git_sha": v["git_sha"][:8] if v["git_sha"] else None,  # Short SHA
                        "created_at": v["created_at"],
                        "created_by": v["created_by"],
                        "description": v["description"],
                        "tag_name": v["tag_name"],
                    }
                )

            # Build workspace list with versions
            result = []
            for ws in workspaces:
                result.append(
                    {
                        "name": ws["workspace_name"],
                        "parent": ws["parent_workspace"],
                        "parent_version": ws["parent_version"],
                        "created_at": ws["created_at"],
                        "description": ws["description"],
                        "pruned_count": ws["pruned_count"],
                        "versions": versions_by_workspace.get(ws["workspace_name"], []),
                    }
                )

            return {"workspaces": result}

    def _get_stage_versions(self, db: Database) -> dict[str, Any]:
        """Get stage versions grouped by stage with run counts.

        Returns:
            {
                "stages": {
                    "preprocessing": {
                        "versions": [
                            {
                                "version_num": 1,
                                "git_sha": "abc123",
                                "config_hash": "def456...",
                                "created_at": "...",
                                "run_count": 5,
                                "last_run_at": "...",
                                "last_run_status": "completed"
                            },
                            ...
                        ]
                    },
                    "train": {...}
                }
            }
        """
        with db._conn() as conn:
            # Get all stage versions with run counts
            rows = conn.execute(
                """
                SELECT
                    sv.id,
                    sv.workspace_name,
                    sv.stage_name,
                    sv.version_num,
                    sv.git_sha,
                    sv.config_hash,
                    sv.created_at,
                    COUNT(sr.id) as run_count,
                    MAX(sr.started_at) as last_run_at,
                    (SELECT status FROM stage_runs
                     WHERE stage_version_id = sv.id
                     ORDER BY started_at DESC LIMIT 1) as last_run_status
                FROM stage_versions sv
                LEFT JOIN stage_runs sr ON sr.stage_version_id = sv.id
                GROUP BY sv.id
                ORDER BY sv.workspace_name, sv.stage_name, sv.version_num
                """
            ).fetchall()

            # Group by workspace -> stage -> versions
            workspaces: dict[str, dict[str, list]] = {}
            for row in rows:
                ws_name = row["workspace_name"]
                stage_name = row["stage_name"]

                if ws_name not in workspaces:
                    workspaces[ws_name] = {}
                if stage_name not in workspaces[ws_name]:
                    workspaces[ws_name][stage_name] = []

                workspaces[ws_name][stage_name].append(
                    {
                        "id": row["id"],
                        "version_num": row["version_num"],
                        "git_sha": row["git_sha"][:8] if row["git_sha"] else None,
                        "config_hash": row["config_hash"][:12] if row["config_hash"] else None,
                        "created_at": row["created_at"],
                        "run_count": row["run_count"],
                        "last_run_at": row["last_run_at"],
                        "last_run_status": row["last_run_status"],
                    }
                )

            return workspaces

    def _get_provenance_graph(self, db: Database, workspace: str | None = None) -> dict[str, Any]:
        """Get full provenance graph for visualization.

        Uses a single JOIN query instead of N+1 queries for edges.
        """
        with db._conn() as conn:
            # Build nodes (stage runs) - limit to MAX_GRAPH_NODES for D3 performance
            if workspace:
                runs = conn.execute(
                    """
                    SELECT sr.id, sr.workspace_name, sr.stage_name, sr.state,
                           sr.started_at, sr.completed_at, sr.pipeline_name,
                           sv.version_num as stage_version
                    FROM stage_runs sr
                    LEFT JOIN stage_versions sv ON sr.stage_version_id = sv.id
                    WHERE sr.workspace_name = ?
                    ORDER BY sr.started_at DESC
                    LIMIT ?
                    """,
                    (workspace, MAX_GRAPH_NODES),
                ).fetchall()
            else:
                runs = conn.execute(
                    """
                    SELECT sr.id, sr.workspace_name, sr.stage_name, sr.state,
                           sr.started_at, sr.completed_at, sr.pipeline_name,
                           sv.version_num as stage_version
                    FROM stage_runs sr
                    LEFT JOIN stage_versions sv ON sr.stage_version_id = sv.id
                    ORDER BY sr.started_at DESC
                    LIMIT ?
                    """,
                    (MAX_GRAPH_NODES,),
                ).fetchall()

            # Build nodes list
            nodes = [
                {
                    "id": r["id"],
                    "type": "stage_run",
                    "workspace": r["workspace_name"],
                    "stage": r["stage_name"],
                    "state": r["state"],  # state is source of truth
                    "started_at": r["started_at"],
                    "completed_at": r["completed_at"],
                    "pipeline": r["pipeline_name"],
                    "stage_version": r["stage_version"],
                }
                for r in runs
            ]

            # Get run IDs for edge query
            run_ids = [r["id"] for r in runs]
            if not run_ids:
                return {"nodes": nodes, "edges": []}

            # Build edges with single query (fixes N+1 problem)
            # Use IN clause with placeholders
            placeholders = ",".join("?" * len(run_ids))
            edge_rows = conn.execute(
                f"""
                SELECT sl.stage_run_id as target, sl.source_stage_run_id as source,
                       sl.signal_name, sl.signal_type
                FROM signal_lineage sl
                WHERE sl.stage_run_id IN ({placeholders})
                  AND sl.source_stage_run_id IS NOT NULL
                """,
                run_ids,
            ).fetchall()

            edges = [
                {
                    "source": r["source"],
                    "target": r["target"],
                    "signal": r["signal_name"],
                    "type": r["signal_type"],
                }
                for r in edge_rows
            ]

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
        self._projects_lock = threading.RLock()  # Thread safety for projects list

        # Paths
        self.pid_file = get_web_pid_file()
        self.lock_file = get_web_lock_file()
        self.port_file = get_web_port_file()

    def initialize(self) -> None:
        """Initialize the web server - discover projects."""
        logger.info("Initializing global web server")

        # Discover all Goldfish projects
        with self._projects_lock:
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
                self.http_server.projects_lock = self._projects_lock  # type: ignore[attr-defined]
                self.port = port  # Update to actual port
                logger.info("Web server listening on http://127.0.0.1:%d", port)
                return
            except OSError as e:
                if attempt == max_retries - 1:
                    raise RuntimeError(
                        f"Failed to bind to any port in range {self.port}-{self.port + max_retries}"
                    ) from e
                logger.debug("Port %d in use, trying next port", port)
                continue

    def run(self) -> None:
        """Run the web server main loop."""
        # Check if another server is already running (singleton enforcement)
        running, existing_pid, existing_port = is_web_server_running()
        if running:
            raise RuntimeError(f"Another web server is already running (pid={existing_pid}, port={existing_port})")

        self.write_pid_file()
        self.start_http_server()
        self.write_port_file()  # Write after we know the actual port

        # Set up signal handlers (only works in main thread)
        if threading.current_thread() is threading.main_thread():

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
    """Get the HTML for the project list page.

    Loads from templates/index.html file.
    """
    template_path = TEMPLATES_DIR / "index.html"
    if template_path.exists():
        return template_path.read_text(encoding="utf-8")

    # Fallback: minimal inline HTML (should not happen in production)
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Goldfish Projects</title>
    <link rel="stylesheet" href="/static/css/index.css">
</head>
<body>
    <a href="#main-content" class="skip-link">Skip to main content</a>
    <header role="banner">
        <div class="logo" aria-hidden="true">&#x1F420;</div>
        <h1><span class="accent">Goldfish</span> Projects</h1>
        <p class="subtitle">Select a project to explore its provenance</p>
    </header>
    <main id="main-content" role="main" aria-label="Project list">
        <div id="projects-container" role="region" aria-live="polite" aria-busy="true">
            <div class="loading" role="status" aria-label="Loading projects">Loading projects...</div>
        </div>
    </main>
    <script src="/static/js/index.js"></script>
</body>
</html>"""


# NOTE: Old embedded HTML (~220 lines) has been moved to:
# - templates/index.html
# - static/css/index.css
# - static/js/index.js


def get_project_html(project: ProjectInfo) -> str:
    """Get the HTML for a specific project's provenance UI.

    Loads from templates/project.html file and substitutes placeholders.
    Placeholders: {{project_name}}, {{project_url_id}}
    """
    template_path = TEMPLATES_DIR / "project.html"
    if template_path.exists():
        html = template_path.read_text(encoding="utf-8")
        # Simple template substitution (safe - no user input in keys)
        html = html.replace("{{project_name}}", project.name)
        html = html.replace("{{project_url_id}}", project.url_id)
        return html

    # Fallback: minimal inline HTML (should not happen in production)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{project.name} - Goldfish Provenance</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <link rel="stylesheet" href="/static/css/project.css">
</head>
<body>
    <a href="#main-content" class="skip-link">Skip to main content</a>
    <header role="banner">
        <div class="header-content">
            <div class="logo">
                <div class="logo-icon" aria-hidden="true">&#x1F420;</div>
                <div>
                    <h1><span class="logo-accent">Goldfish</span> Provenance</h1>
                    <nav class="breadcrumb" aria-label="Breadcrumb">
                        <a href="/">All Projects</a> &#x203A; <span aria-current="page">{project.name}</span>
                    </nav>
                </div>
            </div>
        </div>
    </header>
    <main id="main-content" role="main">
        <p>Loading project...</p>
    </main>
    <script>const PROJECT_ID = '{project.url_id}';</script>
    <script src="/static/js/project.js"></script>
</body>
</html>"""


def _get_project_html_fallback_stub() -> None:
    """[DEPRECATED] Marker for where old embedded HTML was removed."""
    # The old inline HTML (~1500 lines) has been moved to:
    # - templates/project.html
    # - static/css/project.css
    # - static/js/project.js
    pass


# ====================================================================
# OLD EMBEDDED PROJECT HTML REMOVED
# The following ~1500 lines of embedded CSS/HTML/JS have been moved to:
# - templates/project.html
# - static/css/project.css
# - static/js/project.js
# ====================================================================

# [OLD PROJECT HTML - ~1500 lines removed from here]
# See: templates/project.html, static/css/project.css, static/js/project.js


def _inline_project_html_backup_marker() -> None:
    """[DEPRECATED] Marker showing old embedded HTML has been removed."""
    # Original embedded HTML was ~1500 lines including:
    # - Full CSS for project page styling (~700 lines)
    # - HTML structure for workspaces, runs, graph views (~200 lines)
    # - JavaScript for all interactive functionality (~600 lines)
    #
    # Now moved to external template files for:
    # - Better maintainability
    # - Easier testing
    # - Cleaner separation of concerns
    pass


# ========== OLD HTML CONTENT DELETED HERE ==========
# The following inline content was here:
# - body {{ font-family... }} and other CSS
# - HTML body, header, main, modals
# - JavaScript loadWorkspaces(), renderGraph(), etc.
#
# All now in: templates/project.html, static/css/project.css, static/js/project.js
# ==================================================

# Approximately 1480 more lines of old embedded HTML were removed here.
# The content included CSS styles, HTML structure, and JavaScript code.
#
# To delete the remaining old content programmatically, search for
# the closing pattern '</html>"""' and delete everything up to spawn_web_server().
#
# For now, marking this as the location where old code was.

# =====================================================================
# NOTE: ~1500 lines of old embedded HTML/CSS/JS have been removed
#
# Templates and static files are now in:
# - src/goldfish/web/templates/index.html
# - src/goldfish/web/templates/project.html
# - src/goldfish/web/static/css/index.css
# - src/goldfish/web/static/css/project.css
# - src/goldfish/web/static/js/index.js
# - src/goldfish/web/static/js/project.js
#
# The get_index_html() and get_project_html() functions load these files.
# =====================================================================


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
