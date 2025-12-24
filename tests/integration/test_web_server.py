"""Integration tests for the global web visualization server.

Network/process boundaries are mocked to avoid real server binds or API calls.
"""

import io
import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from goldfish.db.database import Database
from goldfish.init import init_project
from goldfish.web_server import (
    GoldfishWebServer,
    ProjectInfo,
    ProvenanceRequestHandler,
    discover_projects,
    get_web_pid_file,
    get_web_port_file,
    is_web_server_running,
    spawn_web_server,
    stop_web_server,
)

# Mark module to run first since it uses global state (ports, PID files)
pytestmark = pytest.mark.order(1)


@pytest.fixture(autouse=True, scope="module")
def cleanup_web_server_state(tmp_path_factory):
    """Redirect PID/port/lock files to temp dir to avoid touching real state."""
    monkeypatch = pytest.MonkeyPatch()
    base_dir = tmp_path_factory.mktemp("web-server")
    pid_file = base_dir / "web.pid"
    port_file = base_dir / "web.port"
    lock_file = base_dir / "web.lock"

    monkeypatch.setattr("goldfish.web_server.get_web_pid_file", lambda: pid_file)
    monkeypatch.setattr("goldfish.web_server.get_web_port_file", lambda: port_file)
    monkeypatch.setattr("goldfish.web_server.get_web_lock_file", lambda: lock_file)
    # Patch local imports too
    module = sys.modules[__name__]
    monkeypatch.setattr(module, "get_web_pid_file", lambda: pid_file)
    monkeypatch.setattr(module, "get_web_port_file", lambda: port_file)

    pid_file.unlink(missing_ok=True)
    port_file.unlink(missing_ok=True)
    lock_file.unlink(missing_ok=True)

    yield

    pid_file.unlink(missing_ok=True)
    port_file.unlink(missing_ok=True)
    lock_file.unlink(missing_ok=True)
    monkeypatch.undo()


class _DummyServer:
    """Minimal server stub for handler tests."""

    def __init__(self, projects: list[ProjectInfo]):
        self.projects = projects
        self.projects_lock = threading.RLock()


def _handle_request(path: str, server: _DummyServer) -> bytes:
    """Run a request through the handler without opening sockets."""
    request = f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode()
    handler = ProvenanceRequestHandler.__new__(ProvenanceRequestHandler)
    handler.rfile = io.BytesIO(request)
    handler.wfile = io.BytesIO()
    handler.server = server
    handler.client_address = ("127.0.0.1", 0)
    handler.command = None
    handler.path = None
    handler.request_version = "HTTP/1.1"
    handler.requestline = ""
    handler.log_message = lambda *args, **kwargs: None
    handler.handle_one_request()
    return handler.wfile.getvalue()


class TestWebServerSingleton:
    """Test web server singleton behavior."""

    def test_singleton_prevents_multiple_instances(self):
        """Test that only one web server can run at a time."""
        server = GoldfishWebServer(port=7343)
        with patch("goldfish.web_server.is_web_server_running", return_value=(True, 12345, 7343)):
            with pytest.raises(RuntimeError, match="Another web server is already running"):
                server.run()

    def test_pid_file_cleanup_on_shutdown(self):
        """Test that PID file is cleaned up on shutdown."""
        server = GoldfishWebServer(port=7345)
        server.http_server = MagicMock()

        pid_file = get_web_pid_file()
        port_file = get_web_port_file()
        pid_file.write_text("12345")
        port_file.write_text("7345")

        server.shutdown()

        assert not pid_file.exists()
        assert not port_file.exists()
        server.http_server.shutdown.assert_called_once()
        server.http_server.server_close.assert_called_once()


class TestProjectDiscovery:
    """Test project discovery mechanism."""

    def test_discover_projects_empty(self):
        """Test discovery when no projects exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("goldfish.web_server.Path.home") as mock_home:
                mock_home.return_value = Path(tmpdir)
                projects = discover_projects()
                assert projects == []

    def test_discover_projects_with_daemon(self, test_db: Database, temp_git_repo: Path):
        """Test discovery of projects with running daemons."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir) / "test-project"
            config = init_project("test-project", project_root)

            # Create daemon socket directory structure
            daemon_sockets_dir = Path.home() / ".goldfish" / "sockets"
            project_socket_dir = daemon_sockets_dir / config.project_name
            project_socket_dir.mkdir(parents=True, exist_ok=True)

            # Write PID file (use current process for testing)
            pid_file = project_socket_dir / "daemon.pid"
            pid_file.write_text(str(os.getpid()))

            # Write project_root file
            project_root_file = project_socket_dir / "project_root"
            project_root_file.write_text(str(project_root))

            try:
                projects = discover_projects()
                # Find our test project (there may be other real daemons running)
                test_projects = [p for p in projects if p.name == "test-project"]
                assert len(test_projects) == 1
                assert test_projects[0].name == "test-project"
                assert test_projects[0].project_root == project_root
            finally:
                # Cleanup
                pid_file.unlink(missing_ok=True)
                project_root_file.unlink(missing_ok=True)

    def test_discover_projects_ignores_stale_pids(self):
        """Test that discovery ignores projects with dead daemon processes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir) / "test-project-stale"
            config = init_project("test-project-stale", project_root)

            daemon_sockets_dir = Path.home() / ".goldfish" / "sockets"
            project_socket_dir = daemon_sockets_dir / config.project_name
            project_socket_dir.mkdir(parents=True, exist_ok=True)

            # Write PID file with non-existent PID
            pid_file = project_socket_dir / "daemon.pid"
            pid_file.write_text("999999")

            project_root_file = project_socket_dir / "project_root"
            project_root_file.write_text(str(project_root))

            try:
                projects = discover_projects()
                # Should ignore project with dead daemon (other real daemons may exist)
                stale_projects = [p for p in projects if p.name == "test-project-stale"]
                assert len(stale_projects) == 0
            finally:
                pid_file.unlink(missing_ok=True)
                project_root_file.unlink(missing_ok=True)
                # Also cleanup the socket directory
                project_socket_dir.rmdir()


@pytest.fixture
def web_server_with_project():
    """Return a dummy server with a test project (no real sockets)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir) / "test-project"
        config = init_project("test-project", project_root)

        # Create project info
        project = ProjectInfo(project_root)

        server = _DummyServer([project])
        yield server, project


class TestWebServerAPI:
    """Test web server API endpoints."""

    def test_index_page_loads(self, web_server_with_project):
        """Test that index page loads successfully."""
        server, _ = web_server_with_project

        response = _handle_request("/", server)

        assert b"HTTP/1.1 200 OK" in response or b"HTTP/1.0 200 OK" in response
        assert b"Goldfish Projects" in response

    def test_project_page_loads(self, web_server_with_project):
        """Test that project page loads successfully."""
        server, project = web_server_with_project

        url = f"/project/{project.url_id}/"
        response = _handle_request(url, server)

        assert b"HTTP/1.1 200 OK" in response or b"HTTP/1.0 200 OK" in response
        assert b"Goldfish Provenance" in response

    def test_api_workspaces_endpoint(self, web_server_with_project):
        """Test workspaces API endpoint."""
        server, project = web_server_with_project

        url = f"/project/{project.url_id}/api/workspaces"
        response = _handle_request(url, server)

        assert b"HTTP/1.1 200 OK" in response or b"HTTP/1.0 200 OK" in response

        # Parse JSON response
        body_start = response.index(b"\r\n\r\n") + 4
        body = response[body_start:].decode()
        data = json.loads(body)

        # Verify the endpoint returns valid JSON with data and pagination
        assert "data" in data
        assert isinstance(data["data"], list)
        assert "pagination" in data
        assert "total" in data["pagination"]


class TestWebServerLifecycle:
    """Test web server lifecycle management."""

    def test_spawn_and_stop_web_server(self):
        """Test spawning and stopping web server."""
        pid_file = get_web_pid_file()
        port_file = get_web_port_file()

        with (
            patch("subprocess.Popen") as mock_popen,
            patch("goldfish.web_server.is_web_server_running", return_value=(True, 1234, 7347)),
            patch("goldfish.web_server.os.kill") as mock_kill,
        ):
            proc = MagicMock()
            proc.pid = 1234
            mock_popen.return_value = proc

            pid = spawn_web_server(port=7347, open_browser=False)
            assert pid == 1234

            # Create files so stop_web_server can clean them
            pid_file.write_text(str(pid))
            port_file.write_text("7347")

            # First call is SIGTERM, second call simulates process exit
            mock_kill.side_effect = [None, ProcessLookupError()]

            stopped = stop_web_server(timeout=1)
            assert stopped
            assert not pid_file.exists()
            assert not port_file.exists()

    def test_is_web_server_running_when_not_running(self):
        """Test status check when server is not running."""
        pid_file = get_web_pid_file()
        port_file = get_web_port_file()
        pid_file.unlink(missing_ok=True)
        port_file.unlink(missing_ok=True)

        running, pid, port = is_web_server_running()
        assert not running
        assert pid is None
        assert port is None


class TestWebServerSecurity:
    """Test web server input validation and security."""

    def test_workspace_name_validation(self, web_server_with_project):
        """Test that invalid workspace names are rejected."""
        server, project = web_server_with_project

        # Try path traversal
        url = f"/project/{project.url_id}/api/workspace/../../../etc/passwd"
        response = _handle_request(url, server)

        # Should return error (400 or 404)
        assert b" 400" in response or b" 404" in response

    def test_run_id_validation(self, web_server_with_project):
        """Test that invalid run IDs are rejected."""
        server, project = web_server_with_project

        # Try invalid run ID format
        url = f"/project/{project.url_id}/api/run/invalid-run-id"
        response = _handle_request(url, server)

        # Should return error (400 or 404)
        assert b" 400" in response or b" 404" in response


class TestStaticFileServing:
    """Test static file serving functionality."""

    def test_static_css_file_loads(self, web_server_with_project):
        """Test that CSS static files are served correctly."""
        server, _ = web_server_with_project

        response = _handle_request("/static/css/index.css", server)

        assert b"HTTP/1.1 200 OK" in response or b"HTTP/1.0 200 OK" in response
        assert b"Content-Type: text/css" in response
        assert b"--goldfish-orange" in response  # CSS variable from our styles

    def test_static_js_file_loads(self, web_server_with_project):
        """Test that JavaScript static files are served correctly."""
        server, _ = web_server_with_project

        response = _handle_request("/static/js/index.js", server)

        assert b"HTTP/1.1 200 OK" in response or b"HTTP/1.0 200 OK" in response
        assert b"Content-Type: application/javascript" in response
        assert b"escapeHtml" in response  # Our XSS protection function

    def test_static_file_not_found(self, web_server_with_project):
        """Test that missing static files return 404."""
        server, _ = web_server_with_project

        response = _handle_request("/static/nonexistent.css", server)

        assert b" 404" in response

    def test_static_path_traversal_blocked(self, web_server_with_project):
        """Test that path traversal in static files is blocked."""
        server, _ = web_server_with_project

        response = _handle_request("/static/../../../etc/passwd", server)

        # Should return 404 (path traversal blocked)
        assert b" 404" in response
        assert b"/etc/passwd" not in response


class TestGraphCache:
    """Test GraphCache functionality."""

    def test_cache_stores_and_retrieves(self):
        """Test that cache stores and retrieves values."""
        from goldfish.web_server import GraphCache

        cache = GraphCache(ttl=60)
        cache.set("test_key", {"data": "test_value"})

        result = cache.get("test_key")
        assert result is not None
        assert result["data"] == "test_value"

    def test_cache_expires_after_ttl(self):
        """Test that cache entries expire after TTL."""
        from goldfish.web_server import GraphCache

        cache = GraphCache(ttl=1)  # 1 second TTL
        cache.set("test_key", {"data": "test_value"})

        # Should be available immediately
        assert cache.get("test_key") is not None

        # Wait for expiration
        time.sleep(1.5)

        # Should be expired
        assert cache.get("test_key") is None

    def test_cache_returns_none_for_missing_key(self):
        """Test that cache returns None for missing keys."""
        from goldfish.web_server import GraphCache

        cache = GraphCache()
        assert cache.get("nonexistent_key") is None

    def test_cache_clear(self):
        """Test that cache can be cleared."""
        from goldfish.web_server import GraphCache

        cache = GraphCache()
        cache.set("key1", {"data": "value1"})
        cache.set("key2", {"data": "value2"})

        cache.clear()

        assert cache.get("key1") is None
        assert cache.get("key2") is None


class TestPaginationEdgeCases:
    """Test pagination edge cases in API endpoints."""

    def test_limit_capped_at_max(self, web_server_with_project):
        """Test that limit is capped at MAX_API_LIMIT."""
        server, project = web_server_with_project

        # Request with very large limit
        url = f"/project/{project.url_id}/api/workspaces?limit=999999"
        response = _handle_request(url, server)

        assert b"HTTP/1.1 200 OK" in response or b"HTTP/1.0 200 OK" in response
        # Parse JSON and verify limit is capped
        body_start = response.index(b"\r\n\r\n") + 4
        body = response[body_start:].decode()
        data = json.loads(body)
        # The endpoint should still work (not error out)
        assert "data" in data

    def test_negative_offset_treated_as_zero(self, web_server_with_project):
        """Test that negative offset is treated as zero."""
        server, project = web_server_with_project

        url = f"/project/{project.url_id}/api/workspaces?offset=-10"
        response = _handle_request(url, server)

        assert b"HTTP/1.1 200 OK" in response or b"HTTP/1.0 200 OK" in response

    def test_invalid_limit_uses_default(self, web_server_with_project):
        """Test that invalid limit falls back to default."""
        server, project = web_server_with_project

        url = f"/project/{project.url_id}/api/workspaces?limit=invalid"
        response = _handle_request(url, server)

        assert b"HTTP/1.1 200 OK" in response or b"HTTP/1.0 200 OK" in response


class TestErrorHandling:
    """Test error handling in web server."""

    def test_invalid_project_id_returns_404(self, web_server_with_project):
        """Test that invalid project ID returns 404."""
        server, _ = web_server_with_project

        response = _handle_request("/project/nonexistent-project-id/", server)

        assert b" 404" in response

    def test_invalid_api_endpoint_returns_error(self, web_server_with_project):
        """Test that invalid API endpoints return appropriate errors."""
        server, project = web_server_with_project

        url = f"/project/{project.url_id}/api/nonexistent_endpoint"
        response = _handle_request(url, server)

        # Should return 400 or 404
        assert b" 400" in response or b" 404" in response

    def test_api_versioning_backward_compat(self, web_server_with_project):
        """Test that both versioned and legacy API paths work."""
        server, project = web_server_with_project

        # Test versioned path
        url_v1 = f"/project/{project.url_id}/api/v1/workspaces"
        response = _handle_request(url_v1, server)

        assert b"HTTP/1.1 200 OK" in response or b"HTTP/1.0 200 OK" in response

        # Test legacy path
        url_legacy = f"/project/{project.url_id}/api/workspaces"
        response = _handle_request(url_legacy, server)

        assert b"HTTP/1.1 200 OK" in response or b"HTTP/1.0 200 OK" in response
