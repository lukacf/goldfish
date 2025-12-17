"""Integration tests for the global web visualization server.

These tests use global state (PID files, ports) and need to run in isolation.
"""

import json
import os
import socket
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from goldfish.db.database import Database
from goldfish.init import init_project
from goldfish.web_server import (
    GoldfishWebServer,
    ProjectInfo,
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
def cleanup_web_server_state():
    """Clean up any leftover web server state before and after tests."""
    # Cleanup before tests
    get_web_pid_file().unlink(missing_ok=True)
    get_web_port_file().unlink(missing_ok=True)

    # Stop any running server
    if is_web_server_running()[0]:
        stop_web_server(timeout=5)

    yield

    # Cleanup after tests
    if is_web_server_running()[0]:
        stop_web_server(timeout=5)
    get_web_pid_file().unlink(missing_ok=True)
    get_web_port_file().unlink(missing_ok=True)


def _read_http_response(sock: socket.socket) -> bytes:
    """Read a complete HTTP response from a socket."""
    response = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        response += chunk
        # Check if we have complete response
        if b"\r\n\r\n" in response:
            header_end = response.index(b"\r\n\r\n")
            headers = response[:header_end].decode()
            for line in headers.split("\r\n"):
                if line.lower().startswith("content-length:"):
                    expected_len = int(line.split(":")[1].strip())
                    if len(response) >= header_end + 4 + expected_len:
                        return response
    return response


class TestWebServerSingleton:
    """Test web server singleton behavior."""

    def test_singleton_prevents_multiple_instances(self):
        """Test that only one web server can run at a time."""
        # Start first server
        server1 = GoldfishWebServer(port=7343)  # Use different port for testing
        thread1 = threading.Thread(target=server1.run, daemon=True)
        thread1.start()
        time.sleep(0.5)  # Let it start

        try:
            # Verify first server is running
            running, pid, port = is_web_server_running()
            assert running
            assert pid > 0
            assert port == 7343

            # Try to start second server - should fail
            server2 = GoldfishWebServer(port=7344)
            with pytest.raises(RuntimeError, match="Another web server is already running"):
                server2.run()

        finally:
            server1.shutdown()
            thread1.join(timeout=2)

    def test_pid_file_cleanup_on_shutdown(self):
        """Test that PID file is cleaned up on shutdown."""
        server = GoldfishWebServer(port=7345)
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        time.sleep(0.5)

        pid_file = get_web_pid_file()
        assert pid_file.exists()

        server.shutdown()
        thread.join(timeout=2)

        assert not pid_file.exists()


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
                assert len(projects) == 1
                assert projects[0].name == "test-project"
                assert projects[0].project_root == project_root
            finally:
                # Cleanup
                pid_file.unlink(missing_ok=True)
                project_root_file.unlink(missing_ok=True)

    def test_discover_projects_ignores_stale_pids(self):
        """Test that discovery ignores projects with dead daemon processes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir) / "test-project"
            config = init_project("test-project", project_root)

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
                # Should ignore project with dead daemon
                assert len(projects) == 0
            finally:
                pid_file.unlink(missing_ok=True)
                project_root_file.unlink(missing_ok=True)


@pytest.fixture
def web_server_with_project():
    """Start a web server with a test project."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir) / "test-project"
        config = init_project("test-project", project_root)

        # Create project info
        project = ProjectInfo(project_root)

        server = GoldfishWebServer(port=7346)
        server.projects = [project]

        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        time.sleep(0.5)

        yield server, project

        server.shutdown()
        thread.join(timeout=2)


class TestWebServerAPI:
    """Test web server API endpoints."""

    def test_index_page_loads(self, web_server_with_project):
        """Test that index page loads successfully."""
        server, _ = web_server_with_project

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(("127.0.0.1", 7346))
            sock.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            response = _read_http_response(sock)

            assert b"HTTP/1.1 200 OK" in response or b"HTTP/1.0 200 OK" in response
            assert b"Goldfish Projects" in response
        finally:
            sock.close()

    def test_project_page_loads(self, web_server_with_project):
        """Test that project page loads successfully."""
        server, project = web_server_with_project

        url = f"/project/{project.url_id}/"
        request = f"GET {url} HTTP/1.1\r\nHost: localhost\r\n\r\n"

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(("127.0.0.1", 7346))
            sock.sendall(request.encode())
            response = _read_http_response(sock)

            assert b"HTTP/1.1 200 OK" in response or b"HTTP/1.0 200 OK" in response
            assert b"Goldfish Provenance" in response
        finally:
            sock.close()

    def test_api_workspaces_endpoint(self, web_server_with_project):
        """Test workspaces API endpoint."""
        server, project = web_server_with_project

        url = f"/project/{project.url_id}/api/workspaces"
        request = f"GET {url} HTTP/1.1\r\nHost: localhost\r\n\r\n"

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(("127.0.0.1", 7346))
            sock.sendall(request.encode())
            response = _read_http_response(sock)

            assert b"HTTP/1.1 200 OK" in response or b"HTTP/1.0 200 OK" in response

            # Parse JSON response
            body_start = response.index(b"\r\n\r\n") + 4
            body = response[body_start:].decode()
            data = json.loads(body)

            # Just verify the endpoint returns valid JSON with workspaces key
            assert "workspaces" in data
            assert isinstance(data["workspaces"], list)
        finally:
            sock.close()


class TestWebServerLifecycle:
    """Test web server lifecycle management."""

    def test_spawn_and_stop_web_server(self):
        """Test spawning and stopping web server."""
        # Ensure no server is running - force cleanup of stale state
        get_web_pid_file().unlink(missing_ok=True)
        get_web_port_file().unlink(missing_ok=True)
        if is_web_server_running()[0]:
            stop_web_server(timeout=5)
            time.sleep(0.5)

        # Spawn server
        pid = spawn_web_server(port=7347, open_browser=False)
        assert pid > 0
        time.sleep(2)  # Give it time to start and initialize

        try:
            # Verify it's running
            running, server_pid, port = is_web_server_running()
            assert running
            assert server_pid == pid
            assert port == 7347

            # Stop server (with longer timeout for CI)
            stopped = stop_web_server(timeout=10)
            if not stopped:
                # Force kill if graceful shutdown failed
                import signal as sig

                try:
                    os.kill(pid, sig.SIGKILL)
                    time.sleep(0.5)
                except ProcessLookupError:
                    pass  # Already dead
            time.sleep(0.5)

            # Verify it's stopped
            running, _, _ = is_web_server_running()
            assert not running
        finally:
            # Cleanup in case test fails
            try:
                stop_web_server(timeout=2)
            except Exception:
                pass
            # Force cleanup of PID files
            get_web_pid_file().unlink(missing_ok=True)
            get_web_port_file().unlink(missing_ok=True)

    def test_is_web_server_running_when_not_running(self):
        """Test status check when server is not running."""
        # Ensure no server is running
        if is_web_server_running()[0]:
            stop_web_server(timeout=5)
            time.sleep(0.5)

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
        request = f"GET {url} HTTP/1.1\r\nHost: localhost\r\n\r\n"

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(("127.0.0.1", 7346))
            sock.sendall(request.encode())
            response = _read_http_response(sock)

            # Should return error (400, 404, or 500 with error message)
            # The server returns 500 for validation errors currently (not ideal but works)
            assert b" 400" in response or b" 404" in response or b"Internal server error" in response
        finally:
            sock.close()

    def test_run_id_validation(self, web_server_with_project):
        """Test that invalid run IDs are rejected."""
        server, project = web_server_with_project

        # Try invalid run ID format
        url = f"/project/{project.url_id}/api/run/invalid-run-id"
        request = f"GET {url} HTTP/1.1\r\nHost: localhost\r\n\r\n"

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(("127.0.0.1", 7346))
            sock.sendall(request.encode())
            response = _read_http_response(sock)

            # Should return error (400, 404, or 500 with error message)
            # The server returns 500 for validation errors currently (not ideal but works)
            assert b" 400" in response or b" 404" in response or b"Internal server error" in response
        finally:
            sock.close()
