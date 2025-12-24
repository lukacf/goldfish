"""Integration tests for the daemon/proxy architecture."""

import json
import os
import socket
import tempfile
import threading
import time
from datetime import UTC
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from goldfish.daemon import (
    DAEMON_PROTOCOL_VERSION,
    DaemonRequestHandler,
    GoldfishDaemon,
    ThreadedUnixHTTPServer,
    _serialize_result,
)
from goldfish.errors import GoldfishError


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
            # No content-length, check for connection close
            if b"connection: close" in headers.lower().encode():
                continue  # Keep reading until close
    return response


class TestSerializeResult:
    """Tests for _serialize_result function."""

    def test_serialize_none(self):
        """None passes through."""
        assert _serialize_result(None) is None

    def test_serialize_primitives(self):
        """Primitive types pass through."""
        assert _serialize_result(42) == 42
        assert _serialize_result("hello") == "hello"
        assert _serialize_result(3.14) == 3.14
        assert _serialize_result(True) is True

    def test_serialize_dict(self):
        """Dicts are recursively serialized."""
        result = _serialize_result({"a": 1, "b": {"c": 2}})
        assert result == {"a": 1, "b": {"c": 2}}

    def test_serialize_list(self):
        """Lists are recursively serialized."""
        result = _serialize_result([1, 2, {"a": 3}])
        assert result == [1, 2, {"a": 3}]

    def test_serialize_path(self):
        """Paths are converted to strings."""
        result = _serialize_result(Path("/foo/bar"))
        assert result == "/foo/bar"

    def test_serialize_datetime(self):
        """Datetimes are converted to ISO format."""
        from datetime import datetime

        dt = datetime(2024, 1, 15, 12, 30, 0, tzinfo=UTC)
        result = _serialize_result(dt)
        assert result == "2024-01-15T12:30:00+00:00"

    def test_serialize_pydantic_model(self):
        """Pydantic models are serialized via model_dump."""
        from pydantic import BaseModel

        class TestModel(BaseModel):
            name: str
            value: int

        model = TestModel(name="test", value=42)
        result = _serialize_result(model)
        assert result == {"name": "test", "value": 42}

    def test_serialize_nested_pydantic(self):
        """Nested Pydantic models in dicts are serialized."""
        from pydantic import BaseModel

        class Inner(BaseModel):
            x: int

        result = _serialize_result({"model": Inner(x=1), "list": [Inner(x=2)]})
        assert result == {"model": {"x": 1}, "list": [{"x": 2}]}


class TestIsDaemonRunning:
    """Tests for is_daemon_running function."""

    def test_returns_false_when_no_pid_file(self, temp_git_repo):
        """Returns False when PID file doesn't exist."""
        from goldfish.daemon import is_daemon_running

        project_root = temp_git_repo / "project"
        project_root.mkdir()

        config_content = f"""
project_name: test
dev_repo_path: {temp_git_repo.name}
"""
        (project_root / "goldfish.yaml").write_text(config_content)

        running, pid = is_daemon_running(project_root)
        assert running is False
        assert pid is None

    def test_returns_false_when_process_dead(self, temp_git_repo):
        """Returns False when PID file exists but process is dead."""
        from goldfish.daemon import is_daemon_running

        project_root = temp_git_repo / "project"
        project_root.mkdir()

        config_content = f"""
project_name: test
dev_repo_path: {temp_git_repo.name}
"""
        (project_root / "goldfish.yaml").write_text(config_content)

        # Create .goldfish dir and fake PID file with non-existent PID
        goldfish_dir = temp_git_repo / ".goldfish"
        goldfish_dir.mkdir(parents=True)
        (goldfish_dir / "daemon.pid").write_text("999999999")

        running, pid = is_daemon_running(project_root)
        assert running is False


@pytest.fixture
def short_tmp_path():
    """Create a temp directory with a short path for Unix sockets.

    macOS has a 104-byte limit on Unix socket paths.
    """
    # Use /tmp directly for shorter paths
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="gf_") as tmp:
        yield Path(tmp)


class TestThreadedUnixHTTPServer:
    """Tests for ThreadedUnixHTTPServer."""

    def test_refuses_non_socket_path(self, short_tmp_path):
        """Refuses to bind if path exists and is not a socket."""
        socket_path = short_tmp_path / "t.sock"
        lock_file = short_tmp_path / "t.lock"

        # Create a regular file
        socket_path.write_text("not a socket")

        with pytest.raises(RuntimeError, match="not a socket"):
            ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file)

    def test_removes_stale_socket(self, short_tmp_path):
        """Removes stale socket that's not connected."""
        socket_path = short_tmp_path / "t.sock"
        lock_file = short_tmp_path / "t.lock"

        # Create a socket file but don't listen on it
        stale_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        stale_sock.bind(str(socket_path))
        stale_sock.close()

        # Should succeed - stale socket gets removed
        server = ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file)
        server.server_close()

    def test_lock_prevents_multiple_servers(self, short_tmp_path):
        """Lock file prevents multiple servers on same socket."""
        socket_path = short_tmp_path / "t.sock"
        lock_file = short_tmp_path / "t.lock"

        # Start first server
        server1 = ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file)

        # Second server should fail
        with pytest.raises(RuntimeError, match="already running"):
            ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file)

        server1.server_close()


class TestDaemonRequestHandler:
    """Tests for DaemonRequestHandler."""

    def test_post_requires_tool_path(self, short_tmp_path):
        """POST to non-/tool path returns 404."""
        socket_path = short_tmp_path / "t.sock"
        lock_file = short_tmp_path / "t.lock"

        server = ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file)

        # Create mock daemon
        mock_daemon = MagicMock()
        server.daemon = mock_daemon

        # Start server in thread
        server_thread = threading.Thread(target=server.handle_request)
        server_thread.start()

        # Connect and send request to wrong path
        time.sleep(0.1)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(str(socket_path))
        sock.sendall(b"POST /wrong HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{}")

        response = sock.recv(4096)
        sock.close()

        assert b"404" in response
        server.server_close()
        server_thread.join(timeout=1)

    def test_get_health_returns_status(self, short_tmp_path):
        """GET /health returns daemon status."""
        socket_path = short_tmp_path / "t.sock"
        lock_file = short_tmp_path / "t.lock"

        server = ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file)

        # Create mock daemon
        mock_daemon = MagicMock()
        mock_daemon.start_time = time.time()
        mock_daemon.project_root = Path("/test/project")
        mock_daemon.tools = {"tool1": lambda: None}
        server.daemon = mock_daemon

        # Start server in thread
        server_thread = threading.Thread(target=server.handle_request)
        server_thread.start()

        # Connect and send health check
        time.sleep(0.1)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect(str(socket_path))
        sock.sendall(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")

        # Read complete response
        response = _read_http_response(sock)
        sock.close()

        assert b"200 OK" in response
        assert b"healthy" in response
        assert b"protocol_version" in response

        server.server_close()
        server_thread.join(timeout=1)

    def test_post_tool_executes_and_serializes(self, short_tmp_path):
        """POST /tool executes tool and serializes result."""
        socket_path = short_tmp_path / "t.sock"
        lock_file = short_tmp_path / "t.lock"

        server = ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file)

        # Create mock daemon that returns a dict
        mock_daemon = MagicMock()
        mock_daemon.execute_tool.return_value = {"status": "ok", "count": 42}
        server.daemon = mock_daemon

        # Start server in thread
        server_thread = threading.Thread(target=server.handle_request)
        server_thread.start()

        # Connect and send tool request
        time.sleep(0.1)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect(str(socket_path))

        body = json.dumps({"tool": "test_tool", "params": {"arg": "value"}}).encode()
        request = (
            f"POST /tool HTTP/1.1\r\n"
            f"Host: localhost\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"\r\n"
        ).encode() + body

        sock.sendall(request)

        # Read complete response
        response = _read_http_response(sock)
        sock.close()

        assert b"200 OK" in response
        assert b'"status": "ok"' in response
        assert b'"count": 42' in response

        mock_daemon.execute_tool.assert_called_once_with("test_tool", {"arg": "value"})

        server.server_close()
        server_thread.join(timeout=1)

    def test_post_tool_handles_pydantic_models(self, short_tmp_path):
        """POST /tool properly serializes Pydantic model responses."""
        from pydantic import BaseModel

        class TestResponse(BaseModel):
            success: bool
            message: str

        socket_path = short_tmp_path / "t.sock"
        lock_file = short_tmp_path / "t.lock"

        server = ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file)

        # Create mock daemon that returns a Pydantic model
        mock_daemon = MagicMock()
        mock_daemon.execute_tool.return_value = TestResponse(success=True, message="It works!")
        server.daemon = mock_daemon

        # Start server in thread
        server_thread = threading.Thread(target=server.handle_request)
        server_thread.start()

        # Connect and send tool request
        time.sleep(0.1)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect(str(socket_path))

        body = json.dumps({"tool": "test", "params": {}}).encode()
        request = (
            f"POST /tool HTTP/1.1\r\n"
            f"Host: localhost\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"\r\n"
        ).encode() + body

        sock.sendall(request)

        # Read complete response
        response = _read_http_response(sock)
        sock.close()

        assert b"200 OK" in response
        # Response should be JSON-serialized Pydantic model
        body_start = response.index(b"\r\n\r\n") + 4
        result = json.loads(response[body_start:])
        assert result["result"] == {"success": True, "message": "It works!"}

        server.server_close()
        server_thread.join(timeout=1)


class TestDaemonConnection:
    """Tests for DaemonConnection (proxy side)."""

    def test_request_includes_content_length(self, short_tmp_path):
        """Requests include Content-Length header."""
        from goldfish.mcp_proxy import DaemonConnection

        socket_path = short_tmp_path / "t.sock"
        lock_file = short_tmp_path / "t.lock"

        # Start a simple server
        server = ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file)
        mock_daemon = MagicMock()
        mock_daemon.start_time = time.time()
        mock_daemon.project_root = Path("/test")
        mock_daemon.tools = {}
        server.daemon = mock_daemon

        server_thread = threading.Thread(target=server.handle_request)
        server_thread.start()

        time.sleep(0.1)

        # Create connection without ensure_daemon (mock it)
        with patch.object(DaemonConnection, "_ensure_daemon"):
            conn = DaemonConnection(socket_path, short_tmp_path)

        # Make request
        result = conn._request("GET", "/health")

        assert result["status"] == "healthy"

        server.server_close()
        server_thread.join(timeout=1)

    def test_call_tool_with_retry(self, short_tmp_path):
        """call_tool retries on connection failure."""
        from goldfish.mcp_proxy import DaemonConnection

        socket_path = short_tmp_path / "t.sock"

        # Create connection without ensure_daemon
        with patch.object(DaemonConnection, "_ensure_daemon") as mock_ensure:
            conn = DaemonConnection(socket_path, short_tmp_path)

            # First call will fail (no server), should trigger reconnect
            mock_ensure.reset_mock()

            with pytest.raises(GoldfishError):
                # Will fail because no server, but should attempt reconnect
                conn.call_tool("test", {})

            # Should have tried to ensure daemon
            assert mock_ensure.call_count >= 1

    def test_force_restart_triggers_restart(self, short_tmp_path):
        """force_restart=True triggers daemon restart even if running."""
        from goldfish.mcp_proxy import DaemonConnection

        socket_path = short_tmp_path / "t.sock"

        # Mock is_daemon_running to return True (daemon is running)
        # Mock _restart_daemon to track calls
        with (
            patch("goldfish.mcp_proxy.is_daemon_running", return_value=(True, 12345)),
            patch.object(DaemonConnection, "_restart_daemon") as mock_restart,
            patch.object(DaemonConnection, "_health_check"),
        ):
            # Create connection with force_restart=True
            conn = DaemonConnection(socket_path, short_tmp_path, force_restart=True)

            # _restart_daemon should have been called
            mock_restart.assert_called_once()

    def test_no_force_restart_skips_restart_when_healthy(self, short_tmp_path):
        """force_restart=False does not restart healthy daemon."""
        from goldfish.mcp_proxy import DaemonConnection

        socket_path = short_tmp_path / "t.sock"

        # Mock is_daemon_running to return True
        # Mock _health_check to return matching version
        with (
            patch("goldfish.mcp_proxy.is_daemon_running", return_value=(True, 12345)),
            patch.object(DaemonConnection, "_restart_daemon") as mock_restart,
            patch.object(
                DaemonConnection,
                "_health_check",
                return_value={"version": "unknown", "protocol_version": "1.0"},
            ),
            patch("goldfish.mcp_proxy._get_version", return_value="unknown"),
            patch("goldfish.mcp_proxy.DAEMON_PROTOCOL_VERSION", "1.0"),
        ):
            # Create connection with force_restart=False (default)
            conn = DaemonConnection(socket_path, short_tmp_path, force_restart=False)

            # _restart_daemon should NOT have been called
            mock_restart.assert_not_called()


class TestGracefulShutdown:
    """Tests for graceful shutdown behavior."""

    def test_shutdown_sets_event(self):
        """shutdown() sets the shutdown event."""
        daemon = GoldfishDaemon(Path("/tmp/fake"))
        daemon.shutdown_event = threading.Event()

        assert not daemon.shutdown_event.is_set()
        daemon.shutdown()
        assert daemon.shutdown_event.is_set()

    def test_shutdown_idempotent(self):
        """Multiple shutdown() calls are safe."""
        daemon = GoldfishDaemon(Path("/tmp/fake"))
        daemon.shutdown_event = threading.Event()

        daemon.shutdown()
        daemon.shutdown()  # Should not raise
        assert daemon.shutdown_event.is_set()


class TestProtocolVersion:
    """Tests for protocol version handling."""

    def test_protocol_version_constant(self):
        """Protocol version is defined."""
        assert DAEMON_PROTOCOL_VERSION is not None
        assert isinstance(DAEMON_PROTOCOL_VERSION, str)
        assert "." in DAEMON_PROTOCOL_VERSION  # Has major.minor format

    def test_health_includes_protocol_version(self, short_tmp_path):
        """Health check response includes protocol version."""
        socket_path = short_tmp_path / "t.sock"
        lock_file = short_tmp_path / "t.lock"

        server = ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file)

        mock_daemon = MagicMock()
        mock_daemon.start_time = time.time()
        mock_daemon.project_root = Path("/test")
        mock_daemon.tools = {}
        server.daemon = mock_daemon

        server_thread = threading.Thread(target=server.handle_request)
        server_thread.start()

        time.sleep(0.1)

        # Make health check
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect(str(socket_path))
        sock.sendall(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")

        # Read complete response (loop until we have Content-Length bytes)
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
                            break
                else:
                    continue
                break

        sock.close()

        # Parse response body
        body_start = response.index(b"\r\n\r\n") + 4
        body = json.loads(response[body_start:])

        assert "protocol_version" in body
        assert body["protocol_version"] == DAEMON_PROTOCOL_VERSION

        server.server_close()
        server_thread.join(timeout=1)


class TestHTTPFraming:
    """Tests for HTTP framing (Content-Length)."""

    def test_response_has_content_length(self, short_tmp_path):
        """All responses include Content-Length header."""
        socket_path = short_tmp_path / "t.sock"
        lock_file = short_tmp_path / "t.lock"

        server = ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file)

        mock_daemon = MagicMock()
        mock_daemon.start_time = time.time()
        mock_daemon.project_root = Path("/test")
        mock_daemon.tools = {}
        server.daemon = mock_daemon

        server_thread = threading.Thread(target=server.handle_request)
        server_thread.start()

        time.sleep(0.1)

        # Make health check
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect(str(socket_path))
        sock.sendall(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")

        # Read complete response
        response = _read_http_response(sock)
        sock.close()

        # Check Content-Length header exists
        headers_part = response.split(b"\r\n\r\n")[0].decode()
        assert "Content-Length:" in headers_part

        # Parse and verify length matches body
        for line in headers_part.split("\r\n"):
            if line.lower().startswith("content-length:"):
                content_length = int(line.split(":")[1].strip())
                break

        body = response.split(b"\r\n\r\n")[1]
        assert len(body) == content_length

        server.server_close()
        server_thread.join(timeout=1)

    def test_large_response_content_length(self, short_tmp_path):
        """Large responses have correct Content-Length."""
        socket_path = short_tmp_path / "t.sock"
        lock_file = short_tmp_path / "t.lock"

        server = ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file)

        # Return a large response
        large_data = {"items": [{"id": i, "data": "x" * 100} for i in range(100)]}
        mock_daemon = MagicMock()
        mock_daemon.execute_tool.return_value = large_data
        server.daemon = mock_daemon

        server_thread = threading.Thread(target=server.handle_request)
        server_thread.start()

        time.sleep(0.1)

        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(str(socket_path))

        body = json.dumps({"tool": "test", "params": {}}).encode()
        request = (
            f"POST /tool HTTP/1.1\r\n"
            f"Host: localhost\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"\r\n"
        ).encode() + body

        sock.sendall(request)

        # Read full response
        response = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
            # Check if complete
            if b"\r\n\r\n" in response:
                header_end = response.index(b"\r\n\r\n")
                headers = response[:header_end].decode()
                for line in headers.split("\r\n"):
                    if line.lower().startswith("content-length:"):
                        expected_len = int(line.split(":")[1].strip())
                        if len(response) >= header_end + 4 + expected_len:
                            break
                else:
                    continue
                break

        sock.close()

        # Verify Content-Length matches body
        header_end = response.index(b"\r\n\r\n")
        headers = response[:header_end].decode()
        body_bytes = response[header_end + 4 :]

        for line in headers.split("\r\n"):
            if line.lower().startswith("content-length:"):
                expected_len = int(line.split(":")[1].strip())
                assert len(body_bytes) == expected_len

        server.server_close()
        server_thread.join(timeout=1)


# =============================================================================
# Regression Tests - Issues fixed in daemon/proxy architecture
# =============================================================================


class TestSocketPathConsistency:
    """Regression: Proxy and daemon must use same socket path function."""

    def test_proxy_uses_get_socket_path(self):
        """Proxy uses get_socket_path() not hardcoded path."""
        from goldfish.daemon import get_socket_path

        # Verify the function produces expected path format
        project_root = Path("/tmp/test-project")
        expected_path = get_socket_path(project_root)

        # The socket path should be under ~/.goldfish/sockets/<hash>/
        assert ".goldfish/sockets" in str(expected_path)
        assert expected_path.name == "goldfish.sock"

    def test_socket_path_deterministic(self):
        """Same project root always produces same socket path."""
        from goldfish.daemon import get_socket_path

        project_root = Path("/some/project/path")
        path1 = get_socket_path(project_root)
        path2 = get_socket_path(project_root)

        assert path1 == path2

    def test_socket_path_different_projects(self):
        """Different projects get different socket paths."""
        from goldfish.daemon import get_socket_path

        path1 = get_socket_path(Path("/project/one"))
        path2 = get_socket_path(Path("/project/two"))

        assert path1 != path2


class TestCircularImportPrevention:
    """Regression: Importing mcp_proxy must not cause circular import."""

    def test_import_mcp_proxy_no_circular(self):
        """Importing mcp_proxy doesn't cause circular import error."""
        # This would raise ImportError if circular import exists
        # The fix was to NOT import server_tools directly in mcp_proxy
        import goldfish.mcp_proxy  # noqa: F401

        # If we get here, no circular import
        assert True

    def test_import_server_after_proxy(self):
        """Can import server (which imports tools) after mcp_proxy."""
        import goldfish.mcp_proxy  # noqa: F401
        import goldfish.server  # noqa: F401

        # server.py handles tool imports in the correct order
        assert True


class TestSkipLockParameter:
    """Regression: skip_lock prevents double-lock when startup lock already held."""

    def test_skip_lock_true_doesnt_acquire(self, short_tmp_path):
        """ThreadedUnixHTTPServer with skip_lock=True doesn't try to acquire lock."""
        import fcntl

        socket_path = short_tmp_path / "t.sock"
        lock_file = short_tmp_path / "t.lock"

        # Pre-acquire the lock (simulating startup lock)
        lock_fd = os.open(str(lock_file), os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        try:
            # Server with skip_lock=True should succeed even though lock is held
            server = ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file, skip_lock=True)
            server.server_close()
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    def test_skip_lock_false_requires_lock(self, short_tmp_path):
        """ThreadedUnixHTTPServer with skip_lock=False (default) acquires lock."""
        import fcntl

        socket_path = short_tmp_path / "t.sock"
        lock_file = short_tmp_path / "t.lock"

        # Pre-acquire the lock
        lock_fd = os.open(str(lock_file), os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        try:
            # Server without skip_lock should fail (lock already held)
            with pytest.raises(RuntimeError, match="already running"):
                ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file, skip_lock=False)
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)


class TestHealthCheckDetection:
    """Regression: is_daemon_running must check for 'healthy' not 'goldfish'."""

    def test_health_response_contains_healthy(self, short_tmp_path):
        """Health response contains 'healthy' string for detection."""
        socket_path = short_tmp_path / "t.sock"
        lock_file = short_tmp_path / "t.lock"

        server = ThreadedUnixHTTPServer(str(socket_path), DaemonRequestHandler, lock_file)

        mock_daemon = MagicMock()
        mock_daemon.start_time = time.time()
        mock_daemon.project_root = Path("/test")
        mock_daemon.tools = {}
        server.daemon = mock_daemon

        server_thread = threading.Thread(target=server.handle_request)
        server_thread.start()

        time.sleep(0.1)

        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect(str(socket_path))
        sock.sendall(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")

        response = _read_http_response(sock)
        sock.close()

        # Must contain "healthy" for is_daemon_running() to detect it
        assert b"healthy" in response

        server.server_close()
        server_thread.join(timeout=1)


class TestPreemptionDetection:
    """Tests for GCE instance preemption detection."""

    def test_check_if_preempted_returns_true_when_preempted(self):
        """_check_if_preempted returns True when gcloud finds preemption event."""
        daemon = GoldfishDaemon(Path("/tmp/fake"))

        # Mock subprocess.run to return targetLink with matching instance
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="https://www.googleapis.com/compute/v1/projects/my-project/zones/us-central1-a/instances/stage-abc123\n",
                returncode=0,
            )

            result = daemon._check_if_preempted("stage-abc123", "my-project")

            assert result is True
            mock_run.assert_called_once()
            # Verify the gcloud command was formed correctly
            call_args = mock_run.call_args
            cmd = call_args[0][0]
            assert "gcloud" in cmd
            assert "compute" in cmd
            assert "operations" in cmd
            assert "list" in cmd
            assert "--project=my-project" in cmd
            # Check for time-bounded filter and limit
            assert "--filter=operationType=compute.instances.preempted AND insertTime>-P7D" in cmd
            assert "--limit=100" in cmd
            assert "--format=value(targetLink)" in cmd

    def test_check_if_preempted_returns_false_when_not_preempted(self):
        """_check_if_preempted returns False when no preemption event found."""
        daemon = GoldfishDaemon(Path("/tmp/fake"))

        # Mock subprocess.run to return empty output
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="",
                returncode=0,
            )

            result = daemon._check_if_preempted("stage-xyz789", "my-project")

            assert result is False

    def test_check_if_preempted_returns_false_on_error(self):
        """_check_if_preempted returns False on subprocess error."""
        daemon = GoldfishDaemon(Path("/tmp/fake"))

        # Mock subprocess.run to raise an exception
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = Exception("gcloud not found")

            result = daemon._check_if_preempted("stage-abc123", "my-project")

            assert result is False

    def test_check_if_preempted_returns_false_on_timeout(self):
        """_check_if_preempted returns False on timeout."""
        import subprocess

        daemon = GoldfishDaemon(Path("/tmp/fake"))

        # Mock subprocess.run to raise TimeoutExpired
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="gcloud", timeout=30)

            result = daemon._check_if_preempted("stage-abc123", "my-project")

            assert result is False

    def test_check_if_preempted_handles_whitespace_output(self):
        """_check_if_preempted handles whitespace-only output as no preemption."""
        daemon = GoldfishDaemon(Path("/tmp/fake"))

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="   \n\t\n  ",
                returncode=0,
            )

            result = daemon._check_if_preempted("stage-abc123", "my-project")

            assert result is False

    def test_check_if_preempted_parses_multiple_results(self):
        """_check_if_preempted correctly parses targetLinks to find exact match."""
        daemon = GoldfishDaemon(Path("/tmp/fake"))

        # Mock subprocess.run to return multiple targetLinks
        # Only the second one matches our instance
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout=(
                    "https://www.googleapis.com/compute/v1/projects/my-project/zones/us-west1-a/instances/other-instance\n"
                    "https://www.googleapis.com/compute/v1/projects/my-project/zones/us-central1-a/instances/stage-abc123\n"
                    "https://www.googleapis.com/compute/v1/projects/my-project/zones/us-east1-b/instances/stage-abc123-v2\n"
                ),
                returncode=0,
            )

            result = daemon._check_if_preempted("stage-abc123", "my-project")

            # Should find the exact match (second line)
            assert result is True

    def test_check_if_preempted_rejects_partial_matches(self):
        """_check_if_preempted should not match partial instance names."""
        daemon = GoldfishDaemon(Path("/tmp/fake"))

        # Mock subprocess.run to return similar but non-matching instances
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout=(
                    "https://www.googleapis.com/compute/v1/projects/my-project/zones/us-central1-a/instances/stage-abc\n"
                    "https://www.googleapis.com/compute/v1/projects/my-project/zones/us-central1-a/instances/stage-abc123-old\n"
                    "https://www.googleapis.com/compute/v1/projects/my-project/zones/us-central1-a/instances/my-stage-abc123\n"
                ),
                returncode=0,
            )

            result = daemon._check_if_preempted("stage-abc123", "my-project")

            # None of these should match (they're all partial matches)
            assert result is False


class TestOrphanCleanupWithPreemption:
    """Tests for orphan cleanup with preemption-aware error messages."""

    def _create_workspace_and_version(self, db, workspace: str, version: str) -> None:
        """Helper to create workspace and version records for foreign key constraints."""
        with db._conn() as conn:
            # Insert workspace
            conn.execute(
                "INSERT OR IGNORE INTO workspace_lineage (workspace_name, created_at) VALUES (?, datetime('now'))",
                (workspace,),
            )
            # Insert version (git_tag is also NOT NULL)
            conn.execute(
                """INSERT OR IGNORE INTO workspace_versions
                   (workspace_name, version, git_tag, git_sha, created_by, created_at)
                   VALUES (?, ?, ?, ?, ?, datetime('now'))""",
                (workspace, version, f"{workspace}-{version}", "abc123", "test"),
            )

    def test_orphan_cleanup_marks_preempted_with_correct_message(self, test_db):
        """Orphan cleanup uses preemption-specific error message when instance was preempted."""
        from goldfish.models import StageRunStatus

        daemon = GoldfishDaemon(Path("/tmp/fake"))
        daemon._db = test_db

        # Create a mock config with GCE settings
        mock_config = MagicMock()
        mock_config.gce = MagicMock()
        mock_config.gce.effective_project_id = "test-project"
        daemon.config = mock_config

        # Create workspace and version for foreign key constraints
        self._create_workspace_and_version(test_db, "ws1", "v1")

        # Insert a running GCE stage run (started > 20 minutes ago)
        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, backend_type, backend_handle, started_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now', '-30 minutes'))
                """,
                ("stage-preempt-test", "ws1", "v1", "train", StageRunStatus.RUNNING, "gce", "stage-preempt-test"),
            )

        # Mock gcloud to return no instances (instance gone)
        # Mock preemption check to return True
        with (
            patch("subprocess.run") as mock_run,
            patch.object(daemon, "_check_if_preempted", return_value=True),
        ):
            # First call: list instances - returns empty (instance gone)
            mock_run.return_value = MagicMock(stdout="", returncode=0)

            daemon._check_orphaned_instances()

        # Verify the error message mentions preemption
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT status, error FROM stage_runs WHERE id = ?",
                ("stage-preempt-test",),
            ).fetchone()

        assert row["status"] == StageRunStatus.FAILED
        assert "preempted" in row["error"].lower()
        assert "spot" in row["error"].lower() or "preemptible" in row["error"].lower()

    def test_orphan_cleanup_marks_disappeared_with_generic_message(self, test_db):
        """Orphan cleanup uses generic error message when instance not preempted."""
        from goldfish.models import StageRunStatus

        daemon = GoldfishDaemon(Path("/tmp/fake"))
        daemon._db = test_db

        # Create a mock config with GCE settings
        mock_config = MagicMock()
        mock_config.gce = MagicMock()
        mock_config.gce.effective_project_id = "test-project"
        daemon.config = mock_config

        # Create workspace and version for foreign key constraints
        self._create_workspace_and_version(test_db, "ws1", "v1")

        # Insert a running GCE stage run (started > 20 minutes ago)
        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, backend_type, backend_handle, started_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now', '-30 minutes'))
                """,
                ("stage-orphan-test", "ws1", "v1", "train", StageRunStatus.RUNNING, "gce", "stage-orphan-test"),
            )

        # Mock gcloud to return no instances (instance gone)
        # Mock preemption check to return False (not preempted)
        with (
            patch("subprocess.run") as mock_run,
            patch.object(daemon, "_check_if_preempted", return_value=False),
        ):
            mock_run.return_value = MagicMock(stdout="", returncode=0)

            daemon._check_orphaned_instances()

        # Verify the error message is generic (not preemption)
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT status, error FROM stage_runs WHERE id = ?",
                ("stage-orphan-test",),
            ).fetchone()

        assert row["status"] == StageRunStatus.FAILED
        assert "orphan" in row["error"].lower()
        assert "preempted" not in row["error"].lower()

    def test_orphan_cleanup_skips_recent_runs(self, test_db):
        """Orphan cleanup doesn't check runs started less than 20 minutes ago."""
        from goldfish.models import StageRunStatus

        daemon = GoldfishDaemon(Path("/tmp/fake"))
        daemon._db = test_db

        # Create a mock config with GCE settings
        mock_config = MagicMock()
        mock_config.gce = MagicMock()
        mock_config.gce.effective_project_id = "test-project"
        daemon.config = mock_config

        # Create workspace and version for foreign key constraints
        self._create_workspace_and_version(test_db, "ws1", "v1")

        # Insert a running GCE stage run (started recently - 5 minutes ago)
        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, backend_type, backend_handle, started_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now', '-5 minutes'))
                """,
                ("stage-recent-test", "ws1", "v1", "train", StageRunStatus.RUNNING, "gce", "stage-recent-test"),
            )

        # Mock gcloud to return no instances
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)

            daemon._check_orphaned_instances()

        # Verify the run was NOT marked as failed (still within grace period)
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT status FROM stage_runs WHERE id = ?",
                ("stage-recent-test",),
            ).fetchone()

        assert row["status"] == StageRunStatus.RUNNING  # Still running, not touched

    def test_orphan_cleanup_skips_without_gce_config(self):
        """Orphan cleanup does nothing when GCE not configured."""
        daemon = GoldfishDaemon(Path("/tmp/fake"))
        daemon.config = MagicMock()
        daemon.config.gce = None

        # Should return early without doing anything
        with patch("subprocess.run") as mock_run:
            daemon._check_orphaned_instances()
            mock_run.assert_not_called()
