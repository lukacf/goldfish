"""Regression tests for server initialization.

These tests verify that configure_server and reset_server properly
manage the module-level _project_root variable, preventing the
"Server not initialized with project root" error.

Bug context: configure_server() was not calling _set_project_root(),
so tools like get_backup_status, validate_config, reload_config that
use _get_project_root() would fail even after the server was configured.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest


def test_configure_server_sets_project_root():
    """Regression test: configure_server must set _project_root.

    Bug: configure_server() was not calling _set_project_root(), causing
    tools that use _get_project_root() to fail with "Server not initialized
    with project root" error.
    """
    from goldfish.server import configure_server, reset_server
    from goldfish.server_core import _get_project_root

    # Start clean
    reset_server()

    # Configure server
    test_path = Path("/tmp/test_project_root")
    configure_server(
        project_root=test_path,
        config=MagicMock(),
        db=MagicMock(),
        workspace_manager=MagicMock(),
        state_manager=MagicMock(),
        job_launcher=MagicMock(),
        job_tracker=MagicMock(),
        pipeline_manager=MagicMock(),
        dataset_registry=MagicMock(),
        stage_executor=MagicMock(),
        pipeline_executor=MagicMock(),
    )

    # _get_project_root should now work (this was the bug)
    result = _get_project_root()
    assert result == test_path.resolve()

    # Cleanup
    reset_server()


def test_reset_server_clears_project_root():
    """Regression test: reset_server must clear _project_root.

    Ensures reset_server() properly clears state for test isolation.
    """
    from goldfish.errors import GoldfishError
    from goldfish.server import configure_server, reset_server
    from goldfish.server_core import _get_project_root

    # Configure server first
    configure_server(
        project_root=Path("/tmp/test"),
        config=MagicMock(),
        db=MagicMock(),
        workspace_manager=MagicMock(),
        state_manager=MagicMock(),
        job_launcher=MagicMock(),
        job_tracker=MagicMock(),
        pipeline_manager=MagicMock(),
        dataset_registry=MagicMock(),
        stage_executor=MagicMock(),
        pipeline_executor=MagicMock(),
    )

    # Reset should clear it
    reset_server()

    # Now _get_project_root should raise
    with pytest.raises(GoldfishError, match="not initialized"):
        _get_project_root()


def test_get_project_root_raises_before_init():
    """Verify _get_project_root raises with clear error before initialization."""
    from goldfish.errors import GoldfishError
    from goldfish.server import reset_server
    from goldfish.server_core import _get_project_root

    reset_server()

    with pytest.raises(GoldfishError, match="Server not initialized with project root"):
        _get_project_root()


def test_daemon_initialize_sets_project_root():
    """Regression test: daemon.initialize() must set _project_root.

    Bug: Daemon called set_context() but not _set_project_root(), causing
    tools that use _get_project_root() to fail with "Server not initialized
    with project root" error even after daemon initialization.
    """
    from goldfish.context import set_context
    from goldfish.server_core import _get_project_root, _reset_project_root

    # Start clean
    _reset_project_root()
    set_context(None)

    # Create a mock daemon that simulates the initialize() pattern
    mock_project_root = Path("/tmp/test_daemon_project")
    mock_config = MagicMock()
    mock_config.get_dev_repo_path.return_value = Path("/tmp/test_daemon_dev")
    mock_config.db_path = ".goldfish/goldfish.db"
    mock_config.state_md.path = "STATE.md"
    mock_config.gcs = None
    mock_config.gce = None
    mock_config.jobs.backend = "local"

    # Simulate what daemon.initialize() does (after the fix)
    from goldfish.context import ServerContext
    from goldfish.server_core import _set_project_root

    ctx = ServerContext(
        project_root=mock_project_root,
        config=mock_config,
        db=MagicMock(),
        workspace_manager=MagicMock(),
        state_manager=MagicMock(),
        job_launcher=MagicMock(),
        job_tracker=MagicMock(),
        pipeline_manager=MagicMock(),
        dataset_registry=MagicMock(),
        stage_executor=MagicMock(),
        pipeline_executor=MagicMock(),
        metadata_bus=MagicMock(),
    )
    _set_project_root(mock_project_root)
    set_context(ctx)

    # _get_project_root should now work (this was the bug)
    result = _get_project_root()
    assert result == mock_project_root.resolve()

    # Cleanup
    _reset_project_root()
    set_context(None)
