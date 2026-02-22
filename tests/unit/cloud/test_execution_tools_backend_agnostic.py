"""Tests that execution_tools and state_machine/cancel are backend-agnostic.

These tests verify that execution_tools.py and cancel.py do not contain any
backend-specific conditionals (e.g., `if backend_type == "gce"`). All backend-specific
behavior should be encapsulated in the RunBackend implementations via BackendCapabilities.
"""

from __future__ import annotations

import ast
from pathlib import Path


def _check_backend_conditionals(source: str, file_name: str) -> list[str]:
    """Check source code for backend-specific conditionals.

    Args:
        source: Python source code to check
        file_name: File name for error messages

    Returns:
        List of violation messages
    """
    tree = ast.parse(source)
    violations: list[str] = []

    for node in ast.walk(tree):
        # Check for comparisons like: backend == "gce" or backend_type == "gce"
        if isinstance(node, ast.Compare):
            for comparator in node.comparators:
                if isinstance(comparator, ast.Constant):
                    if comparator.value in ("gce", "local"):
                        # Get line number for error message
                        line = getattr(node, "lineno", "?")
                        violations.append(f"{file_name}:{line}: Backend-specific comparison to '{comparator.value}'")

    return violations


def test_no_backend_type_conditionals_in_execution_tools() -> None:
    """Verify execution_tools.py has no backend_type == 'gce' or 'local' checks.

    Backend-specific behavior should be in RunBackend implementations or
    BackendCapabilities, not in the server tools layer.
    """
    execution_tools_path = (
        Path(__file__).parent.parent.parent.parent / "src" / "goldfish" / "server_tools" / "execution_tools.py"
    )

    source = execution_tools_path.read_text()
    violations = _check_backend_conditionals(source, "execution_tools.py")

    if violations:
        msg = (
            "execution_tools.py contains backend-specific conditionals. "
            "Move this logic to RunBackend implementations or BackendCapabilities.\n"
            "Violations:\n" + "\n".join(f"  - {v}" for v in violations)
        )
        raise AssertionError(msg)


def test_no_backend_type_conditionals_in_cancel() -> None:
    """Verify cancel.py has no backend_type == 'gce' or 'local' checks.

    Backend-specific termination should use the RunBackend protocol via
    the factory pattern, not inline conditionals.
    """
    cancel_path = Path(__file__).parent.parent.parent.parent / "src" / "goldfish" / "state_machine" / "cancel.py"

    source = cancel_path.read_text()
    violations = _check_backend_conditionals(source, "cancel.py")

    if violations:
        msg = (
            "cancel.py contains backend-specific conditionals. "
            "Use create_backend_for_cleanup() factory instead.\n"
            "Violations:\n" + "\n".join(f"  - {v}" for v in violations)
        )
        raise AssertionError(msg)


def test_backend_capabilities_has_sync_behavior_fields() -> None:
    """Verify BackendCapabilities has fields for sync behavior.

    The capabilities should include:
    - ack_timeout_seconds: Default ACK timeout for sync operations
    - logs_unavailable_message: Message to show when logs can't be fetched
    - has_launch_delay: Whether this backend has a delay between launch and running
    - timeout_becomes_pending: Whether ACK timeout means sync pending (not failure)
    - status_message_for_preparing: Message to show for PREPARING status
    - zone_resolution_method: How to resolve zones ("config" or "handle")
    """
    from goldfish.cloud.contracts import BackendCapabilities

    caps = BackendCapabilities()

    # These fields should exist
    assert hasattr(caps, "ack_timeout_seconds"), "Missing ack_timeout_seconds field"
    assert hasattr(caps, "logs_unavailable_message"), "Missing logs_unavailable_message field"
    assert hasattr(caps, "has_launch_delay"), "Missing has_launch_delay field"
    assert hasattr(caps, "timeout_becomes_pending"), "Missing timeout_becomes_pending field"
    assert hasattr(caps, "status_message_for_preparing"), "Missing status_message_for_preparing field"
    assert hasattr(caps, "zone_resolution_method"), "Missing zone_resolution_method field"


def test_local_backend_capabilities_sync_behavior() -> None:
    """Verify LocalRunBackend capabilities have correct sync behavior defaults."""
    from unittest.mock import patch

    from goldfish.cloud.adapters.local.run_backend import LocalRunBackend

    backend = LocalRunBackend()

    with patch.object(backend, "_check_nvidia_runtime", return_value=False):
        caps = backend.capabilities

    # Local backend should have shorter timeouts (runs locally)
    assert caps.ack_timeout_seconds == 1.0, "Local backend should have 1s ACK timeout"
    assert caps.has_launch_delay is False, "Local backend has no launch delay"
    assert caps.logs_unavailable_message == "Logs not available"
    assert caps.timeout_becomes_pending is False, "Local backend ACK timeout is failure"
    assert caps.status_message_for_preparing, "Local backend needs preparing message"
    assert caps.zone_resolution_method == "config", "Local backend uses config for zones"


def test_gce_backend_capabilities_sync_behavior() -> None:
    """Verify GCERunBackend capabilities have correct sync behavior defaults."""
    from unittest.mock import MagicMock, patch

    # Patch the GCELauncher import since it requires GCP credentials
    with patch("goldfish.cloud.adapters.gcp.gce_launcher.GCELauncher") as mock_launcher:
        mock_launcher.return_value = MagicMock()
        from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend

        backend = GCERunBackend(project_id="test-project", zones=["us-central1-a"])
        caps = backend.capabilities

    # GCE backend should have longer timeouts (network latency)
    assert caps.ack_timeout_seconds == 3.0, "GCE backend should have 3s ACK timeout"
    assert caps.has_launch_delay is True, "GCE backend has launch delay"
    assert "GCE" in caps.logs_unavailable_message or "synced" in caps.logs_unavailable_message.lower()
    assert caps.timeout_becomes_pending is True, "GCE backend ACK timeout means pending"
    assert caps.status_message_for_preparing, "GCE backend needs preparing message"
    assert caps.zone_resolution_method == "handle", "GCE backend uses handle.zone"
