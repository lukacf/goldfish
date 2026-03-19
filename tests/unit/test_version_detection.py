"""Test that version detection works correctly.

Bug: _get_version() used importlib.metadata.version("goldfish") which fails
in uvx environments because the package is named "goldfish-ml". Both proxy
and daemon returned "unknown", so the version mismatch check was always
skipped — the daemon never got restarted on upgrade.
"""

from __future__ import annotations


def test_proxy_get_version_returns_real_version() -> None:
    """Proxy _get_version must return the actual package version, not 'unknown'."""
    from goldfish.mcp_proxy import _get_version

    version = _get_version()
    assert version != "unknown"
    assert "." in version  # e.g., "0.2.6"


def test_daemon_get_version_returns_real_version() -> None:
    """Daemon _get_version must return the actual package version, not 'unknown'."""
    from goldfish.daemon import _get_version

    version = _get_version()
    assert version != "unknown"
    assert "." in version


def test_proxy_and_daemon_versions_match() -> None:
    """Proxy and daemon must report the same version."""
    from goldfish.daemon import _get_version as daemon_version
    from goldfish.mcp_proxy import _get_version as proxy_version

    assert daemon_version() == proxy_version()
