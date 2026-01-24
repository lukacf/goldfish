"""Unit tests for GCPIdentity adapter.

Tests the GCP implementation of instance identity discovery.
All metadata server HTTP calls are mocked.
"""

from __future__ import annotations

import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from goldfish.cloud.adapters.gcp.identity import GCPIdentity

# --- Fixtures ---


@pytest.fixture
def identity():
    """Create a GCPIdentity instance."""
    return GCPIdentity()


@pytest.fixture
def mock_urlopen():
    """Mock urllib.request.urlopen."""
    with patch("goldfish.cloud.adapters.gcp.identity.urllib.request.urlopen") as mock:
        yield mock


# --- Initialization Tests ---


class TestGCPIdentityInit:
    """Tests for GCPIdentity initialization."""

    def test_init_creates_empty_cache(self):
        """Init creates empty metadata cache."""
        identity = GCPIdentity()
        assert identity._cache == {}


# --- Fetch Metadata Tests ---


class TestGCPIdentityFetchMetadata:
    """Tests for _fetch_metadata method."""

    def test_fetch_metadata_makes_http_request(self, identity, mock_urlopen):
        """Fetch metadata makes HTTP request to metadata server."""
        mock_response = MagicMock()
        mock_response.read.return_value = b"test-value"
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = identity._fetch_metadata("project/project-id")

        assert result == "test-value"
        mock_urlopen.assert_called_once()

    def test_fetch_metadata_includes_metadata_flavor_header(self, identity, mock_urlopen):
        """Fetch metadata includes Metadata-Flavor: Google header."""
        mock_response = MagicMock()
        mock_response.read.return_value = b"value"
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        identity._fetch_metadata("instance/name")

        # Check the Request object passed to urlopen
        call_args = mock_urlopen.call_args[0][0]
        assert call_args.get_header("Metadata-flavor") == "Google"

    def test_fetch_metadata_uses_correct_url(self, identity, mock_urlopen):
        """Fetch metadata uses correct metadata server URL."""
        mock_response = MagicMock()
        mock_response.read.return_value = b"value"
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        identity._fetch_metadata("instance/name")

        call_args = mock_urlopen.call_args[0][0]
        assert call_args.full_url == "http://metadata.google.internal/computeMetadata/v1/instance/name"

    def test_fetch_metadata_caches_result(self, identity, mock_urlopen):
        """Fetch metadata caches result for subsequent calls."""
        mock_response = MagicMock()
        mock_response.read.return_value = b"cached-value"
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        # First call
        result1 = identity._fetch_metadata("project/project-id")
        # Second call
        result2 = identity._fetch_metadata("project/project-id")

        assert result1 == "cached-value"
        assert result2 == "cached-value"
        # Should only call urlopen once due to caching
        assert mock_urlopen.call_count == 1

    def test_fetch_metadata_returns_none_on_url_error(self, identity, mock_urlopen):
        """Fetch metadata returns None on URL error (not on GCE)."""
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        result = identity._fetch_metadata("project/project-id")

        assert result is None

    def test_fetch_metadata_caches_none_on_error(self, identity, mock_urlopen):
        """Fetch metadata caches None result to avoid repeated failed requests."""
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        identity._fetch_metadata("project/project-id")
        identity._fetch_metadata("project/project-id")

        # Should only try once due to caching
        assert mock_urlopen.call_count == 1
        assert identity._cache["project/project-id"] is None

    def test_fetch_metadata_returns_none_on_timeout(self, identity, mock_urlopen):
        """Fetch metadata returns None on timeout."""
        mock_urlopen.side_effect = TimeoutError()

        result = identity._fetch_metadata("instance/name")

        assert result is None

    def test_fetch_metadata_strips_whitespace(self, identity, mock_urlopen):
        """Fetch metadata strips leading/trailing whitespace."""
        mock_response = MagicMock()
        mock_response.read.return_value = b"  test-instance  \n"
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = identity._fetch_metadata("instance/name")

        assert result == "test-instance"


# --- Get Project ID Tests ---


class TestGCPIdentityGetProjectId:
    """Tests for get_project_id method."""

    def test_get_project_id_returns_project(self, identity, mock_urlopen):
        """Get project ID returns project from metadata."""
        mock_response = MagicMock()
        mock_response.read.return_value = b"my-gcp-project"
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = identity.get_project_id()

        assert result == "my-gcp-project"

    def test_get_project_id_returns_none_when_not_on_gce(self, identity, mock_urlopen):
        """Get project ID returns None when not on GCE."""
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        result = identity.get_project_id()

        assert result is None


# --- Get Instance Name Tests ---


class TestGCPIdentityGetInstanceName:
    """Tests for get_instance_name method."""

    def test_get_instance_name_returns_name(self, identity, mock_urlopen):
        """Get instance name returns instance name from metadata."""
        mock_response = MagicMock()
        mock_response.read.return_value = b"goldfish-stage-abc123"
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = identity.get_instance_name()

        assert result == "goldfish-stage-abc123"

    def test_get_instance_name_returns_none_when_not_on_gce(self, identity, mock_urlopen):
        """Get instance name returns None when not on GCE."""
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        result = identity.get_instance_name()

        assert result is None


# --- Get Zone Tests ---


class TestGCPIdentityGetZone:
    """Tests for get_zone method."""

    def test_get_zone_extracts_zone_name(self, identity, mock_urlopen):
        """Get zone extracts zone name from full path."""
        mock_response = MagicMock()
        mock_response.read.return_value = b"projects/123456789/zones/us-central1-a"
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = identity.get_zone()

        assert result == "us-central1-a"

    def test_get_zone_handles_different_zone_formats(self, identity, mock_urlopen):
        """Get zone handles different zone formats."""
        test_cases = [
            ("projects/12345/zones/us-west1-b", "us-west1-b"),
            ("projects/999/zones/europe-west1-c", "europe-west1-c"),
            ("projects/1/zones/asia-east1-a", "asia-east1-a"),
        ]

        for full_path, expected_zone in test_cases:
            identity._cache.clear()  # Clear cache between tests
            mock_response = MagicMock()
            mock_response.read.return_value = full_path.encode()
            mock_response.__enter__ = MagicMock(return_value=mock_response)
            mock_response.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_response

            result = identity.get_zone()

            assert result == expected_zone

    def test_get_zone_returns_none_when_not_on_gce(self, identity, mock_urlopen):
        """Get zone returns None when not on GCE."""
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        result = identity.get_zone()

        assert result is None


# --- Get Instance ID Tests ---


class TestGCPIdentityGetInstanceId:
    """Tests for get_instance_id method."""

    def test_get_instance_id_returns_numeric_id(self, identity, mock_urlopen):
        """Get instance ID returns numeric ID."""
        mock_response = MagicMock()
        mock_response.read.return_value = b"123456789012345678"
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = identity.get_instance_id()

        assert result == "123456789012345678"

    def test_get_instance_id_returns_none_when_not_on_gce(self, identity, mock_urlopen):
        """Get instance ID returns None when not on GCE."""
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        result = identity.get_instance_id()

        assert result is None


# --- Is Preemptible Tests ---


class TestGCPIdentityIsPreemptible:
    """Tests for is_preemptible method."""

    def test_is_preemptible_returns_true_for_spot_instance(self, identity, mock_urlopen):
        """Is preemptible returns True for spot/preemptible instance."""
        mock_response = MagicMock()
        mock_response.read.return_value = b"true"
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = identity.is_preemptible()

        assert result is True

    def test_is_preemptible_returns_false_for_standard_instance(self, identity, mock_urlopen):
        """Is preemptible returns False for standard instance."""
        mock_response = MagicMock()
        mock_response.read.return_value = b"false"
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = identity.is_preemptible()

        assert result is False

    def test_is_preemptible_returns_false_when_not_on_gce(self, identity, mock_urlopen):
        """Is preemptible returns False when not on GCE."""
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        result = identity.is_preemptible()

        assert result is False


# --- Clear Cache Tests ---


class TestGCPIdentityClearCache:
    """Tests for clear_cache method."""

    def test_clear_cache_empties_cache(self, identity, mock_urlopen):
        """Clear cache empties the metadata cache."""
        # Populate cache
        mock_response = MagicMock()
        mock_response.read.return_value = b"value"
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        identity._fetch_metadata("project/project-id")
        assert len(identity._cache) > 0

        # Clear cache
        identity.clear_cache()

        assert identity._cache == {}

    def test_clear_cache_allows_refetch(self, identity, mock_urlopen):
        """Clear cache allows re-fetching values."""
        mock_response = MagicMock()
        mock_response.read.return_value = b"first-value"
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        # First fetch
        result1 = identity._fetch_metadata("instance/name")
        assert result1 == "first-value"

        # Clear and update mock
        identity.clear_cache()
        mock_response.read.return_value = b"second-value"

        # Second fetch should get new value
        result2 = identity._fetch_metadata("instance/name")
        assert result2 == "second-value"

        # Should have made two HTTP calls
        assert mock_urlopen.call_count == 2
