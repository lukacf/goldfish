"""GCP Identity adapter for instance identity discovery.

Provides instance identity using GCP metadata server.
Only works when running inside a GCE instance.
"""

from __future__ import annotations

import logging
import urllib.error
import urllib.request

from goldfish.errors import GoldfishError

logger = logging.getLogger(__name__)

# GCE metadata server URL
_METADATA_URL = "http://metadata.google.internal/computeMetadata/v1/"
_METADATA_HEADERS = {"Metadata-Flavor": "Google"}
_TIMEOUT_SECONDS = 2


class GCPIdentityError(GoldfishError):
    """Error accessing GCP identity metadata."""

    pass


class GCPIdentity:
    """GCP implementation of InstanceIdentity protocol.

    Uses the GCE metadata server to discover instance identity.
    This only works when running inside a GCE instance.

    Metadata server endpoints:
    - project/project-id -> project ID
    - instance/name -> instance name
    - instance/zone -> zone (projects/123/zones/us-central1-a)
    - instance/id -> numeric instance ID
    - instance/scheduling/preemptible -> true/false
    """

    def __init__(self) -> None:
        """Initialize GCPIdentity."""
        # Cache results to avoid repeated metadata queries
        self._cache: dict[str, str | None] = {}

    def _fetch_metadata(self, path: str) -> str | None:
        """Fetch a value from the metadata server.

        Args:
            path: Metadata path (e.g., "project/project-id")

        Returns:
            Metadata value, or None if not available
        """
        if path in self._cache:
            return self._cache[path]

        url = f"{_METADATA_URL}{path}"
        req = urllib.request.Request(url, headers=_METADATA_HEADERS)

        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as response:
                value: str = response.read().decode("utf-8").strip()
                self._cache[path] = value
                return value
        except urllib.error.URLError as e:
            logger.debug("Failed to fetch metadata %s: %s", path, e)
            self._cache[path] = None
            return None
        except TimeoutError:
            logger.debug("Timeout fetching metadata %s", path)
            self._cache[path] = None
            return None

    def get_project_id(self) -> str | None:
        """Get the GCP project ID.

        Returns:
            Project ID string, or None if not on GCE
        """
        return self._fetch_metadata("project/project-id")

    def get_instance_name(self) -> str | None:
        """Get the GCE instance name.

        Returns:
            Instance name string, or None if not on GCE
        """
        return self._fetch_metadata("instance/name")

    def get_zone(self) -> str | None:
        """Get the zone this instance is running in.

        The metadata server returns full zone path like:
        "projects/123456789/zones/us-central1-a"

        We extract just the zone name.

        Returns:
            Zone string (e.g., "us-central1-a"), or None if not on GCE
        """
        full_zone = self._fetch_metadata("instance/zone")
        if full_zone:
            # Extract zone name from full path
            # Format: projects/PROJECT_NUM/zones/ZONE_NAME
            parts = full_zone.split("/")
            if parts:
                return parts[-1]
        return None

    def get_instance_id(self) -> str | None:
        """Get the numeric GCE instance ID.

        Returns:
            Instance ID string, or None if not on GCE
        """
        return self._fetch_metadata("instance/id")

    def is_preemptible(self) -> bool:
        """Check if this instance is preemptible/spot.

        Returns:
            True if instance is preemptible, False otherwise
        """
        value = self._fetch_metadata("instance/scheduling/preemptible")
        return value == "true" if value else False

    def clear_cache(self) -> None:
        """Clear the metadata cache.

        Useful for testing or when instance attributes might change.
        """
        self._cache.clear()
