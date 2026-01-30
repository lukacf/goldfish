"""Local Identity adapter for container identity discovery.

Provides instance identity for local Docker containers.
Identity can be configured or inferred from environment.
"""

from __future__ import annotations

import os
import socket
import uuid
from dataclasses import dataclass


@dataclass
class LocalIdentityConfig:
    """Configuration for LocalIdentity.

    Allows explicit setting of identity values for testing/simulation.
    If not set, values are inferred from environment.
    """

    project_id: str | None = None
    instance_name: str | None = None
    zone: str | None = None
    instance_id: str | None = None
    is_preemptible: bool = False


class LocalIdentity:
    """Local implementation of InstanceIdentity protocol.

    For local Docker containers, identity is either:
    1. Explicitly configured (for testing/simulation)
    2. Inferred from environment variables and hostname

    This allows local development to simulate being inside a cloud instance.
    """

    def __init__(self, config: LocalIdentityConfig | None = None) -> None:
        """Initialize LocalIdentity.

        Args:
            config: Optional configuration. If not provided, identity
                   is inferred from environment.
        """
        self._config = config or LocalIdentityConfig()
        # Generate stable instance ID if not configured
        self._generated_id = str(uuid.uuid4())

    def get_project_id(self) -> str | None:
        """Get the project ID.

        Priority:
        1. Explicit config
        2. GOLDFISH_PROJECT_ID env var
        3. None

        Returns:
            Project ID string, or None if not available
        """
        if self._config.project_id:
            return self._config.project_id
        return os.environ.get("GOLDFISH_PROJECT_ID")

    def get_instance_name(self) -> str | None:
        """Get instance name.

        Priority:
        1. Explicit config
        2. GOLDFISH_INSTANCE_NAME env var
        3. Container hostname
        4. None

        Returns:
            Instance name string, or None if not available
        """
        if self._config.instance_name:
            return self._config.instance_name
        if env_name := os.environ.get("GOLDFISH_INSTANCE_NAME"):
            return env_name
        # Fall back to hostname (container ID for Docker containers)
        try:
            hostname = socket.gethostname()
            # Docker container hostnames are typically the short container ID
            if hostname and len(hostname) == 12 and hostname.isalnum():
                return hostname
            return hostname if hostname else None
        except OSError:
            return None

    def get_zone(self) -> str | None:
        """Get zone.

        Priority:
        1. Explicit config
        2. GOLDFISH_ZONE env var
        3. "local" (default for local runs)

        Returns:
            Zone string, or None if not available
        """
        if self._config.zone:
            return self._config.zone
        return os.environ.get("GOLDFISH_ZONE", "local")

    def get_instance_id(self) -> str | None:
        """Get unique instance identifier.

        Priority:
        1. Explicit config
        2. GOLDFISH_INSTANCE_ID env var
        3. Container ID from cgroup (if in Docker)
        4. Generated UUID

        Returns:
            Instance ID string
        """
        if self._config.instance_id:
            return self._config.instance_id
        if env_id := os.environ.get("GOLDFISH_INSTANCE_ID"):
            return env_id
        # Try to get container ID from cgroup
        container_id = self._get_container_id()
        if container_id:
            return container_id
        # Fall back to generated ID
        return self._generated_id

    def is_preemptible(self) -> bool:
        """Check if this instance is preemptible.

        For local runs, this is always False unless explicitly configured.

        Returns:
            True if configured as preemptible, False otherwise
        """
        if self._config.is_preemptible:
            return True
        return os.environ.get("GOLDFISH_PREEMPTIBLE", "").lower() in ("true", "1", "yes")

    def _get_container_id(self) -> str | None:
        """Try to get Docker container ID from cgroup.

        Returns:
            Container ID if running in Docker, None otherwise
        """
        try:
            # Try /proc/self/cgroup (Linux)
            cgroup_path = "/proc/self/cgroup"
            if os.path.exists(cgroup_path):
                with open(cgroup_path) as f:
                    for line in f:
                        # Format: hierarchy-ID:controller-list:cgroup-path
                        # Docker container IDs appear in the cgroup path
                        parts = line.strip().split(":")
                        if len(parts) >= 3:
                            cgroup_path_part = parts[2]
                            # Look for docker container ID pattern
                            if "/docker/" in cgroup_path_part:
                                container_id = cgroup_path_part.split("/docker/")[-1]
                                if container_id and len(container_id) >= 12:
                                    return container_id[:12]  # Short ID
            return None
        except OSError:
            return None
