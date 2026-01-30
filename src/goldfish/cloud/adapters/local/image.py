"""Local Image adapters for building and managing container images.

Provides local Docker-based implementations of ImageBuilder and ImageRegistry.
"""

from __future__ import annotations

import logging
import subprocess
import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from goldfish.errors import GoldfishError

logger = logging.getLogger(__name__)


class ImageBuildError(GoldfishError):
    """Error during image build."""

    pass


class ImageRegistryError(GoldfishError):
    """Error during registry operations."""

    pass


@dataclass
class BuildStatus:
    """Status of an ongoing or completed build."""

    build_id: str
    status: str  # "pending", "building", "completed", "failed"
    started_at: datetime
    completed_at: datetime | None = None
    error: str | None = None
    logs: list[str] = field(default_factory=list)
    image_tag: str | None = None


class LocalImageBuilder:
    """Local implementation of ImageBuilder protocol using Docker.

    Builds container images using the local Docker daemon.
    Supports both synchronous and asynchronous builds.
    """

    def __init__(self, platform: str = "linux/amd64") -> None:
        """Initialize LocalImageBuilder.

        Args:
            platform: Target platform for builds (default: linux/amd64 for GCE compatibility)
        """
        self._platform = platform
        self._builds: dict[str, BuildStatus] = {}
        self._builds_lock = threading.Lock()

    def _check_docker_available(self) -> None:
        """Check if Docker daemon is available.

        Raises:
            ImageBuildError: If Docker is not available
        """
        try:
            result = subprocess.run(
                ["docker", "info"],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
            if result.returncode != 0:
                raise ImageBuildError("Docker daemon not responding")
        except FileNotFoundError as e:
            raise ImageBuildError("Docker not installed") from e
        except subprocess.TimeoutExpired as e:
            raise ImageBuildError("Docker daemon timed out") from e

    def build(
        self,
        context_path: Path,
        dockerfile_path: Path,
        image_tag: str,
        build_args: dict[str, str] | None = None,
        no_cache: bool = False,
    ) -> str:
        """Build a container image synchronously.

        Args:
            context_path: Path to build context directory
            dockerfile_path: Path to Dockerfile
            image_tag: Tag for the built image
            build_args: Optional build arguments
            no_cache: If True, disable layer caching

        Returns:
            The image tag of the built image

        Raises:
            ImageBuildError: If build fails
        """
        self._check_docker_available()

        cmd = [
            "docker",
            "build",
            "--platform",
            self._platform,
            "-f",
            str(dockerfile_path),
            "-t",
            image_tag,
        ]

        if build_args:
            for key, value in build_args.items():
                cmd.extend(["--build-arg", f"{key}={value}"])

        if no_cache:
            cmd.append("--no-cache")

        cmd.append(str(context_path))

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=30 * 60,  # 30 minute timeout
            )

            if result.returncode != 0:
                # Get last 50 lines of output for error message
                lines = (result.stderr or result.stdout or "").splitlines()
                tail = "\n".join(lines[-50:]) if len(lines) > 50 else result.stderr or result.stdout
                raise ImageBuildError(f"Docker build failed:\n{tail}")

            return image_tag

        except subprocess.TimeoutExpired as e:
            raise ImageBuildError("Docker build timed out after 30 minutes") from e

    def build_async(
        self,
        context_path: Path,
        dockerfile_path: Path,
        image_tag: str,
        build_args: dict[str, str] | None = None,
        no_cache: bool = False,
    ) -> str:
        """Start an asynchronous image build.

        Args:
            context_path: Path to build context directory
            dockerfile_path: Path to Dockerfile
            image_tag: Tag for the built image
            build_args: Optional build arguments
            no_cache: If True, disable layer caching

        Returns:
            Build ID for status polling

        Raises:
            ImageBuildError: If build submission fails
        """
        self._check_docker_available()

        build_id = f"build-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)

        build_status = BuildStatus(
            build_id=build_id,
            status="pending",
            started_at=now,
        )

        with self._builds_lock:
            self._builds[build_id] = build_status

        # Start build in background thread
        thread = threading.Thread(
            target=self._run_build,
            args=(build_id, context_path, dockerfile_path, image_tag, build_args, no_cache),
            daemon=True,
        )
        thread.start()

        return build_id

    def _run_build(
        self,
        build_id: str,
        context_path: Path,
        dockerfile_path: Path,
        image_tag: str,
        build_args: dict[str, str] | None,
        no_cache: bool,
    ) -> None:
        """Execute build in background thread."""
        with self._builds_lock:
            self._builds[build_id].status = "building"

        cmd = [
            "docker",
            "build",
            "--platform",
            self._platform,
            "-f",
            str(dockerfile_path),
            "-t",
            image_tag,
        ]

        if build_args:
            for key, value in build_args.items():
                cmd.extend(["--build-arg", f"{key}={value}"])

        if no_cache:
            cmd.append("--no-cache")

        cmd.append(str(context_path))

        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )

            logs: list[str] = []
            for line in process.stdout or []:
                logs.append(line.rstrip())
                with self._builds_lock:
                    self._builds[build_id].logs = logs[-100:]  # Keep last 100 lines

            process.wait()

            now = datetime.now(UTC)
            with self._builds_lock:
                if process.returncode == 0:
                    self._builds[build_id].status = "completed"
                    self._builds[build_id].image_tag = image_tag
                else:
                    self._builds[build_id].status = "failed"
                    self._builds[build_id].error = "\n".join(logs[-20:])
                self._builds[build_id].completed_at = now

        except Exception as e:
            now = datetime.now(UTC)
            with self._builds_lock:
                self._builds[build_id].status = "failed"
                self._builds[build_id].error = str(e)
                self._builds[build_id].completed_at = now

    def get_build_status(self, build_id: str) -> dict[str, Any]:
        """Get status of an async build.

        Args:
            build_id: Build ID from build_async()

        Returns:
            Dict with status info

        Raises:
            GoldfishError: If build_id not found
        """
        with self._builds_lock:
            if build_id not in self._builds:
                raise GoldfishError(f"Unknown build ID: {build_id}")
            status = self._builds[build_id]

        return {
            "build_id": status.build_id,
            "status": status.status,
            "started_at": status.started_at.isoformat(),
            "completed_at": status.completed_at.isoformat() if status.completed_at else None,
            "image_tag": status.image_tag,
            "error": status.error,
            "logs_tail": status.logs[-20:] if status.logs else [],
        }


class LocalImageRegistry:
    """Local implementation of ImageRegistry protocol.

    For local development, this primarily provides image existence checks
    and basic tag/push operations using local Docker daemon.
    """

    def __init__(self) -> None:
        """Initialize LocalImageRegistry."""
        pass

    def _check_docker_available(self) -> None:
        """Check if Docker daemon is available.

        Raises:
            ImageRegistryError: If Docker is not available
        """
        try:
            result = subprocess.run(
                ["docker", "info"],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
            if result.returncode != 0:
                raise ImageRegistryError("Docker daemon not responding")
        except FileNotFoundError as e:
            raise ImageRegistryError("Docker not installed") from e
        except subprocess.TimeoutExpired as e:
            raise ImageRegistryError("Docker daemon timed out") from e

    def push(self, local_tag: str, registry_tag: str) -> str:
        """Push a local image to the registry.

        Args:
            local_tag: Local image tag
            registry_tag: Full registry tag

        Returns:
            The pushed registry tag

        Raises:
            ImageRegistryError: If push fails
        """
        self._check_docker_available()

        # First tag the local image with registry tag
        tag_result = subprocess.run(
            ["docker", "tag", local_tag, registry_tag],
            capture_output=True,
            text=True,
            check=False,
        )
        if tag_result.returncode != 0:
            raise ImageRegistryError(f"Failed to tag image: {tag_result.stderr}")

        # Push to registry
        push_result = subprocess.run(
            ["docker", "push", registry_tag],
            capture_output=True,
            text=True,
            check=False,
            timeout=30 * 60,  # 30 minute timeout
        )
        if push_result.returncode != 0:
            raise ImageRegistryError(f"Failed to push image: {push_result.stderr}")

        return registry_tag

    def pull(self, registry_tag: str) -> str:
        """Pull an image from the registry.

        Args:
            registry_tag: Full registry tag

        Returns:
            The local tag of the pulled image

        Raises:
            ImageRegistryError: If pull fails
        """
        self._check_docker_available()

        result = subprocess.run(
            ["docker", "pull", registry_tag],
            capture_output=True,
            text=True,
            check=False,
            timeout=30 * 60,  # 30 minute timeout
        )
        if result.returncode != 0:
            raise ImageRegistryError(f"Failed to pull image: {result.stderr}")

        return registry_tag

    def exists(self, registry_tag: str) -> bool:
        """Check if an image exists in the registry.

        For local registry checks, this verifies the image exists locally.
        For remote registries, this would need to query the registry API.

        Args:
            registry_tag: Full registry tag

        Returns:
            True if image exists locally
        """
        try:
            result = subprocess.run(
                ["docker", "image", "inspect", registry_tag],
                capture_output=True,
                check=False,
                timeout=10,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    def delete(self, registry_tag: str) -> None:
        """Delete an image.

        For local registries, this removes the local image.

        Args:
            registry_tag: Full registry tag

        Raises:
            ImageRegistryError: If delete fails
        """
        self._check_docker_available()

        result = subprocess.run(
            ["docker", "rmi", registry_tag],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0 and "No such image" not in result.stderr:
            raise ImageRegistryError(f"Failed to delete image: {result.stderr}")
