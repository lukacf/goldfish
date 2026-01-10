"""Docker base image management for Goldfish.

Manages two layers of Docker images:
1. Goldfish base images (goldfish-base-{cpu,gpu}) - foundation with ML libraries
2. Project images ({project}-{cpu,gpu}) - extend base with project customizations

The MCP tool exposes both layers so AI can build/push the entire stack.

Supports two build backends:
- local: Build using local Docker daemon (default)
- cloud: Build using Google Cloud Build (recommended for GPU images)
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from goldfish.config import DockerConfig, GoldfishConfig
from goldfish.errors import (
    BaseImageBuildError,
    BaseImageNotFoundError,
    CloudBuildError,
    CloudBuildNotConfiguredError,
    DockerNotAvailableError,
    GoldfishError,
    RegistryNotConfiguredError,
)
from goldfish.infra.profiles import (
    BASE_IMAGE_CPU,
    BASE_IMAGE_GPU,
    BASE_IMAGE_VERSION,
    PUBLIC_BASE_IMAGE_CPU,
    PUBLIC_BASE_IMAGE_GPU,
)
from goldfish.validation import validate_image_type

if TYPE_CHECKING:
    from goldfish.db.database import Database

# Directory containing goldfish base image Dockerfiles
BASE_IMAGES_DIR = Path(__file__).parent


@dataclass
class BuildStatus:
    """Status of an ongoing or completed build."""

    build_id: str
    image_type: str
    status: str  # "pending", "building", "completed", "failed"
    started_at: datetime
    completed_at: datetime | None = None
    error: str | None = None
    logs: list[str] = field(default_factory=list)
    image_tag: str | None = None


@dataclass
class ImageInfo:
    """Information about a Docker image."""

    image_type: str
    local_tag: str | None
    registry_tag: str | None
    has_local: bool
    has_registry: bool
    dockerfile_path: Path | None
    extra_packages: list[str]
    needs_rebuild: bool


class BaseImageManager:
    """Manages project-level Docker base images.

    Project images extend goldfish-base-{cpu,gpu} with project-specific
    customizations. Customization approaches:
    1. extra_packages in goldfish.yaml - pip packages added to base
    2. Dockerfile.{cpu,gpu} in project root - full control

    Image naming: {project_name}-{image_type}:{version}
    e.g., mlm-gpu:v1

    Supports two build backends:
    - local: Build using local Docker daemon (default)
    - cloud: Build using Google Cloud Build (recommended for GPU images)

    Attributes:
        project_root: Path to the user's project root
        config: GoldfishConfig instance
        project_name: Name for image tagging
        docker_config: Docker customization config
        db: Optional database for persistent build tracking
    """

    def __init__(
        self,
        project_root: Path,
        config: GoldfishConfig,
        db: Database | None = None,
    ):
        """Initialize the manager.

        Args:
            project_root: Path to the user's project root (where goldfish.yaml lives)
            config: Loaded GoldfishConfig instance
            db: Optional database for persistent build tracking (required for cloud builds)
        """
        self.project_root = project_root
        self.config = config
        self.project_name = config.project_name
        self.docker_config: DockerConfig = config.docker
        self.db = db

        # In-memory build tracking (for local builds without db)
        self._builds: dict[str, BuildStatus] = {}
        self._builds_lock = threading.Lock()

    def _check_docker_available(self) -> None:
        """Check if Docker daemon is available.

        Raises:
            DockerNotAvailableError: If Docker is not running
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
                raise DockerNotAvailableError("Docker daemon not responding")
        except FileNotFoundError as e:
            raise DockerNotAvailableError("Docker not installed") from e
        except subprocess.TimeoutExpired as e:
            raise DockerNotAvailableError("Docker daemon timed out") from e

    def _get_artifact_registry(self) -> str:
        """Get artifact registry URL from config.

        Returns:
            Registry URL (e.g., "us-docker.pkg.dev/project/goldfish")

        Raises:
            RegistryNotConfiguredError: If registry not configured
        """
        if not self.config.gce:
            raise RegistryNotConfiguredError()
        registry = self.config.gce.effective_artifact_registry
        if not registry:
            raise RegistryNotConfiguredError()
        return registry

    def _get_project_image_tag(self, image_type: str, for_registry: bool = False) -> str:
        """Get the project-specific image tag.

        Args:
            image_type: "cpu" or "gpu"
            for_registry: If True, include registry URL prefix

        Returns:
            Image tag (e.g., "mlm-gpu:v1" or "us-docker.pkg.dev/.../mlm-gpu:v1")
        """
        local_tag = f"{self.project_name}-{image_type}:v1"
        if for_registry:
            registry = self._get_artifact_registry()
            return f"{registry}/{local_tag}"
        return local_tag

    def _get_base_image_tag(self, image_type: str) -> str:
        """Get the goldfish base image tag to use as FROM.

        Uses registry image if configured, otherwise public fallback.

        Args:
            image_type: "cpu" or "gpu"

        Returns:
            Full base image tag for FROM directive
        """
        base_name = BASE_IMAGE_GPU if image_type == "gpu" else BASE_IMAGE_CPU

        # Try to use registry base image
        try:
            registry = self._get_artifact_registry()
            return f"{registry}/{base_name}:{BASE_IMAGE_VERSION}"
        except RegistryNotConfiguredError:
            # Fall back to public images
            if image_type == "gpu":
                return PUBLIC_BASE_IMAGE_GPU
            return PUBLIC_BASE_IMAGE_CPU

    # =========================================================================
    # Goldfish Base Image Methods (goldfish-base-{cpu,gpu})
    # =========================================================================

    def _get_goldfish_base_dockerfile_path(self, image_type: str) -> Path:
        """Get path to goldfish base Dockerfile.

        Args:
            image_type: "cpu" or "gpu"

        Returns:
            Path to Dockerfile.cpu or Dockerfile.gpu in goldfish source
        """
        return BASE_IMAGES_DIR / f"Dockerfile.{image_type}"

    def _get_goldfish_base_local_tag(self, image_type: str) -> str:
        """Get local tag for goldfish base image.

        Args:
            image_type: "cpu" or "gpu"

        Returns:
            Tag like "goldfish-base-gpu:v4"
        """
        base_name = BASE_IMAGE_GPU if image_type == "gpu" else BASE_IMAGE_CPU
        return f"{base_name}:{BASE_IMAGE_VERSION}"

    def _get_goldfish_base_registry_tag(self, image_type: str) -> str:
        """Get registry tag for goldfish base image.

        Args:
            image_type: "cpu" or "gpu"

        Returns:
            Full registry tag like "us-docker.pkg.dev/.../goldfish-base-gpu:v4"

        Raises:
            RegistryNotConfiguredError: If registry not configured
        """
        registry = self._get_artifact_registry()
        base_name = BASE_IMAGE_GPU if image_type == "gpu" else BASE_IMAGE_CPU
        return f"{registry}/{base_name}:{BASE_IMAGE_VERSION}"

    def _check_goldfish_base_exists_in_registry(self, image_type: str) -> bool:
        """Check if goldfish base image exists in registry.

        Args:
            image_type: "cpu" or "gpu"

        Returns:
            True if image exists in registry
        """
        try:
            registry_tag = self._get_goldfish_base_registry_tag(image_type)
            return self._check_registry_image_exists(registry_tag)
        except RegistryNotConfiguredError:
            return False

    # =========================================================================
    # Project Image Methods ({project}-{cpu,gpu})
    # =========================================================================

    def _get_project_dockerfile_path(self, image_type: str) -> Path | None:
        """Check if project has a custom Dockerfile.

        Args:
            image_type: "cpu" or "gpu"

        Returns:
            Path to custom Dockerfile if exists, None otherwise
        """
        dockerfile = self.project_root / f"Dockerfile.{image_type}"
        if dockerfile.exists():
            return dockerfile
        return None

    def _get_extra_packages(self, image_type: str) -> list[str]:
        """Get extra packages for image type from config.

        Args:
            image_type: "cpu" or "gpu"

        Returns:
            List of pip install arguments
        """
        return self.docker_config.extra_packages.get(image_type, [])

    def _generate_dockerfile_content(self, image_type: str) -> str:
        """Generate Dockerfile content for project image.

        Uses base goldfish image + extra_packages from config.

        Args:
            image_type: "cpu" or "gpu"

        Returns:
            Dockerfile content string
        """
        base_image = self._get_base_image_tag(image_type)
        extra_packages = self._get_extra_packages(image_type)

        lines = [
            f"# Auto-generated project image for {self.project_name}",
            f"FROM {base_image}",
            "",
        ]

        if extra_packages:
            lines.append("# Project-specific packages")
            for pkg in extra_packages:
                lines.append(f"RUN pip install --no-cache-dir {pkg}")
            lines.append("")

        lines.append("WORKDIR /app")
        lines.append("")

        return "\n".join(lines)

    def _get_effective_dockerfile(self, image_type: str) -> tuple[str, Path | None]:
        """Get effective Dockerfile content for image type.

        Priority:
        1. Custom Dockerfile.{type} in project root
        2. Auto-generated from base + extra_packages

        Args:
            image_type: "cpu" or "gpu"

        Returns:
            Tuple of (dockerfile_content, source_path or None if generated)
        """
        custom_path = self._get_project_dockerfile_path(image_type)
        if custom_path:
            return custom_path.read_text(), custom_path

        return self._generate_dockerfile_content(image_type), None

    def _compute_dockerfile_hash(self, image_type: str) -> str:
        """Compute hash of effective Dockerfile for change detection.

        Args:
            image_type: "cpu" or "gpu"

        Returns:
            SHA256 hash of Dockerfile content
        """
        content, _ = self._get_effective_dockerfile(image_type)
        return hashlib.sha256(content.encode()).hexdigest()[:12]

    def _check_local_image_exists(self, image_tag: str) -> bool:
        """Check if a local Docker image exists.

        Args:
            image_tag: Image tag to check

        Returns:
            True if image exists locally
        """
        try:
            result = subprocess.run(
                ["docker", "image", "inspect", image_tag],
                capture_output=True,
                check=False,
                timeout=10,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    def _check_registry_image_exists(self, registry_tag: str) -> bool:
        """Check if image exists in registry.

        Uses gcloud to check manifest without pulling.

        Args:
            registry_tag: Full registry image tag

        Returns:
            True if image exists in registry
        """
        try:
            result = subprocess.run(
                ["gcloud", "artifacts", "docker", "images", "describe", registry_tag, "--quiet"],
                capture_output=True,
                check=False,
                timeout=30,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    def list_images(self) -> dict[str, Any]:
        """List available images with their status.

        Shows both goldfish base images and project images.

        Returns:
            Dict with image info for both base and project layers
        """
        self._check_docker_available()

        registry_configured = self.config.gce is not None and self.config.gce.effective_artifact_registry is not None

        # Goldfish base images
        base_images = {}
        for image_type in ("cpu", "gpu"):
            local_tag = self._get_goldfish_base_local_tag(image_type)
            has_local = self._check_local_image_exists(local_tag)

            try:
                registry_tag = self._get_goldfish_base_registry_tag(image_type)
                has_registry = self._check_registry_image_exists(registry_tag)
            except RegistryNotConfiguredError:
                registry_tag = None
                has_registry = False

            dockerfile_path = self._get_goldfish_base_dockerfile_path(image_type)

            base_images[image_type] = {
                "local_tag": local_tag if has_local else None,
                "registry_tag": registry_tag if has_registry else None,
                "has_local": has_local,
                "has_registry": has_registry,
                "dockerfile_path": str(dockerfile_path),
                "status": "ready" if has_registry else ("local_only" if has_local else "not_built"),
            }

        # Project images
        project_images = {}
        for image_type in ("cpu", "gpu"):
            local_tag = self._get_project_image_tag(image_type, for_registry=False)
            has_local = self._check_local_image_exists(local_tag)

            try:
                registry_tag = self._get_project_image_tag(image_type, for_registry=True)
                has_registry = self._check_registry_image_exists(registry_tag)
            except RegistryNotConfiguredError:
                registry_tag = None
                has_registry = False

            custom_dockerfile = self._get_project_dockerfile_path(image_type)
            extra_packages = self._get_extra_packages(image_type)

            project_images[image_type] = {
                "local_tag": local_tag if has_local else None,
                "registry_tag": registry_tag if has_registry else None,
                "has_local": has_local,
                "has_registry": has_registry,
                "customization": {
                    "dockerfile": str(custom_dockerfile) if custom_dockerfile else None,
                    "extra_packages": extra_packages,
                },
                "base_image": self._get_base_image_tag(image_type),
            }

        return {
            "project": self.project_name,
            "base_images": base_images,
            "project_images": project_images,
            "registry_configured": registry_configured,
        }

    def inspect_image(self, image_type: str) -> dict[str, Any]:
        """Get detailed info about an image type.

        Shows base image, effective Dockerfile, extra packages,
        and file paths for editing.

        Args:
            image_type: "cpu" or "gpu"

        Returns:
            Dict with detailed image info
        """
        validate_image_type(image_type)

        dockerfile_content, source_path = self._get_effective_dockerfile(image_type)
        local_tag = self._get_project_image_tag(image_type, for_registry=False)

        try:
            registry_tag = self._get_project_image_tag(image_type, for_registry=True)
        except RegistryNotConfiguredError:
            registry_tag = None

        return {
            "image_type": image_type,
            "project": self.project_name,
            "local_tag": local_tag,
            "registry_tag": registry_tag,
            "base_image": self._get_base_image_tag(image_type),
            "customization": {
                "source": "custom_dockerfile" if source_path else "config_extra_packages",
                "dockerfile_path": str(source_path) if source_path else None,
                "extra_packages": self._get_extra_packages(image_type),
                "config_path": str(self.project_root / "goldfish.yaml"),
            },
            "effective_dockerfile": dockerfile_content,
            "dockerfile_hash": self._compute_dockerfile_hash(image_type),
        }

    def check_images(self) -> dict[str, Any]:
        """Check if images need rebuild or push.

        Compares Dockerfile hashes to detect changes.

        Returns:
            Dict with rebuild/push recommendations per image type
        """
        self._check_docker_available()

        results = {}
        for image_type in ("cpu", "gpu"):
            local_tag = self._get_project_image_tag(image_type, for_registry=False)
            has_local = self._check_local_image_exists(local_tag)

            try:
                registry_tag = self._get_project_image_tag(image_type, for_registry=True)
                has_registry = self._check_registry_image_exists(registry_tag)
            except RegistryNotConfiguredError:
                registry_tag = None
                has_registry = False

            # Determine if rebuild needed
            needs_rebuild = not has_local
            needs_push = has_local and not has_registry

            # Check if customization changed
            custom_dockerfile = self._get_project_dockerfile_path(image_type)
            extra_packages = self._get_extra_packages(image_type)
            has_customization = custom_dockerfile is not None or len(extra_packages) > 0

            results[image_type] = {
                "has_local": has_local,
                "has_registry": has_registry,
                "has_customization": has_customization,
                "needs_rebuild": needs_rebuild,
                "needs_push": needs_push,
                "recommendation": self._get_recommendation(needs_rebuild, needs_push, has_customization),
            }

        return {
            "project": self.project_name,
            "checks": results,
        }

    def _get_recommendation(self, needs_rebuild: bool, needs_push: bool, has_customization: bool) -> str:
        """Get actionable recommendation string."""
        if not has_customization:
            return "No customization - using goldfish base images directly"
        if needs_rebuild:
            return "Run build to create local image"
        if needs_push:
            return "Run push to deploy to registry"
        return "Up to date"

    def build_image(
        self,
        image_type: str,
        no_cache: bool = False,
        wait: bool = False,
        target: str = "project",
        backend: str = "local",
    ) -> dict[str, Any]:
        """Build a Docker image.

        Args:
            image_type: "cpu" or "gpu"
            no_cache: Force rebuild without Docker cache
            wait: If True, block until complete; if False, return immediately
                  (only affects local builds; cloud builds always return immediately)
            target: "base" for goldfish-base-* or "project" for {project}-*
            backend: "local" for Docker daemon or "cloud" for Cloud Build

        Returns:
            Dict with build_id and status (and result if wait=True for local builds)
        """
        validate_image_type(image_type)
        if target not in ("base", "project"):
            raise GoldfishError(f"Invalid target: {target}. Must be 'base' or 'project'")
        if backend not in ("local", "cloud"):
            raise GoldfishError(f"Invalid backend: {backend}. Must be 'local' or 'cloud'")

        # Cloud builds require database and GCE config
        if backend == "cloud":
            if not self.db:
                raise CloudBuildNotConfiguredError()
            if not self.config.gce:
                raise CloudBuildNotConfiguredError()
            return self._build_with_cloud_build(image_type, no_cache, target)

        # Local builds require Docker
        self._check_docker_available()

        # Generate build ID
        build_id = f"build-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)

        # Get image tags
        if target == "base":
            image_tag = self._get_goldfish_base_local_tag(image_type)
            try:
                registry_tag = self._get_goldfish_base_registry_tag(image_type)
            except RegistryNotConfiguredError:
                registry_tag = None
        else:
            image_tag = self._get_project_image_tag(image_type, for_registry=False)
            try:
                registry_tag = self._get_project_image_tag(image_type, for_registry=True)
            except RegistryNotConfiguredError:
                registry_tag = None

        # Create build status (in-memory)
        build_status = BuildStatus(
            build_id=build_id,
            image_type=image_type,
            status="pending",
            started_at=now,
        )

        with self._builds_lock:
            self._builds[build_id] = build_status

        # Also store in database if available
        if self.db:
            self.db.insert_docker_build(
                build_id=build_id,
                image_type=image_type,
                target=target,
                backend="local",
                started_at=now.isoformat(),
                image_tag=image_tag,
                registry_tag=registry_tag,
            )

        if wait:
            # Synchronous build
            self._run_build(build_id, image_type, no_cache, target)
            return self.get_build_status(build_id)
        else:
            # Async build
            thread = threading.Thread(
                target=self._run_build,
                args=(build_id, image_type, no_cache, target),
                daemon=True,
            )
            thread.start()
            target_name = "goldfish base" if target == "base" else "project"
            return {
                "build_id": build_id,
                "status": "pending",
                "image_type": image_type,
                "target": target,
                "backend": "local",
                "message": f"Building {target_name} {image_type} image. Use get_build_status('{build_id}') to check progress.",
            }

    def _run_build(self, build_id: str, image_type: str, no_cache: bool, target: str = "project") -> None:
        """Execute the build (runs in thread for async builds).

        Args:
            build_id: Build ID for status tracking
            image_type: "cpu" or "gpu"
            no_cache: Force rebuild without Docker cache
            target: "base" or "project"
        """
        with self._builds_lock:
            self._builds[build_id].status = "building"

        # Update DB status
        if self.db:
            self.db.update_docker_build_status(build_id, "building")

        try:
            if target == "base":
                # Build goldfish base image
                dockerfile_path = self._get_goldfish_base_dockerfile_path(image_type)
                image_tag = self._get_goldfish_base_local_tag(image_type)
                build_context = BASE_IMAGES_DIR

                if not dockerfile_path.exists():
                    raise GoldfishError(f"Base Dockerfile not found: {dockerfile_path}")

                self._execute_docker_build(build_id, dockerfile_path, image_tag, build_context, no_cache)
            else:
                # Build project image
                self._run_project_build(build_id, image_type, no_cache)

        except Exception as e:
            now = datetime.now(UTC)
            with self._builds_lock:
                self._builds[build_id].status = "failed"
                self._builds[build_id].completed_at = now
                self._builds[build_id].error = str(e)
            if self.db:
                self.db.update_docker_build_status(build_id, "failed", error=str(e), completed_at=now.isoformat())

    def _run_project_build(self, build_id: str, image_type: str, no_cache: bool) -> None:
        """Execute project image build.

        Args:
            build_id: Build ID for status tracking
            image_type: "cpu" or "gpu"
            no_cache: Force rebuild without Docker cache
        """
        import tempfile

        # Get effective Dockerfile
        dockerfile_content, source_path = self._get_effective_dockerfile(image_type)
        image_tag = self._get_project_image_tag(image_type, for_registry=False)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # Write Dockerfile
            dockerfile_path = tmppath / "Dockerfile"
            dockerfile_path.write_text(dockerfile_content)

            # Copy project source if custom Dockerfile (it might need project files)
            if source_path:
                # Use project root as context when custom Dockerfile exists
                build_context = self.project_root
                dockerfile_path = source_path
            else:
                build_context = tmppath

            self._execute_docker_build(build_id, dockerfile_path, image_tag, build_context, no_cache)

    def _execute_docker_build(
        self,
        build_id: str,
        dockerfile_path: Path,
        image_tag: str,
        build_context: Path,
        no_cache: bool,
    ) -> None:
        """Execute docker build command.

        Args:
            build_id: Build ID for status tracking
            dockerfile_path: Path to Dockerfile
            image_tag: Tag for the built image
            build_context: Build context directory
            no_cache: Force rebuild without Docker cache
        """
        build_cmd = [
            "docker",
            "build",
            "--platform",
            "linux/amd64",
            "-f",
            str(dockerfile_path),
            "-t",
            image_tag,
        ]

        if no_cache:
            build_cmd.append("--no-cache")

        build_cmd.append(str(build_context))

        # Run build and capture output
        process = subprocess.Popen(
            build_cmd,
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

        if process.returncode != 0:
            raise BaseImageBuildError(
                image_tag.split(":")[0],
                "Build failed",
                logs_tail="\n".join(logs[-50:]),
            )

        # Success
        now = datetime.now(UTC)
        with self._builds_lock:
            self._builds[build_id].status = "completed"
            self._builds[build_id].completed_at = now
            self._builds[build_id].image_tag = image_tag

        if self.db:
            self.db.update_docker_build_status(build_id, "completed", completed_at=now.isoformat(), image_tag=image_tag)

    def get_build_status(self, build_id: str) -> dict[str, Any]:
        """Get status of a build.

        For cloud builds, polls Cloud Build API and updates database.
        For local builds, returns in-memory status (or from database).

        Args:
            build_id: Build ID from build_image()

        Returns:
            Dict with build status info
        """
        # First check database for cloud builds
        if self.db:
            db_build = self.db.get_docker_build(build_id)
            if db_build:
                # For cloud builds, poll the Cloud Build API if still in progress
                if db_build["backend"] == "cloud" and db_build["status"] in ("pending", "building"):
                    self._poll_cloud_build_status(build_id, db_build["cloud_build_id"])
                    db_build = self.db.get_docker_build(build_id)
                    if db_build is None:
                        raise GoldfishError(f"Build {build_id} disappeared from database")

                return {
                    "build_id": db_build["id"],
                    "image_type": db_build["image_type"],
                    "target": db_build["target"],
                    "backend": db_build["backend"],
                    "status": db_build["status"],
                    "started_at": db_build["started_at"],
                    "completed_at": db_build["completed_at"],
                    "error": db_build["error"],
                    "logs_uri": db_build["logs_uri"],
                    "image_tag": db_build["image_tag"],
                    "registry_tag": db_build["registry_tag"],
                    "cloud_build_id": db_build["cloud_build_id"],
                }

        # Fall back to in-memory status for local builds
        with self._builds_lock:
            if build_id not in self._builds:
                raise GoldfishError(f"Unknown build ID: {build_id}")
            status = self._builds[build_id]

        return {
            "build_id": status.build_id,
            "image_type": status.image_type,
            "target": "project",  # Legacy in-memory builds are project builds
            "backend": "local",
            "status": status.status,
            "started_at": status.started_at.isoformat(),
            "completed_at": status.completed_at.isoformat() if status.completed_at else None,
            "error": status.error,
            "logs_tail": status.logs[-20:] if status.logs else [],
            "image_tag": status.image_tag,
        }

    def push_image(self, image_type: str, target: str = "project") -> dict[str, Any]:
        """Push an image to Artifact Registry.

        Args:
            image_type: "cpu" or "gpu"
            target: "base" for goldfish-base-* or "project" for {project}-*

        Returns:
            Dict with push result
        """
        import shutil

        validate_image_type(image_type)
        if target not in ("base", "project"):
            raise GoldfishError(f"Invalid target: {target}. Must be 'base' or 'project'")
        self._check_docker_available()

        # Get appropriate tags based on target
        if target == "base":
            local_tag = self._get_goldfish_base_local_tag(image_type)
            registry_tag = self._get_goldfish_base_registry_tag(image_type)
        else:
            local_tag = self._get_project_image_tag(image_type, for_registry=False)
            registry_tag = self._get_project_image_tag(image_type, for_registry=True)

        if not self._check_local_image_exists(local_tag):
            raise BaseImageNotFoundError(local_tag)

        registry_url = self._get_artifact_registry()

        # Configure Docker authentication
        registry_domain = registry_url.split("/")[0]
        if not shutil.which("gcloud"):
            raise GoldfishError("gcloud not found; install Google Cloud SDK for registry push.")

        auth_result = subprocess.run(
            ["gcloud", "auth", "configure-docker", registry_domain, "--quiet"],
            capture_output=True,
            text=True,
            check=False,
        )
        if auth_result.returncode != 0:
            raise GoldfishError(f"Failed to configure Docker authentication: {auth_result.stderr}")

        # Tag for registry
        tag_result = subprocess.run(
            ["docker", "tag", local_tag, registry_tag],
            capture_output=True,
            text=True,
            check=False,
        )
        if tag_result.returncode != 0:
            raise GoldfishError(f"Docker tag failed: {tag_result.stderr}")

        # Push to registry
        push_result = subprocess.run(
            ["docker", "push", registry_tag],
            capture_output=True,
            text=True,
            check=False,
        )
        if push_result.returncode != 0:
            raise GoldfishError(f"Docker push failed: {push_result.stderr}")

        target_name = "goldfish base" if target == "base" else "project"
        return {
            "success": True,
            "target": target,
            "image_type": image_type,
            "local_tag": local_tag,
            "registry_tag": registry_tag,
            "message": f"Successfully pushed {target_name} {image_type} image to {registry_tag}",
        }

    # =========================================================================
    # Cloud Build Methods
    # =========================================================================

    def _build_with_cloud_build(
        self,
        image_type: str,
        no_cache: bool,
        target: str,
    ) -> dict[str, Any]:
        """Build image using Google Cloud Build.

        Cloud builds always return immediately with a build_id.
        Use get_build_status() to poll for completion.

        Args:
            image_type: "cpu" or "gpu"
            no_cache: Force rebuild without Docker cache
            target: "base" or "project"

        Returns:
            Dict with build_id and status
        """
        import os
        import shutil

        if not shutil.which("gcloud"):
            raise CloudBuildError("gcloud not found; install Google Cloud SDK for Cloud Build")

        # Get project ID
        assert self.config.gce is not None  # Checked by caller
        project_id = self.config.gce.effective_project_id

        # Generate build ID and timestamps
        build_id = f"build-{uuid.uuid4().hex[:8]}"
        now = datetime.now(UTC)

        # Get registry tag (required for cloud builds)
        if target == "base":
            registry_tag = self._get_goldfish_base_registry_tag(image_type)
            dockerfile_path = self._get_goldfish_base_dockerfile_path(image_type)
            build_context = BASE_IMAGES_DIR
        else:
            registry_tag = self._get_project_image_tag(image_type, for_registry=True)
            # For project builds, need to prepare Dockerfile
            dockerfile_content, source_path = self._get_effective_dockerfile(image_type)
            if source_path:
                dockerfile_path = source_path
                build_context = self.project_root
            else:
                # Write generated Dockerfile to temp location
                tmpdir = tempfile.mkdtemp(prefix="goldfish-build-")
                dockerfile_path = Path(tmpdir) / "Dockerfile"
                dockerfile_path.write_text(dockerfile_content)
                build_context = Path(tmpdir)

        # Get Cloud Build config
        cloud_config = self.config.docker.cloud_build

        # Build cloudbuild.yaml config
        build_args = [
            "build",
            "--platform",
            "linux/amd64",
            "-t",
            registry_tag,
            "-f",
            dockerfile_path.name,
            ".",
        ]
        if no_cache:
            build_args.append("--no-cache")

        cloudbuild_config = {
            "steps": [
                {
                    "name": "gcr.io/cloud-builders/docker",
                    "args": build_args,
                }
            ],
            "images": [registry_tag],
            "timeout": f"{cloud_config.timeout_minutes * 60}s",
            "options": {
                "machineType": cloud_config.machine_type,
                "diskSizeGb": cloud_config.disk_size_gb,
            },
        }

        # Write cloudbuild.yaml to temp file
        config_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, prefix="cloudbuild-")
        try:
            yaml.dump(cloudbuild_config, config_file)
            config_file.close()

            # Insert build record before submission
            assert self.db is not None  # Checked by caller
            self.db.insert_docker_build(
                build_id=build_id,
                image_type=image_type,
                target=target,
                backend="cloud",
                started_at=now.isoformat(),
                registry_tag=registry_tag,
            )

            # Submit to Cloud Build
            result = subprocess.run(
                [
                    "gcloud",
                    "builds",
                    "submit",
                    "--config",
                    config_file.name,
                    "--project",
                    project_id,
                    "--async",
                    "--format=json",
                    str(build_context),
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                # Update DB with failure
                self.db.update_docker_build_status(
                    build_id,
                    "failed",
                    error=f"Cloud Build submission failed: {result.stderr}",
                    completed_at=datetime.now(UTC).isoformat(),
                )
                raise CloudBuildError(
                    f"Failed to submit build: {result.stderr}",
                )

            # Parse Cloud Build ID from output
            try:
                output = json.loads(result.stdout)
                cloud_build_id = output.get("id") or output.get("name", "").split("/")[-1]
                logs_uri = output.get("logUrl")
            except (json.JSONDecodeError, KeyError):
                # Try to parse from stderr (gcloud sometimes prints there)
                cloud_build_id = None
                logs_uri = None
                for line in result.stderr.split("\n"):
                    if "Logs are available at" in line:
                        logs_uri = line.split("[")[-1].rstrip("]").strip()
                    if line.startswith("ID:"):
                        cloud_build_id = line.split(":")[-1].strip()

            # Update DB with Cloud Build ID
            self.db.update_docker_build_status(
                build_id,
                "building",
                cloud_build_id=cloud_build_id,
                logs_uri=logs_uri,
            )

            target_name = "goldfish base" if target == "base" else "project"
            return {
                "build_id": build_id,
                "status": "building",
                "image_type": image_type,
                "target": target,
                "backend": "cloud",
                "cloud_build_id": cloud_build_id,
                "logs_uri": logs_uri,
                "registry_tag": registry_tag,
                "message": f"Cloud Build submitted for {target_name} {image_type} image. "
                f"Use get_build_status('{build_id}') to check progress.",
            }

        finally:
            # Clean up temp config file
            os.unlink(config_file.name)

    def _poll_cloud_build_status(
        self,
        build_id: str,
        cloud_build_id: str | None,
    ) -> None:
        """Poll Cloud Build API and update database.

        Args:
            build_id: Our internal build ID
            cloud_build_id: GCP Cloud Build operation ID
        """
        if not cloud_build_id:
            return

        if not self.config.gce:
            return

        project_id = self.config.gce.effective_project_id

        result = subprocess.run(
            [
                "gcloud",
                "builds",
                "describe",
                cloud_build_id,
                "--project",
                project_id,
                "--format=json",
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            # Can't get status, leave as-is
            return

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError:
            return

        # Map Cloud Build status to our status
        status_map = {
            "QUEUED": "pending",
            "WORKING": "building",
            "SUCCESS": "completed",
            "FAILURE": "failed",
            "CANCELLED": "cancelled",
            "TIMEOUT": "failed",
            "EXPIRED": "failed",
        }

        cloud_status = data.get("status", "UNKNOWN")
        our_status = status_map.get(cloud_status, "building")

        # Prepare update kwargs
        update_kwargs: dict[str, Any] = {}

        if our_status in ("completed", "failed", "cancelled"):
            update_kwargs["completed_at"] = datetime.now(UTC).isoformat()

        if our_status == "failed":
            error_detail = data.get("statusDetail") or f"Cloud Build {cloud_status}"
            update_kwargs["error"] = error_detail

        if our_status == "completed":
            # Get the built image tag
            images = data.get("images", [])
            if images:
                update_kwargs["registry_tag"] = images[0]

        logs_uri = data.get("logUrl")
        if logs_uri:
            update_kwargs["logs_uri"] = logs_uri

        # Update database
        if self.db:
            self.db.update_docker_build_status(build_id, our_status, **update_kwargs)
