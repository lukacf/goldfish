"""GCP Image adapters for building and managing container images.

Provides GCP-specific implementations of ImageBuilder (Cloud Build)
and ImageRegistry (Artifact Registry).
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from goldfish.errors import CloudBuildError, CloudBuildNotConfiguredError, GoldfishError

if TYPE_CHECKING:
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)


class ArtifactRegistryError(GoldfishError):
    """Error during Artifact Registry operations."""

    pass


class CloudBuildImageBuilder:
    """GCP implementation of ImageBuilder protocol using Cloud Build.

    Builds container images using Google Cloud Build for fast, scalable builds.
    Particularly useful for GPU images which are slow to build locally.
    """

    def __init__(
        self,
        project_id: str,
        registry_url: str,
        machine_type: str = "E2_HIGHCPU_32",
        timeout_minutes: int = 30,
        disk_size_gb: int = 100,
        db: Database | None = None,
    ) -> None:
        """Initialize CloudBuildImageBuilder.

        Args:
            project_id: GCP project ID
            registry_url: Artifact Registry URL (e.g., "us-docker.pkg.dev/project/repo")
            machine_type: Cloud Build machine type
            timeout_minutes: Build timeout in minutes
            disk_size_gb: Build disk size in GB
            db: Optional database for persistent build tracking
        """
        self._project_id = project_id
        self._registry_url = registry_url
        self._machine_type = machine_type
        self._timeout_minutes = timeout_minutes
        self._disk_size_gb = disk_size_gb
        self._db = db

    def _check_gcloud_available(self) -> None:
        """Check if gcloud CLI is available.

        Raises:
            CloudBuildNotConfiguredError: If gcloud is not installed
        """
        if not shutil.which("gcloud"):
            raise CloudBuildNotConfiguredError()

    def build(
        self,
        context_path: Path,
        dockerfile_path: Path,
        image_tag: str,
        build_args: dict[str, str] | None = None,
        no_cache: bool = False,
    ) -> str:
        """Build a container image synchronously using Cloud Build.

        This submits the build and waits for completion.

        Args:
            context_path: Path to build context directory
            dockerfile_path: Path to Dockerfile
            image_tag: Tag for the built image
            build_args: Optional build arguments
            no_cache: If True, disable layer caching

        Returns:
            The registry tag of the built image

        Raises:
            CloudBuildError: If build fails
        """
        build_id = self.build_async(context_path, dockerfile_path, image_tag, build_args, no_cache)

        # Poll for completion
        while True:
            status = self.get_build_status(build_id)
            if status["status"] == "completed":
                return status["image_tag"] or image_tag
            elif status["status"] == "failed":
                raise CloudBuildError(f"Cloud Build failed: {status.get('error', 'Unknown error')}")
            # Wait before polling again
            import time

            time.sleep(10)

    def build_async(
        self,
        context_path: Path,
        dockerfile_path: Path,
        image_tag: str,
        build_args: dict[str, str] | None = None,
        no_cache: bool = False,
    ) -> str:
        """Start an asynchronous image build using Cloud Build.

        Args:
            context_path: Path to build context directory
            dockerfile_path: Path to Dockerfile
            image_tag: Tag for the built image
            build_args: Optional build arguments
            no_cache: If True, disable layer caching

        Returns:
            Build ID for status polling

        Raises:
            CloudBuildError: If build submission fails
        """
        self._check_gcloud_available()

        # Ensure image_tag includes registry URL
        if not image_tag.startswith(self._registry_url):
            registry_tag = f"{self._registry_url}/{image_tag}"
        else:
            registry_tag = image_tag

        # Build docker command args
        docker_args = [
            "build",
            "--platform",
            "linux/amd64",
            "-t",
            registry_tag,
            "-f",
            dockerfile_path.name,
        ]

        if build_args:
            for key, value in build_args.items():
                docker_args.extend(["--build-arg", f"{key}={value}"])

        if no_cache:
            docker_args.append("--no-cache")

        docker_args.append(".")

        # Create cloudbuild.yaml config
        cloudbuild_config = {
            "steps": [
                {
                    "name": "gcr.io/cloud-builders/docker",
                    "args": docker_args,
                }
            ],
            "images": [registry_tag],
            "timeout": f"{self._timeout_minutes * 60}s",
            "options": {
                "machineType": self._machine_type,
                "diskSizeGb": self._disk_size_gb,
            },
        }

        # Write config to temp file
        config_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, prefix="cloudbuild-")
        try:
            yaml.dump(cloudbuild_config, config_file)
            config_file.close()

            # Submit to Cloud Build
            result = subprocess.run(
                [
                    "gcloud",
                    "builds",
                    "submit",
                    "--config",
                    config_file.name,
                    "--project",
                    self._project_id,
                    "--async",
                    "--format=json",
                    str(context_path),
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                raise CloudBuildError(f"Failed to submit Cloud Build: {result.stderr}")

            # Parse Cloud Build ID from output
            cloud_build_id: str | None = None
            try:
                output = json.loads(result.stdout)
                cloud_build_id = output.get("id") or output.get("name", "").split("/")[-1]
            except (json.JSONDecodeError, KeyError):
                # Try to parse from stderr
                pass
                for line in result.stderr.split("\n"):
                    if line.startswith("ID:"):
                        cloud_build_id = line.split(":")[-1].strip()
                        break

            if not cloud_build_id:
                raise CloudBuildError("Could not parse Cloud Build ID from response")

            # At this point cloud_build_id is guaranteed to be non-None
            assert cloud_build_id is not None  # for type narrowing

            # Store build info in database if available
            if self._db:
                self._db.insert_docker_build(
                    build_id=cloud_build_id,
                    image_type="custom",
                    target="image",
                    backend="cloud",
                    started_at=datetime.now(UTC).isoformat(),
                    registry_tag=registry_tag,
                    cloud_build_id=cloud_build_id,
                )

            return cloud_build_id

        finally:
            os.unlink(config_file.name)

    def get_build_status(self, build_id: str) -> dict[str, Any]:
        """Get status of a Cloud Build.

        Args:
            build_id: Cloud Build ID from build_async()

        Returns:
            Dict with status info
        """
        self._check_gcloud_available()

        result = subprocess.run(
            [
                "gcloud",
                "builds",
                "describe",
                build_id,
                "--project",
                self._project_id,
                "--format=json",
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            return {
                "build_id": build_id,
                "status": "unknown",
                "error": f"Failed to get build status: {result.stderr}",
            }

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError:
            return {
                "build_id": build_id,
                "status": "unknown",
                "error": "Failed to parse build status",
            }

        # Map Cloud Build status to our status
        status_map = {
            "QUEUED": "pending",
            "WORKING": "building",
            "SUCCESS": "completed",
            "FAILURE": "failed",
            "CANCELLED": "failed",
            "TIMEOUT": "failed",
            "EXPIRED": "failed",
        }

        cloud_status = data.get("status", "UNKNOWN")
        our_status = status_map.get(cloud_status, "building")

        result_dict: dict[str, Any] = {
            "build_id": build_id,
            "status": our_status,
            "cloud_status": cloud_status,
            "logs_uri": data.get("logUrl"),
        }

        if our_status == "completed":
            images = data.get("images", [])
            result_dict["image_tag"] = images[0] if images else None

        if our_status == "failed":
            result_dict["error"] = data.get("statusDetail") or f"Cloud Build {cloud_status}"

        return result_dict


class ArtifactRegistryImageRegistry:
    """GCP implementation of ImageRegistry protocol using Artifact Registry.

    Manages container images in Google Artifact Registry.
    """

    def __init__(self, project_id: str, registry_url: str) -> None:
        """Initialize ArtifactRegistryImageRegistry.

        Args:
            project_id: GCP project ID
            registry_url: Artifact Registry URL (e.g., "us-docker.pkg.dev/project/repo")
        """
        self._project_id = project_id
        self._registry_url = registry_url
        self._auth_configured = False

    def _check_gcloud_available(self) -> None:
        """Check if gcloud CLI is available.

        Raises:
            ArtifactRegistryError: If gcloud is not installed
        """
        if not shutil.which("gcloud"):
            raise ArtifactRegistryError("gcloud not found; install Google Cloud SDK")

    def _check_docker_available(self) -> None:
        """Check if Docker daemon is available.

        Raises:
            ArtifactRegistryError: If Docker is not available
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
                raise ArtifactRegistryError("Docker daemon not responding")
        except FileNotFoundError as e:
            raise ArtifactRegistryError("Docker not installed") from e
        except subprocess.TimeoutExpired as e:
            raise ArtifactRegistryError("Docker daemon timed out") from e

    def _configure_docker_auth(self) -> None:
        """Configure Docker authentication for Artifact Registry."""
        if self._auth_configured:
            return

        self._check_gcloud_available()

        # Extract registry domain from URL
        registry_domain = self._registry_url.split("/")[0]

        result = subprocess.run(
            ["gcloud", "auth", "configure-docker", registry_domain, "--quiet"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise ArtifactRegistryError(f"Failed to configure Docker authentication: {result.stderr}")

        self._auth_configured = True

    def push(self, local_tag: str, registry_tag: str) -> str:
        """Push a local image to Artifact Registry.

        Args:
            local_tag: Local image tag
            registry_tag: Full registry tag

        Returns:
            The pushed registry tag

        Raises:
            ArtifactRegistryError: If push fails
        """
        self._check_docker_available()
        self._configure_docker_auth()

        # Tag the local image with registry tag
        tag_result = subprocess.run(
            ["docker", "tag", local_tag, registry_tag],
            capture_output=True,
            text=True,
            check=False,
        )
        if tag_result.returncode != 0:
            raise ArtifactRegistryError(f"Failed to tag image: {tag_result.stderr}")

        # Push to registry
        push_result = subprocess.run(
            ["docker", "push", registry_tag],
            capture_output=True,
            text=True,
            check=False,
            timeout=30 * 60,
        )
        if push_result.returncode != 0:
            raise ArtifactRegistryError(f"Failed to push image: {push_result.stderr}")

        return registry_tag

    def pull(self, registry_tag: str) -> str:
        """Pull an image from Artifact Registry.

        Args:
            registry_tag: Full registry tag

        Returns:
            The local tag of the pulled image

        Raises:
            ArtifactRegistryError: If pull fails
        """
        self._check_docker_available()
        self._configure_docker_auth()

        result = subprocess.run(
            ["docker", "pull", registry_tag],
            capture_output=True,
            text=True,
            check=False,
            timeout=30 * 60,
        )
        if result.returncode != 0:
            raise ArtifactRegistryError(f"Failed to pull image: {result.stderr}")

        return registry_tag

    def exists(self, registry_tag: str) -> bool:
        """Check if an image exists in Artifact Registry.

        Uses gcloud to check without pulling the image.

        Args:
            registry_tag: Full registry tag

        Returns:
            True if image exists
        """
        self._check_gcloud_available()

        result = subprocess.run(
            [
                "gcloud",
                "artifacts",
                "docker",
                "images",
                "describe",
                registry_tag,
                "--project",
                self._project_id,
                "--quiet",
            ],
            capture_output=True,
            check=False,
            timeout=30,
        )
        return result.returncode == 0

    def delete(self, registry_tag: str) -> None:
        """Delete an image from Artifact Registry.

        Args:
            registry_tag: Full registry tag

        Raises:
            ArtifactRegistryError: If delete fails
        """
        self._check_gcloud_available()

        result = subprocess.run(
            [
                "gcloud",
                "artifacts",
                "docker",
                "images",
                "delete",
                registry_tag,
                "--project",
                self._project_id,
                "--quiet",
                "--delete-tags",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0 and "NOT_FOUND" not in result.stderr:
            raise ArtifactRegistryError(f"Failed to delete image: {result.stderr}")
