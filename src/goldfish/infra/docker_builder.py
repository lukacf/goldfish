"""Docker image building for Goldfish stage execution."""

import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

from goldfish.errors import GoldfishError


class DockerBuilder:
    """Build Docker images for stage execution."""

    def generate_dockerfile(self, workspace_dir: Path) -> str:
        """Generate Dockerfile for workspace.

        Args:
            workspace_dir: Path to workspace directory

        Returns:
            Dockerfile content as string
        """
        # Check for optional files/directories
        has_requirements = (workspace_dir / "requirements.txt").exists()
        has_loaders = (workspace_dir / "loaders").exists()

        dockerfile = "FROM python:3.11-slim\n\n"

        # Install dependencies (optional)
        if has_requirements:
            dockerfile += """# Install dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

"""

        dockerfile += """# Install Goldfish IO library
# TODO: Package goldfish.io separately and install from wheel
# For now, assume it's available in the image or mounted

# Copy workspace code
COPY modules/ /app/modules/
COPY configs/ /app/configs/
"""

        if has_loaders:
            dockerfile += "COPY loaders/ /app/loaders/\n"

        dockerfile += """
WORKDIR /app

# Entrypoint will be overridden at runtime
CMD ["/bin/bash"]
"""

        return dockerfile

    def build_image(
        self,
        workspace_dir: Path,
        workspace_name: str,
        version: str,
        use_cache: bool = True
    ) -> str:
        """Build Docker image for workspace.

        Uses a temporary directory as build context to avoid dirtying
        the workspace with a Dockerfile.

        Args:
            workspace_dir: Path to workspace directory
            workspace_name: Workspace name
            version: Version identifier
            use_cache: Use Docker layer caching (default True)

        Returns:
            Image tag (e.g., "goldfish-test_ws-v1")

        Raises:
            GoldfishError: If docker build fails
        """
        # Generate image tag
        image_tag = self._generate_image_tag(workspace_name, version)

        # Create temporary build context to avoid dirtying workspace
        with tempfile.TemporaryDirectory(prefix="goldfish-docker-") as tmp_dir:
            build_context = Path(tmp_dir)

            # Copy required workspace files to build context
            if (workspace_dir / "requirements.txt").exists():
                shutil.copy2(workspace_dir / "requirements.txt", build_context / "requirements.txt")

            if (workspace_dir / "modules").exists():
                shutil.copytree(workspace_dir / "modules", build_context / "modules")

            if (workspace_dir / "configs").exists():
                shutil.copytree(workspace_dir / "configs", build_context / "configs")

            if (workspace_dir / "loaders").exists():
                shutil.copytree(workspace_dir / "loaders", build_context / "loaders")

            # Generate Dockerfile in build context (not workspace)
            dockerfile_content = self.generate_dockerfile(workspace_dir)
            dockerfile_path = build_context / "Dockerfile"
            dockerfile_path.write_text(dockerfile_content)

            # Build image for linux/amd64 (GCE target platform)
            build_cmd = ["docker", "build", "--platform", "linux/amd64", "-t", image_tag]

            if not use_cache:
                build_cmd.append("--no-cache")

            build_cmd.append(str(build_context))

            try:
                result = subprocess.run(
                    build_cmd,
                    capture_output=True,
                    text=True,
                    check=False
                )

                if result.returncode != 0:
                    raise GoldfishError(
                        f"Docker build failed: {result.stderr}"
                    )

                return image_tag

            except FileNotFoundError:
                raise GoldfishError(
                    "Docker not found. Please install Docker to build images."
                )

    def push_image(
        self,
        local_tag: str,
        registry_url: str,
        workspace_name: str,
        version: str
    ) -> str:
        """Push Docker image to Artifact Registry.

        Args:
            local_tag: Local image tag (e.g., "goldfish-test_ws-v1")
            registry_url: Registry URL (e.g., "us-docker.pkg.dev/project/goldfish")
            workspace_name: Workspace name
            version: Version identifier

        Returns:
            Full registry image tag

        Raises:
            GoldfishError: If push fails
        """
        # Validate registry URL format (must be host/path, no scheme)
        if not registry_url or "://" in registry_url or "/" not in registry_url:
            raise GoldfishError(
                f"Invalid artifact_registry URL: {registry_url}. "
                "Expected format like us-docker.pkg.dev/<project>/<repo>"
            )

        # Generate sanitized image name
        sanitized_workspace = re.sub(r'[^a-z0-9._-]', '_', workspace_name.lower())
        sanitized_version = re.sub(r'[^a-z0-9._-]', '_', version.lower())
        image_name = f"goldfish-{sanitized_workspace}-{sanitized_version}"

        # Build full registry tag
        registry_tag = f"{registry_url}/{image_name}"

        try:
            # Configure Docker authentication with gcloud (idempotent)
            # Extract registry domain from URL (e.g., "us-docker.pkg.dev")
            registry_domain = registry_url.split('/')[0]
            auth_result = subprocess.run(
                ["gcloud", "auth", "configure-docker", registry_domain, "--quiet"],
                capture_output=True,
                text=True,
                check=False
            )
            if auth_result.returncode != 0:
                raise GoldfishError(
                    f"Failed to configure Docker authentication: {auth_result.stderr}"
                )

            # Tag for registry
            tag_result = subprocess.run(
                ["docker", "tag", local_tag, registry_tag],
                capture_output=True,
                text=True,
                check=False
            )

            if tag_result.returncode != 0:
                raise GoldfishError(
                    f"Docker tag failed: {tag_result.stderr}"
                )

            # Push to registry
            push_result = subprocess.run(
                ["docker", "push", registry_tag],
                capture_output=True,
                text=True,
                check=False
            )

            if push_result.returncode != 0:
                raise GoldfishError(
                    f"Docker push failed: {push_result.stderr}"
                )

            return registry_tag

        except FileNotFoundError:
            raise GoldfishError(
                "Docker not found. Please install Docker to push images."
            )

    def _generate_image_tag(self, workspace_name: str, version: str) -> str:
        """Generate Docker image tag.

        Tags follow format: goldfish-{workspace}-{version}
        Invalid characters are replaced with underscores.

        Args:
            workspace_name: Workspace name
            version: Version identifier

        Returns:
            Sanitized image tag
        """
        # Sanitize workspace name (Docker tags allow: [a-z0-9._-])
        sanitized_workspace = re.sub(r'[^a-z0-9._-]', '_', workspace_name.lower())

        # SECURITY: Sanitize version to prevent command injection
        # Docker tags allow: [a-z0-9._-]
        sanitized_version = re.sub(r'[^a-z0-9._-]', '_', version.lower())

        return f"goldfish-{sanitized_workspace}-{sanitized_version}"
