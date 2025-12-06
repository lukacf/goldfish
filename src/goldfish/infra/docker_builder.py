"""Docker image building for Goldfish stage execution."""

import re
import subprocess
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
        # Check for optional loaders directory
        has_loaders = (workspace_dir / "loaders").exists()

        dockerfile = f"""FROM python:3.11-slim

# Install dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install Goldfish IO library
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

        # Generate Dockerfile
        dockerfile_content = self.generate_dockerfile(workspace_dir)
        dockerfile_path = workspace_dir / "Dockerfile"
        dockerfile_path.write_text(dockerfile_content)

        # Build image
        build_cmd = ["docker", "build", "-t", image_tag]

        if not use_cache:
            build_cmd.append("--no-cache")

        build_cmd.append(str(workspace_dir))

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
