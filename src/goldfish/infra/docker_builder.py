"""Docker image building for Goldfish stage execution."""

import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from goldfish.errors import GoldfishError

# Paths to goldfish runtime modules (relative to this file)
GOLDFISH_IO_PATH = Path(__file__).parent.parent / "io" / "__init__.py"
GOLDFISH_METRICS_PATH = Path(__file__).parent.parent / "metrics"
GOLDFISH_SVS_PATH = Path(__file__).parent.parent / "svs"
# Metrics and SVS depend on validation, errors, and utils modules
GOLDFISH_VALIDATION_PATH = Path(__file__).parent.parent / "validation.py"
GOLDFISH_ERRORS_PATH = Path(__file__).parent.parent / "errors.py"
GOLDFISH_UTILS_PATH = Path(__file__).parent.parent / "utils"

# Default base image when none specified (backwards compatibility)
DEFAULT_BASE_IMAGE = "python:3.11-slim"


class DockerBuilder:
    """Build Docker images for stage execution.

    Uses pre-built base images with common ML libraries. If a workspace has
    a requirements.txt, those are installed on top of the base image for
    project-specific dependencies.
    """

    def __init__(self, config: object | None = None):
        # Store config for backend checks (may be GoldfishConfig or partial)
        self.config = config

    def generate_dockerfile(self, workspace_dir: Path, base_image: str | None = None) -> str:
        """Generate Dockerfile for workspace.

        Args:
            workspace_dir: Path to workspace directory
            base_image: Base image to use (e.g., "us-docker.pkg.dev/.../goldfish-base-cpu:v1")
                       Defaults to python:3.11-slim if not specified

        Returns:
            Dockerfile content as string
        """
        # Use provided base image or fallback
        base = base_image or DEFAULT_BASE_IMAGE

        # Check for optional files/directories
        has_requirements = (workspace_dir / "requirements.txt").exists()
        has_loaders = (workspace_dir / "loaders").exists()

        # Detect image type to determine user handling
        # - Jupyter images (quay.io/jupyter/*) run as non-root user (jovyan, uid 1000)
        # - Goldfish custom images (goldfish-base-*) run as non-root user (goldfish, uid 1000)
        # - NVIDIA NGC images (nvcr.io/nvidia/*) run as root
        # - Other images default to root-compatible mode
        is_nonroot_image = "jupyter" in base.lower() or "goldfish-base" in base.lower()

        dockerfile = f"FROM {base}\n\n"

        # Install additional dependencies from requirements.txt if present
        # Note: Pre-built base images already have common ML libraries
        # requirements.txt is for project-specific extras only
        if has_requirements:
            if is_nonroot_image:
                # Non-root images (Jupyter, Goldfish) run as uid 1000
                dockerfile += """# Install additional project dependencies
USER root
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt
USER 1000

"""
            else:
                # NVIDIA NGC and other images run as root
                dockerfile += """# Install additional project dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

"""

        if is_nonroot_image:
            # Non-root images: use --chown for uid 1000 user
            # VERSION arg busts cache when workspace version changes
            dockerfile += """# Cache-bust: version changes invalidate subsequent layers
ARG VERSION
RUN echo "Building version: ${VERSION}"

# Install Goldfish IO library
COPY --chown=1000:100 goldfish_io/ /app/goldfish_io/
ENV PYTHONPATH="/app/goldfish_io:/app/modules:/app:${PYTHONPATH}"

# Copy workspace code
COPY --chown=1000:100 modules/ /app/modules/
COPY --chown=1000:100 configs/ /app/configs/
"""
            if has_loaders:
                dockerfile += "COPY --chown=1000:100 loaders/ /app/loaders/\n"
        else:
            # Root images: no chown needed
            # VERSION arg busts cache when workspace version changes
            dockerfile += """# Cache-bust: version changes invalidate subsequent layers
ARG VERSION
RUN echo "Building version: ${VERSION}"

# Install Goldfish IO library
COPY goldfish_io/ /app/goldfish_io/
ENV PYTHONPATH="/app/goldfish_io:/app/modules:/app:${PYTHONPATH}"

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
        use_cache: bool = True,
        base_image: str | None = None,
    ) -> str:
        """Build Docker image for workspace.

        Uses a temporary directory as build context to avoid dirtying
        the workspace with a Dockerfile.

        Args:
            workspace_dir: Path to workspace directory
            workspace_name: Workspace name
            version: Version identifier
            use_cache: Use Docker layer caching (default True)
            base_image: Pre-built base image to use (e.g., "registry/goldfish-base-cpu:v1")
                       If None, falls back to python:3.11-slim

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

            # Copy goldfish.io module into build context
            # This creates a goldfish/io package structure so `from goldfish.io import ...` works
            goldfish_io_dest = build_context / "goldfish_io" / "goldfish" / "io"
            goldfish_io_dest.mkdir(parents=True, exist_ok=True)
            if GOLDFISH_IO_PATH.exists():
                shutil.copy2(GOLDFISH_IO_PATH, goldfish_io_dest / "__init__.py")
                # Create parent __init__.py for package structure
                (build_context / "goldfish_io" / "goldfish" / "__init__.py").write_text(
                    '"""Goldfish ML package (container runtime)."""\n'
                )
                (build_context / "goldfish_io" / "__init__.py").write_text("")

            # Copy goldfish.metrics module into build context
            # This enables `from goldfish.metrics import log_metric, log_metrics, ...`
            if GOLDFISH_METRICS_PATH.exists() and GOLDFISH_METRICS_PATH.is_dir():
                goldfish_metrics_dest = build_context / "goldfish_io" / "goldfish" / "metrics"
                shutil.copytree(
                    GOLDFISH_METRICS_PATH,
                    goldfish_metrics_dest,
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
                )

            # Copy goldfish.svs module into build context
            if GOLDFISH_SVS_PATH.exists() and GOLDFISH_SVS_PATH.is_dir():
                goldfish_svs_dest = build_context / "goldfish_io" / "goldfish" / "svs"
                shutil.copytree(
                    GOLDFISH_SVS_PATH,
                    goldfish_svs_dest,
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
                )

            # Copy goldfish.validation, goldfish.errors, and goldfish.utils (metrics/SVS dependencies)
            goldfish_pkg_dest = build_context / "goldfish_io" / "goldfish"
            if GOLDFISH_VALIDATION_PATH.exists():
                shutil.copy2(GOLDFISH_VALIDATION_PATH, goldfish_pkg_dest / "validation.py")
            if GOLDFISH_ERRORS_PATH.exists():
                shutil.copy2(GOLDFISH_ERRORS_PATH, goldfish_pkg_dest / "errors.py")
            if GOLDFISH_UTILS_PATH.exists() and GOLDFISH_UTILS_PATH.is_dir():
                shutil.copytree(
                    GOLDFISH_UTILS_PATH,
                    goldfish_pkg_dest / "utils",
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
                )

            # Generate Dockerfile in build context (not workspace)
            dockerfile_content = self.generate_dockerfile(workspace_dir, base_image=base_image)
            dockerfile_path = build_context / "Dockerfile"
            dockerfile_path.write_text(dockerfile_content)

            # Build image; force amd64 only when targeting GCE
            build_cmd = ["docker", "build"]
            backend = getattr(getattr(self.config, "jobs", None), "backend", None) if self.config else None
            if backend == "gce":
                build_cmd += ["--platform", "linux/amd64"]
            build_cmd += ["-t", image_tag]

            # Pass version as build arg to bust cache on version change
            build_cmd += ["--build-arg", f"VERSION={version}"]

            if not use_cache:
                build_cmd.append("--no-cache")

            build_cmd.append(str(build_context))

            try:
                result = subprocess.run(build_cmd, capture_output=True, text=True, check=False)

                if result.returncode != 0:
                    raise GoldfishError(f"Docker build failed: {result.stderr}")

                return image_tag

            except FileNotFoundError as err:
                raise GoldfishError("Docker not found. Please install Docker to build images.") from err

    def push_image(self, local_tag: str, registry_url: str, workspace_name: str, version: str) -> str:
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
                f"Invalid artifact_registry URL: {registry_url}. Expected format: us-docker.pkg.dev/<project>/<repo>"
            )

        # Generate sanitized image name
        sanitized_workspace = re.sub(r"[^a-z0-9._-]", "_", workspace_name.lower())
        sanitized_version = re.sub(r"[^a-z0-9._-]", "_", version.lower())
        image_name = f"goldfish-{sanitized_workspace}-{sanitized_version}"

        # Build full registry tag
        registry_tag = f"{registry_url}/{image_name}"

        try:
            # Configure Docker authentication with gcloud (idempotent but always validated)
            registry_domain = registry_url.split("/")[0]
            if not shutil.which("gcloud"):
                raise GoldfishError("gcloud not found; configure gcloud before pushing images.")

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
                ["docker", "tag", local_tag, registry_tag], capture_output=True, text=True, check=False
            )

            if tag_result.returncode != 0:
                raise GoldfishError(f"Docker tag failed: {tag_result.stderr}")

            # Push to registry
            push_result = subprocess.run(["docker", "push", registry_tag], capture_output=True, text=True, check=False)

            if push_result.returncode != 0:
                raise GoldfishError(f"Docker push failed: {push_result.stderr}")

            return registry_tag

        except FileNotFoundError as err:
            raise GoldfishError("Docker not found. Please install Docker to push images.") from err

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
        sanitized_workspace = re.sub(r"[^a-z0-9._-]", "_", workspace_name.lower())

        # SECURITY: Sanitize version to prevent command injection
        # Docker tags allow: [a-z0-9._-]
        sanitized_version = re.sub(r"[^a-z0-9._-]", "_", version.lower())

        return f"goldfish-{sanitized_workspace}-{sanitized_version}"
