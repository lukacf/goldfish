"""Docker image building for Goldfish stage execution."""

from __future__ import annotations

import hashlib
import logging
import re
import shutil
import subprocess
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from goldfish.errors import GoldfishError

if TYPE_CHECKING:
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)

# Paths to goldfish runtime modules (relative to this file)
GOLDFISH_IO_PATH = Path(__file__).parent.parent / "io" / "__init__.py"
GOLDFISH_METRICS_PATH = Path(__file__).parent.parent / "metrics"
GOLDFISH_SVS_PATH = Path(__file__).parent.parent / "svs"
GOLDFISH_CLOUD_PATH = Path(__file__).parent.parent / "cloud"
# Metrics and SVS depend on validation, errors, and utils modules
GOLDFISH_VALIDATION_PATH = Path(__file__).parent.parent / "validation.py"
GOLDFISH_ERRORS_PATH = Path(__file__).parent.parent / "errors.py"
GOLDFISH_UTILS_PATH = Path(__file__).parent.parent / "utils"
GOLDFISH_CONFIG_PATH = Path(__file__).parent.parent / "config.py"
GOLDFISH_RUST_PATH = Path(__file__).parent.parent.parent.parent / "goldfish-rust"

# Default base image when none specified (backwards compatibility)
DEFAULT_BASE_IMAGE = "python:3.11-slim"


class DockerBuilder:
    """Build Docker images for stage execution.

    Uses pre-built base images with common ML libraries. If a workspace has
    a requirements.txt, those are installed on top of the base image for
    project-specific dependencies.
    """

    def __init__(self, config: object | None = None, db: Database | None = None):
        # Store config for backend checks (may be GoldfishConfig or partial)
        self.config = config
        # Database for tracking Cloud Build workspace builds
        self.db = db

    def _get_agent_cli_packages(self) -> list[str]:
        """Return CLI packages to install based on SVS config."""
        svs = getattr(self.config, "svs", None) if self.config else None
        if not svs or not getattr(svs, "enabled", False):
            return []
        if not getattr(svs, "ai_post_run_enabled", False):
            return []

        provider = getattr(svs, "agent_provider", None)
        if not isinstance(provider, str):
            return []
        provider_map = {
            "codex_cli": "@openai/codex",
            "gemini_cli": "@google/gemini-cli",
        }
        package = provider_map.get(provider)
        return [package] if package else []

    def _render_agent_install_block(self, packages: list[str], is_nonroot_image: bool) -> str:
        """Render Dockerfile block to install agent CLI packages."""
        if not packages:
            return ""

        npm_packages = " ".join(packages)
        header = "# Install SVS agent CLI (Node + npm)\n"
        user_prefix = "USER root\n" if is_nonroot_image else ""
        user_suffix = "USER 1000\n" if is_nonroot_image else ""
        return (
            f"{header}{user_prefix}"
            "RUN if ! command -v npm >/dev/null 2>&1; then \\\n"
            "      if command -v apt-get >/dev/null 2>&1; then \\\n"
            "        apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && \\\n"
            "        curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \\\n"
            "        apt-get install -y --no-install-recommends nodejs && \\\n"
            "        rm -rf /var/lib/apt/lists/*; \\\n"
            "      elif command -v apk >/dev/null 2>&1; then \\\n"
            "        apk add --no-cache nodejs npm; \\\n"
            "      else \\\n"
            '        echo "No supported package manager for Node.js install" >&2; exit 1; \\\n'
            "      fi; \\\n"
            "    fi && npm install -g "
            f"{npm_packages}\n"
            f"{user_suffix}\n"
        )

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
        has_entrypoints = (workspace_dir / "entrypoints").exists()
        has_goldfish_rust = GOLDFISH_RUST_PATH.exists()

        # Detect image type to determine user handling
        # - Jupyter images (quay.io/jupyter/*) run as non-root user (jovyan, uid 1000)
        # - Goldfish custom images (goldfish-base-*) run as non-root user (goldfish, uid 1000)
        # - NVIDIA NGC images (nvcr.io/nvidia/*) run as root
        # - Other images default to root-compatible mode
        is_nonroot_image = "jupyter" in base.lower() or "goldfish-base" in base.lower()
        agent_cli_packages = self._get_agent_cli_packages()

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

        # Install agent CLI packages when SVS post-run reviews are enabled
        dockerfile += self._render_agent_install_block(agent_cli_packages, is_nonroot_image)

        # Always use --chown=1000:100 for local execution compatibility
        # The local executor runs containers as --user 1000:1000 for security
        # Root users can still read files owned by uid 1000, so this works for all images
        #
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
        if has_entrypoints:
            dockerfile += "COPY --chown=1000:100 entrypoints/ /app/entrypoints/\n"
        if has_goldfish_rust:
            # Copy Cargo manifests first for dependency caching, then source
            # This allows cargo to cache dependencies when only source changes
            dockerfile += """
# Cargo dependency caching: copy manifests first, build deps, then copy source
COPY --chown=1000:100 goldfish-rust/Cargo.toml goldfish-rust/Cargo.lock /app/goldfish-rust/
RUN mkdir -p /app/goldfish-rust/src && echo '// placeholder for dependency caching' > /app/goldfish-rust/src/lib.rs
RUN if command -v cargo >/dev/null 2>&1; then \\
      cd /app/goldfish-rust && cargo fetch 2>/dev/null || true; \\
    fi
COPY --chown=1000:100 goldfish-rust/src/ /app/goldfish-rust/src/
"""

        dockerfile += """
WORKDIR /app

# Entrypoint will be overridden at runtime
CMD ["/bin/bash"]
"""

        return dockerfile

    @contextmanager
    def prepare_build_context(
        self,
        workspace_dir: Path,
        workspace_name: str,
        version: str,
        base_image: str | None = None,
    ) -> Generator[tuple[Path, Path, str], None, None]:
        """Prepare build context for external image building.

        This context manager sets up the build context directory with all necessary
        files (workspace code, goldfish libraries, Dockerfile) and yields the paths.
        The caller is responsible for the actual build (e.g., using ImageBuilder).

        Usage with CloudBuildImageBuilder:
            with docker_builder.prepare_build_context(ws_dir, ws_name, ver, base_img) as (ctx, df, tag):
                image_builder.build(ctx, df, tag)

        Args:
            workspace_dir: Path to workspace directory
            workspace_name: Workspace name
            version: Version identifier
            base_image: Pre-built base image to use

        Yields:
            Tuple of (context_path, dockerfile_path, image_tag)
        """
        # Generate image tag
        image_tag = self._generate_image_tag(workspace_name, version)

        # Create temporary build context
        with tempfile.TemporaryDirectory(prefix="goldfish-docker-") as tmp_dir:
            build_context = Path(tmp_dir)

            # Copy workspace files to build context
            self._prepare_build_context(workspace_dir, build_context)

            # Generate and write Dockerfile
            dockerfile_content = self.generate_dockerfile(workspace_dir, base_image=base_image)
            dockerfile_path = build_context / "Dockerfile"
            dockerfile_path.write_text(dockerfile_content)

            yield build_context, dockerfile_path, image_tag

    def build_image(
        self,
        workspace_dir: Path,
        workspace_name: str,
        version: str,
        use_cache: bool = True,
        base_image: str | None = None,
    ) -> str:
        """Build Docker image for workspace using local Docker.

        Uses a temporary directory as build context to avoid dirtying
        the workspace with a Dockerfile.

        For Cloud Build support, use prepare_build_context() with ImageBuilder:
            with docker_builder.prepare_build_context(ws_dir, ws, ver, base) as (ctx, df, tag):
                image_builder = factory.create_image_builder()  # CloudBuildImageBuilder for GCE
                image_builder.build(ctx, df, tag)

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
        with self.prepare_build_context(workspace_dir, workspace_name, version, base_image) as (
            build_context,
            dockerfile_path,
            image_tag,
        ):
            # Build image
            build_cmd = ["docker", "build"]
            build_cmd += ["-t", image_tag, "-f", str(dockerfile_path)]

            # Pass version as build arg to bust cache on version change
            build_cmd += ["--build-arg", f"VERSION={version}"]

            if not use_cache:
                build_cmd.append("--no-cache")

            build_cmd.append(str(build_context))

            try:
                # Set a 15-minute timeout for the build process to avoid hanging the daemon
                # if Docker Desktop stalls or network is extremely slow.
                timeout_sec = 15 * 60
                result = subprocess.run(build_cmd, capture_output=True, text=True, check=False, timeout=timeout_sec)

                if result.returncode != 0:
                    # Capture last few lines of log to provide more context than just the header
                    lines = (result.stderr or "").splitlines()
                    tail = "\n".join(lines[-20:]) if len(lines) > 20 else result.stderr
                    raise GoldfishError(f"Docker build failed (last 20 lines):\n{tail}")

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
                f"Invalid artifact_registry URL: {registry_url}. Expected format: <region>-docker.pkg.dev/<project>/<repo>"
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
            push_result = subprocess.run(
                ["docker", "push", registry_tag],
                capture_output=True,
                text=True,
                check=False,
                timeout=15 * 60,
            )

            if push_result.returncode != 0:
                raise GoldfishError(f"Docker push failed: {push_result.stderr}")

            return registry_tag

        except FileNotFoundError as err:
            raise GoldfishError("Docker not found. Please install Docker to push images.") from err

    def remove_image(self, image_tag: str) -> None:
        """Remove local Docker image.

        Args:
            image_tag: Tag of the image to remove
        """
        try:
            subprocess.run(["docker", "rmi", image_tag], capture_output=True, check=False)
        except Exception as e:
            logger.debug(f"Failed to remove image {image_tag}: {e}")

    def _prepare_build_context(self, workspace_dir: Path, build_context: Path) -> None:
        """Copy workspace files to build context.

        Args:
            workspace_dir: Source workspace directory
            build_context: Destination build context directory
        """
        # Copy required workspace files to build context
        if (workspace_dir / "requirements.txt").exists():
            shutil.copy2(workspace_dir / "requirements.txt", build_context / "requirements.txt")

        if (workspace_dir / "modules").exists():
            shutil.copytree(workspace_dir / "modules", build_context / "modules")

        if (workspace_dir / "configs").exists():
            shutil.copytree(workspace_dir / "configs", build_context / "configs")

        if (workspace_dir / "loaders").exists():
            shutil.copytree(workspace_dir / "loaders", build_context / "loaders")

        if (workspace_dir / "entrypoints").exists():
            shutil.copytree(workspace_dir / "entrypoints", build_context / "entrypoints")

        if GOLDFISH_RUST_PATH.exists():
            shutil.copytree(
                GOLDFISH_RUST_PATH,
                build_context / "goldfish-rust",
                ignore=shutil.ignore_patterns("target", ".git", "__pycache__"),
            )

        # Copy goldfish.io module into build context
        goldfish_pkg_dest = build_context / "goldfish_io" / "goldfish"
        goldfish_pkg_dest.mkdir(parents=True, exist_ok=True)
        (goldfish_pkg_dest / "__init__.py").write_text('"""Goldfish ML package (container runtime)."""\n')
        (build_context / "goldfish_io" / "__init__.py").write_text("")

        # Copy goldfish sub-packages
        for subpkg, path in [
            ("io", GOLDFISH_IO_PATH.parent),
            ("metrics", GOLDFISH_METRICS_PATH),
            ("svs", GOLDFISH_SVS_PATH),
            ("utils", GOLDFISH_UTILS_PATH),
            ("cloud", GOLDFISH_CLOUD_PATH),
        ]:
            if path.exists() and path.is_dir():
                dest = goldfish_pkg_dest / subpkg
                shutil.copytree(
                    path,
                    dest,
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
                )
            elif path.exists() and path.is_file() and subpkg == "io":
                dest = goldfish_pkg_dest / "io"
                dest.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, dest / "__init__.py")

        # Copy goldfish.validation, goldfish.errors, and goldfish.config (top-level modules)
        if GOLDFISH_VALIDATION_PATH.exists():
            shutil.copy2(GOLDFISH_VALIDATION_PATH, goldfish_pkg_dest / "validation.py")
        if GOLDFISH_ERRORS_PATH.exists():
            shutil.copy2(GOLDFISH_ERRORS_PATH, goldfish_pkg_dest / "errors.py")
        if GOLDFISH_CONFIG_PATH.exists():
            shutil.copy2(GOLDFISH_CONFIG_PATH, goldfish_pkg_dest / "config.py")

    def _get_previous_version_tag(self, workspace_name: str, _version: str) -> str | None:
        """Get any previous version's registry tag for Docker layer caching.

        For effective caching, we want the most recent successful build
        for this workspace, regardless of which version it was built for.
        Docker layers are workspace-specific, not version-specific.

        Args:
            workspace_name: Workspace name
            _version: Current version (unused - kept for API compatibility)

        Returns:
            Previous version's registry tag if found, None otherwise
        """
        if not self.db:
            return None

        # Get the most recent completed build for this workspace (any version)
        # This enables cache-from to work across version boundaries
        prev_build = self.db.get_latest_docker_build_for_workspace(workspace_name)
        if prev_build:
            return prev_build.get("registry_tag")

        return None

    def _get_image_type_from_base(self, base_image: str | None) -> str:
        """Determine image type (cpu/gpu) from base image.

        Args:
            base_image: Base image name

        Returns:
            "gpu" if base image contains "gpu", else "cpu"
        """
        if base_image and "gpu" in base_image.lower():
            return "gpu"
        return "cpu"

    def _compute_content_hash(self, build_context: Path, dockerfile_content: str, base_image: str | None) -> str:
        """Compute SHA256 hash of the build context for cache detection.

        The hash includes:
        - All files in the build context (sorted for determinism)
        - The Dockerfile content
        - The base image tag (different base = different hash)

        Args:
            build_context: Path to the build context directory
            dockerfile_content: Generated Dockerfile content
            base_image: Base image tag

        Returns:
            SHA256 hex digest
        """
        hasher = hashlib.sha256()

        # Include base image in hash (different base image = different hash)
        hasher.update(f"base:{base_image or 'default'}\n".encode())

        # Include Dockerfile content
        hasher.update(f"dockerfile:{dockerfile_content}\n".encode())

        # Include all files in build context (sorted for determinism)
        all_files = sorted(build_context.rglob("*"))
        for file_path in all_files:
            if file_path.is_file():
                # Include relative path and content
                rel_path = file_path.relative_to(build_context)
                hasher.update(f"file:{rel_path}\n".encode())
                try:
                    hasher.update(file_path.read_bytes())
                except OSError:
                    # Skip unreadable files
                    pass

        return hasher.hexdigest()

    def _image_exists_in_registry(self, registry_tag: str, project_id: str) -> bool:
        """Check if an image already exists in Artifact Registry.

        Args:
            registry_tag: Full registry tag (e.g., "us-docker.pkg.dev/proj/repo/image:tag")
            project_id: GCP project ID

        Returns:
            True if image exists, False otherwise
        """
        try:
            result = subprocess.run(
                [
                    "gcloud",
                    "artifacts",
                    "docker",
                    "images",
                    "describe",
                    registry_tag,
                    "--project",
                    project_id,
                    "--format=json",
                ],
                capture_output=True,
                text=True,
                check=False,
                timeout=30,  # Quick timeout for describe
            )
            # returncode 0 means image exists
            return result.returncode == 0
        except Exception as e:
            # On any error (timeout, gcloud not found, etc.), assume image doesn't exist
            logger.debug("Image existence check failed: %s", e)
            return False

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
