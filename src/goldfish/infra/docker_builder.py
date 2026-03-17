"""Docker image building for Goldfish stage execution."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import subprocess
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
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
GOLDFISH_CONFIG_PATH = Path(__file__).parent.parent / "config"
GOLDFISH_RUST_PATH = Path(__file__).parent.parent.parent.parent / "goldfish-rust"

# Default base image when none specified (backwards compatibility)
DEFAULT_BASE_IMAGE = "python:3.11-slim"

_SVS_AGENT_CLI_PACKAGES_BUILD_ARG = "SVS_AGENT_CLI_PACKAGES"

_SECRET_BUILD_ARG_KEY_RE = re.compile(r"(api[_-]?key|token|secret|password)", re.IGNORECASE)
_SECRET_BUILD_ARG_VALUE_RES = [
    re.compile(r"(?i)^bearer\s+[A-Za-z0-9._-]+$"),
    re.compile(r"sk-[a-zA-Z0-9]{20,}"),
    re.compile(r"ghp_[A-Za-z0-9]{36}"),
]


def _normalize_build_arg_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _validate_build_args(build_args: dict[str, str]) -> None:
    for key, value in build_args.items():
        if _normalize_build_arg_name(key) == "buildcontexthash":
            raise GoldfishError(
                "BuildContext.build_args MUST NOT include build_context_hash (circular dependency in build hash computation)."
            )

        if _SECRET_BUILD_ARG_KEY_RE.search(key):
            raise GoldfishError(f"BuildContext.build_args MUST NOT contain secrets: forbidden build arg name '{key}'.")

        for pattern in _SECRET_BUILD_ARG_VALUE_RES:
            if pattern.search(value):
                raise GoldfishError(
                    f"BuildContext.build_args MUST NOT contain secrets: build arg '{key}' value looks like a secret."
                )


@dataclass(frozen=True, slots=True)
class BuildContext:
    """All inputs that affect Docker image generation."""

    dockerfile_hash: str
    git_sha: str
    goldfish_runtime_hash: str
    base_image: str
    base_image_digest: str | None
    requirements_hash: str
    build_args: dict[str, str]

    def __post_init__(self) -> None:
        _validate_build_args(self.build_args)


def compute_build_context_hash(build_context: BuildContext) -> str:
    """Compute deterministic build context hash for Docker image cache keying.

    Returns:
        Full 64-character SHA256 hex digest. A short prefix (e.g. [:16]) may be
        used for image tags/display only.
    """
    payload = {
        "dockerfile_hash": build_context.dockerfile_hash,
        "git_sha": build_context.git_sha,
        "goldfish_runtime_hash": build_context.goldfish_runtime_hash,
        "base_image_digest": build_context.base_image_digest or build_context.base_image,
        "requirements_hash": build_context.requirements_hash,
        "build_args": sorted(build_context.build_args.items()),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def compute_goldfish_runtime_hash(goldfish_root: Path | None = None) -> str:
    """Compute a deterministic hash of Goldfish runtime files copied into images."""
    root = goldfish_root or (Path(__file__).parent.parent)

    include_paths = [
        root / "io",
        root / "svs",
        root / "metrics",
        root / "utils",
        root / "cloud",
        root / "validation.py",
        root / "errors.py",
        root / "config",
    ]

    files: list[tuple[str, Path]] = []
    for path in include_paths:
        if path.is_dir():
            for candidate in path.rglob("*"):
                if not candidate.is_file():
                    continue
                rel_parts = candidate.relative_to(path).parts
                if "__pycache__" in rel_parts or candidate.suffix == ".pyc":
                    continue
                files.append((candidate.relative_to(root).as_posix(), candidate))
        else:
            files.append((path.relative_to(root).as_posix(), path))

    repo_root = root.parent.parent
    rust_root = repo_root / "goldfish-rust"
    if rust_root.exists():
        rust_include_paths = [
            rust_root / "Cargo.toml",
            rust_root / "Cargo.lock",
            rust_root / "src",
        ]
        for path in rust_include_paths:
            if path.is_dir():
                for candidate in path.rglob("*"):
                    if not candidate.is_file():
                        continue
                    rel_parts = candidate.relative_to(path).parts
                    if "__pycache__" in rel_parts or candidate.suffix == ".pyc":
                        continue
                    rel = candidate.relative_to(rust_root).as_posix()
                    files.append((f"goldfish-rust/{rel}", candidate))
            elif path.exists():
                rel = path.relative_to(rust_root).as_posix()
                files.append((f"goldfish-rust/{rel}", path))

    hasher = hashlib.sha256()
    for rel, file_path in sorted(files, key=lambda item: item[0]):
        hasher.update(f"path:{rel}\n".encode())
        hasher.update(file_path.read_bytes())

    return hasher.hexdigest()


def compute_requirements_hash(workspace_dir: Path) -> str:
    """Compute deterministic requirements.txt hash.

    If requirements.txt is missing, uses the hash of empty string.
    """
    requirements_path = workspace_dir / "requirements.txt"
    try:
        contents = requirements_path.read_bytes()
    except FileNotFoundError:
        contents = b""
    return hashlib.sha256(contents).hexdigest()


def _find_unpinned_requirements(requirements_text: str) -> list[str]:
    unpinned: list[str] = []
    for raw_line in requirements_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        if "==" in line or "@" in line:
            continue
        if ">=" in line or not any(op in line for op in ("==", ">=", "<=", "!=", "~=", "<", ">")):
            unpinned.append(line)
    return unpinned


def resolve_base_image_digest(base_image: str) -> str | None:
    """Resolve a base image tag to its digest via registry tooling.

    For Artifact Registry images (``*.pkg.dev``), this uses ``gcloud artifacts docker images describe``
    to return a digest like ``sha256:...``. If resolution fails, returns ``None`` and logs a warning.
    """
    if "pkg.dev" not in base_image:
        return None

    try:
        result = subprocess.run(
            ["gcloud", "artifacts", "docker", "images", "describe", base_image, "--format=json"],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except FileNotFoundError:
        logger.warning("Base image digest resolution skipped (gcloud not found): %s", base_image)
        return None
    except Exception as e:
        logger.warning("Base image digest resolution failed: %s (%s)", base_image, e)
        return None

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        logger.warning("Base image digest resolution failed: %s (%s)", base_image, stderr or "unknown error")
        return None

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        logger.warning("Base image digest resolution returned invalid JSON: %s", base_image)
        return None

    digest = (
        (data.get("image_summary") or {}).get("digest")
        or (data.get("imageSummary") or {}).get("digest")
        or data.get("digest")
    )
    if isinstance(digest, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        return digest

    # Fallback: try extracting any digest-looking value from output.
    match = re.search(r"sha256:[0-9a-f]{64}", result.stdout)
    if match:
        return match.group(0)

    logger.warning("Base image digest resolution succeeded but digest not found: %s", base_image)
    return None


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

    def _get_svs_provider_name(self) -> str | None:
        """Return the active SVS agent provider name, or None if SVS reviews are disabled."""
        svs = getattr(self.config, "svs", None) if self.config else None
        if not svs or not getattr(svs, "enabled", False):
            return None

        during_run_enabled = getattr(svs, "ai_during_run_enabled", False)
        post_run_enabled = getattr(svs, "ai_post_run_enabled", False)
        if not (during_run_enabled or post_run_enabled):
            return None

        provider = getattr(svs, "agent_provider", None)
        if not isinstance(provider, str):
            return None
        return provider

    def _get_agent_cli_packages(self) -> list[str]:
        """Return CLI packages to install based on SVS config.

        The anthropic_api provider uses claude-agent-sdk which requires the Claude
        Code CLI. On Linux, the SDK's pure Python wheel (no bundled CLI) is installed,
        so we must install the CLI separately via npm.
        """
        provider = self._get_svs_provider_name()
        if not provider:
            return []

        # Meerkat uses a Rust binary (rkat-rpc), not an npm package.
        # It's handled separately via _render_meerkat_install_block().
        if provider == "meerkat":
            return []

        # Map SVS agent provider names to npm package names
        # anthropic_api: Uses claude-agent-sdk which wraps Claude Code CLI
        #                SDK on Linux has no bundled CLI, so we install it here
        provider_map = {
            "anthropic_api": "@anthropic-ai/claude-code",
            "codex_cli": "@openai/codex",
            "gemini_cli": "@google/gemini-cli",
        }
        package = provider_map.get(provider)
        return [package] if package else []

    def _is_meerkat_provider(self) -> bool:
        """Check if the configured SVS agent provider is meerkat."""
        return self._get_svs_provider_name() == "meerkat"

    # Pin rkat-rpc version for reproducible Docker builds.
    # Bump this when upgrading meerkat-sdk in pyproject.toml.
    RKAT_RPC_VERSION = "v0.4.12"

    # GitHub repo hosting rkat-rpc release artifacts.
    RKAT_REPO = "lukacf/meerkat"

    def _render_meerkat_install_block(self, is_nonroot_image: bool) -> str:
        """Render Dockerfile block to install rkat-rpc binary for Meerkat SDK."""
        header = "# Install Meerkat rkat-rpc binary\n"
        user_prefix = "USER root\n" if is_nonroot_image else ""
        user_suffix = "USER 1000\n" if is_nonroot_image else ""
        version = self.RKAT_RPC_VERSION  # e.g. "v0.4.0"
        semver = version.lstrip("v")  # e.g. "0.4.0"
        repo = self.RKAT_REPO
        return (
            f"{header}{user_prefix}"
            "# Install meerkat-sdk Python package and rkat-rpc binary\n"
            f"RUN pip install --no-cache-dir 'meerkat-sdk>={semver}'\n"
            "# Detect architecture at build time using uname (works with standard docker build and buildx)\n"
            "RUN MACHINE=$(uname -m) && \\\n"
            '    case "${MACHINE}" in x86_64) ARCH=x86_64-unknown-linux-gnu;; aarch64) ARCH=aarch64-unknown-linux-gnu;; *) ARCH=${MACHINE};; esac && \\\n'
            f'    curl -fsSL -o /tmp/rkat-rpc.tar.gz "https://github.com/{repo}/releases/download/{version}/rkat-rpc-{semver}-${{ARCH}}.tar.gz" && \\\n'
            "    tar xzf /tmp/rkat-rpc.tar.gz -C /usr/local/bin/ rkat-rpc && \\\n"
            "    chmod +x /usr/local/bin/rkat-rpc && \\\n"
            "    rm /tmp/rkat-rpc.tar.gz\n"
            f"{user_suffix}\n"
        )

    def _render_agent_install_block(self, is_nonroot_image: bool) -> str:
        """Render Dockerfile block to install agent CLI packages."""
        header = "# Install SVS agent CLI (Node + npm)\n"
        user_prefix = "USER root\n" if is_nonroot_image else ""
        user_suffix = "USER 1000\n" if is_nonroot_image else ""
        return (
            f'{header}ARG {_SVS_AGENT_CLI_PACKAGES_BUILD_ARG}=""\n{user_prefix}'
            f'RUN if [ -n "${{{_SVS_AGENT_CLI_PACKAGES_BUILD_ARG}}}" ]; then \\\n'
            "      if ! command -v npm >/dev/null 2>&1; then \\\n"
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
            f"    fi && npm install -g ${{{_SVS_AGENT_CLI_PACKAGES_BUILD_ARG}}}; \\\n"
            "    fi\n"
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
        use_meerkat = self._is_meerkat_provider()

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
        if agent_cli_packages:
            dockerfile += self._render_agent_install_block(is_nonroot_image)

        # Install rkat-rpc binary when Meerkat is the SVS agent provider
        if use_meerkat:
            dockerfile += self._render_meerkat_install_block(is_nonroot_image)

        # Always use --chown=1000:100 for local execution compatibility
        # The local executor runs containers as --user 1000:1000 for security
        # Root users can still read files owned by uid 1000, so this works for all images
        #
        # VERSION arg busts cache when workspace version changes
        #
        # For non-root images: agent/meerkat install ends with USER 1000, but chown requires root
        if is_nonroot_image and (agent_cli_packages or use_meerkat):
            dockerfile += """# Cache-bust: version changes invalidate subsequent layers
ARG VERSION
RUN echo "Building version: ${VERSION}"

# Create /app owned by user 1000 (required so tools like Claude CLI can write to $HOME=/app)
USER root
RUN mkdir -p /app && chown 1000:100 /app
USER 1000

# Install Goldfish IO library
COPY --chown=1000:100 goldfish_io/ /app/goldfish_io/
ENV PYTHONPATH="/app/goldfish_io:/app/modules:/app:${PYTHONPATH}"

# Copy workspace code
COPY --chown=1000:100 modules/ /app/modules/
COPY --chown=1000:100 configs/ /app/configs/
"""
        else:
            dockerfile += """# Cache-bust: version changes invalidate subsequent layers
ARG VERSION
RUN echo "Building version: ${VERSION}"

# Create /app owned by user 1000 (required so tools like Claude CLI can write to $HOME=/app)
RUN mkdir -p /app && chown 1000:100 /app

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

        # Capture pip freeze as root (non-root images switch to USER 1000 after
        # agent install, so /app may not be writable by that user)
        if is_nonroot_image:
            dockerfile += """
# Capture resolved Python environment for reproducibility audits
USER root
RUN python -m pip freeze > /app/pip-freeze.txt && chown 1000:100 /app/pip-freeze.txt
USER 1000
"""
        else:
            dockerfile += """
# Capture resolved Python environment for reproducibility audits
RUN python -m pip freeze > /app/pip-freeze.txt
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
    ) -> Generator[tuple[BuildContext, Path, Path, str], None, None]:
        """Prepare build context for external image building.

        This context manager sets up the build context directory with all necessary
        files (workspace code, goldfish libraries, Dockerfile) and yields the paths.
        The caller is responsible for the actual build (e.g., using ImageBuilder).

        Usage with CloudBuildImageBuilder:
            with docker_builder.prepare_build_context(ws_dir, ws_name, ver, base_img) as (build_ctx, ctx, df, tag):
                image_builder.build(ctx, df, tag)

        Args:
            workspace_dir: Path to workspace directory
            workspace_name: Workspace name
            version: Version identifier
            base_image: Pre-built base image to use

        Yields:
            Tuple of (BuildContext, context_path, dockerfile_path, image_tag)
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

            requirements_path = workspace_dir / "requirements.txt"
            if requirements_path.exists():
                try:
                    unpinned = _find_unpinned_requirements(requirements_path.read_text())
                except Exception:
                    unpinned = []
                if unpinned:
                    sample = ", ".join(unpinned[:5])
                    suffix = "..." if len(unpinned) > 5 else ""
                    logger.warning(
                        "requirements.txt contains unpinned dependencies (>= or no version specifier): %s%s",
                        sample,
                        suffix,
                    )

            dockerfile_hash = hashlib.sha256(dockerfile_content.encode("utf-8")).hexdigest()
            git_sha = ""
            if self.db:
                version_row = self.db.get_version(workspace_name, version)
                if version_row and version_row.get("git_sha"):
                    git_sha = str(version_row["git_sha"])
            if not git_sha:
                git_sha = version

            agent_cli_packages = self._get_agent_cli_packages()
            build_args = {
                "VERSION": git_sha,
                _SVS_AGENT_CLI_PACKAGES_BUILD_ARG: " ".join(agent_cli_packages),
            }

            build_ctx = BuildContext(
                dockerfile_hash=dockerfile_hash,
                git_sha=git_sha,
                goldfish_runtime_hash=compute_goldfish_runtime_hash(),
                base_image=base_image or DEFAULT_BASE_IMAGE,
                base_image_digest=resolve_base_image_digest(base_image or DEFAULT_BASE_IMAGE),
                requirements_hash=compute_requirements_hash(workspace_dir),
                build_args=build_args,
            )
            yield build_ctx, build_context, dockerfile_path, image_tag

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
            with docker_builder.prepare_build_context(ws_dir, ws, ver, base) as (build_ctx, ctx, df, tag):
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
        image_tag, _ = self.build_image_with_context_hash(
            workspace_dir=workspace_dir,
            workspace_name=workspace_name,
            version=version,
            use_cache=use_cache,
            base_image=base_image,
        )
        return image_tag

    def build_image_with_context_hash(
        self,
        workspace_dir: Path,
        workspace_name: str,
        version: str,
        *,
        use_cache: bool = True,
        base_image: str | None = None,
    ) -> tuple[str, str]:
        """Build Docker image for workspace using local Docker, returning build_context_hash.

        This is a thin wrapper around :meth:`prepare_build_context` that computes the
        deterministic build context hash and returns it alongside the built image tag.

        Returns:
            Tuple of (image_tag, build_context_hash)
        """
        with self.prepare_build_context(workspace_dir, workspace_name, version, base_image) as (
            build_ctx,
            build_context,
            dockerfile_path,
            image_tag,
        ):
            build_context_hash = compute_build_context_hash(build_ctx)

            build_cmd = ["docker", "build"]
            build_cmd += ["-t", image_tag, "-f", str(dockerfile_path)]

            keys = sorted(build_ctx.build_args)
            if "VERSION" in build_ctx.build_args:
                keys.remove("VERSION")
                keys.insert(0, "VERSION")
            for key in keys:
                build_cmd += ["--build-arg", f"{key}={build_ctx.build_args[key]}"]

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

                return image_tag, build_context_hash

            except FileNotFoundError as err:
                raise GoldfishError("Docker not found. Please install Docker to build images.") from err

    def capture_pip_freeze_from_image(self, image_tag: str) -> str | None:
        """Capture pip freeze output from a built image (best-effort).

        Returns:
            The content of ``/app/pip-freeze.txt`` when available, otherwise None.
        """
        try:
            result = subprocess.run(
                ["docker", "run", "--rm", image_tag, "cat", "/app/pip-freeze.txt"],
                capture_output=True,
                text=True,
                check=False,
                timeout=60,
            )
        except FileNotFoundError:
            logger.warning("Docker not found; skipping pip freeze capture for %s", image_tag)
            return None
        except Exception as e:
            logger.warning("Failed to capture pip freeze for %s: %s", image_tag, e)
            return None

        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            logger.warning("Failed to capture pip freeze for %s: %s", image_tag, stderr or "unknown error")
            return None

        return result.stdout

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
            ("config", GOLDFISH_CONFIG_PATH),
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
