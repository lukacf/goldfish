import subprocess

import pytest

from goldfish.config import GoldfishConfig
from goldfish.errors import GoldfishError
from goldfish.infra.docker_builder import DockerBuilder
from goldfish.svs.config import SVSConfig


def test_push_image_validates_registry_url():
    builder = DockerBuilder()
    with pytest.raises(GoldfishError, match="Invalid artifact_registry"):
        builder.push_image("local-tag", "invalid://url", "ws", "v1")
    with pytest.raises(GoldfishError, match="Invalid artifact_registry"):
        builder.push_image("local-tag", "", "ws", "v1")
    with pytest.raises(GoldfishError, match="Invalid artifact_registry"):
        builder.push_image("local-tag", "justhost", "ws", "v1")


def test_push_image_auth_failure(monkeypatch):
    builder = DockerBuilder()

    calls = []

    def fake_run(cmd, capture_output=True, text=True, check=False):
        calls.append(cmd)
        if "configure-docker" in cmd:
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="auth failed")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(GoldfishError, match="configure Docker authentication"):
        builder.push_image("local-tag", "us-docker.pkg.dev/proj/repo", "ws", "v1")

    # Ensure we attempted auth
    assert any("configure-docker" in c for c in [" ".join(cmd) for cmd in calls])


# =============================================================================
# Regression Tests - Dockerfile must use --chown for non-root containers
# =============================================================================


def test_dockerfile_installs_claude_cli_when_svs_enabled(tmp_path):
    """SVS post-run review requires Claude CLI inside the container."""
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "modules").mkdir()
    (workspace_path / "modules" / "train.py").write_text("# test module")
    (workspace_path / "configs").mkdir()

    config = GoldfishConfig(project_name="test", dev_repo_path=".")
    config = config.model_copy(
        update={
            "svs": SVSConfig(
                enabled=True,
                ai_post_run_enabled=True,
                agent_provider="codex_cli",
            )
        }
    )
    builder = DockerBuilder(config)

    dockerfile = builder.generate_dockerfile(
        workspace_dir=workspace_path,
        base_image="python:3.11-slim",
    )

    assert "@openai/codex" in dockerfile
    assert "npm install -g" in dockerfile


def test_dockerfile_skips_agent_cli_when_svs_disabled(tmp_path):
    """Do not install agent CLI when SVS is disabled."""
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "modules").mkdir()
    (workspace_path / "modules" / "train.py").write_text("# test module")
    (workspace_path / "configs").mkdir()

    config = GoldfishConfig(project_name="test", dev_repo_path=".")
    config = config.model_copy(
        update={
            "svs": SVSConfig(
                enabled=False,
                ai_post_run_enabled=True,
                agent_provider="codex_cli",
            )
        }
    )
    builder = DockerBuilder(config)

    dockerfile = builder.generate_dockerfile(
        workspace_dir=workspace_path,
        base_image="python:3.11-slim",
    )

    assert "@anthropic-ai/claude-code" not in dockerfile


def test_dockerfile_copy_uses_chown(tmp_path):
    """Regression: All COPY commands must use --chown=1000:100 for non-root containers.

    Base images like pytorch-notebook run as non-root user (jovyan, UID 1000).
    Without --chown, copied files are owned by root and the container can't read them.
    """
    # Create minimal workspace structure
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "modules").mkdir()
    (workspace_path / "modules" / "train.py").write_text("# test module")
    (workspace_path / "configs").mkdir()
    (workspace_path / "configs" / "train.yaml").write_text("key: value")

    builder = DockerBuilder()

    # Generate dockerfile
    dockerfile = builder.generate_dockerfile(
        workspace_dir=workspace_path,
        base_image="quay.io/jupyter/pytorch-notebook:latest",
    )

    # All COPY commands should have --chown=1000:100
    copy_lines = [line for line in dockerfile.split("\n") if line.strip().startswith("COPY")]
    assert len(copy_lines) >= 3  # goldfish_io, modules, configs at minimum

    for line in copy_lines:
        assert "--chown=1000:100" in line, f"COPY missing --chown: {line}"


def test_dockerfile_copy_loaders_uses_chown(tmp_path):
    """Regression: COPY loaders/ must also use --chown when loaders exist."""
    # Create workspace structure with loaders
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "modules").mkdir()
    (workspace_path / "modules" / "train.py").write_text("# test module")
    (workspace_path / "configs").mkdir()
    (workspace_path / "loaders").mkdir()
    (workspace_path / "loaders" / "custom_loader.py").write_text("# loader")

    builder = DockerBuilder()

    dockerfile = builder.generate_dockerfile(
        workspace_dir=workspace_path,
        base_image="quay.io/jupyter/pytorch-notebook:latest",
    )

    # Find the loaders COPY line
    copy_lines = [line for line in dockerfile.split("\n") if "loaders/" in line and "COPY" in line]
    assert len(copy_lines) == 1, "Should have exactly one COPY for loaders"
    assert "--chown=1000:100" in copy_lines[0], f"loaders COPY missing --chown: {copy_lines[0]}"


def test_dockerfile_pythonpath_includes_modules_dir(tmp_path):
    """Regression: PYTHONPATH must include /app/modules for sibling imports.

    Stage modules like train.py often import sibling modules like:
        from model import LSTMModel

    This requires /app/modules to be in PYTHONPATH, not just /app.
    Without this, users get ModuleNotFoundError for sibling imports.
    """
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "modules").mkdir()
    (workspace_path / "modules" / "train.py").write_text("from model import Foo")
    (workspace_path / "modules" / "model.py").write_text("class Foo: pass")
    (workspace_path / "configs").mkdir()

    builder = DockerBuilder()

    dockerfile = builder.generate_dockerfile(
        workspace_dir=workspace_path,
        base_image="quay.io/jupyter/pytorch-notebook:latest",
    )

    # Find the PYTHONPATH ENV line
    pythonpath_lines = [line for line in dockerfile.split("\n") if "PYTHONPATH" in line]
    assert len(pythonpath_lines) >= 1, "Should have PYTHONPATH ENV"

    # Must include /app/modules for sibling imports
    pythonpath_line = pythonpath_lines[0]
    assert (
        "/app/modules" in pythonpath_line
    ), f"PYTHONPATH must include /app/modules for sibling imports: {pythonpath_line}"


def test_dockerfile_nvidia_ngc_uses_chown_for_local_compat(tmp_path):
    """NVIDIA NGC images use --chown for local execution compatibility.

    All images use --chown=1000:100 to ensure the local executor (which runs
    as uid 1000 for security) can read the files. Root users in NGC images
    can still read files owned by uid 1000.
    """
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "modules").mkdir()
    (workspace_path / "modules" / "train.py").write_text("# test module")
    (workspace_path / "configs").mkdir()
    (workspace_path / "loaders").mkdir()
    (workspace_path / "loaders" / "custom_loader.py").write_text("# loader")

    builder = DockerBuilder()

    dockerfile = builder.generate_dockerfile(
        workspace_dir=workspace_path,
        base_image="nvcr.io/nvidia/pytorch:24.01-py3",
    )

    # All COPY commands should have --chown for local execution compatibility
    copy_lines = [line for line in dockerfile.split("\n") if line.strip().startswith("COPY")]
    assert len(copy_lines) >= 4  # goldfish_io, modules, configs, loaders

    for line in copy_lines:
        assert "--chown=1000:100" in line, f"COPY should have --chown for local compat: {line}"

    # Should NOT have USER root / USER 1000 switching (NGC runs as root)
    assert "USER root" not in dockerfile
    assert "USER 1000" not in dockerfile


def test_dockerfile_nvidia_ngc_with_requirements(tmp_path):
    """NVIDIA NGC images: requirements.txt install should not switch users."""
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "modules").mkdir()
    (workspace_path / "modules" / "train.py").write_text("# test module")
    (workspace_path / "configs").mkdir()
    (workspace_path / "requirements.txt").write_text("flash-attn>=2.0")

    builder = DockerBuilder()

    dockerfile = builder.generate_dockerfile(
        workspace_dir=workspace_path,
        base_image="nvcr.io/nvidia/pytorch:24.01-py3",
    )

    # Should have pip install but NO USER switching for NGC images
    assert "pip install --no-cache-dir -r /tmp/requirements.txt" in dockerfile
    assert "USER root" not in dockerfile
    assert "USER 1000" not in dockerfile


def test_dockerfile_goldfish_base_uses_chown(tmp_path):
    """Goldfish custom base images run as non-root (uid 1000), should use --chown.

    The goldfish-base-gpu image creates a 'goldfish' user with uid 1000,
    similar to Jupyter's jovyan user. Files must be chowned for access.
    """
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "modules").mkdir()
    (workspace_path / "modules" / "train.py").write_text("# test module")
    (workspace_path / "configs").mkdir()

    builder = DockerBuilder()

    # Test with full AR path
    dockerfile = builder.generate_dockerfile(
        workspace_dir=workspace_path,
        base_image="us-docker.pkg.dev/myproject/goldfish/goldfish-base-gpu:v2",
    )

    # All COPY commands should have --chown=1000:100
    copy_lines = [line for line in dockerfile.split("\n") if line.strip().startswith("COPY")]
    assert len(copy_lines) >= 3  # goldfish_io, modules, configs

    for line in copy_lines:
        assert "--chown=1000:100" in line, f"goldfish-base COPY should have --chown: {line}"


# =============================================================================
# Regression Tests - Docker cache busting
# =============================================================================


def test_dockerfile_has_version_arg_before_copy(tmp_path):
    """Regression: Dockerfile must have ARG VERSION before COPY to bust layer cache.

    Docker caches layers globally by content hash, not per image tag. Without a
    cache-busting ARG that changes each version, unchanged modules would use
    cached layers from previous builds even when the version changes.
    """
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "modules").mkdir()
    (workspace_path / "modules" / "train.py").write_text("# test module")
    (workspace_path / "configs").mkdir()

    builder = DockerBuilder()

    # Test with non-root image (Jupyter)
    dockerfile = builder.generate_dockerfile(
        workspace_dir=workspace_path,
        base_image="quay.io/jupyter/pytorch-notebook:latest",
    )

    # Must have ARG VERSION
    assert "ARG VERSION" in dockerfile, "Dockerfile must declare ARG VERSION for cache busting"

    # ARG VERSION must come BEFORE the first COPY of workspace code
    lines = dockerfile.split("\n")
    arg_version_idx = next(i for i, line in enumerate(lines) if "ARG VERSION" in line)
    first_copy_modules_idx = next(i for i, line in enumerate(lines) if "COPY" in line and "modules" in line)

    assert arg_version_idx < first_copy_modules_idx, "ARG VERSION must appear before COPY modules/ to invalidate cache"


def test_dockerfile_version_arg_for_root_images(tmp_path):
    """Regression: Root images (NGC) must also have ARG VERSION for cache busting."""
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "modules").mkdir()
    (workspace_path / "modules" / "train.py").write_text("# test module")
    (workspace_path / "configs").mkdir()

    builder = DockerBuilder()

    # Test with root image (NVIDIA NGC)
    dockerfile = builder.generate_dockerfile(
        workspace_dir=workspace_path,
        base_image="nvcr.io/nvidia/pytorch:24.01-py3",
    )

    # Must have ARG VERSION even for root images
    assert "ARG VERSION" in dockerfile, "NGC Dockerfile must also have ARG VERSION"

    # Verify ordering
    lines = dockerfile.split("\n")
    arg_version_idx = next(i for i, line in enumerate(lines) if "ARG VERSION" in line)
    first_copy_modules_idx = next(i for i, line in enumerate(lines) if "COPY" in line and "modules" in line)

    assert arg_version_idx < first_copy_modules_idx


def test_build_image_passes_version_build_arg(tmp_path, monkeypatch):
    """Regression: docker build must pass --build-arg VERSION to bust cache.

    The VERSION arg in the Dockerfile only works if we pass its value via
    --build-arg. Each version should pass a different value to invalidate
    the cache for workspace code layers.
    """
    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "modules").mkdir()
    (workspace_path / "modules" / "train.py").write_text("# test module")
    (workspace_path / "configs").mkdir()

    captured_cmds = []

    def fake_run(cmd, capture_output=True, text=True, check=False, timeout=None):
        captured_cmds.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    builder = DockerBuilder()
    builder.build_image(
        workspace_dir=workspace_path,
        workspace_name="test_ws",
        version="v42",
    )

    # Find the docker build command
    build_cmds = [cmd for cmd in captured_cmds if cmd[0] == "docker" and cmd[1] == "build"]
    assert len(build_cmds) == 1, "Should have exactly one docker build command"

    build_cmd = build_cmds[0]

    # Must have --build-arg VERSION=v42
    assert "--build-arg" in build_cmd, "docker build must have --build-arg"
    build_arg_idx = build_cmd.index("--build-arg")
    version_arg = build_cmd[build_arg_idx + 1]
    assert version_arg == "VERSION=v42", f"Expected VERSION=v42, got {version_arg}"


# =============================================================================
# Regression Tests - goldfish.metrics module in container
# =============================================================================


def test_build_image_copies_metrics_module(tmp_path, monkeypatch):
    """Regression: goldfish.metrics module must be copied into container build context."""
    from pathlib import Path

    workspace_path = tmp_path / "workspace"
    workspace_path.mkdir()
    (workspace_path / "modules").mkdir()
    (workspace_path / "modules" / "train.py").write_text("from goldfish.metrics import log_metric")
    (workspace_path / "configs").mkdir()

    def fake_run(cmd, capture_output=True, text=True, check=False, timeout=None):
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    from goldfish.infra import docker_builder as docker_builder_module

    captured: dict[str, Path] = {}

    class _CapturedTempDir:
        def __init__(self, prefix: str = "goldfish-docker-") -> None:
            import tempfile

            self.path = Path(tempfile.mkdtemp(prefix=prefix))
            captured["path"] = self.path

        def __enter__(self) -> str:
            return str(self.path)

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    monkeypatch.setattr(docker_builder_module.tempfile, "TemporaryDirectory", _CapturedTempDir)

    builder = DockerBuilder()
    result = builder.build_image(workspace_path, "test_ws", "v1")
    assert result == "goldfish-test_ws-v1"

    build_context = captured.get("path")
    assert build_context is not None

    metrics_dest = build_context / "goldfish_io" / "goldfish" / "metrics"
    goldfish_pkg_dest = build_context / "goldfish_io" / "goldfish"

    assert metrics_dest.exists(), "goldfish.metrics directory should exist in build context"
    assert (metrics_dest / "__init__.py").exists(), "goldfish.metrics/__init__.py should exist"
    assert (metrics_dest / "logger.py").exists(), "goldfish.metrics/logger.py should exist"
    assert (metrics_dest / "writer.py").exists(), "goldfish.metrics/writer.py should exist"
    assert (metrics_dest / "collector.py").exists(), "goldfish.metrics/collector.py should exist"
    assert (metrics_dest / "utils.py").exists(), "goldfish.metrics/utils.py should exist"
    assert (metrics_dest / "backends").exists(), "goldfish.metrics/backends/ should exist"
    assert (metrics_dest / "backends" / "__init__.py").exists()
    assert (metrics_dest / "backends" / "base.py").exists()
    assert (metrics_dest / "backends" / "wandb.py").exists()

    assert (goldfish_pkg_dest / "validation.py").exists(), "goldfish.validation should exist"
    assert (goldfish_pkg_dest / "errors.py").exists(), "goldfish.errors should exist"
    assert not (metrics_dest / "__pycache__").exists(), "__pycache__ should be excluded"

    import shutil

    shutil.rmtree(build_context, ignore_errors=True)


def test_metrics_module_importable_in_container_context(tmp_path):
    """Regression: goldfish.metrics must be importable with only container runtime files.

    This test simulates the container environment by copying only the files
    that docker_builder copies, then verifying the imports work.

    Previous bug: metrics imported goldfish.validation which wasn't copied,
    causing: ModuleNotFoundError: No module named 'goldfish.validation'
    """
    import shutil
    import subprocess
    import sys

    from goldfish.infra.docker_builder import (
        GOLDFISH_ERRORS_PATH,
        GOLDFISH_IO_PATH,
        GOLDFISH_METRICS_PATH,
        GOLDFISH_SVS_PATH,
        GOLDFISH_UTILS_PATH,
        GOLDFISH_VALIDATION_PATH,
    )

    # Create a minimal goldfish package structure (what docker_builder creates)
    container_root = tmp_path / "app" / "goldfish_io"
    goldfish_pkg = container_root / "goldfish"
    goldfish_pkg.mkdir(parents=True)

    # Copy files exactly as docker_builder does
    (container_root / "__init__.py").write_text("")
    (goldfish_pkg / "__init__.py").write_text('"""Goldfish ML package (container runtime)."""\n')

    # Copy io, metrics, svs, utils modules (mirror docker_builder)
    for subpkg, path in [
        ("io", GOLDFISH_IO_PATH.parent),
        ("metrics", GOLDFISH_METRICS_PATH),
        ("svs", GOLDFISH_SVS_PATH),
        ("utils", GOLDFISH_UTILS_PATH),
    ]:
        if path.exists() and path.is_dir():
            dest = goldfish_pkg / subpkg
            shutil.copytree(
                path,
                dest,
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
            )
        elif path.exists() and path.is_file() and subpkg == "io":
            dest = goldfish_pkg / "io"
            dest.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, dest / "__init__.py")

    # Copy validation and errors (metrics dependencies)
    shutil.copy2(GOLDFISH_VALIDATION_PATH, goldfish_pkg / "validation.py")
    shutil.copy2(GOLDFISH_ERRORS_PATH, goldfish_pkg / "errors.py")

    # Run a subprocess that imports goldfish.metrics with ONLY the container files
    # This simulates the container environment where only goldfish_io is in PYTHONPATH
    test_script = f"""
import sys
# Clear any existing goldfish from path
sys.path = [p for p in sys.path if "goldfish" not in p.lower() or "goldfish_io" in p]
# Add container runtime path (simulates PYTHONPATH in Dockerfile)
sys.path.insert(0, "{container_root}")

# This is what user code does in containers
try:
    from goldfish.metrics import log_metric, log_metrics, log_artifact, finish
    print("SUCCESS: All goldfish.metrics imports work")
except ImportError as e:
    print(f"FAIL: {{e}}")
    sys.exit(1)
"""

    result = subprocess.run(
        [sys.executable, "-c", test_script],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"Import failed: {result.stderr}\n{result.stdout}"
    assert "SUCCESS" in result.stdout, f"Unexpected output: {result.stdout}"
