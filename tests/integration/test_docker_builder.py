import subprocess

import pytest

from goldfish.errors import GoldfishError
from goldfish.infra.docker_builder import DockerBuilder


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


def test_dockerfile_nvidia_ngc_no_chown(tmp_path):
    """NVIDIA NGC images run as root, so COPY should NOT use --chown.

    Unlike Jupyter images which run as non-root user (jovyan), NVIDIA NGC
    containers run as root and don't need the user switching or chown flags.
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

    # COPY commands should NOT have --chown for NGC images
    copy_lines = [line for line in dockerfile.split("\n") if line.strip().startswith("COPY")]
    assert len(copy_lines) >= 4  # goldfish_io, modules, configs, loaders

    for line in copy_lines:
        assert "--chown" not in line, f"NGC image COPY should NOT have --chown: {line}"

    # Should NOT have USER root / USER 1000 switching
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
