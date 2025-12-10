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
