"""RunBackend contract tests.

These tests are abstract: concrete backend implementations should subclass
RunBackendContract and provide fixtures for a RunBackend and a RunSpec.
"""

from __future__ import annotations

import subprocess
from abc import ABC, abstractmethod
from unittest.mock import patch

import pytest

from goldfish.cloud.adapters.local.run_backend import LocalRunBackend
from goldfish.cloud.contracts import BackendStatus, RunSpec
from goldfish.cloud.protocols import RunBackend


class RunBackendContract(ABC):
    """Abstract contract tests for RunBackend implementations."""

    @pytest.fixture
    @abstractmethod
    def run_backend(self) -> RunBackend:
        """Return a RunBackend implementation under test."""

    @pytest.fixture
    @abstractmethod
    def run_spec(self) -> RunSpec:
        """Return a minimal RunSpec usable by the backend under test."""

    def test_launch_when_called_then_get_status_returns_backend_status(
        self, run_backend: RunBackend, run_spec: RunSpec
    ) -> None:
        """launch() returns handle and get_status() returns BackendStatus."""
        handle = run_backend.launch(run_spec)
        status = run_backend.get_status(handle)
        assert isinstance(status, BackendStatus)


class TestLocalRunBackend(RunBackendContract):
    """RunBackendContract for LocalRunBackend."""

    @pytest.fixture
    def run_backend(self) -> RunBackend:
        container_id = "deadbeefdeadbeef"

        def fake_run(cmd: list[str], *args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
            if cmd[:2] == ["docker", "run"]:
                return subprocess.CompletedProcess(cmd, 0, stdout=f"{container_id}\n", stderr="")
            if cmd[:2] == ["docker", "inspect"] and "{{.State.Status}}:{{.State.ExitCode}}" in cmd:
                return subprocess.CompletedProcess(cmd, 0, stdout="running:0\n", stderr="")
            raise AssertionError(f"Unexpected subprocess command: {cmd}")

        with patch("goldfish.cloud.adapters.local.run_backend.subprocess.run", side_effect=fake_run):
            yield LocalRunBackend()

    @pytest.fixture
    def run_spec(self) -> RunSpec:
        return RunSpec(
            stage_run_id="stage-10951234",
            workspace_name="ws-1",
            stage_name="train",
            image="alpine:latest",
        )
