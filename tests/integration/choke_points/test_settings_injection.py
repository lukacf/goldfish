"""Choke-point integration tests for Settings → Components boundary."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

import goldfish
from goldfish.config.settings import GoldfishSettings


def test_goldfish_settings_when_mutated_raises_frozen_instance_error(tmp_path: Path) -> None:
    """REQ-014: settings must be immutable once created (Gate 2: RED OK)."""
    settings = GoldfishSettings(
        project_name="test",
        dev_repo_path=tmp_path / "dev",
        workspaces_path=tmp_path / "workspaces",
        backend="local",
        db_path=tmp_path / "db.sqlite",
        db_backend="sqlite",
        log_format="console",
        log_level="INFO",
        stage_timeout=60,
        gce_launch_timeout=10,
    )

    with pytest.raises(FrozenInstanceError):
        settings.project_name = "mutated"  # type: ignore[misc]


def test_stage_executor_when_refactored_uses_goldfish_settings_not_goldfish_config() -> None:
    """REQ-015: components should receive settings threading (Gate 2: RED OK)."""
    package_root = Path(goldfish.__file__).resolve().parent
    source = (package_root / "jobs" / "stage_executor.py").read_text(encoding="utf-8")
    assert "GoldfishSettings" in source, "Expected StageExecutor to accept and store GoldfishSettings"
