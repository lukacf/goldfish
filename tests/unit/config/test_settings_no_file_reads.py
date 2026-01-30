"""Unit tests for settings threading (no config file reads in core).

REQ-015 (spec): Settings threading.
"""

from __future__ import annotations

import builtins
from pathlib import Path

from goldfish.config.settings import GoldfishSettings


def test_stage_executor_when_constructed_with_settings_does_not_open_files(tmp_path: Path, monkeypatch) -> None:
    """StageExecutor should accept explicit settings without reading config files."""
    settings = GoldfishSettings(
        project_name="unit-settings-test",
        dev_repo_path=tmp_path / "dev-repo",
        workspaces_path=tmp_path / "workspaces",
        backend="local",
        db_path=tmp_path / "goldfish.db",
        db_backend="sqlite",
        log_format="json",
        log_level="INFO",
        stage_timeout=60,
        gce_launch_timeout=60,
    )

    def _fail_open(*args: object, **kwargs: object) -> object:  # pragma: no cover - defensive
        raise AssertionError(f"Unexpected file open: args={args!r} kwargs={kwargs!r}")

    monkeypatch.setattr(builtins, "open", _fail_open)

    from goldfish.jobs.stage_executor import StageExecutor

    executor = StageExecutor(settings=settings)
    assert executor.settings is settings
