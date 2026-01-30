"""E2E scenario tests for settings injection (no file reads in core).

REQ-015 (spec): Settings threading.
"""

from __future__ import annotations

from pathlib import Path

from goldfish.config.settings import GoldfishSettings


def test_settings_injection_when_executor_constructed_uses_explicit_settings(tmp_path: Path) -> None:
    """REQ-015: Stage execution components receive GoldfishSettings."""
    settings = GoldfishSettings(
        project_name="e2e-settings-test",
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

    from goldfish.jobs.stage_executor import StageExecutor

    _ = StageExecutor(settings=settings)
