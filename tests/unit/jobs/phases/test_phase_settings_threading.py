"""Unit tests for settings threading in phase functions.

REQ-015 (spec): Settings threading.
"""

from __future__ import annotations

from pathlib import Path


def test_phase_functions_when_defined_use_settings_from_context() -> None:
    """Phase functions should read settings only from StageRunContext."""
    repo_root = Path(__file__).resolve().parents[4]
    phases_root = repo_root / "src" / "goldfish" / "jobs" / "phases"

    phase_modules = [
        "build.py",
        "finalize.py",
        "launch.py",
        "monitor.py",
        "resolve.py",
        "review.py",
        "sync.py",
        "validate.py",
    ]

    offenders: list[str] = []
    for rel in phase_modules:
        content = (phases_root / rel).read_text(encoding="utf-8")
        if "ctx.settings" not in content:
            offenders.append(rel)

    assert offenders == [], f"Phase modules missing ctx.settings usage: {offenders}"
