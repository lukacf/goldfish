"""Import boundary enforcement contract tests.

This suite asserts that import-linter is wired and that all boundary contracts pass.
"""

from __future__ import annotations

import subprocess


def test_import_linter_when_running_make_lint_imports_has_zero_violations() -> None:
    """Import boundary contracts are enforced with zero violations."""

    result = subprocess.run(
        ["make", "lint-imports"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, (result.stdout + result.stderr).strip()
