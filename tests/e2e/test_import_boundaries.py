"""E2E scenario tests for import boundary enforcement.

E2E-002 (spec): Import linter passes in CI.
"""

from __future__ import annotations

import subprocess


def test_import_boundaries_when_running_lint_imports_has_zero_violations() -> None:
    """E2E-002: Import boundary contracts are enforced."""
    result = subprocess.run(
        ["make", "lint-imports"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, (result.stdout + result.stderr).strip()
