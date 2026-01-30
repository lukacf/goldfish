"""Makefile wiring contract tests for import boundary enforcement."""

from __future__ import annotations

import re
from pathlib import Path


def test_makefile_when_read_has_lint_imports_target() -> None:
    """Phase 6 requires a `make lint-imports` target that runs import-linter."""

    makefile_path = Path(__file__).resolve().parents[2] / "Makefile"
    makefile = makefile_path.read_text("utf-8")

    assert re.search(r"^lint-imports\s*:\s*$", makefile, flags=re.MULTILINE) is not None
    assert re.search(r"^\t(\S+\s+)*lint-imports(\s+.*)?$", makefile, flags=re.MULTILINE) is not None
