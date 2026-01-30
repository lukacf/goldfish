"""Unified settings dataclass for Goldfish (architecture redesign)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass(frozen=True, slots=True)
class GoldfishSettings:
    """Immutable settings container passed through the system."""

    project_name: str
    dev_repo_path: Path
    workspaces_path: Path
    backend: Literal["local", "gce"]
    db_path: Path
    db_backend: Literal["sqlite", "postgres"]
    log_format: Literal["json", "console"]
    log_level: str
    stage_timeout: int
    gce_launch_timeout: int
