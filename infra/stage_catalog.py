"""Helpers for loading and resolving stage artifact catalog entries."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional

CATALOG_PATH = Path("catalog/stage_artifacts.json")


def load_catalog(path: Path | None = None) -> Dict:
    target = path or CATALOG_PATH
    if not target.exists():
        raise FileNotFoundError(f"Stage catalog not found at {target}")
    with target.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def resolve_stage(stage: str, version: Optional[str] = None, *, catalog_path: Path | None = None) -> Dict:
    catalog = load_catalog(catalog_path)
    stages = catalog.get("stages", {})
    if stage not in stages:
        raise KeyError(f"Stage '{stage}' missing from catalog")
    stage_info = stages[stage]
    target_version = version or stage_info.get("promoted")
    if not target_version:
        raise KeyError(f"Stage '{stage}' has no promoted version and no version override was supplied")
    versions = stage_info.get("versions", {})
    if target_version not in versions:
        raise KeyError(f"Stage '{stage}' version '{target_version}' missing from catalog")
    entry = versions[target_version]
    entry.setdefault("resolved_version", target_version)
    return entry
