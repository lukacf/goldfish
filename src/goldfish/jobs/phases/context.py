"""Shared context passed between stage execution phases."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from goldfish.cloud.protocols import ObjectStorage, RunBackend
from goldfish.config.settings import GoldfishSettings
from goldfish.models import PipelineDef

StageConfig = dict[str, Any]
Pipeline = PipelineDef


@dataclass(slots=True)
class StageRunContext:
    """Explicit phase context for a stage run."""

    stage_run_id: str
    workspace_name: str
    stage_name: str
    version: str
    pipeline: Pipeline
    stage_config: StageConfig
    run_backend: RunBackend
    storage: ObjectStorage
    settings: GoldfishSettings
