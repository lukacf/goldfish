"""Choke-point integration tests for Core → Adapter boundary (via Factory)."""

from __future__ import annotations

from pathlib import Path

import goldfish
from goldfish.cloud.factory import AdapterFactory
from goldfish.cloud.protocols import RunBackend
from goldfish.config import GoldfishConfig


def test_adapter_factory_when_local_backend_creates_run_backend_protocol(test_config: GoldfishConfig) -> None:
    """Factory should return a protocol-typed adapter (Gate 2: RED OK)."""
    factory = AdapterFactory(test_config)
    backend = factory.create_run_backend()
    assert isinstance(backend, RunBackend)


def test_core_modules_when_refactored_do_not_import_cloud_adapters() -> None:
    """CONTRACT-008: only cloud/factory.py may import adapters (enforced later)."""
    package_root = Path(goldfish.__file__).resolve().parent
    stage_executor_source = (package_root / "jobs" / "stage_executor.py").read_text(encoding="utf-8")
    assert "goldfish.cloud.adapters" not in stage_executor_source
