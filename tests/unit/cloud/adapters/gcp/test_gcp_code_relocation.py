"""Unit tests for GCP code relocation into the adapter package."""

from __future__ import annotations

from pathlib import Path


def test_gce_launcher_when_imported_from_gcp_adapter_succeeds() -> None:
    """GCELauncher should live under goldfish.cloud.adapters.gcp."""
    from goldfish.cloud.adapters.gcp.gce_launcher import GCELauncher, GCELaunchResult

    assert GCELauncher is not None
    assert GCELaunchResult is not None


def test_startup_builder_when_imported_from_gcp_adapter_succeeds() -> None:
    """startup_builder should live under goldfish.cloud.adapters.gcp."""
    from goldfish.cloud.adapters.gcp.startup_builder import build_startup_script

    assert callable(build_startup_script)


def test_metadata_bus_when_imported_from_gcp_adapter_succeeds() -> None:
    """GCPMetadataBus should live under goldfish.cloud.adapters.gcp."""
    from goldfish.cloud.adapters.gcp.metadata_bus import GCPMetadataBus

    assert hasattr(GCPMetadataBus, "set_signal")


def test_gce_launcher_when_imported_from_infra_raises_module_not_found() -> None:
    """infra/gce_launcher.py should be removed after relocation."""
    repo_root = Path(__file__).resolve().parents[5]
    assert not (repo_root / "src" / "goldfish" / "infra" / "gce_launcher.py").exists()


def test_startup_builder_when_imported_from_infra_raises_module_not_found() -> None:
    """infra/startup_builder.py should be removed after relocation."""
    repo_root = Path(__file__).resolve().parents[5]
    assert not (repo_root / "src" / "goldfish" / "infra" / "startup_builder.py").exists()


def test_metadata_gcp_when_imported_from_infra_raises_module_not_found() -> None:
    """infra/metadata/gcp.py should be removed after relocation."""
    repo_root = Path(__file__).resolve().parents[5]
    assert not (repo_root / "src" / "goldfish" / "infra" / "metadata" / "gcp.py").exists()
