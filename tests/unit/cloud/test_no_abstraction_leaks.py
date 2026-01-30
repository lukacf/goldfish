"""Tests that provider-specific details do not leak into Goldfish core."""

from __future__ import annotations

from pathlib import Path


def test_no_backend_string_comparisons_in_jobs_and_pipeline() -> None:
    """Core should not branch on config.jobs.backend strings (use capabilities/injection)."""
    repo_root = Path(__file__).resolve().parents[3]
    src_root = repo_root / "src" / "goldfish"

    offenders: list[str] = []
    for rel_root in (Path("jobs"), Path("pipeline")):
        for path in (src_root / rel_root).rglob("*.py"):
            try:
                content = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue

            if "jobs.backend ==" not in content:
                continue

            offenders.append(path.relative_to(src_root).as_posix())

    assert offenders == [], f"backend string comparisons found in core: {offenders}"


def test_no_backend_string_comparisons_when_scanning_config_and_infra_finds_none() -> None:
    """Core infra/config should not branch on backend strings (use capabilities/injection)."""
    repo_root = Path(__file__).resolve().parents[3]
    src_root = repo_root / "src" / "goldfish"

    offenders: list[str] = []

    # Config should not branch on jobs.backend
    for path in (src_root / "config").rglob("*.py"):
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if "jobs.backend ==" in content:
            offenders.append(path.relative_to(src_root).as_posix())

    # Infra should not branch on specific backend strings like "gce"
    for path in (src_root / "infra").rglob("*.py"):
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if '== "gce"' in content:
            offenders.append(path.relative_to(src_root).as_posix())

    assert offenders == [], f"backend string comparisons found in core: {offenders}"


def test_no_backend_type_branching_in_contracts_when_scanning_source_finds_none() -> None:
    """Shared contracts should not branch on backend_type strings."""
    repo_root = Path(__file__).resolve().parents[3]
    src_root = repo_root / "src" / "goldfish"
    contracts_path = src_root / "cloud" / "contracts.py"

    content = contracts_path.read_text(encoding="utf-8")

    assert "backend_type ==" not in content


def test_no_gs_uri_string_handling_outside_gcp_adapters() -> None:
    """Avoid constructing/checking gs:// strings outside the GCP adapter package."""
    repo_root = Path(__file__).resolve().parents[3]
    src_root = repo_root / "src" / "goldfish"

    offenders: list[str] = []

    for path in src_root.rglob("*.py"):
        rel = path.relative_to(src_root).as_posix()
        if rel.startswith("cloud/adapters/gcp/"):
            continue

        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue

        if 'startswith("gs://")' in content:
            offenders.append(f"{rel}:startswith")
        if "startswith('gs://')" in content:
            offenders.append(f"{rel}:startswith")
        if 'f"gs://' in content:
            offenders.append(f"{rel}:fstring")
        if "f'gs://" in content:
            offenders.append(f"{rel}:fstring")

    assert offenders == [], f"gs:// string handling found outside gcp adapter: {offenders}"
