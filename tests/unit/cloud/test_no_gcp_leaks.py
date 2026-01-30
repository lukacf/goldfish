"""Tests that GCP dependencies are isolated to the GCP adapter package."""

from __future__ import annotations

import ast
from pathlib import Path


def test_google_cloud_imports_when_outside_gcp_adapters_fails() -> None:
    """google.cloud imports should only exist under cloud/adapters/gcp/."""
    repo_root = Path(__file__).resolve().parents[3]
    src_root = repo_root / "src" / "goldfish"

    offenders: list[str] = []

    for path in src_root.rglob("*.py"):
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue

        if "from google.cloud" not in content and "import google.cloud" not in content:
            continue

        rel = path.relative_to(src_root).as_posix()
        if not rel.startswith("cloud/adapters/gcp/"):
            offenders.append(rel)

    assert offenders == [], f"google.cloud imports found outside gcp adapter: {offenders}"


def test_gs_uri_literals_when_outside_gcp_adapters_are_absent() -> None:
    """gs:// literals should only exist under cloud/adapters/gcp/."""
    repo_root = Path(__file__).resolve().parents[3]
    src_root = repo_root / "src" / "goldfish"

    offenders: list[str] = []

    for path in src_root.rglob("*.py"):
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue

        if "gs://" not in content:
            continue

        rel = path.relative_to(src_root).as_posix()
        if rel.startswith("cloud/adapters/gcp/"):
            continue

        # Include the first matching line number to make remediation fast.
        for line_num, line in enumerate(content.splitlines(), start=1):
            if "gs://" in line:
                offenders.append(f"{rel}:{line_num}")
                break

    assert offenders == [], f"gs:// literals found outside gcp adapter: {offenders}"


def test_gcp_adapter_imports_when_outside_factory_and_adapter_fails() -> None:
    """goldfish.cloud.adapters.gcp imports should only exist in the adapter boundary."""
    repo_root = Path(__file__).resolve().parents[3]
    src_root = repo_root / "src" / "goldfish"

    offenders: list[str] = []

    for path in src_root.rglob("*.py"):
        rel = path.relative_to(src_root).as_posix()
        if rel.startswith("cloud/adapters/gcp/"):
            continue
        if rel == "cloud/factory.py":
            continue

        content = path.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(rel))

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module is None:
                    continue
                if node.module.startswith("goldfish.cloud.adapters.gcp"):
                    offenders.append(f"{rel}:{node.lineno}")
                    continue
                if node.module == "goldfish.cloud.adapters" and any(alias.name == "gcp" for alias in node.names):
                    offenders.append(f"{rel}:{node.lineno}")
            elif isinstance(node, ast.Import):
                if any(alias.name.startswith("goldfish.cloud.adapters.gcp") for alias in node.names):
                    offenders.append(f"{rel}:{node.lineno}")

    assert offenders == [], f"gcp adapter imports found outside factory boundary: {offenders}"


def test_gcloud_usage_in_core_modules_is_absent() -> None:
    """GCE-specific `gcloud` CLI usage must not appear in Core modules.

    Core must interact with GCE only via the RunBackend protocol and adapters.
    """
    repo_root = Path(__file__).resolve().parents[3]
    src_root = repo_root / "src" / "goldfish"

    offenders: list[str] = []
    for rel in (
        "daemon.py",
        "state_machine/event_emission.py",
        "state_machine/exit_code.py",
    ):
        content = (src_root / rel).read_text(encoding="utf-8")
        if "gcloud" in content:
            offenders.append(rel)

    assert offenders == [], f"gcloud usage found in core modules: {offenders}"


def test_backend_type_string_switch_in_validation_is_absent() -> None:
    """Validation should not dispatch on backend_type strings in Core."""
    repo_root = Path(__file__).resolve().parents[3]
    src_root = repo_root / "src" / "goldfish"

    content = (src_root / "validation.py").read_text(encoding="utf-8")
    assert 'backend_type == "local"' not in content
    assert 'backend_type == "gce"' not in content


def test_provider_cli_usage_when_in_core_modules_fails() -> None:
    """Provider CLI strings must not appear in core modules.

    Core must interact with providers only via adapters/protocols.
    """
    repo_root = Path(__file__).resolve().parents[3]
    src_root = repo_root / "src" / "goldfish"

    offenders: list[str] = []
    for rel in (
        "daemon.py",
        "state_machine/exit_code.py",
    ):
        content = (src_root / rel).read_text(encoding="utf-8")
        for token in ("gce-cli", "gsutil"):
            if token in content:
                offenders.append(f"{rel}:{token}")

    assert offenders == [], f"provider CLI usage found in core modules: {offenders}"


def test_backend_type_switching_when_in_stage_daemon_fails() -> None:
    """StageDaemon must not branch on backend_type strings.

    Backend-specific behavior should live behind RunBackend/ObjectStorage adapters.
    """
    repo_root = Path(__file__).resolve().parents[3]
    src_root = repo_root / "src" / "goldfish"

    content = (src_root / "state_machine" / "stage_daemon.py").read_text(encoding="utf-8")
    assert "backend_type ==" not in content


def test_backend_type_sql_filtering_when_in_daemon_fails() -> None:
    """GoldfishDaemon must not filter by backend_type in SQL in core code."""
    repo_root = Path(__file__).resolve().parents[3]
    src_root = repo_root / "src" / "goldfish"

    content = (src_root / "daemon.py").read_text(encoding="utf-8")
    assert "backend_type = 'gce'" not in content
    assert 'backend_type = "gce"' not in content
