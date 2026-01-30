"""Import-linter configuration contract tests.

Phase 6 (Import Boundary Enforcement) requires import-linter to be configured via pyproject.toml.
"""

from __future__ import annotations

import tomllib
from pathlib import Path


def test_import_linter_config_when_parsing_pyproject_has_expected_contracts() -> None:
    """pyproject.toml defines the import boundary contracts from the architecture spec."""

    pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    config = tomllib.loads(pyproject_path.read_text("utf-8"))

    importlinter = config.get("tool", {}).get("importlinter")
    assert importlinter is not None

    assert importlinter.get("root_packages") == ["goldfish"]

    contracts = importlinter.get("contracts")
    assert isinstance(contracts, list)

    by_name = {c.get("name"): c for c in contracts}
    assert set(by_name.keys()) == {
        "core-no-api-surface",
        "protocols-no-adapters",
        "infra-no-api",
        "only-factory-imports-adapters",
    }

    assert by_name["core-no-api-surface"] == {
        "name": "core-no-api-surface",
        "type": "forbidden",
        "source_modules": ["goldfish.jobs", "goldfish.workspace.manager", "goldfish.pipeline"],
        "forbidden_modules": ["goldfish.server", "goldfish.server_tools"],
    }
    assert by_name["protocols-no-adapters"] == {
        "name": "protocols-no-adapters",
        "type": "forbidden",
        "source_modules": ["goldfish.cloud.protocols", "goldfish.cloud.contracts"],
        "forbidden_modules": ["goldfish.cloud.adapters"],
    }
    assert by_name["infra-no-api"] == {
        "name": "infra-no-api",
        "type": "forbidden",
        "source_modules": ["goldfish.db", "goldfish.cloud.adapters", "goldfish.infra"],
        "forbidden_modules": ["goldfish.server", "goldfish.server_tools"],
    }
    assert by_name["only-factory-imports-adapters"] == {
        "name": "only-factory-imports-adapters",
        "type": "protected",
        "protected_modules": ["goldfish.cloud.adapters"],
        "allowed_importers": ["goldfish.cloud.factory"],
    }
