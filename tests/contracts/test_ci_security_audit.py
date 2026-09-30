"""Security audit bootstrap must remediate vulnerable runner tooling."""

import shlex
from pathlib import Path

import yaml
from packaging.requirements import Requirement


def test_security_audit_upgrades_runner_setuptools_before_freezing() -> None:
    """The audit includes runner packages, so setuptools must meet the fixed floor."""
    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/ci.yml").read_text())
    steps = workflow["jobs"]["security-audit"]["steps"]
    audit_index = next(i for i, step in enumerate(steps) if "pip-audit --strict" in step.get("run", ""))

    for step in steps[:audit_index]:
        for line in step.get("run", "").splitlines():
            tokens = shlex.split(line)
            if tokens[:4] != ["uv", "pip", "install", "--system"]:
                continue
            for token in tokens[4:]:
                if token.startswith("setuptools"):
                    requirement = Requirement(token)
                    assert "83.0.0" in requirement.specifier
                    assert "82.0.1" not in requirement.specifier
                    return

    raise AssertionError("Upgrade runner setuptools before auditing the installed environment")
