"""Review failures expose diagnostics without dumping tool output."""

import json
import os
import subprocess
from pathlib import Path

import yaml


def test_review_failure_reports_error_without_tool_output(tmp_path: Path) -> None:
    """Only the failed result should appear in the public workflow log."""
    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/claude-code-review.yml").read_text())
    steps = workflow["jobs"]["claude-review"]["steps"]
    step = next(step for step in steps if step.get("name") == "Report review failure")
    assert "failure()" in step["if"]

    execution_file = tmp_path / "execution.json"
    execution_file.write_text(
        json.dumps(
            [
                {"type": "assistant", "message": "private tool output"},
                {"type": "result", "is_error": False, "result": "successful review"},
                {"type": "result", "is_error": True, "result": "API request failed"},
            ]
        )
    )
    result = subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        env={**os.environ, "EXECUTION_FILE": str(execution_file)},
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "API request failed"
