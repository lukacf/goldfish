"""Unit tests for pre_run_review phase."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from goldfish.config.settings import GoldfishSettings
from goldfish.jobs.phases.context import StageRunContext
from goldfish.jobs.phases.review import pre_run_review
from goldfish.models import PipelineDef


def _ctx() -> StageRunContext:
    settings = GoldfishSettings(
        project_name="test",
        dev_repo_path=Path("/tmp/dev-repo"),
        workspaces_path=Path("/tmp/workspaces"),
        backend="local",
        db_path=Path("/tmp/goldfish.db"),
        db_backend="sqlite",
        log_format="console",
        log_level="INFO",
        stage_timeout=60,
        gce_launch_timeout=60,
    )
    return StageRunContext(
        stage_run_id="stage-abc123",
        workspace_name="ws",
        stage_name="train",
        version="v1",
        pipeline=PipelineDef(name="placeholder", stages=[]),
        stage_config={},
        run_backend=MagicMock(),
        storage=MagicMock(),
        settings=settings,
    )


def test_pre_run_review_when_skipped_returns_none() -> None:
    """skip_review should bypass review call."""
    deps = MagicMock()
    deps.config.pre_run_review.enabled = True

    ctx = _ctx()
    review = pre_run_review(
        deps,
        ctx,
        pipeline=PipelineDef(name="p", stages=[]),
        reason_structured={"description": "x"},
        git_sha="sha",
        input_context=[],
        config_override=None,
        skip_review=True,
    )

    assert review is None
    deps._perform_pre_run_review.assert_not_called()


def test_pre_run_review_when_enabled_records_and_returns_review() -> None:
    """Enabled review should call perform + record."""
    deps = MagicMock()
    deps.config.pre_run_review.enabled = True
    deps._perform_pre_run_review.return_value = {"summary": "ok"}

    ctx = _ctx()
    pipeline = PipelineDef(name="p", stages=[])
    review = pre_run_review(
        deps,
        ctx,
        pipeline=pipeline,
        reason_structured={"description": "x"},
        git_sha="sha",
        input_context=[{"input": "raw"}],
        config_override={"k": "v"},
        skip_review=False,
    )

    assert review == {"summary": "ok"}
    deps._perform_pre_run_review.assert_called_once()
    deps._record_pre_run_review.assert_called_once_with("stage-abc123", {"summary": "ok"})
