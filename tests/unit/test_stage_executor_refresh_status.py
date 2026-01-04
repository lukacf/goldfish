"""Unit tests for StageExecutor refresh_status_once handling."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import StageRunProgress, StageRunStatus


def _create_running_stage_run(test_db) -> str:
    test_db.create_workspace_lineage("ws", description="test")
    test_db.create_version("ws", "v1", "tag-v1", "sha123", "manual")
    run_id = "stage-refresh123"
    test_db.create_stage_run(
        stage_run_id=run_id,
        workspace_name="ws",
        version="v1",
        stage_name="train",
        pipeline_run_id=None,
        pipeline_name=None,
        config={},
        inputs={},
        profile=None,
        hints=None,
        backend_type="gce",
        backend_handle=run_id,
    )
    test_db.update_stage_run_status(run_id, StageRunStatus.RUNNING, progress=StageRunProgress.RUNNING)
    return run_id


def test_refresh_status_once_not_found_completed(test_db, test_config, tmp_path, monkeypatch):
    """Missing instance should finalize as completed when exit_code=0."""
    run_id = _create_running_stage_run(test_db)
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute("UPDATE stage_runs SET started_at=? WHERE id=?", (started_at, run_id))

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "gce"
    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
    )

    executor.gce_launcher.get_instance_status = MagicMock(return_value="not_found")
    executor.gce_launcher._get_exit_code = MagicMock(return_value=0)
    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

    status = executor.refresh_status_once(run_id)

    assert status == StageRunStatus.COMPLETED
    executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageRunStatus.COMPLETED)


def test_refresh_status_once_not_found_failed(test_db, test_config, tmp_path, monkeypatch):
    """Missing instance should finalize as failed when exit_code!=0."""
    run_id = _create_running_stage_run(test_db)
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute("UPDATE stage_runs SET started_at=? WHERE id=?", (started_at, run_id))

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "gce"
    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
    )

    executor.gce_launcher.get_instance_status = MagicMock(return_value="not_found")
    executor.gce_launcher._get_exit_code = MagicMock(return_value=1)
    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

    status = executor.refresh_status_once(run_id)

    assert status == StageRunStatus.FAILED
    executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageRunStatus.FAILED)
