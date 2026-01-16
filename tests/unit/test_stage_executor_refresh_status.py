"""Unit tests for StageExecutor refresh_status_once handling."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock
from uuid import uuid4

from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import StageRunProgress, StageRunStatus
from goldfish.state_machine.exit_code import ExitCodeResult


def _create_running_stage_run(test_db) -> str:
    test_db.create_workspace_lineage("ws", description="test")
    test_db.create_version("ws", "v1", "tag-v1", "sha123", "manual")
    run_id = f"stage-{uuid4().hex[:8]}"
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
    executor.gce_launcher._get_exit_code = MagicMock(return_value=ExitCodeResult.from_code(0))
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
    executor.gce_launcher._get_exit_code = MagicMock(return_value=ExitCodeResult.from_code(1))
    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

    status = executor.refresh_status_once(run_id)

    assert status == StageRunStatus.FAILED
    executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageRunStatus.FAILED)


def test_refresh_status_once_not_found_during_build_phase(test_db, test_config, tmp_path, monkeypatch):
    """Instance not_found during BUILD phase should NOT trigger timeout.

    When Cloud Build is running, the instance doesn't exist yet. The refresh
    should return not_found without marking the run as failed, regardless of
    elapsed time - unless there's evidence the instance actually ran (exit code in GCS).
    """
    run_id = _create_running_stage_run(test_db)
    # Set started_at to 1 hour ago - would normally trigger timeout
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute(
            "UPDATE stage_runs SET started_at=?, progress=? WHERE id=?",
            (started_at, StageRunProgress.BUILD, run_id),
        )

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
    executor.gce_launcher._get_exit_code = MagicMock(
        return_value=ExitCodeResult.from_not_found()
    )  # No exit code = never ran
    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

    status = executor.refresh_status_once(run_id)

    # Should return not_found without triggering timeout/finalization
    assert status == "not_found"
    executor._finalize_stage_run.assert_not_called()


def test_refresh_status_once_not_found_during_launch_phase(test_db, test_config, tmp_path, monkeypatch):
    """Instance not_found during LAUNCH phase should NOT trigger timeout.

    When instance is being created, it may not be visible in GCE API yet.
    The refresh should return not_found without marking the run as failed -
    unless there's evidence the instance actually ran (exit code in GCS).
    """
    run_id = _create_running_stage_run(test_db)
    # Set started_at to 1 hour ago - would normally trigger timeout
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute(
            "UPDATE stage_runs SET started_at=?, progress=? WHERE id=?",
            (started_at, StageRunProgress.LAUNCH, run_id),
        )

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
    executor.gce_launcher._get_exit_code = MagicMock(
        return_value=ExitCodeResult.from_not_found()
    )  # No exit code = never ran
    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

    status = executor.refresh_status_once(run_id)

    # Should return not_found without triggering timeout/finalization
    assert status == "not_found"
    executor._finalize_stage_run.assert_not_called()


def test_refresh_status_once_not_found_during_launch_but_exit_code_exists(test_db, test_config, tmp_path, monkeypatch):
    """Preempted instance during LAUNCH phase should finalize if exit code exists.

    If an instance was preempted between poll intervals, progress might still
    be LAUNCH but there's an exit code file in GCS proving it ran. In this case,
    we should finalize based on the exit code.
    """
    run_id = _create_running_stage_run(test_db)
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute(
            "UPDATE stage_runs SET started_at=?, progress=? WHERE id=?",
            (started_at, StageRunProgress.LAUNCH, run_id),
        )

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
    executor.gce_launcher._get_exit_code = MagicMock(
        return_value=ExitCodeResult.from_code(1)
    )  # Exit code exists = ran and failed
    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

    status = executor.refresh_status_once(run_id)

    # Should finalize as FAILED because exit_code=1
    assert status == StageRunStatus.FAILED
    executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageRunStatus.FAILED)

    # Check error message indicates preemption
    row = test_db.get_stage_run(run_id)
    assert row is not None
    assert "preempted" in row["error"].lower() or "terminated" in row["error"].lower()


# =============================================================================
# Tests for wait_for_completion() preemption handling
# =============================================================================


def test_wait_for_completion_preemption_with_running_progress(test_db, test_config, tmp_path, monkeypatch):
    """wait_for_completion should finalize preempted instance that was running.

    When an instance was running (progress=RUNNING) and gets preempted,
    wait_for_completion should check the exit code and finalize appropriately
    instead of throwing an exception.
    """
    run_id = _create_running_stage_run(test_db)
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute(
            "UPDATE stage_runs SET started_at=?, progress=? WHERE id=?",
            (started_at, StageRunProgress.RUNNING, run_id),
        )

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
    executor.gce_launcher._get_exit_code = MagicMock(
        return_value=ExitCodeResult.from_code(1)
    )  # Failed with exit code 1
    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

    status = executor.wait_for_completion(run_id)

    # Should finalize as FAILED
    assert status == StageRunStatus.FAILED
    executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageRunStatus.FAILED)

    # Check error message
    row = test_db.get_stage_run(run_id)
    assert row is not None
    assert "preempted" in row["error"].lower() or "terminated" in row["error"].lower()


def test_wait_for_completion_preemption_with_launch_progress_and_exit_code(test_db, test_config, tmp_path, monkeypatch):
    """wait_for_completion should finalize preempted instance even if progress is LAUNCH.

    If an instance was preempted between poll intervals, progress might still be LAUNCH.
    But if there's an exit code in GCS, we know it ran and should finalize it.
    """
    run_id = _create_running_stage_run(test_db)
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute(
            "UPDATE stage_runs SET started_at=?, progress=? WHERE id=?",
            (started_at, StageRunProgress.LAUNCH, run_id),
        )

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
    executor.gce_launcher._get_exit_code = MagicMock(return_value=ExitCodeResult.from_code(0))  # Completed successfully
    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

    status = executor.wait_for_completion(run_id)

    # Should finalize as COMPLETED since exit_code=0
    assert status == StageRunStatus.COMPLETED
    executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageRunStatus.COMPLETED)


def test_wait_for_completion_launch_failure_no_exit_code(test_db, test_config, tmp_path, monkeypatch):
    """wait_for_completion should mark as failed if instance never ran.

    If progress is LAUNCH and there's no exit code, the instance never ran.
    This should be marked as FAILED with an appropriate error message.
    """
    run_id = _create_running_stage_run(test_db)
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute(
            "UPDATE stage_runs SET started_at=?, progress=? WHERE id=?",
            (started_at, StageRunProgress.LAUNCH, run_id),
        )

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
    executor.gce_launcher._get_exit_code = MagicMock(return_value=ExitCodeResult.from_not_found())  # No exit code
    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")
    monkeypatch.setenv("GOLDFISH_GCE_LAUNCH_TIMEOUT", "0")  # Also set launch timeout for LAUNCH phase

    status = executor.wait_for_completion(run_id)

    # Should return FAILED (no finalize needed since it never ran)
    assert status == StageRunStatus.FAILED

    # Check error message indicates launch failure
    row = test_db.get_stage_run(run_id)
    assert row is not None
    assert "not found" in row["error"].lower() or "failed to launch" in row["error"].lower()
