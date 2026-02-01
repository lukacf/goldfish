"""Unit tests for StageExecutor refresh_status_once handling."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock
from uuid import uuid4

from goldfish.cloud.contracts import BackendStatus, RunStatus
from goldfish.errors import NotFoundError
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.state_machine.types import StageState

# Note: These tests use state instead of the deprecated progress column


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
    # Set state to RUNNING (state machine is source of truth)
    with test_db._conn() as conn:
        conn.execute("UPDATE stage_runs SET state = ? WHERE id = ?", (StageState.RUNNING.value, run_id))
    return run_id


def test_refresh_status_once_not_found_completed(test_db, test_config, tmp_path, monkeypatch):
    """Missing instance should finalize as completed when exit_code=0 in GCS."""
    run_id = _create_running_stage_run(test_db)
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute("UPDATE stage_runs SET started_at=? WHERE id=?", (started_at, run_id))

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "gce"

    # Create mock run_backend that returns COMPLETED status
    # (GCE backend internally recovers exit code from GCS and returns COMPLETED)
    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True)
    mock_backend.get_status.return_value = BackendStatus(status=RunStatus.COMPLETED, exit_code=0)

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
        run_backend=mock_backend,
    )

    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

    status = executor.refresh_status_once(run_id)

    assert status == StageState.COMPLETED
    executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageState.COMPLETED)


def test_refresh_status_once_not_found_failed(test_db, test_config, tmp_path, monkeypatch):
    """Missing instance should finalize as failed when exit_code!=0 in GCS."""
    run_id = _create_running_stage_run(test_db)
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute("UPDATE stage_runs SET started_at=? WHERE id=?", (started_at, run_id))

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "gce"

    # Create mock run_backend that returns FAILED status
    # (GCE backend internally recovers exit code from GCS and returns FAILED)
    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True)
    mock_backend.get_status.return_value = BackendStatus(status=RunStatus.FAILED, exit_code=1)

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
        run_backend=mock_backend,
    )

    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

    status = executor.refresh_status_once(run_id)

    assert status == StageState.FAILED
    executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageState.FAILED)


def test_refresh_status_once_not_found_during_build_phase(test_db, test_config, tmp_path, monkeypatch):
    """Instance not_found during BUILD phase should NOT trigger timeout.

    When Cloud Build is running, the instance doesn't exist yet. The refresh
    should return not_found without marking the run as failed, regardless of
    elapsed time - unless there's evidence the instance actually ran (exit code in GCS).
    """
    run_id = _create_running_stage_run(test_db)
    # Set started_at to 1 hour ago - would normally trigger timeout
    # State machine: state=BUILDING means we're still building the image
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute(
            "UPDATE stage_runs SET started_at=?, state=? WHERE id=?",
            (started_at, "building", run_id),
        )

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "gce"

    # Create mock run_backend that raises NotFoundError
    # (GCE backend raises NotFoundError when instance not found and no GCS exit code)
    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True)
    mock_backend.get_status.side_effect = NotFoundError(f"instance:{run_id}")

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
        run_backend=mock_backend,
    )

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
    # State machine: state=LAUNCHING means we're creating the GCE instance
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute(
            "UPDATE stage_runs SET started_at=?, state=? WHERE id=?",
            (started_at, "launching", run_id),
        )

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "gce"

    # Create mock run_backend that raises NotFoundError
    # (GCE backend raises NotFoundError when instance not found and no GCS exit code)
    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True)
    mock_backend.get_status.side_effect = NotFoundError(f"instance:{run_id}")

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
        run_backend=mock_backend,
    )

    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

    status = executor.refresh_status_once(run_id)

    # Should return not_found without triggering timeout/finalization
    assert status == "not_found"
    executor._finalize_stage_run.assert_not_called()


def test_refresh_status_once_not_found_during_launch_but_exit_code_exists(test_db, test_config, tmp_path, monkeypatch):
    """Preempted instance during LAUNCHING state should finalize if exit code exists.

    If an instance was preempted between poll intervals, state might still
    be LAUNCHING but there's an exit code file in GCS proving it ran. In this case,
    we should finalize based on the exit code.
    """
    run_id = _create_running_stage_run(test_db)
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute(
            "UPDATE stage_runs SET started_at=?, state=? WHERE id=?",
            (started_at, StageState.LAUNCHING.value, run_id),
        )

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "gce"

    # Create mock run_backend that returns FAILED status with preemption
    # (GCE backend returns FAILED when exit code in GCS indicates failure)
    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True)
    mock_backend.get_status.return_value = BackendStatus(
        status=RunStatus.FAILED,
        exit_code=1,
        termination_cause="preemption",
    )

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
        run_backend=mock_backend,
    )

    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

    status = executor.refresh_status_once(run_id)

    # Should finalize as FAILED because exit_code=1
    assert status == StageState.FAILED
    executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageState.FAILED)


# =============================================================================
# Tests for wait_for_completion() preemption handling
# =============================================================================


def test_wait_for_completion_preemption_with_running_state(test_db, test_config, tmp_path, monkeypatch):
    """wait_for_completion should finalize preempted instance that was running.

    When an instance was running (state=RUNNING) and gets preempted,
    wait_for_completion should check the exit code and finalize appropriately
    instead of throwing an exception.
    """
    run_id = _create_running_stage_run(test_db)
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute(
            "UPDATE stage_runs SET started_at=?, state=? WHERE id=?",
            (started_at, StageState.RUNNING.value, run_id),
        )

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "gce"

    # Create mock run_backend that returns FAILED status with preemption
    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True)
    mock_backend.get_status.return_value = BackendStatus(
        status=RunStatus.FAILED,
        exit_code=1,
        termination_cause="preemption",
    )

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
        run_backend=mock_backend,
    )

    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

    status = executor.wait_for_completion(run_id)

    # Should finalize as FAILED
    assert status == StageState.FAILED
    executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageState.FAILED)


def test_wait_for_completion_preemption_with_launching_state_and_exit_code(test_db, test_config, tmp_path, monkeypatch):
    """wait_for_completion should finalize preempted instance even if state is LAUNCHING.

    If an instance was preempted between poll intervals, state might still be LAUNCHING.
    But if there's an exit code in GCS, we know it ran and should finalize it.
    """
    run_id = _create_running_stage_run(test_db)
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute(
            "UPDATE stage_runs SET started_at=?, state=? WHERE id=?",
            (started_at, StageState.LAUNCHING.value, run_id),
        )

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "gce"

    # Create mock run_backend that returns COMPLETED status
    # (GCE backend internally recovers exit code from GCS and returns COMPLETED)
    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True)
    mock_backend.get_status.return_value = BackendStatus(
        status=RunStatus.COMPLETED,
        exit_code=0,
    )

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
        run_backend=mock_backend,
    )

    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

    status = executor.wait_for_completion(run_id)

    # Should finalize as COMPLETED since exit_code=0
    assert status == StageState.COMPLETED
    executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageState.COMPLETED)


def test_wait_for_completion_launch_failure_no_exit_code(test_db, test_config, tmp_path, monkeypatch):
    """wait_for_completion should mark as failed if instance never ran.

    If state is LAUNCHING and there's no exit code, the instance never ran.
    This should be marked as TERMINATED with an appropriate error message.
    """
    run_id = _create_running_stage_run(test_db)
    started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    with test_db._conn() as conn:
        conn.execute(
            "UPDATE stage_runs SET started_at=?, state=? WHERE id=?",
            (started_at, StageState.LAUNCHING.value, run_id),
        )

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "gce"

    # Create mock run_backend that raises NotFoundError
    # (GCE backend raises NotFoundError when instance not found and no GCS exit code)
    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True)
    mock_backend.get_status.side_effect = NotFoundError(f"instance:{run_id}")

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
        run_backend=mock_backend,
    )

    executor._finalize_stage_run = MagicMock()
    monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")
    monkeypatch.setenv("GOLDFISH_GCE_LAUNCH_TIMEOUT", "0")  # Also set launch timeout for LAUNCH phase

    status = executor.wait_for_completion(run_id)

    # Should return TERMINATED via INSTANCE_LOST (no finalize needed since it never ran)
    assert status == StageState.TERMINATED

    # Check error message indicates launch failure
    row = test_db.get_stage_run(run_id)
    assert row is not None
    assert "not found" in row["error"].lower() or "failed to launch" in row["error"].lower()


# =============================================================================
# Tests for UNKNOWN status handling (GCS/API errors)
# =============================================================================


def test_refresh_status_once_unknown_status_logs_message(test_db, test_config, tmp_path, caplog):
    """REGRESSION: UNKNOWN status should be logged with error details.

    Bug: When run_backend.get_status() returned UNKNOWN (e.g., due to GCS error),
    refresh_status_once silently ignored it without logging.

    Fix: Log UNKNOWN status with the message from BackendStatus.
    """
    import logging

    run_id = _create_running_stage_run(test_db)

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "gce"

    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True, has_launch_delay=True)
    # Return UNKNOWN status with a message
    mock_backend.get_status.return_value = BackendStatus(
        status=RunStatus.UNKNOWN,
        message="GCS error: Connection refused",
    )

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
        run_backend=mock_backend,
    )

    with caplog.at_level(logging.WARNING):
        status = executor.refresh_status_once(run_id)

    # Should return "unknown" status
    assert status == "unknown"

    # Key assertion: warning should be logged with the error message
    assert any("UNKNOWN" in record.message for record in caplog.records)
    assert any("GCS error" in record.message or "Connection refused" in record.message for record in caplog.records)


def test_refresh_status_once_unknown_does_not_finalize(test_db, test_config, tmp_path):
    """REGRESSION: UNKNOWN status should NOT finalize the run.

    UNKNOWN means we couldn't determine the status (transient error).
    The run might still be running, so we shouldn't finalize it.
    """
    run_id = _create_running_stage_run(test_db)

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "gce"

    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(supports_gpu=True, supports_spot=True, has_launch_delay=True)
    mock_backend.get_status.return_value = BackendStatus(
        status=RunStatus.UNKNOWN,
        message="API timeout",
    )

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
        run_backend=mock_backend,
    )

    executor._finalize_stage_run = MagicMock()

    executor.refresh_status_once(run_id)

    # Key assertion: should NOT call finalize
    executor._finalize_stage_run.assert_not_called()

    # Run should still be in RUNNING state
    row = test_db.get_stage_run(run_id)
    assert row is not None
    assert row["state"] == StageState.RUNNING.value
