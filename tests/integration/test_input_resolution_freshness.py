"""Test that input resolution prefers freshness over 'success' status."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import SignalDef, StageDef, StageRunStatus


@pytest.fixture
def executor(test_db, test_config, temp_dir):
    """Create a StageExecutor with mocked dependencies."""
    workspace_manager = MagicMock()
    workspace_manager.get_workspace_path.return_value = Path(temp_dir)
    workspace_manager.get_all_slots.return_value = [
        MagicMock(workspace="test_workspace", slot="w1"),
    ]
    workspace_manager.sync_and_version.return_value = ("v1", "abc123")

    pipeline_manager = MagicMock()

    executor = StageExecutor(
        db=test_db,
        config=test_config,
        workspace_manager=workspace_manager,
        pipeline_manager=pipeline_manager,
        project_root=Path(temp_dir),
        dataset_registry=MagicMock(),
    )
    return executor


def test_prefers_newest_unreviewed_over_old_success(test_db, executor):
    """
    Scenario:
    1. Run A (old) -> COMPLETED, outcome='success'
    2. Run B (new) -> COMPLETED, outcome=None (unreviewed)

    Expected:
    Resolution should pick Run B because it is newer, even though Run A is explicitly marked success.
    This supports rapid iteration.
    """
    workspace = "test_workspace"
    test_db.create_workspace_lineage(workspace, description="Test")
    test_db.create_version(workspace, "v1", "tag-1", "sha1", "run")

    # 1. Old successful run
    test_db.create_stage_run(
        stage_run_id="stage-old-success",
        workspace_name=workspace,
        version="v1",
        stage_name="preprocess",
    )
    with test_db._conn() as conn:
        conn.execute("UPDATE stage_runs SET started_at = ? WHERE id = ?", ("2025-01-01T10:00:00Z", "stage-old-success"))

    test_db.update_stage_run_status("stage-old-success", StageRunStatus.COMPLETED)
    test_db.update_run_outcome("stage-old-success", "success")
    test_db.add_signal_with_source("stage-old-success", "data", "directory", "gs://old/data")

    # 2. New unreviewed run (started later)
    test_db.create_stage_run(
        stage_run_id="stage-new-unreviewed",
        workspace_name=workspace,
        version="v1",
        stage_name="preprocess",
    )
    with test_db._conn() as conn:
        conn.execute(
            "UPDATE stage_runs SET started_at = ? WHERE id = ?", ("2025-01-01T12:00:00Z", "stage-new-unreviewed")
        )

    test_db.update_stage_run_status("stage-new-unreviewed", StageRunStatus.COMPLETED)
    # outcome is NULL by default
    test_db.add_signal_with_source("stage-new-unreviewed", "data", "directory", "gs://new/data")

    # Define consumer stage
    stage = StageDef(
        name="train", inputs={"data": SignalDef(name="data", type="directory", from_stage="preprocess")}, outputs={}
    )

    # Resolve
    inputs, sources, _ = executor._resolve_inputs(workspace, stage)

    # Verify: Should pick the NEW run
    assert inputs["data"] == "gs://new/data"
    assert sources["data"]["source_stage_run_id"] == "stage-new-unreviewed"


def test_skips_bad_results(test_db, executor):
    """
    Scenario:
    1. Run A (old) -> COMPLETED, outcome='success'
    2. Run B (new) -> COMPLETED, outcome='bad_results'

    Expected:
    Resolution should skip Run B and pick Run A.
    """
    workspace = "test_workspace"
    test_db.create_workspace_lineage(workspace, description="Test")
    test_db.create_version(workspace, "v1", "tag-1", "sha1", "run")

    # 1. Old successful run
    test_db.create_stage_run(
        stage_run_id="stage-old-success",
        workspace_name=workspace,
        version="v1",
        stage_name="preprocess",
    )
    with test_db._conn() as conn:
        conn.execute("UPDATE stage_runs SET started_at = ? WHERE id = ?", ("2025-01-01T10:00:00Z", "stage-old-success"))

    test_db.update_stage_run_status("stage-old-success", StageRunStatus.COMPLETED)
    test_db.update_run_outcome("stage-old-success", "success")
    test_db.add_signal_with_source("stage-old-success", "data", "directory", "gs://old/data")

    # 2. New BAD run
    test_db.create_stage_run(
        stage_run_id="stage-new-bad",
        workspace_name=workspace,
        version="v1",
        stage_name="preprocess",
    )
    with test_db._conn() as conn:
        conn.execute("UPDATE stage_runs SET started_at = ? WHERE id = ?", ("2025-01-01T12:00:00Z", "stage-new-bad"))

    test_db.update_stage_run_status("stage-new-bad", StageRunStatus.COMPLETED)
    test_db.update_run_outcome("stage-new-bad", "bad_results")
    test_db.add_signal_with_source("stage-new-bad", "data", "directory", "gs://new/data")

    # Define consumer stage
    stage = StageDef(
        name="train", inputs={"data": SignalDef(name="data", type="directory", from_stage="preprocess")}, outputs={}
    )

    # Resolve
    inputs, sources, _ = executor._resolve_inputs(workspace, stage)

    # Verify: Should pick the OLD success run (skipping the new bad one)
    assert inputs["data"] == "gs://old/data"
    assert sources["data"]["source_stage_run_id"] == "stage-old-success"
