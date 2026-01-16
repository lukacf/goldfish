"""Integration tests: StageExecutor must drive the state machine (not legacy status/progress).

These tests ensure StageExecutor emits state machine events for the main lifecycle:
- PREPARING → BUILDING (BUILD_START)
- BUILDING → LAUNCHING (BUILD_OK)
- PREPARING → FAILED (PREPARE_FAIL / SVS_BLOCK)
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from goldfish.db.database import Database
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import PipelineDef, SignalDef, StageDef, StageRunStatus
from goldfish.state_machine import EventContext, StageEvent, StageState, transition


def _setup_workspace(db: Database, workspace: str = "test_workspace", version: str = "v1") -> None:
    db.create_workspace_lineage(workspace, description="Test")
    db.create_version(workspace, version, f"{workspace}-{version}", "abc123", "run")


def _get_run_state(db: Database, run_id: str) -> str | None:
    with db._conn() as conn:
        row = conn.execute("SELECT state FROM stage_runs WHERE id = ?", (run_id,)).fetchone()
        return row["state"] if row else None


def _get_transition_events(db: Database, run_id: str) -> list[str]:
    with db._conn() as conn:
        rows = conn.execute(
            "SELECT event FROM stage_state_transitions WHERE stage_run_id = ? ORDER BY id",
            (run_id,),
        ).fetchall()
        return [r["event"] for r in rows]


def test_run_stage_emits_build_start_and_build_ok(test_db: Database, test_config, tmp_path: Path) -> None:
    """run_stage() should advance state to LAUNCHING by emitting BUILD_START + BUILD_OK."""
    _setup_workspace(test_db)

    pipeline_manager = MagicMock()
    pipeline_manager.get_pipeline.return_value = PipelineDef(
        name="test_pipeline",
        stages=[
            StageDef(
                name="preprocess",
                inputs={"raw_data": SignalDef(name="raw_data", type="dataset", dataset="test_data")},
                outputs={"features": SignalDef(name="features", type="npy")},
            )
        ],
    )

    dataset_registry = MagicMock()
    dataset_registry.get_dataset.return_value = MagicMock(gcs_location="gs://bucket/datasets/test_data")

    workspace_manager = MagicMock()
    workspace_manager.get_workspace_path.return_value = tmp_path
    workspace_manager.get_all_slots.return_value = [MagicMock(workspace="test_workspace", slot="w1")]
    workspace_manager.sync_and_version.return_value = ("v1", "abc123")

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "local"
    config.svs.enabled = False
    config.pre_run_review.enabled = False

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=workspace_manager,
        pipeline_manager=pipeline_manager,
        project_root=tmp_path,
        dataset_registry=dataset_registry,
    )

    executor._build_docker_image = MagicMock(return_value="goldfish-test-v1")
    executor._launch_container = MagicMock()

    info = executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="Test run", wait=False)

    assert info.status == StageRunStatus.RUNNING
    assert _get_run_state(test_db, info.stage_run_id) == "launching"

    events = _get_transition_events(test_db, info.stage_run_id)
    assert events[:3] == ["run_start", "build_start", "build_ok"]


def test_preflight_blocked_run_emits_prepare_fail(test_db: Database, test_config, tmp_path: Path) -> None:
    """SVS preflight failures should transition PREPARE_FAIL (PREPARING → FAILED)."""
    _setup_workspace(test_db)

    pipeline_manager = MagicMock()
    pipeline_manager.get_pipeline.return_value = PipelineDef(
        name="test_pipeline",
        stages=[StageDef(name="train", inputs={}, outputs={})],
    )

    workspace_manager = MagicMock()
    workspace_manager.get_workspace_path.return_value = tmp_path
    workspace_manager.get_all_slots.return_value = [MagicMock(workspace="test_workspace", slot="w1")]
    workspace_manager.sync_and_version.return_value = ("v1", "abc123")

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "local"
    config.svs.enabled = True
    config.pre_run_review.enabled = False

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=workspace_manager,
        pipeline_manager=pipeline_manager,
        project_root=tmp_path,
        dataset_registry=None,
    )

    with patch(
        "goldfish.jobs.stage_executor.validate_pipeline_run",
        return_value={"validation_errors": ["bad config"], "warnings": []},
    ):
        info = executor.run_stage(workspace="test_workspace", stage_name="train", reason="Test run", wait=False)

    assert info.status == StageRunStatus.FAILED
    assert _get_run_state(test_db, info.stage_run_id) == "failed"
    assert "prepare_fail" in _get_transition_events(test_db, info.stage_run_id)


def test_pre_run_review_block_emits_svs_block(test_db: Database, test_config, tmp_path: Path) -> None:
    """Blocking pre-run review should transition SVS_BLOCK (PREPARING → FAILED)."""
    _setup_workspace(test_db)

    pipeline_manager = MagicMock()
    pipeline_manager.get_pipeline.return_value = PipelineDef(
        name="test_pipeline",
        stages=[StageDef(name="train", inputs={}, outputs={})],
    )

    workspace_manager = MagicMock()
    workspace_manager.get_workspace_path.return_value = tmp_path
    workspace_manager.get_all_slots.return_value = [MagicMock(workspace="test_workspace", slot="w1")]
    workspace_manager.sync_and_version.return_value = ("v1", "abc123")

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "local"
    config.svs.enabled = False
    config.pre_run_review.enabled = True

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=workspace_manager,
        pipeline_manager=pipeline_manager,
        project_root=tmp_path,
        dataset_registry=None,
    )

    blocking_review = MagicMock()
    blocking_review.has_blocking_issues = True
    blocking_review.summary = "bad"
    blocking_review.error_count = 0
    blocking_review.issues = []

    with patch.object(executor, "_perform_pre_run_review", return_value=blocking_review):
        with patch.object(executor, "_record_pre_run_review"):
            info = executor.run_stage(workspace="test_workspace", stage_name="train", reason="Test run", wait=False)

    assert info.status == StageRunStatus.FAILED
    assert _get_run_state(test_db, info.stage_run_id) == "failed"
    assert "svs_block" in _get_transition_events(test_db, info.stage_run_id)


def test_finalize_completed_emits_exit_success_and_finalize_ok(test_db: Database, test_config, tmp_path: Path) -> None:
    """_finalize_stage_run() should complete the state machine on success (RUNNING→FINALIZING→COMPLETED)."""
    _setup_workspace(test_db)

    run_id = "stage-acde1234"
    test_db.create_stage_run(
        stage_run_id=run_id,
        workspace_name="test_workspace",
        version="v1",
        stage_name="train",
        backend_type="local",
        backend_handle=run_id,
    )

    # Move to RUNNING via state machine
    transition(test_db, run_id, StageEvent.BUILD_START, EventContext(timestamp=datetime.now(UTC), source="executor"))
    transition(test_db, run_id, StageEvent.BUILD_OK, EventContext(timestamp=datetime.now(UTC), source="executor"))
    transition(test_db, run_id, StageEvent.LAUNCH_OK, EventContext(timestamp=datetime.now(UTC), source="executor"))
    assert _get_run_state(test_db, run_id) == StageState.RUNNING.value

    config = test_config.model_copy(deep=True)
    config.jobs.backend = "local"
    config.svs.enabled = False
    config.pre_run_review.enabled = False

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
    )

    # Avoid touching real Docker/logs/metrics in this test.
    executor._record_output_signals = MagicMock()
    executor._collect_metrics = MagicMock()
    executor._run_post_run_svs_review = MagicMock()
    executor._collect_svs_manifests = MagicMock()
    executor._persist_logs = MagicMock(return_value=None)
    executor.local_executor.get_container_logs = MagicMock(return_value="")
    executor.local_executor.remove_container = MagicMock()
    # Avoid warning-based FINALIZE_FAIL from auto-results extraction when no experiment record exists.
    with patch("goldfish.jobs.stage_executor.ExperimentRecordManager") as mock_mgr:
        mock_mgr.return_value.extract_auto_results.return_value = None
        executor._finalize_stage_run(run_id, backend="local", status=StageRunStatus.COMPLETED)

    assert _get_run_state(test_db, run_id) == StageState.COMPLETED.value
    events = _get_transition_events(test_db, run_id)
    assert "exit_success" in events
    assert "finalize_ok" in events
