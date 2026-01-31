"""Integration tests for full lineage flow improvements."""

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np

from goldfish.db.database import Database
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.lineage.manager import LineageManager
from goldfish.models import PipelineDef, SignalDef, StageDef
from goldfish.state_machine import EventContext, StageEvent, transition


def _transition_to_completed(db: Database, stage_run_id: str) -> None:
    """Transition a stage run to COMPLETED state via state machine (v1.2 lifecycle)."""
    ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
    transition(db, stage_run_id, StageEvent.BUILD_START, ctx)
    transition(db, stage_run_id, StageEvent.BUILD_OK, ctx)
    transition(db, stage_run_id, StageEvent.LAUNCH_OK, ctx)
    success_ctx = EventContext(timestamp=datetime.now(UTC), source="executor", exit_code=0, exit_code_exists=True)
    transition(db, stage_run_id, StageEvent.EXIT_SUCCESS, success_ctx)
    transition(db, stage_run_id, StageEvent.POST_RUN_OK, ctx)
    # v1.2: Now need USER_FINALIZE to reach COMPLETED
    finalize_ctx = EventContext(timestamp=datetime.now(UTC), source="mcp_tool")
    transition(db, stage_run_id, StageEvent.USER_FINALIZE, finalize_ctx)


def test_full_lineage_flow(test_db, test_config, temp_dir, mocker):
    """Verify that lineage tracking, fingerprinting, and tools work end-to-end."""
    # Setup
    workspace = "test-ws"
    test_db.create_workspace_lineage(workspace, description="test")
    test_db.create_version(workspace, "v1", "v1", "sha1", "manual")

    # 1. Preprocess Stage Definition
    preprocess_def = StageDef(
        name="preprocess", inputs={}, outputs={"features": SignalDef(name="features", type="npy")}
    )

    # Mock WorkspaceManager to avoid real git ops
    mock_wm = MagicMock()
    mock_wm.get_all_slots.return_value = [MagicMock(workspace=workspace, slot="w1")]
    mock_wm.sync_and_version.return_value = ("v1", "sha1")
    mock_wm.get_workspace_path.return_value = temp_dir

    # Mock PipelineManager
    mock_pm = MagicMock()
    mock_pm.get_pipeline.return_value = PipelineDef(name="default", stages=[preprocess_def])

    executor = StageExecutor(
        db=test_db,
        config=test_config,
        workspace_manager=mock_wm,
        pipeline_manager=mock_pm,
        project_root=temp_dir,
    )

    # Mocking execution steps
    executor._build_docker_image = MagicMock(return_value=("img-v1", "0" * 64))
    executor._launch_container = MagicMock()

    # Run preprocess
    run_info = executor.run_stage(workspace, "preprocess", reason="prep")
    run_id_prep = run_info.stage_run_id

    # Create fake output file for fingerprinting
    # run_dir = self.dev_repo / ".goldfish" / "runs" / stage_run_id
    # dev_repo is set via test_config.dev_repo_path (which is temp_dir / "test-dev" in conftest)
    dev_repo = Path(test_config.dev_repo_path)
    out_dir = dev_repo / ".goldfish" / "runs" / run_id_prep / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    features_path = out_dir / "features.npy"
    np.save(features_path, np.array([1, 2, 3]))

    # Manually trigger output recording
    executor._record_output_signals(run_id_prep, workspace, "preprocess")

    # Verify fingerprinting in DB
    signals = test_db.list_signals(stage_run_id=run_id_prep)
    assert len(signals) == 1
    assert signals[0]["stats_json"] is not None
    stats = json.loads(signals[0]["stats_json"])
    assert stats["shape"] == [3]
    assert stats["type"] == "tensor"

    # Mark as completed so train stage can use it
    _transition_to_completed(test_db, run_id_prep)
    test_db.update_stage_run_outcome(run_id_prep, outcome="success")

    # 2. Train Stage Definition
    train_def = StageDef(
        name="train",
        inputs={"data": SignalDef(name="data", type="npy", from_stage="preprocess", signal="features")},
        outputs={},
    )
    mock_pm.get_pipeline.return_value = PipelineDef(name="default", stages=[preprocess_def, train_def])

    # Run train
    run_info_train = executor.run_stage(workspace, "train", reason="train")
    run_id_train = run_info_train.stage_run_id

    # Verify downstream tracking in DB
    # Inputs for a run are recorded with stage_run_id = the consuming run
    signals_in = test_db.list_signals(stage_run_id=run_id_train)
    assert len(signals_in) == 1
    assert signals_in[0]["signal_type"] == "input"
    assert signals_in[0]["source_stage_run_id"] == run_id_prep

    # Verify downstream tracking in LineageManager
    lm = LineageManager(test_db, mock_wm)
    provenance = lm.get_run_provenance(run_id_prep)
    assert len(provenance["downstream"]) == 1
    assert provenance["downstream"][0]["stage_run_id"] == run_id_train

    # Verify execution_tools.inspect_run (contains provenance with both inputs and downstream)
    from goldfish.server_tools import execution_tools

    mocker.patch("goldfish.server_tools.execution_tools._get_db", return_value=test_db)
    # Configure mock to return None so workspace string is used directly
    mock_wm.get_workspace_for_slot.return_value = None
    mocker.patch("goldfish.server_tools.execution_tools._get_workspace_manager", return_value=mock_wm)
    # Using 'include=["provenance"]' to verify consolidation
    inspect_result = execution_tools.inspect_run.fn(run_id=run_id_prep, include=["provenance"])
    provenance = inspect_result["provenance"]

    # Downstream should show the train run
    assert len(provenance["downstream"]) == 1
    assert provenance["downstream"][0]["stage_run_id"] == run_id_train
    assert provenance["downstream"][0]["stage"] == "train"
