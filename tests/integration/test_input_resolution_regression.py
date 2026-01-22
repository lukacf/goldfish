"""Regression tests for input resolution fixes.

Covers:
1. Pipeline run prioritization (use upstream from same pipeline).
2. Input override using stage run ID (string).
3. Input override using explicit run/signal dictionary.
4. Robustness against dictionary-based overrides (no sqlite3 crash).
"""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from goldfish.db.database import Database
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import SignalDef, StageDef
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
    # Mock launch bits to avoid real subprocesses
    executor._build_docker_image = MagicMock(return_value="goldfish-test-v1")
    executor.local_executor.launch_container = MagicMock(return_value="container-1")

    return executor


def test_input_resolution_prioritizes_same_pipeline(test_db, executor):
    """Should prefer an upstream run from the same pipeline even if an older 'success' exists."""
    workspace = "test_workspace"
    test_db.create_workspace_lineage(workspace, description="Test")
    test_db.create_version(workspace, "v1", "tag-1", "sha1", "run")

    # 1. Create an OLD 'success' run
    test_db.create_stage_run(
        stage_run_id="stage-old-success",
        workspace_name=workspace,
        version="v1",
        stage_name="preprocess",
    )
    _transition_to_completed(test_db, "stage-old-success")
    test_db.update_run_outcome("stage-old-success", "success")
    test_db.add_signal_with_source("stage-old-success", "data", "directory", "gs://old-success/data")

    # 2. Create a NEWER 'unreviewed' run in a DIFFERENT pipeline
    test_db.create_stage_run(
        stage_run_id="stage-new-unreviewed",
        workspace_name=workspace,
        version="v1",
        stage_name="preprocess",
        pipeline_run_id="prun-other",
    )
    _transition_to_completed(test_db, "stage-new-unreviewed")
    test_db.add_signal_with_source("stage-new-unreviewed", "data", "directory", "gs://new-unreviewed/data")

    # 3. Create a NEW run in the TARGET pipeline
    pipeline_id = "prun-target"
    test_db.create_stage_run(
        stage_run_id="stage-target-upstream",
        workspace_name=workspace,
        version="v1",
        stage_name="preprocess",
        pipeline_run_id=pipeline_id,
    )
    _transition_to_completed(test_db, "stage-target-upstream")
    test_db.add_signal_with_source("stage-target-upstream", "data", "directory", "gs://target-pipeline/data")

    # Define stage that needs this input
    stage = StageDef(
        name="train", inputs={"data": SignalDef(name="data", type="directory", from_stage="preprocess")}, outputs={}
    )

    # Resolve inputs for the target pipeline
    inputs, sources, _ = executor._resolve_inputs(workspace, stage, pipeline_run_id=pipeline_id)

    # VERIFY: Should have picked the run from the same pipeline
    assert inputs["data"] == "gs://target-pipeline/data"
    assert sources["data"]["source_stage_run_id"] == "stage-target-upstream"


def test_input_override_by_run_id_string(test_db, executor):
    """Should resolve an input when an explicit stage-XXXX ID is provided as a string."""
    workspace = "test_workspace"
    test_db.create_workspace_lineage(workspace, description="Test")
    test_db.create_version(workspace, "v1", "tag-1", "sha1", "run")

    # Create an arbitrary run to point to
    test_db.create_stage_run(
        stage_run_id="stage-explicit-123",
        workspace_name=workspace,
        version="v1",
        stage_name="preprocess",
    )
    test_db.add_signal_with_source("stage-explicit-123", "tokens", "directory", "gs://explicit/tokens")

    stage = StageDef(
        name="train", inputs={"tokens": SignalDef(name="tokens", type="directory", from_stage="preprocess")}, outputs={}
    )

    # Resolve with override
    inputs, sources, _ = executor._resolve_inputs(workspace, stage, inputs_override={"tokens": "stage-explicit-123"})

    assert inputs["tokens"] == "gs://explicit/tokens"
    assert sources["tokens"]["source_stage_run_id"] == "stage-explicit-123"


def test_input_override_by_run_dict(test_db, executor):
    """Should resolve an input when an explicit dictionary with run and signal is provided."""
    workspace = "test_workspace"
    test_db.create_workspace_lineage(workspace, description="Test")
    test_db.create_version(workspace, "v1", "tag-1", "sha1", "run")

    # Create a run with multiple signals
    test_db.create_stage_run(
        stage_run_id="stage-multi-output",
        workspace_name=workspace,
        version="v1",
        stage_name="preprocess",
    )
    test_db.add_signal_with_source("stage-multi-output", "tokens_a", "directory", "gs://multi/a")
    test_db.add_signal_with_source("stage-multi-output", "tokens_ac", "directory", "gs://multi/ac")

    stage = StageDef(
        name="train", inputs={"tokens": SignalDef(name="tokens", type="directory", from_stage="preprocess")}, outputs={}
    )

    # Resolve with explicit dict override (pointing to tokens_ac instead of default 'tokens')
    inputs, sources, _ = executor._resolve_inputs(
        workspace, stage, inputs_override={"tokens": {"from_run": "stage-multi-output", "signal": "tokens_ac"}}
    )

    assert inputs["tokens"] == "gs://multi/ac"
    assert sources["tokens"]["source_stage_run_id"] == "stage-multi-output"


def test_no_sqlite_crash_on_dict_override(test_db, executor):
    """Regression: Ensuring dictionary-based overrides don't crash the source registry lookup."""
    workspace = "test_workspace"
    test_db.create_workspace_lineage(workspace, description="Test")

    stage = StageDef(
        name="train",
        # Input that is NOT 'from_stage' so it triggers different resolution logic
        inputs={"extra": SignalDef(name="extra", type="file")},
        outputs={},
    )

    # This dictionary override (without from_run) should NOT crash the DB lookup
    # and should just fall through to being used as a literal string.
    inputs, _, _ = executor._resolve_inputs(workspace, stage, inputs_override={"extra": {"some": "complex_object"}})

    # Should have converted to string safely
    assert "complex_object" in inputs["extra"]
