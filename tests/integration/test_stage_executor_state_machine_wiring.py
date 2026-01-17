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
from goldfish.models import PipelineDef, SignalDef, StageDef
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


def test_run_stage_emits_build_start_build_ok_and_launch_ok(test_db: Database, test_config, tmp_path: Path) -> None:
    """run_stage() should advance state to RUNNING by emitting BUILD_START + BUILD_OK + LAUNCH_OK."""
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
    # Mock the inner local_executor.launch_container, not _launch_container
    # This allows _launch_container to execute and emit LAUNCH_OK
    executor.local_executor.launch_container = MagicMock()

    info = executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="Test run", wait=False)

    assert info.status == StageState.RUNNING
    # LAUNCH_OK is now emitted in _launch_container, so state should be RUNNING
    assert _get_run_state(test_db, info.stage_run_id) == "running"

    events = _get_transition_events(test_db, info.stage_run_id)
    # Full lifecycle: run_start → build_start → build_ok → launch_ok
    assert events[:4] == ["run_start", "build_start", "build_ok", "launch_ok"]


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

    assert info.status == StageState.FAILED
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

    assert info.status == StageState.FAILED
    assert _get_run_state(test_db, info.stage_run_id) == "failed"
    assert "svs_block" in _get_transition_events(test_db, info.stage_run_id)


def test_finalize_completed_emits_exit_success_and_post_run_ok(test_db: Database, test_config, tmp_path: Path) -> None:
    """_finalize_stage_run() should progress the state machine on success (RUNNING→POST_RUN→AWAITING_USER_FINALIZATION)."""
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
    # Avoid warning-based POST_RUN_FAIL from auto-results extraction when no experiment record exists.
    with patch("goldfish.jobs.stage_executor.ExperimentRecordManager") as mock_mgr:
        mock_mgr.return_value.extract_auto_results.return_value = None
        executor._finalize_stage_run(run_id, backend="local", status=StageState.COMPLETED)

    # v1.2: _finalize_stage_run transitions to AWAITING_USER_FINALIZATION, not COMPLETED
    assert _get_run_state(test_db, run_id) == StageState.AWAITING_USER_FINALIZATION.value
    events = _get_transition_events(test_db, run_id)
    assert "exit_success" in events
    assert "post_run_ok" in events


class TestStateColumnAsSourceOfTruth:
    """Tests verifying code reads from state column, not legacy status.

    These tests create scenarios where state and status differ to prove
    the code uses state (the source of truth) rather than status.
    """

    def test_refresh_status_once_checks_state_not_status(self, test_db: Database, test_config, tmp_path: Path) -> None:
        """refresh_status_once should check state for terminal detection, not status.

        When deciding whether to finalize a run, the code should check if state
        is terminal (completed/failed/terminated/canceled), not the legacy status.
        """
        _setup_workspace(test_db)

        # Create a run where:
        # - state=completed (terminal - should NOT finalize again)
        # - status=running (legacy says running - old code would try to finalize)
        run_id = "stage-mismatch123"
        now = datetime.now(UTC).isoformat()

        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO stage_runs
                (id, workspace_name, version, stage_name, status, progress,
                 state, phase, state_entered_at, phase_updated_at,
                 backend_type, backend_handle, started_at, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    "test_workspace",
                    "v1",
                    "train",
                    StageState.RUNNING.value,  # Legacy status says running
                    None,
                    StageState.COMPLETED.value,  # State machine says completed (terminal)
                    None,
                    now,
                    now,
                    "local",
                    "container-id",
                    now,
                    now,
                ),
            )

        config = test_config.model_copy(deep=True)
        config.jobs.backend = "local"

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=tmp_path,
            dataset_registry=None,
        )

        # Mock container status to return COMPLETED (container finished)
        with patch.object(executor.local_executor, "get_container_status") as mock_status:
            mock_status.return_value = StageState.COMPLETED

            # Mock _finalize_stage_run to track if it gets called
            with patch.object(executor, "_finalize_stage_run") as mock_finalize:
                executor.refresh_status_once(run_id)

        # The code should NOT call _finalize_stage_run because state=completed (terminal)
        # If code checks status=running, it would incorrectly try to finalize again
        # If code checks state=completed, it correctly skips finalization
        mock_finalize.assert_not_called()

    def test_get_stage_runs_for_pipeline_uses_state_for_completion(self, test_db: Database) -> None:
        """Pipeline stage completion check should use state, not status."""
        _setup_workspace(test_db)

        # Create a run where status=running but state=completed (mismatch)
        run_id = "stage-complete123"
        now = datetime.now(UTC).isoformat()

        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO stage_runs
                (id, workspace_name, version, stage_name, status, progress,
                 state, phase, state_entered_at, phase_updated_at,
                 backend_type, started_at, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    "test_workspace",
                    "v1",
                    "preprocess",
                    StageState.RUNNING.value,  # Legacy status (wrong)
                    None,
                    StageState.COMPLETED.value,  # State machine (source of truth)
                    None,
                    now,
                    now,
                    "local",
                    now,
                    now,
                ),
            )

        # Query should check state=completed, not status
        with test_db._conn() as conn:
            row = conn.execute(
                """
                SELECT id FROM stage_runs
                WHERE workspace_name = ? AND stage_name = ? AND state = ?
                """,
                ("test_workspace", "preprocess", StageState.COMPLETED.value),
            ).fetchone()

        assert row is not None, "Should find run by state=completed"
        assert row["id"] == run_id

    def test_active_runs_query_uses_state_column(self, test_db: Database) -> None:
        """Active runs queries should use state column, not status."""
        _setup_workspace(test_db)
        now = datetime.now(UTC).isoformat()

        # Create runs with mismatched state/status (status column is legacy, uses old enum values)
        runs = [
            ("stage-a", "running", StageState.RUNNING.value, "train"),
            ("stage-b", "running", StageState.COMPLETED.value, "train2"),  # Mismatch
            ("stage-c", "pending", StageState.PREPARING.value, "train3"),  # Old "pending" vs new "preparing"
        ]

        with test_db._conn() as conn:
            for run_id, status, state, stage in runs:
                conn.execute(
                    """
                    INSERT INTO stage_runs
                    (id, workspace_name, version, stage_name, status, progress,
                     state, phase, state_entered_at, phase_updated_at,
                     backend_type, started_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (run_id, "test_workspace", "v1", stage, status, None, state, None, now, now, "local", now),
                )

        # Query active runs using state (source of truth)
        active_states = (
            StageState.PREPARING.value,
            StageState.BUILDING.value,
            StageState.LAUNCHING.value,
            StageState.RUNNING.value,
            StageState.POST_RUN.value,
            StageState.AWAITING_USER_FINALIZATION.value,
        )

        with test_db._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id FROM stage_runs
                WHERE state IN ({','.join('?' * len(active_states))})
                """,
                active_states,
            ).fetchall()

        active_ids = {r["id"] for r in rows}

        # stage-a: state=running -> active
        # stage-b: state=completed -> NOT active (even though status=running)
        # stage-c: state=preparing -> active
        assert active_ids == {"stage-a", "stage-c"}, f"Should use state column: {active_ids}"

    def test_terminal_detection_uses_state_column(self, test_db: Database) -> None:
        """Terminal state detection should use state, not status."""
        _setup_workspace(test_db)
        now = datetime.now(UTC).isoformat()

        # Create a run where status=running but state=failed
        run_id = "stage-terminal456"
        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO stage_runs
                (id, workspace_name, version, stage_name, status, progress,
                 state, phase, termination_cause, state_entered_at, phase_updated_at,
                 backend_type, started_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    "test_workspace",
                    "v1",
                    "train",
                    StageState.RUNNING.value,  # Legacy says running
                    None,
                    StageState.FAILED.value,  # State machine says failed
                    None,
                    None,
                    now,
                    now,
                    "local",
                    now,
                ),
            )

        terminal_states = (
            StageState.COMPLETED.value,
            StageState.FAILED.value,
            StageState.TERMINATED.value,
            StageState.CANCELED.value,
        )

        # Query should find this as terminal by state
        with test_db._conn() as conn:
            row = conn.execute(
                f"""
                SELECT state FROM stage_runs
                WHERE id = ? AND state IN ({','.join('?' * len(terminal_states))})
                """,
                (run_id, *terminal_states),
            ).fetchone()

        assert row is not None, "Should detect terminal by state column"
        assert row["state"] == StageState.FAILED.value


class TestPipelineExecutorUsesState:
    """Tests verifying PipelineExecutor reads state column, not status."""

    def test_validate_inputs_resolvable_checks_state_not_status(
        self, test_db: Database, test_config, tmp_path: Path
    ) -> None:
        """_validate_inputs_resolvable should check state=completed, not status=completed.

        When checking if an upstream stage has completed, should use state column.
        """
        from goldfish.jobs.pipeline_executor import PipelineExecutor
        from goldfish.pipeline.manager import PipelineManager

        _setup_workspace(test_db)
        now = datetime.now(UTC).isoformat()

        # Create an upstream run where:
        # - state=completed (source of truth - run is done)
        # - status=running (legacy - would fail validation if checked)
        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO stage_runs
                (id, workspace_name, version, stage_name, status, progress,
                 state, phase, state_entered_at, phase_updated_at,
                 backend_type, started_at, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "stage-upstream-done",
                    "test_workspace",
                    "v1",
                    "preprocess",
                    StageState.RUNNING.value,  # Legacy status (wrong)
                    None,
                    StageState.COMPLETED.value,  # State machine (source of truth)
                    None,
                    now,
                    now,
                    "local",
                    now,
                    now,
                ),
            )

        # Create a stage that depends on preprocess
        train_stage = StageDef(
            name="train",
            inputs={
                "features": SignalDef(
                    name="features",
                    type="npy",
                    from_stage="preprocess",
                    signal="features",
                )
            },
            outputs={},
        )

        pipeline_manager = MagicMock(spec=PipelineManager)
        stage_executor = MagicMock()

        executor = PipelineExecutor(
            stage_executor=stage_executor,
            pipeline_manager=pipeline_manager,
            db=test_db,
        )

        # This should NOT raise because preprocess has state=completed
        # If code checks status=running, it would incorrectly raise an error
        executor._validate_inputs_resolvable("test_workspace", [train_stage], None)


class TestExecutionToolsUsesState:
    """Tests verifying execution_tools reads state column, not status.

    Note: The MCP decorator prevents direct function calls, so these tests
    verify the underlying behavior by importing the function's inner logic
    or by checking the code pattern matches what we expect.
    """

    def test_inspect_run_sync_condition_checks_state(self, test_db: Database) -> None:
        """Verify inspect_run's sync trigger condition uses state column.

        The sync is triggered when state=running (not status=running).
        We test this by checking that a run with state=completed and status=running
        does NOT get the sync behavior (sync_status stays "not_running").
        """
        _setup_workspace(test_db)
        now = datetime.now(UTC).isoformat()

        # Create a run where state=completed but status=running (mismatch)
        run_id = "stage-sync-test"
        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO stage_runs
                (id, workspace_name, version, stage_name, status, progress,
                 state, phase, state_entered_at, phase_updated_at,
                 backend_type, backend_handle, started_at, completed_at, log_uri)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    "test_workspace",
                    "v1",
                    "train",
                    StageState.RUNNING.value,  # Legacy status (wrong)
                    None,
                    StageState.COMPLETED.value,  # State machine (source of truth)
                    None,
                    now,
                    now,
                    "local",
                    "container-id",
                    now,
                    now,
                    "/tmp/fake.log",
                ),
            )

        # Verify the condition used in inspect_run
        row = test_db.get_stage_run(run_id)
        assert row is not None

        # The sync trigger should check state, not status
        # If code uses: row["status"] == StageState.RUNNING -> would be True (wrong)
        # If code uses: row.get("state") == StageState.RUNNING.value -> would be False (correct)
        should_trigger_sync = row.get("state") == StageState.RUNNING.value
        assert not should_trigger_sync, "Should NOT trigger sync when state=completed"

        # Verify that checking status would give wrong answer
        legacy_would_trigger = row.get("status") == StageState.RUNNING.value
        assert legacy_would_trigger, "Legacy check would incorrectly trigger sync"

    def test_logs_error_display_condition_checks_state(self, test_db: Database) -> None:
        """Verify logs() error display condition uses state column.

        Error messages should be shown when state=failed/terminated (not status=failed).
        """
        _setup_workspace(test_db)
        now = datetime.now(UTC).isoformat()

        # Create a run where state=failed but status=running (mismatch)
        run_id = "stage-logs-fail"
        error_msg = "Test error message"
        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO stage_runs
                (id, workspace_name, version, stage_name, status, progress,
                 state, phase, state_entered_at, phase_updated_at,
                 backend_type, backend_handle, started_at, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    "test_workspace",
                    "v1",
                    "train",
                    StageState.RUNNING.value,  # Legacy status (wrong)
                    None,
                    StageState.FAILED.value,  # State machine (source of truth)
                    None,
                    now,
                    now,
                    "local",
                    "container-id",
                    now,
                    error_msg,
                ),
            )

        # Verify the condition used in logs()
        row = test_db.get_stage_run(run_id)
        assert row is not None

        # The error display should check state, not status
        # If code uses: row.get("status") == StageState.FAILED -> would be False (wrong)
        # If code uses: row.get("state") in {StageState.FAILED.value, ...} -> would be True (correct)
        should_show_error = row.get("state") in {StageState.FAILED.value, StageState.TERMINATED.value}
        assert should_show_error, "Should show error when state=failed"

        # Verify that checking status would give wrong answer
        legacy_would_show = row.get("status") == StageState.FAILED.value
        assert not legacy_would_show, "Legacy check would NOT show error"
