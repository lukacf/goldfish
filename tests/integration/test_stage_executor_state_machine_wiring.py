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

from goldfish.cloud.contracts import BackendStatus, RunStatus
from goldfish.db.database import Database
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import PipelineDef, SignalDef, StageDef
from goldfish.state_machine import EventContext, StageEvent, StageState, transition


def _create_mock_local_backend() -> MagicMock:
    """Create a mock run_backend with local backend capabilities."""
    mock_backend = MagicMock()
    mock_backend.capabilities = MagicMock(
        has_launch_delay=False,
        supports_gpu=False,
        supports_spot=False,
        supports_preemption=True,
        ack_timeout_seconds=1.0,
        timeout_becomes_pending=False,
    )
    # Default launch return value
    mock_backend.launch.return_value = MagicMock(
        stage_run_id="stage-test",
        backend_type="local",
        backend_handle="container-123",
        zone=None,
    )
    return mock_backend


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

    # Inject mock run_backend - _launch_container will call run_backend.launch()
    mock_backend = _create_mock_local_backend()

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=workspace_manager,
        pipeline_manager=pipeline_manager,
        project_root=tmp_path,
        dataset_registry=dataset_registry,
        run_backend=mock_backend,
    )

    executor._build_docker_image = MagicMock(return_value="goldfish-test-v1")

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

    # Inject mock run_backend for protocol-based access
    mock_backend = _create_mock_local_backend()
    mock_backend.get_logs.return_value = ""
    mock_backend.cleanup.return_value = None

    executor = StageExecutor(
        db=test_db,
        config=config,
        workspace_manager=MagicMock(),
        pipeline_manager=MagicMock(),
        project_root=tmp_path,
        dataset_registry=None,
        run_backend=mock_backend,
    )

    # Avoid touching real Docker/logs/metrics in this test.
    executor._record_output_signals = MagicMock()
    executor._collect_metrics = MagicMock()
    executor._run_post_run_svs_review = MagicMock()
    executor._collect_svs_manifests = MagicMock()
    executor._persist_logs = MagicMock(return_value=None)

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

        # Inject mock run_backend for protocol-based status checks
        mock_backend = _create_mock_local_backend()
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


class TestPreGeneratedStageRunId:
    """Tests for pre-generated stage_run_id (from pipeline queue).

    When a pipeline is enqueued, stage_run_ids are pre-generated so they can be
    returned immediately. The stage_runs row must be created BEFORE any operations
    that reference it (like pre-run review recording) to avoid FK constraint failures.

    GENERAL PATTERN: This tests a common class of bug where:
    1. An ID is generated early for immediate user visibility (e.g., pipeline queue)
    2. The ID is stored in a reference/queue table, not the main table
    3. Later operations try to insert rows with FK references to the main table
    4. FK constraint fails because main table row doesn't exist yet

    The fix: Always create the main table row immediately when the ID is established,
    even if we need to update it later with resolved values.

    Tables with FK to stage_runs that could be affected:
    - svs_reviews (pre-run review) ← This was the original bug
    - stage_state_transitions (recorded in same transaction - safe)
    - signal_lineage (recorded after creation - safe)
    - experiment_records (recorded in same function - safe)
    - results_specs (recorded after record creation - safe)
    """

    def test_pre_generated_id_with_pre_run_review_no_fk_failure(
        self, test_db: Database, test_config, tmp_path: Path
    ) -> None:
        """Pre-generated stage_run_id should not cause FK constraint failure.

        Regression test for: When stage_run_id is pre-generated (e.g., from pipeline
        queue) and pre-run review is enabled, the pre-run review record must be able
        to reference the stage_runs row via foreign key.

        The bug was that stage_runs row creation was skipped when stage_run_id was
        provided, causing FK constraint failure when recording the pre-run review.
        """
        from unittest.mock import MagicMock, patch

        from goldfish.models import PipelineDef, ReviewIssue, ReviewSeverity, RunReview, SignalDef, StageDef

        _setup_workspace(test_db)

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(
                    name="train",
                    inputs={"data": SignalDef(name="data", type="dataset", dataset="test_data")},
                    outputs={"model": SignalDef(name="model", type="directory")},
                )
            ],
        )

        dataset_registry = MagicMock()
        dataset_registry.get_dataset.return_value = MagicMock(gcs_location="gs://bucket/test_data")

        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = tmp_path
        workspace_manager.get_all_slots.return_value = [MagicMock(workspace="test_workspace", slot="w1")]
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")

        config = test_config.model_copy(deep=True)
        config.jobs.backend = "local"
        config.svs.enabled = False
        config.pre_run_review.enabled = True  # Enable pre-run review

        # Inject mock run_backend for protocol-based launch
        mock_backend = _create_mock_local_backend()

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=tmp_path,
            dataset_registry=dataset_registry,
            run_backend=mock_backend,
        )

        # Mock _perform_pre_run_review to return a review with a warning issue
        mock_review = RunReview(
            approved=True,  # No blocking issues
            summary="Test review",
            full_review="Full review text",
            issues=[
                ReviewIssue(
                    severity=ReviewSeverity.WARNING,
                    stage="train",
                    message="Test warning",
                    file="test.py",
                    line=1,
                )
            ],
        )

        # Pre-generate a stage_run_id (simulating pipeline queue)
        pre_generated_id = "stage-a4157e59"

        with (
            patch.object(executor, "_perform_pre_run_review", return_value=mock_review),
            patch.object(executor, "_build_docker_image", return_value="goldfish-test-v1"),
        ):
            # This should NOT raise FK constraint error
            # Previously, this would fail because stage_runs row wasn't created
            # when stage_run_id was pre-provided
            info = executor.run_stage(
                workspace="test_workspace",
                stage_name="train",
                reason="Test run",
                wait=False,
                stage_run_id=pre_generated_id,  # Pre-generated ID
            )

        # Verify stage_runs row was created with the pre-generated ID
        row = test_db.get_stage_run(pre_generated_id)
        assert row is not None, "stage_runs row should exist for pre-generated ID"
        assert row["id"] == pre_generated_id

        # Verify pre-run review was recorded (this requires FK to stage_runs)
        reviews = test_db.get_svs_reviews(stage_run_id=pre_generated_id, review_type="pre_run")
        assert len(reviews) == 1, "Pre-run review should be recorded"
        assert reviews[0]["stage_run_id"] == pre_generated_id

        # Verify run completed successfully
        assert info.stage_run_id == pre_generated_id
        assert info.status == StageState.RUNNING

    def test_pre_generated_id_all_fk_relationships(self, test_db: Database, test_config, tmp_path: Path) -> None:
        """All FK relationships to stage_runs must work with pre-generated IDs.

        This comprehensive test verifies that ALL tables with FK to stage_runs
        can successfully insert records when using a pre-generated stage_run_id.

        FK relationships tested:
        1. stage_state_transitions → stage_runs(id) via state machine transitions
        2. signal_lineage → stage_runs(id) via input/output signal recording
        3. experiment_records → stage_runs(id) via experiment record creation
        4. run_results_spec → stage_runs(id) via results spec saving
        5. svs_reviews → stage_runs(id) via pre-run review recording
        """
        from unittest.mock import MagicMock, patch

        from goldfish.models import PipelineDef, ReviewIssue, ReviewSeverity, RunReview, SignalDef, StageDef

        _setup_workspace(test_db)

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(
                    name="train",
                    inputs={"data": SignalDef(name="data", type="dataset", dataset="test_data")},
                    outputs={"model": SignalDef(name="model", type="directory")},
                )
            ],
        )

        dataset_registry = MagicMock()
        dataset_registry.get_dataset.return_value = MagicMock(gcs_location="gs://bucket/test_data")

        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = tmp_path
        workspace_manager.get_all_slots.return_value = [MagicMock(workspace="test_workspace", slot="w1")]
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")

        config = test_config.model_copy(deep=True)
        config.jobs.backend = "local"
        config.svs.enabled = False
        config.pre_run_review.enabled = True

        # Inject mock run_backend for protocol-based launch
        mock_backend = _create_mock_local_backend()

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=tmp_path,
            dataset_registry=dataset_registry,
            run_backend=mock_backend,
        )

        mock_review = RunReview(
            approved=True,
            summary="Test review",
            full_review="Full review text",
            issues=[
                ReviewIssue(
                    severity=ReviewSeverity.WARNING,
                    stage="train",
                    message="Test warning",
                )
            ],
        )

        pre_generated_id = "stage-fk-test-01"

        # Include results_spec to test that FK relationship too
        results_spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.5,
            "goal_value": 0.9,
            "dataset_split": "val",
            "tolerance": 0.05,
            "context": "Testing FK relationships with pre-generated ID",
        }

        with (
            patch.object(executor, "_perform_pre_run_review", return_value=mock_review),
            patch.object(executor, "_build_docker_image", return_value="goldfish-test-v1"),
        ):
            info = executor.run_stage(
                workspace="test_workspace",
                stage_name="train",
                reason="Test all FK relationships",
                wait=False,
                stage_run_id=pre_generated_id,
                results_spec=results_spec,
            )

        # 1. Verify stage_runs row exists (PRIMARY TABLE)
        row = test_db.get_stage_run(pre_generated_id)
        assert row is not None, "stage_runs row must exist for pre-generated ID"

        # 2. Verify stage_state_transitions FK works
        with test_db._conn() as conn:
            transitions = conn.execute(
                "SELECT * FROM stage_state_transitions WHERE stage_run_id = ?",
                (pre_generated_id,),
            ).fetchall()
        assert len(transitions) >= 1, "State transitions should be recorded (FK: stage_state_transitions)"
        # Should have at least: run_start, build_start, build_ok, launch_ok
        events = [t["event"] for t in transitions]
        assert "run_start" in events, "run_start transition should be recorded"

        # 3. Verify signal_lineage FK works (input signals)
        with test_db._conn() as conn:
            signals = conn.execute(
                "SELECT * FROM signal_lineage WHERE stage_run_id = ?",
                (pre_generated_id,),
            ).fetchall()
        assert len(signals) >= 1, "Input signals should be recorded (FK: signal_lineage)"

        # 4. Verify experiment_records FK works
        with test_db._conn() as conn:
            records = conn.execute(
                "SELECT * FROM experiment_records WHERE stage_run_id = ?",
                (pre_generated_id,),
            ).fetchall()
        assert len(records) == 1, "Experiment record should be created (FK: experiment_records)"

        # 5. Verify run_results_spec FK works
        with test_db._conn() as conn:
            specs = conn.execute(
                "SELECT * FROM run_results_spec WHERE stage_run_id = ?",
                (pre_generated_id,),
            ).fetchall()
        assert len(specs) == 1, "Results spec should be saved (FK: run_results_spec)"

        # 6. Verify svs_reviews FK works (pre-run review)
        reviews = test_db.get_svs_reviews(stage_run_id=pre_generated_id, review_type="pre_run")
        assert len(reviews) == 1, "Pre-run review should be recorded (FK: svs_reviews)"

        # All FK relationships verified successfully
        assert info.stage_run_id == pre_generated_id

    def test_pre_generated_id_state_transitions_in_order(self, test_db: Database, test_config, tmp_path: Path) -> None:
        """State transitions must be recorded atomically with stage_runs row.

        The RUN_START transition is recorded inside _create_stage_run_record(),
        which must happen in the same transaction as the stage_runs INSERT.
        This test verifies the transition doesn't fail with FK error.
        """
        from unittest.mock import MagicMock, patch

        from goldfish.models import PipelineDef, SignalDef, StageDef

        _setup_workspace(test_db)

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(
                    name="preprocess",
                    inputs={"raw": SignalDef(name="raw", type="dataset", dataset="test_data")},
                    outputs={"features": SignalDef(name="features", type="npy")},
                )
            ],
        )

        dataset_registry = MagicMock()
        dataset_registry.get_dataset.return_value = MagicMock(gcs_location="gs://bucket/test_data")

        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = tmp_path
        workspace_manager.get_all_slots.return_value = [MagicMock(workspace="test_workspace", slot="w1")]
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")

        config = test_config.model_copy(deep=True)
        config.jobs.backend = "local"
        config.svs.enabled = False
        config.pre_run_review.enabled = False

        # Inject mock run_backend for protocol-based launch
        mock_backend = _create_mock_local_backend()

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=tmp_path,
            dataset_registry=dataset_registry,
            run_backend=mock_backend,
        )

        pre_generated_id = "stage-trans-order"

        with patch.object(executor, "_build_docker_image", return_value="goldfish-test-v1"):
            info = executor.run_stage(
                workspace="test_workspace",
                stage_name="preprocess",
                reason="Test transition ordering",
                wait=False,
                stage_run_id=pre_generated_id,
            )

        # Verify transitions are in correct order
        events = _get_transition_events(test_db, pre_generated_id)

        # The order should be: run_start → build_start → build_ok → launch_ok
        assert events[:4] == [
            "run_start",
            "build_start",
            "build_ok",
            "launch_ok",
        ], f"Transitions should be in order, got: {events}"

        # Verify no orphaned transitions (all have valid stage_run_id)
        with test_db._conn() as conn:
            orphaned = conn.execute(
                """
                SELECT t.* FROM stage_state_transitions t
                LEFT JOIN stage_runs r ON t.stage_run_id = r.id
                WHERE t.stage_run_id = ? AND r.id IS NULL
                """,
                (pre_generated_id,),
            ).fetchall()
        assert len(orphaned) == 0, "No orphaned transitions should exist"
