"""Tests for PipelineExecutor - TDD Phase 4."""

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from goldfish.jobs.pipeline_executor import PipelineExecutor
from goldfish.models import PipelineDef, PipelineStatus, StageDef, StageRunInfo, StageRunStatus

# Track all PipelineExecutors created during tests for cleanup
_executors_to_shutdown: list[PipelineExecutor] = []
_original_init = PipelineExecutor.__init__


def _tracking_init(self, *args, **kwargs):
    _original_init(self, *args, **kwargs)
    _executors_to_shutdown.append(self)


# Monkey-patch to track executors
PipelineExecutor.__init__ = _tracking_init


@pytest.fixture(autouse=True)
def cleanup_pipeline_executors():
    """Ensure all PipelineExecutors are shutdown after each test."""
    _executors_to_shutdown.clear()
    yield
    # Shutdown all executors created during the test
    for executor in _executors_to_shutdown:
        executor.shutdown()
    _executors_to_shutdown.clear()


class DummyStageExecutor:
    """Lightweight stage executor used to exercise queue semantics in tests."""

    def __init__(self, db, version: str = "v1"):
        self.db = db
        self.calls: list[str] = []
        self.call_kwargs: list[dict] = []
        self.version = version
        self._seeded_workspaces: set[str] = set()

    def _ensure_workspace_version(self, workspace: str):
        if workspace in self._seeded_workspaces:
            return
        now = datetime.now(UTC).isoformat()
        with self.db._conn() as conn:
            conn.execute("PRAGMA busy_timeout = 5000")
            conn.execute(
                "INSERT OR IGNORE INTO workspace_lineage (workspace_name, parent_workspace, parent_version, created_at) VALUES (?, NULL, NULL, ?)",
                (workspace, now),
            )
            conn.execute(
                "INSERT OR IGNORE INTO workspace_versions (workspace_name, version, git_tag, git_sha, created_at, created_by, description) VALUES (?, ?, ?, ?, ?, 'test', 'seed for tests')",
                (workspace, self.version, f"{workspace}-{self.version}", "deadbeef", now),
            )
        self._seeded_workspaces.add(workspace)

    def run_stage(
        self,
        workspace: str,
        stage_name: str,
        pipeline_name: str | None = None,
        pipeline_run_id: str | None = None,
        config_override=None,
        inputs_override=None,
        reason: str | None = None,
        reason_structured: dict | None = None,
        wait: bool = False,
        skip_review: bool = False,
        experiment_group: str | None = None,
        results_spec: dict | None = None,
    ) -> StageRunInfo:
        self._ensure_workspace_version(workspace)
        stage_run_id = f"stage-{len(self.calls) + 1}"
        self.calls.append(stage_name)
        self.db.create_stage_run(
            stage_run_id=stage_run_id,
            workspace_name=workspace,
            version=self.version,
            stage_name=stage_name,
            pipeline_run_id=pipeline_run_id,
            pipeline_name=pipeline_name,
            config=config_override or {},
            inputs={},
            reason=reason_structured,  # Store structured reason in DB
            profile=None,
            hints=None,
            backend_type="local",
            backend_handle=stage_run_id,
        )
        # mark running immediately
        self.db.update_stage_run_status(stage_run_id, status=StageRunStatus.RUNNING)
        self.call_kwargs.append(
            {
                "workspace": workspace,
                "stage_name": stage_name,
                "pipeline_name": pipeline_name,
                "reason": reason,
                "reason_structured": reason_structured,
                "wait": wait,
            }
        )
        return StageRunInfo(
            stage_run_id=stage_run_id,
            workspace=workspace,
            version=self.version,
            stage=stage_name,
            pipeline=pipeline_name,
            pipeline_run_id=pipeline_run_id,
            status=StageRunStatus.RUNNING,
        )

    def refresh_status_once(self, stage_run_id: str):  # pragma: no cover - not used in tests
        return StageRunStatus.RUNNING


class TestRunFullPipeline:
    """Test running complete pipeline (all stages)."""

    def test_run_pipeline_executes_all_stages(self, test_db):
        """Should execute all stages in sequence."""
        # Setup
        pipeline_def = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(name="preprocess", inputs={}, outputs={}),
                StageDef(name="tokenize", inputs={}, outputs={}),
                StageDef(name="train", inputs={}, outputs={}),
            ],
        )

        # Mock stage executor
        stage_executor = MagicMock()
        stage_executor.run_stage.side_effect = [
            StageRunInfo(
                stage_run_id="stage-1",
                workspace="test_ws",
                version="v1",
                stage="preprocess",
                status=StageRunStatus.RUNNING,
            ),
            StageRunInfo(
                stage_run_id="stage-2",
                workspace="test_ws",
                version="v1",
                stage="tokenize",
                status=StageRunStatus.RUNNING,
            ),
            StageRunInfo(
                stage_run_id="stage-3", workspace="test_ws", version="v1", stage="train", status=StageRunStatus.RUNNING
            ),
        ]

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def

        executor = PipelineExecutor(stage_executor=stage_executor, pipeline_manager=pipeline_manager, db=test_db)

        # Execute
        result = executor.run_stages(
            workspace="test_ws",
            reason="Test full pipeline",
            async_mode=False,
        )

        stage_runs = result["stage_runs"]
        assert len(stage_runs) == 3
        assert [r["stage"] for r in stage_runs] == ["preprocess", "tokenize", "train"]
        assert stage_executor.run_stage.call_count == 3

    def test_run_pipeline_applies_config_overrides(self, test_db):
        """Should apply stage-specific config overrides."""
        # Setup
        pipeline_def = PipelineDef(
            name="test_pipeline",
            stages=[StageDef(name="preprocess", inputs={}, outputs={}), StageDef(name="train", inputs={}, outputs={})],
        )

        stage_executor = MagicMock()
        stage_executor.run_stage.return_value = StageRunInfo(
            stage_run_id="stage-1", workspace="test_ws", version="v1", stage="preprocess", status=StageRunStatus.RUNNING
        )

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def

        executor = PipelineExecutor(stage_executor=stage_executor, pipeline_manager=pipeline_manager, db=test_db)

        # Execute with config overrides
        config_override = {"preprocess": {"BATCH_SIZE": "128"}, "train": {"EPOCHS": "20"}}

        executor.run_stages(
            workspace="test_ws",
            config_override=config_override,
            reason="Test overrides",
            async_mode=False,
        )

        # Verify config passed to each stage
        assert stage_executor.run_stage.call_count == 2
        # First call (preprocess)
        assert stage_executor.run_stage.call_args_list[0][1]["config_override"] == {"BATCH_SIZE": "128"}
        # Second call (train)
        assert stage_executor.run_stage.call_args_list[1][1]["config_override"] == {"EPOCHS": "20"}


class TestRunSpecificStages:
    """Test running specific pipeline stages."""

    def test_run_specific_stages(self, test_db):
        """Should run only the specified stages."""
        # Setup
        pipeline_def = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(name="preprocess", inputs={}, outputs={}),
                StageDef(name="tokenize", inputs={}, outputs={}),
                StageDef(name="train", inputs={}, outputs={}),
                StageDef(name="evaluate", inputs={}, outputs={}),
            ],
        )

        stage_executor = MagicMock()
        stage_executor.run_stage.side_effect = [
            StageRunInfo(
                stage_run_id="stage-1",
                workspace="test_ws",
                version="v1",
                stage="tokenize",
                status=StageRunStatus.RUNNING,
            ),
            StageRunInfo(
                stage_run_id="stage-2", workspace="test_ws", version="v1", stage="train", status=StageRunStatus.RUNNING
            ),
        ]

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def

        executor = PipelineExecutor(stage_executor=stage_executor, pipeline_manager=pipeline_manager, db=test_db)

        # Execute - run specific stages
        result = executor.run_stages(
            workspace="test_ws",
            stages=["tokenize", "train"],
            reason="Test partial run",
            async_mode=False,
        )

        # Verify
        stage_runs = result["stage_runs"]
        assert len(stage_runs) == 2
        assert [r["stage"] for r in stage_runs] == ["tokenize", "train"]

    def test_run_stages_raises_on_unknown_stage(self, test_db):
        """Should raise error if stage not found in pipeline."""
        # Setup
        pipeline_def = PipelineDef(
            name="test_pipeline",
            stages=[StageDef(name="preprocess", inputs={}, outputs={}), StageDef(name="train", inputs={}, outputs={})],
        )

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def

        executor = PipelineExecutor(stage_executor=MagicMock(), pipeline_manager=pipeline_manager, db=test_db)

        # Execute - should raise
        with pytest.raises(ValueError, match="not found in pipeline"):
            executor.run_stages(workspace="test_ws", stages=["nonexistent"], reason="Test")


class TestAsyncQueueSemantics:
    def _make_executor(self, test_db, pipeline_def, monkeypatch):
        stage_executor = DummyStageExecutor(test_db)
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def

        fake_pool = MagicMock()
        monkeypatch.setattr(PipelineExecutor, "_pool", fake_pool)

        executor = PipelineExecutor(
            stage_executor=stage_executor,
            pipeline_manager=pipeline_manager,
            db=test_db,
        )
        return executor, stage_executor, pipeline_manager, fake_pool

    def test_run_pipeline_async_returns_immediately(self, test_db, monkeypatch):
        pipeline_def = PipelineDef(
            name="train",
            stages=[
                StageDef(name="prep", inputs={}, outputs={}),
                StageDef(name="train", inputs={}, outputs={}),
            ],
        )

        executor, stage_executor, pipeline_manager, fake_pool = self._make_executor(test_db, pipeline_def, monkeypatch)

        result = executor.run_stages(
            workspace="ws",
            pipeline_name="train",
            async_mode=True,
            reason="Testing async",
        )

        assert "pipeline_run_id" in result
        assert "queued" in result
        assert len(result["queued"]) == 2  # all stages queued
        assert result["stages_queued"] == ["prep", "train"]
        fake_pool.submit.assert_called_once()
        # Worker hasn't run yet, so no stages have been executed
        assert stage_executor.calls == []

    def test_pipeline_queue_respects_dependencies(self, test_db, monkeypatch):
        pipeline_def = PipelineDef(
            name="train",
            stages=[
                StageDef(name="prep", inputs={}, outputs={}),
                StageDef(name="train", inputs={}, outputs={}),
            ],
        )

        executor, stage_executor, _, fake_pool = self._make_executor(test_db, pipeline_def, monkeypatch)

        result = executor.run_stages(
            workspace="ws",
            pipeline_name="train",
            async_mode=True,
            reason="queue deps",
        )
        prun = result["pipeline_run_id"]
        assert len(result["queued"]) == 2

        # Process queue to launch first stage (creates stage_run record)
        first_launched = executor._process_pipeline_queue_once(
            prun,
            workspace="ws",
            pipeline_name="train",
            config_override=None,
            inputs_override=None,
            reason="queue deps",
        )
        assert len(first_launched) == 1
        assert first_launched[0].stage == "prep"
        first_stage_id = first_launched[0].stage_run_id

        # Mark first stage complete in DB to satisfy dependency
        test_db.update_stage_run_status(first_stage_id, status=StageRunStatus.COMPLETED)

        # Process queue again - now second stage should be runnable
        second_launched = executor._process_pipeline_queue_once(
            prun,
            workspace="ws",
            pipeline_name="train",
            config_override=None,
            inputs_override=None,
            reason="queue deps",
        )

        assert [sr.stage for sr in second_launched] == ["train"]
        assert stage_executor.calls == ["prep", "train"]
        # submit should have been invoked once by run_pipeline
        fake_pool.submit.assert_called_once()

    def test_worker_loop_cas_prevents_double_launch(self, test_db, monkeypatch):
        pipeline_def = PipelineDef(
            name="train",
            stages=[
                StageDef(name="prep", inputs={}, outputs={}),
            ],
        )

        executor, stage_executor, _, _ = self._make_executor(test_db, pipeline_def, monkeypatch)

        result = executor.run_stages(
            workspace="ws",
            pipeline_name="train",
            async_mode=True,
            reason="cas",
        )
        prun = result["pipeline_run_id"]

        # First call processes and launches the stage
        executor._process_pipeline_queue_once(
            prun,
            workspace="ws",
            pipeline_name="train",
            config_override=None,
            inputs_override=None,
            reason="cas",
        )
        assert len(stage_executor.calls) == 1  # Stage launched once

        # Second pass should NOT relaunch - CAS prevents double launch
        before_calls = len(stage_executor.calls)
        executor._process_pipeline_queue_once(
            prun,
            workspace="ws",
            pipeline_name="train",
            config_override=None,
            inputs_override=None,
            reason="cas",
        )
        assert len(stage_executor.calls) == before_calls  # No new launches

    def test_worker_recovery_on_restart(self, test_db, monkeypatch):
        # Seed DB with a pending pipeline run so __init__ schedules recovery
        pipeline_run_id = "prun-test"
        with test_db._conn() as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (id, workspace_name, pipeline_name, status, started_at) VALUES (?, ?, ?, 'pending', datetime('now'))",
                (pipeline_run_id, "ws", "train"),
            )
            conn.execute(
                "INSERT INTO pipeline_stage_queue (pipeline_run_id, stage_name, deps, status) VALUES (?, ?, ?, 'pending')",
                (pipeline_run_id, "prep", json.dumps([])),
            )

        fake_pool = MagicMock()
        monkeypatch.setattr(PipelineExecutor, "_pool", fake_pool)

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="train", stages=[StageDef(name="prep", inputs={}, outputs={})]
        )

        executor = PipelineExecutor(
            stage_executor=DummyStageExecutor(test_db),
            pipeline_manager=pipeline_manager,
            db=test_db,
        )

        fake_pool.submit.assert_called_once()

    def test_worker_handles_stage_failures(self, test_db, monkeypatch):
        pipeline_run_id = "prun-fail"
        with test_db._conn() as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (id, workspace_name, pipeline_name, status, started_at) VALUES (?, ?, ?, 'pending', datetime('now'))",
                (pipeline_run_id, "ws", "train"),
            )
            conn.execute(
                "INSERT INTO pipeline_stage_queue (pipeline_run_id, stage_name, deps, status) VALUES (?, ?, ?, 'failed')",
                (pipeline_run_id, "prep", json.dumps([])),
            )

        executor, _, _, _ = self._make_executor(
            test_db,
            PipelineDef(name="train", stages=[StageDef(name="prep", inputs={}, outputs={})]),
            monkeypatch,
        )

        executor._worker_loop(
            pipeline_run_id,
            workspace="ws",
            pipeline_name="train",
            config_override=None,
            inputs_override=None,
            reason="fail",
        )

        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT status FROM pipeline_runs WHERE id=?",
                (pipeline_run_id,),
            ).fetchone()
        assert row["status"] == PipelineStatus.FAILED

    def test_run_stage_wait_false_returns_immediately(self, test_db, monkeypatch):
        pipeline_def = PipelineDef(
            name="train",
            stages=[StageDef(name="prep", inputs={}, outputs={})],
        )
        executor, stage_executor, _, _ = self._make_executor(test_db, pipeline_def, monkeypatch)

        result = executor.run_stages(
            workspace="ws",
            pipeline_name="train",
            async_mode=True,
            reason="wait flag",
        )
        prun = result["pipeline_run_id"]

        # run_stages() with async_mode=True only queues stages
        assert len(result["queued"]) == 1
        assert result["queued"][0]["status"] == "pre-run check"

        # Process queue to actually launch the stage
        launched = executor._process_pipeline_queue_once(
            prun,
            workspace="ws",
            pipeline_name="train",
            config_override=None,
            inputs_override=None,
            reason="wait flag",
        )

        assert len(launched) == 1
        assert launched[0].status == StageRunStatus.RUNNING
        # stage_executor.run_stage is invoked once with default wait=False (not supplied)
        assert len(executor.stage_executor.call_kwargs) == 1
        assert executor.stage_executor.call_kwargs[0]["wait"] is False


class TestNamedPipelines:
    def test_get_pipeline_with_name(self, test_db):
        pipeline_def = PipelineDef(
            name="train",
            stages=[StageDef(name="prep", inputs={}, outputs={})],
        )
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def
        stage_executor = DummyStageExecutor(test_db)

        executor = PipelineExecutor(
            stage_executor=stage_executor,
            pipeline_manager=pipeline_manager,
            db=test_db,
        )

        executor.run_stages(workspace="ws", pipeline_name="train", async_mode=False)
        pipeline_manager.get_pipeline.assert_called_with("ws", "train")

    def test_get_pipeline_default_fallback(self, test_db):
        pipeline_def = PipelineDef(
            name="default",
            stages=[StageDef(name="prep", inputs={}, outputs={})],
        )
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def
        stage_executor = DummyStageExecutor(test_db)

        executor = PipelineExecutor(stage_executor, pipeline_manager, test_db)
        executor.run_stages(workspace="ws", pipeline_name=None, async_mode=False)

        pipeline_manager.get_pipeline.assert_called_with("ws", None)

    def test_run_stage_with_pipeline_param(self, test_db):
        pipeline_def = PipelineDef(
            name="train",
            stages=[StageDef(name="prep", inputs={}, outputs={})],
        )
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def
        stage_executor = DummyStageExecutor(test_db)

        executor = PipelineExecutor(stage_executor, pipeline_manager, test_db)
        executor.run_stages(workspace="ws", pipeline_name="train", async_mode=False)

        assert stage_executor.calls == ["prep"]
        row = test_db.list_stage_runs()[0]
        assert row["pipeline_name"] == "train"

    def test_run_stages_with_named_pipeline(self, test_db):
        pipeline_def = PipelineDef(
            name="train",
            stages=[StageDef(name="prep", inputs={}, outputs={}), StageDef(name="train", inputs={}, outputs={})],
        )
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def
        stage_executor = DummyStageExecutor(test_db)

        executor = PipelineExecutor(stage_executor, pipeline_manager, test_db)
        result = executor.run_stages(workspace="ws", pipeline_name="train", async_mode=False)

        assert all(r["pipeline"] == "train" for r in result["stage_runs"])


# =============================================================================
# Regression Tests - Issues fixed in pipeline execution
# =============================================================================


class TestOverridePersistence:
    """Regression: inputs_override and config_override must be persisted to DB."""

    def test_inputs_override_stored_in_db(self, test_db):
        """inputs_override is stored in pipeline_runs table."""
        pipeline_def = PipelineDef(
            name="train",
            stages=[StageDef(name="prep", inputs={}, outputs={})],
        )
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def
        stage_executor = DummyStageExecutor(test_db)

        executor = PipelineExecutor(stage_executor, pipeline_manager, test_db)
        result = executor.run_stages(
            workspace="ws",
            pipeline_name="train",
            inputs_override={"prep": {"data": "gs://bucket/data"}},
            async_mode=True,
        )

        prun_id = result["pipeline_run_id"]
        with test_db._conn() as conn:
            row = conn.execute("SELECT inputs_override FROM pipeline_runs WHERE id=?", (prun_id,)).fetchone()

        assert row is not None
        override = json.loads(row["inputs_override"])
        assert override == {"prep": {"data": "gs://bucket/data"}}

    def test_config_override_stored_in_db(self, test_db):
        """config_override is stored in pipeline_runs table."""
        pipeline_def = PipelineDef(
            name="train",
            stages=[StageDef(name="prep", inputs={}, outputs={})],
        )
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def
        stage_executor = DummyStageExecutor(test_db)

        executor = PipelineExecutor(stage_executor, pipeline_manager, test_db)
        result = executor.run_stages(
            workspace="ws",
            pipeline_name="train",
            config_override={"prep": {"LR": "0.001"}},
            async_mode=True,
        )

        prun_id = result["pipeline_run_id"]
        with test_db._conn() as conn:
            row = conn.execute("SELECT config_override FROM pipeline_runs WHERE id=?", (prun_id,)).fetchone()

        assert row is not None
        override = json.loads(row["config_override"])
        assert override == {"prep": {"LR": "0.001"}}

    def test_overrides_loaded_in_recovery(self, test_db, monkeypatch):
        """Overrides are loaded when recovering inflight pipelines."""
        pipeline_def = PipelineDef(
            name="train",
            stages=[StageDef(name="prep", inputs={}, outputs={})],
        )
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def
        stage_executor = DummyStageExecutor(test_db)

        # Create a pipeline run with overrides directly in DB
        now = datetime.now(UTC).isoformat()
        prun_id = "prun-recovery-test"
        with test_db._conn() as conn:
            conn.execute(
                """INSERT INTO pipeline_runs
                   (id, workspace_name, pipeline_name, status, started_at, config_override, inputs_override)
                   VALUES (?, ?, ?, 'running', ?, ?, ?)""",
                (
                    prun_id,
                    "ws",
                    "train",
                    now,
                    '{"prep": {"LR": "0.01"}}',
                    '{"prep": {"data": "gs://test"}}',
                ),
            )
            conn.execute(
                """INSERT INTO pipeline_stage_queue
                   (pipeline_run_id, stage_name, status) VALUES (?, ?, 'pending')""",
                (prun_id, "prep"),
            )

        # Track what _pool.submit receives
        submitted_args = []

        def mock_submit(fn, *args):
            submitted_args.append(args)
            # Don't actually run the worker

        # Patch the class-level _pool.submit directly
        monkeypatch.setattr(PipelineExecutor._pool, "submit", mock_submit)

        # Creating executor triggers _recover_inflight_pipelines
        executor = PipelineExecutor(stage_executor, pipeline_manager, test_db)

        # Verify overrides were passed to worker
        assert len(submitted_args) == 1
        # Args: pipeline_run_id, workspace, pipeline_name, config_override, inputs_override, reason, reason_structured, results_spec, experiment_group
        _, _, _, config_override, inputs_override, _, _, _, _ = submitted_args[0]
        assert config_override == {"prep": {"LR": "0.01"}}
        assert inputs_override == {"prep": {"data": "gs://test"}}


class TestStageLaunchErrorVisibility:
    """Regression: Stage launch errors must be visible in queue status."""

    def test_launch_error_stored_in_queue(self, test_db, monkeypatch):
        """When stage launch fails, error is stored in pipeline_stage_queue."""
        pipeline_def = PipelineDef(
            name="train",
            stages=[StageDef(name="prep", inputs={}, outputs={})],
        )
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def

        # Stage executor that raises an error
        class FailingStageExecutor(DummyStageExecutor):
            def run_stage(self, **kwargs):
                raise RuntimeError("Stage launch failed: missing dependency")

        stage_executor = FailingStageExecutor(test_db)

        # Disable thread pool
        mock_pool = MagicMock()
        monkeypatch.setattr(
            "goldfish.jobs.pipeline_executor.ThreadPoolExecutor",
            lambda **kw: mock_pool,
        )

        executor = PipelineExecutor(stage_executor, pipeline_manager, test_db)
        result = executor.run_stages(workspace="ws", pipeline_name="train", async_mode=True)

        prun_id = result["pipeline_run_id"]

        # Process queue - this should catch the error
        executor._process_pipeline_queue_once(prun_id, "ws", "train", None, None, "test")

        # Check queue entry has error
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT status, error FROM pipeline_stage_queue WHERE pipeline_run_id=?",
                (prun_id,),
            ).fetchone()

        assert row["status"] == PipelineStatus.FAILED
        assert "Stage launch failed" in row["error"]


class TestInputValidationBeforeQueuing:
    """Regression: Input validation must fail fast before any pipeline run is created."""

    def test_validation_fails_if_upstream_stage_not_completed(self, test_db, monkeypatch):
        """Pipeline fails validation if input depends on non-completed upstream stage."""
        from goldfish.models import SignalDef

        # Pipeline where train depends on prep output
        pipeline_def = PipelineDef(
            name="train",
            stages=[
                StageDef(name="prep", inputs={}, outputs={}),
                StageDef(
                    name="train",
                    inputs={"features": SignalDef(name="features", from_stage="prep", type="npy")},
                    outputs={},
                ),
            ],
        )
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def
        stage_executor = DummyStageExecutor(test_db)

        # Disable thread pool
        mock_pool = MagicMock()
        monkeypatch.setattr(
            "goldfish.jobs.pipeline_executor.ThreadPoolExecutor",
            lambda **kw: mock_pool,
        )

        executor = PipelineExecutor(stage_executor, pipeline_manager, test_db)

        # Try to run just "train" without "prep" having completed
        # This should fail validation BEFORE creating a pipeline run
        from goldfish.errors import GoldfishError

        with pytest.raises(GoldfishError, match="requires input.*from stage"):
            executor.run_stages(
                workspace="ws",
                pipeline_name="train",
                stages=["train"],  # Only run train, not prep
                async_mode=True,
            )

        # Verify NO pipeline_run was created
        with test_db._conn() as conn:
            count = conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
        assert count == 0

    def test_validation_passes_with_override(self, test_db, monkeypatch):
        """Pipeline passes validation if missing input has an override."""
        from goldfish.models import SignalDef

        pipeline_def = PipelineDef(
            name="train",
            stages=[
                StageDef(
                    name="train",
                    inputs={"features": SignalDef(name="features", from_stage="prep", type="npy")},
                    outputs={},
                ),
            ],
        )
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def
        stage_executor = DummyStageExecutor(test_db)

        mock_pool = MagicMock()
        monkeypatch.setattr(
            "goldfish.jobs.pipeline_executor.ThreadPoolExecutor",
            lambda **kw: mock_pool,
        )

        executor = PipelineExecutor(stage_executor, pipeline_manager, test_db)

        # Run with override - should pass validation
        result = executor.run_stages(
            workspace="ws",
            pipeline_name="train",
            stages=["train"],
            inputs_override={"train": {"features": "gs://bucket/features"}},
            async_mode=True,
        )

        # Pipeline run should be created
        assert "pipeline_run_id" in result
        with test_db._conn() as conn:
            count = conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
        assert count == 1


class TestReasonPropagation:
    """Test that reason_structured is correctly propagated through async pipeline execution."""

    def _make_executor(self, test_db, pipeline_def, monkeypatch):
        stage_executor = DummyStageExecutor(test_db)
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def

        fake_pool = MagicMock()
        monkeypatch.setattr(PipelineExecutor, "_pool", fake_pool)

        executor = PipelineExecutor(
            stage_executor=stage_executor,
            pipeline_manager=pipeline_manager,
            db=test_db,
        )
        return executor, stage_executor, pipeline_manager, fake_pool

    def test_reason_structured_stored_in_pipeline_runs(self, test_db, monkeypatch):
        """reason_structured is stored as JSON in pipeline_runs table."""
        from goldfish.models import RunReason

        pipeline_def = PipelineDef(
            name="train",
            stages=[StageDef(name="prep", inputs={}, outputs={})],
        )

        executor, stage_executor, _, _ = self._make_executor(test_db, pipeline_def, monkeypatch)

        reason = RunReason(
            description="Testing new architecture",
            hypothesis="Will improve accuracy by 5%",
            approach="Using transformer layers",
        )

        result = executor.run_stages(
            workspace="ws",
            pipeline_name="train",
            reason="Testing new architecture",
            reason_structured=reason,
            async_mode=True,
        )

        prun_id = result["pipeline_run_id"]
        with test_db._conn() as conn:
            row = conn.execute("SELECT reason_json FROM pipeline_runs WHERE id=?", (prun_id,)).fetchone()

        assert row is not None
        reason_data = json.loads(row["reason_json"])
        assert reason_data["description"] == "Testing new architecture"
        assert reason_data["hypothesis"] == "Will improve accuracy by 5%"
        assert reason_data["approach"] == "Using transformer layers"

    def test_reason_structured_passed_to_stage_in_async_mode(self, test_db, monkeypatch):
        """reason_structured is passed to run_stage() when processing async queue."""
        from goldfish.models import RunReason

        pipeline_def = PipelineDef(
            name="train",
            stages=[StageDef(name="prep", inputs={}, outputs={})],
        )

        executor, stage_executor, _, _ = self._make_executor(test_db, pipeline_def, monkeypatch)

        reason = RunReason(
            description="Testing hypothesis",
            hypothesis="Model will converge faster",
        )

        result = executor.run_stages(
            workspace="ws",
            pipeline_name="train",
            reason="Testing hypothesis",
            reason_structured=reason,
            async_mode=True,
        )

        prun_id = result["pipeline_run_id"]

        # Process queue - this should pass reason_structured to run_stage
        executor._process_pipeline_queue_once(
            prun_id,
            workspace="ws",
            pipeline_name="train",
            config_override=None,
            inputs_override=None,
            reason="Testing hypothesis",
            reason_structured=reason.model_dump(),
        )

        # Verify reason_structured was passed to run_stage
        assert len(stage_executor.call_kwargs) == 1
        call = stage_executor.call_kwargs[0]
        assert call["reason"] == "Testing hypothesis"
        assert call["reason_structured"]["description"] == "Testing hypothesis"
        assert call["reason_structured"]["hypothesis"] == "Model will converge faster"

    def test_reason_structured_loaded_from_db_in_recovery(self, test_db, monkeypatch):
        """reason_structured is loaded from DB when recovering inflight pipelines."""
        pipeline_def = PipelineDef(
            name="train",
            stages=[StageDef(name="prep", inputs={}, outputs={})],
        )
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def
        stage_executor = DummyStageExecutor(test_db)

        # Create a pipeline run with reason_json directly in DB
        now = datetime.now(UTC).isoformat()
        prun_id = "prun-reason-recovery"
        reason_json = json.dumps(
            {
                "description": "Recovery test reason",
                "hypothesis": "Should recover correctly",
                "approach": None,
                "min_result": None,
                "goal": None,
            }
        )
        with test_db._conn() as conn:
            conn.execute(
                """INSERT INTO pipeline_runs
                   (id, workspace_name, pipeline_name, status, started_at, reason_json)
                   VALUES (?, ?, ?, 'running', ?, ?)""",
                (prun_id, "ws", "train", now, reason_json),
            )
            conn.execute(
                """INSERT INTO pipeline_stage_queue
                   (pipeline_run_id, stage_name, status) VALUES (?, ?, 'pending')""",
                (prun_id, "prep"),
            )

        # Track what _pool.submit receives
        submitted_args = []

        def mock_submit(fn, *args):
            submitted_args.append(args)

        monkeypatch.setattr(PipelineExecutor._pool, "submit", mock_submit)

        # Creating executor triggers _recover_inflight_pipelines
        executor = PipelineExecutor(stage_executor, pipeline_manager, test_db)

        # Verify reason_structured was passed to worker
        assert len(submitted_args) == 1
        # Args: prun_id, workspace, pipeline_name, config, inputs, reason, reason_structured, results_spec, experiment_group
        _, _, _, _, _, reason, reason_structured, _, _ = submitted_args[0]
        assert reason == "Recovery test reason"  # Extracted from description
        assert reason_structured["description"] == "Recovery test reason"
        assert reason_structured["hypothesis"] == "Should recover correctly"

    def test_reason_stored_in_stage_run_via_async_pipeline(self, test_db, monkeypatch):
        """Full flow: reason_structured flows from run_stages() to stage_runs.reason_json."""
        from goldfish.models import RunReason

        pipeline_def = PipelineDef(
            name="train",
            stages=[StageDef(name="prep", inputs={}, outputs={})],
        )

        executor, stage_executor, _, _ = self._make_executor(test_db, pipeline_def, monkeypatch)

        reason = RunReason(
            description="Full flow test",
            hypothesis="Reason should be in stage_runs",
        )
        reason_dict = reason.model_dump()

        result = executor.run_stages(
            workspace="ws",
            pipeline_name="train",
            reason="Full flow test",
            reason_structured=reason,
            async_mode=True,
        )

        prun_id = result["pipeline_run_id"]

        # Process queue
        executor._process_pipeline_queue_once(
            prun_id,
            workspace="ws",
            pipeline_name="train",
            config_override=None,
            inputs_override=None,
            reason="Full flow test",
            reason_structured=reason_dict,
        )

        # Verify reason was stored in stage_runs (via DummyStageExecutor)
        stage_runs = test_db.list_stage_runs(workspace_name="ws")
        assert len(stage_runs) == 1

        stage_run = stage_runs[0]
        assert stage_run.get("reason_json") is not None
        stored_reason = json.loads(stage_run["reason_json"])
        assert stored_reason["description"] == "Full flow test"
        assert stored_reason["hypothesis"] == "Reason should be in stage_runs"


class TestResultsSpecPersistence:
    """Test that results_spec is saved during async pipeline execution."""

    def test_results_spec_stored_in_run_results_spec_table(self, test_db, monkeypatch):
        """REGRESSION: results_spec provided to run() should be saved to run_results_spec table.

        Bug: results_spec was accepted by run() and stored in pipeline_runs.results_spec_json
        but never copied to run_results_spec table for individual stage runs.
        """
        from goldfish.experiment_model.records import ExperimentRecordManager

        pipeline_def = PipelineDef(
            name="train",
            stages=[StageDef(name="prep", inputs={}, outputs={})],
        )

        # Create a stage executor that creates experiment records and saves results_spec
        class ExperimentTrackingStageExecutor(DummyStageExecutor):
            def run_stage(
                self,
                workspace: str,
                stage_name: str,
                pipeline_name: str | None = None,
                pipeline_run_id: str | None = None,
                config_override=None,
                inputs_override=None,
                reason: str | None = None,
                reason_structured: dict | None = None,
                wait: bool = False,
                skip_review: bool = False,
                experiment_group: str | None = None,
                results_spec: dict | None = None,  # NEW: accept results_spec
            ) -> StageRunInfo:
                self._ensure_workspace_version(workspace)
                stage_run_id = f"stage-{len(self.calls) + 1}"
                self.calls.append(stage_name)

                # Create stage run
                self.db.create_stage_run(
                    stage_run_id=stage_run_id,
                    workspace_name=workspace,
                    version=self.version,
                    stage_name=stage_name,
                    pipeline_run_id=pipeline_run_id,
                    pipeline_name=pipeline_name,
                    config=config_override or {},
                    inputs={},
                    reason=reason_structured,
                    profile=None,
                    hints=None,
                    backend_type="local",
                    backend_handle=stage_run_id,
                )

                # Create experiment record (critical for results_spec FK)
                exp_manager = ExperimentRecordManager(self.db)
                record_id = exp_manager.create_run_record(
                    workspace_name=workspace,
                    version=self.version,
                    stage_run_id=stage_run_id,
                    experiment_group=experiment_group,
                )

                # Save results_spec immediately after experiment record creation
                # This matches the real StageExecutor behavior
                if results_spec and record_id:
                    exp_manager.save_results_spec(stage_run_id, record_id, results_spec)

                self.db.update_stage_run_status(stage_run_id, status=StageRunStatus.RUNNING)

                return StageRunInfo(
                    stage_run_id=stage_run_id,
                    workspace=workspace,
                    version=self.version,
                    stage=stage_name,
                    pipeline=pipeline_name,
                    pipeline_run_id=pipeline_run_id,
                    record_id=record_id,  # Return the record_id!
                    status=StageRunStatus.RUNNING,
                )

        stage_executor = ExperimentTrackingStageExecutor(test_db)
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def

        # Disable thread pool to control execution
        # Must patch the class-level _pool, not the ThreadPoolExecutor constructor
        # because _pool is created at module import time
        mock_pool = MagicMock()
        monkeypatch.setattr(PipelineExecutor, "_pool", mock_pool)

        executor = PipelineExecutor(stage_executor, pipeline_manager, test_db)

        # The results_spec that should be saved
        results_spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.60,
            "goal_value": 0.80,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing that results_spec is saved during async run.",
        }

        # Run with results_spec
        result = executor.run_stages(
            workspace="ws",
            pipeline_name="train",
            reason="Test results_spec persistence",
            async_mode=True,
            results_spec=results_spec,
        )

        prun_id = result["pipeline_run_id"]

        # Verify results_spec is stored in pipeline_runs table
        with test_db._conn() as conn:
            prun_row = conn.execute(
                "SELECT results_spec_json FROM pipeline_runs WHERE id = ?",
                (prun_id,),
            ).fetchone()
        assert prun_row is not None
        assert prun_row["results_spec_json"] is not None
        stored_pipeline_spec = json.loads(prun_row["results_spec_json"])
        assert stored_pipeline_spec["primary_metric"] == "accuracy"

        # Process queue with results_spec parameter
        launched = executor._process_pipeline_queue_once(
            prun_id,
            workspace="ws",
            pipeline_name="train",
            config_override=None,
            inputs_override=None,
            reason="Test results_spec persistence",
            reason_structured=None,
            results_spec=results_spec,  # Critical: must pass results_spec
            experiment_group=None,
        )

        # Verify stage was launched
        assert len(launched) == 1, f"Expected 1 launched stage, got {len(launched)}"

        # Get the stage_run_id that was created
        stage_runs = test_db.list_stage_runs(workspace_name="ws")
        assert len(stage_runs) == 1, f"Expected 1 stage run, got {len(stage_runs)} - launched: {launched}"
        stage_run_id = stage_runs[0]["id"]

        # REGRESSION CHECK: Verify results_spec is saved to run_results_spec table
        exp_manager = ExperimentRecordManager(test_db)
        stored_spec = exp_manager.get_results_spec(stage_run_id)

        assert stored_spec is not None, (
            "results_spec was not saved to run_results_spec table! "
            "This is the bug - results_spec should be saved after stage launch."
        )
        parsed_spec = json.loads(stored_spec["spec_json"])
        assert parsed_spec["primary_metric"] == "accuracy"
        assert parsed_spec["goal_value"] == 0.80

    def test_results_spec_not_saved_when_no_experiment_record(self, test_db, monkeypatch):
        """When no experiment record is created, results_spec should NOT be saved.

        The DummyStageExecutor doesn't create experiment records, so results_spec
        won't be saved. This verifies the behavior when run_stage() is called
        without creating an experiment record.
        """
        pipeline_def = PipelineDef(
            name="train",
            stages=[StageDef(name="prep", inputs={}, outputs={})],
        )

        # Use the basic DummyStageExecutor which doesn't create experiment records
        stage_executor = DummyStageExecutor(test_db)
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def

        # Disable thread pool - must patch class-level _pool
        mock_pool = MagicMock()
        monkeypatch.setattr(PipelineExecutor, "_pool", mock_pool)

        executor = PipelineExecutor(stage_executor, pipeline_manager, test_db)

        results_spec = {
            "primary_metric": "accuracy",
            "direction": "maximize",
            "min_value": 0.60,
            "goal_value": 0.80,
            "dataset_split": "val",
            "tolerance": 0.01,
            "context": "Testing results_spec not saved without experiment record.",
        }

        result = executor.run_stages(
            workspace="ws",
            pipeline_name="train",
            reason="Test no experiment record",
            async_mode=True,
            results_spec=results_spec,
        )

        prun_id = result["pipeline_run_id"]

        # Process queue - DummyStageExecutor doesn't create experiment records
        executor._process_pipeline_queue_once(
            prun_id,
            workspace="ws",
            pipeline_name="train",
            config_override=None,
            inputs_override=None,
            reason="Test no experiment record",
            reason_structured=None,
            results_spec=results_spec,
            experiment_group=None,
        )

        # Stage was created
        stage_runs = test_db.list_stage_runs(workspace_name="ws")
        assert len(stage_runs) == 1

        # Verify results_spec was NOT saved (because no experiment record was created)
        from goldfish.experiment_model.records import ExperimentRecordManager

        exp_manager = ExperimentRecordManager(test_db)
        stored_spec = exp_manager.get_results_spec(stage_runs[0]["id"])
        assert stored_spec is None, "results_spec should NOT be saved when no experiment record exists"
