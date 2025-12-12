"""Tests for PipelineExecutor - TDD Phase 4."""

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from goldfish.jobs.pipeline_executor import PipelineExecutor
from goldfish.models import PipelineDef, StageDef, StageRunInfo

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
        wait: bool = False,
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
            profile=None,
            hints=None,
            backend_type="local",
            backend_handle=stage_run_id,
        )
        # mark running immediately
        self.db.update_stage_run_status(stage_run_id, status="running")
        self.call_kwargs.append(
            {
                "workspace": workspace,
                "stage_name": stage_name,
                "pipeline_name": pipeline_name,
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
            status="running",
        )

    def refresh_status_once(self, stage_run_id: str):  # pragma: no cover - not used in tests
        return "running"


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
                stage_run_id="stage-1", workspace="test_ws", version="v1", stage="preprocess", status="running"
            ),
            StageRunInfo(stage_run_id="stage-2", workspace="test_ws", version="v1", stage="tokenize", status="running"),
            StageRunInfo(stage_run_id="stage-3", workspace="test_ws", version="v1", stage="train", status="running"),
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
            stage_run_id="stage-1", workspace="test_ws", version="v1", stage="preprocess", status="running"
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
            StageRunInfo(stage_run_id="stage-1", workspace="test_ws", version="v1", stage="tokenize", status="running"),
            StageRunInfo(stage_run_id="stage-2", workspace="test_ws", version="v1", stage="train", status="running"),
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
        test_db.update_stage_run_status(first_stage_id, status="completed")

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
        assert row["status"] == "failed"

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
        assert result["queued"][0]["status"] == "queued"

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
        assert launched[0].status == "running"
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
        # Args: pipeline_run_id, workspace, pipeline_name, config_override, inputs_override, reason
        _, _, _, config_override, inputs_override, _ = submitted_args[0]
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

        assert row["status"] == "failed"
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
