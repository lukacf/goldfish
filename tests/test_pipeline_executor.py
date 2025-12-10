"""Tests for PipelineExecutor - TDD Phase 4."""

import pytest
from unittest.mock import MagicMock

from goldfish.jobs.pipeline_executor import PipelineExecutor
from goldfish.models import PipelineDef, StageDef, SignalDef, StageRunInfo


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
                StageDef(name="train", inputs={}, outputs={})
            ]
        )

        # Mock stage executor
        stage_executor = MagicMock()
        stage_executor.run_stage.side_effect = [
            StageRunInfo(
                stage_run_id="stage-1",
                workspace="test_ws",
                version="v1",
                stage="preprocess",
                status="running"
            ),
            StageRunInfo(
                stage_run_id="stage-2",
                workspace="test_ws",
                version="v1",
                stage="tokenize",
                status="running"
            ),
            StageRunInfo(
                stage_run_id="stage-3",
                workspace="test_ws",
                version="v1",
                stage="train",
                status="running"
            ),
        ]

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def

        executor = PipelineExecutor(
            stage_executor=stage_executor,
            pipeline_manager=pipeline_manager,
            db=test_db
        )

        # Execute
        result = executor.run_pipeline(
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
            stages=[
                StageDef(name="preprocess", inputs={}, outputs={}),
                StageDef(name="train", inputs={}, outputs={})
            ]
        )

        stage_executor = MagicMock()
        stage_executor.run_stage.return_value = StageRunInfo(
            stage_run_id="stage-1",
            workspace="test_ws",
            version="v1",
            stage="preprocess",
            status="running"
        )

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def

        executor = PipelineExecutor(
            stage_executor=stage_executor,
            pipeline_manager=pipeline_manager,
            db=test_db
        )

        # Execute with config overrides
        config_override = {
            "preprocess": {"BATCH_SIZE": "128"},
            "train": {"EPOCHS": "20"}
        }

        executor.run_pipeline(
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


class TestRunPartialPipeline:
    """Test running subset of pipeline stages."""

    def test_run_partial_pipeline_from_to(self, test_db):
        """Should run stages from_stage through to_stage."""
        # Setup
        pipeline_def = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(name="preprocess", inputs={}, outputs={}),
                StageDef(name="tokenize", inputs={}, outputs={}),
                StageDef(name="train", inputs={}, outputs={}),
                StageDef(name="evaluate", inputs={}, outputs={})
            ]
        )

        stage_executor = MagicMock()
        stage_executor.run_stage.side_effect = [
            StageRunInfo(stage_run_id="stage-1", workspace="test_ws", version="v1", stage="tokenize", status="running"),
            StageRunInfo(stage_run_id="stage-2", workspace="test_ws", version="v1", stage="train", status="running"),
        ]

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def

        executor = PipelineExecutor(
            stage_executor=stage_executor,
            pipeline_manager=pipeline_manager,
            db=test_db
        )

        # Execute
        result = executor.run_partial_pipeline(
            workspace="test_ws",
            from_stage="tokenize",
            to_stage="train",
            reason="Test partial run",
            async_mode=False,
        )

        # Verify
        stage_runs = result["stage_runs"]
        assert len(stage_runs) == 2
        assert [r["stage"] for r in stage_runs] == ["tokenize", "train"]

    def test_run_partial_pipeline_raises_on_invalid_stage_order(self, test_db):
        """Should raise error if from_stage comes after to_stage."""
        # Setup
        pipeline_def = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(name="preprocess", inputs={}, outputs={}),
                StageDef(name="tokenize", inputs={}, outputs={}),
                StageDef(name="train", inputs={}, outputs={})
            ]
        )

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def

        executor = PipelineExecutor(
            stage_executor=MagicMock(),
            pipeline_manager=pipeline_manager,
            db=test_db
        )

        # Execute - should raise
        with pytest.raises(ValueError, match="from_stage must come before to_stage"):
            executor.run_partial_pipeline(
                workspace="test_ws",
                from_stage="train",
                to_stage="preprocess",
                reason="Test"
            )

    def test_run_partial_pipeline_raises_on_unknown_stage(self, test_db):
        """Should raise error if stage not found in pipeline."""
        # Setup
        pipeline_def = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(name="preprocess", inputs={}, outputs={}),
                StageDef(name="train", inputs={}, outputs={})
            ]
        )

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = pipeline_def

        executor = PipelineExecutor(
            stage_executor=MagicMock(),
            pipeline_manager=pipeline_manager,
            db=test_db
        )

        # Execute - should raise
        with pytest.raises(ValueError, match="Stage not found"):
            executor.run_partial_pipeline(
                workspace="test_ws",
                from_stage="nonexistent",
                to_stage="train",
                reason="Test"
            )
