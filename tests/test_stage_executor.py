"""Tests for StageExecutor - TDD Phase 4."""

import json
import pytest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, call

from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import PipelineDef, StageDef, SignalDef
from goldfish.errors import GoldfishError


class TestInputResolution:
    """Test resolving stage inputs (datasets, signals, overrides)."""

    def test_resolve_dataset_input(self, test_db, test_config):
        """Should resolve dataset inputs to GCS locations."""
        # Setup
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(
                    name="preprocess",
                    inputs={
                        "raw_data": SignalDef(
                            name="raw_data",
                            type="dataset",
                            dataset="eurusd_raw_v3"
                        )
                    },
                    outputs={}
                )
            ]
        )

        dataset_registry = MagicMock()
        dataset_registry.get_dataset.return_value = MagicMock(
            gcs_location="gs://bucket/datasets/eurusd_raw_v3"
        )

        workspace_manager = MagicMock()

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            dataset_registry=dataset_registry
        )

        # Execute
        stage = pipeline_manager.get_pipeline.return_value.stages[0]
        inputs = executor._resolve_inputs("test_workspace", stage)

        # Verify
        assert inputs["raw_data"] == "gs://bucket/datasets/eurusd_raw_v3"
        dataset_registry.get_dataset.assert_called_once_with("eurusd_raw_v3")

    def test_resolve_signal_input_from_previous_stage(self, test_db, test_config):
        """Should resolve signal inputs from previous stage runs."""
        # Setup: Create a previous stage run with output signal
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "tag-v1", "sha123", "manual")
        stage_run_id = "stage-abc123"
        test_db.create_stage_run(
            stage_run_id=stage_run_id,
            workspace_name="test_workspace",
            version="v1",
            stage_name="preprocess"
        )
        test_db.update_stage_run_status(stage_run_id, "completed")
        test_db.add_signal(
            stage_run_id=stage_run_id,
            signal_name="features",
            signal_type="npy",
            storage_location="gs://bucket/runs/stage-abc123/features"
        )

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(name="preprocess", inputs={}, outputs={}),
                StageDef(
                    name="tokenize",
                    inputs={
                        "features": SignalDef(
                            name="features",
                            type="npy",
                            from_stage="preprocess"
                        )
                    },
                    outputs={}
                )
            ]
        )

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=pipeline_manager,
            dataset_registry=MagicMock()
        )

        # Execute
        stage = pipeline_manager.get_pipeline.return_value.stages[1]
        inputs = executor._resolve_inputs("test_workspace", stage)

        # Verify
        assert inputs["features"] == "gs://bucket/runs/stage-abc123/features"

    def test_resolve_input_with_override(self, test_db, test_config):
        """Should use override when provided."""
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(
                    name="preprocess",
                    inputs={
                        "raw_data": SignalDef(
                            name="raw_data",
                            type="dataset",
                            dataset="eurusd_raw_v3"
                        )
                    },
                    outputs={}
                )
            ]
        )

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=pipeline_manager,
            dataset_registry=MagicMock()
        )

        # Execute with override
        stage = pipeline_manager.get_pipeline.return_value.stages[0]
        inputs = executor._resolve_inputs(
            "test_workspace",
            stage,
            inputs_override={"raw_data": "gs://bucket/debug/test_data.csv"}
        )

        # Verify override was used
        assert inputs["raw_data"] == "gs://bucket/debug/test_data.csv"

    def test_resolve_signal_raises_when_previous_stage_not_run(
        self, test_db, test_config
    ):
        """Should raise error when signal source stage hasn't been run."""
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(name="preprocess", inputs={}, outputs={}),
                StageDef(
                    name="tokenize",
                    inputs={
                        "features": SignalDef(
                            name="features",
                            type="npy",
                            from_stage="preprocess"
                        )
                    },
                    outputs={}
                )
            ]
        )

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=pipeline_manager,
            dataset_registry=MagicMock()
        )

        # Execute - should raise because preprocess hasn't been run
        stage = pipeline_manager.get_pipeline.return_value.stages[1]
        with pytest.raises(GoldfishError, match="No successful run found"):
            executor._resolve_inputs("test_workspace", stage)


class TestAutoVersioning:
    """Test automatic workspace versioning on stage runs."""

    def test_auto_version_creates_git_tag(
        self, test_db, test_config
    ):
        """Should create git tag and database version record."""
        # Setup workspace lineage
        test_db.create_workspace_lineage("test_workspace", description="Test")

        pipeline_manager = MagicMock()

        # Mock git operations
        workspace_manager = MagicMock()
        workspace_manager.git_layer.get_current_sha.return_value = "abc123"
        workspace_manager.git_layer.create_tag = MagicMock()

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            dataset_registry=MagicMock()
        )

        # Execute
        version = executor._auto_version("test_workspace", "preprocess", "Testing stage")

        # Verify
        assert version == "v1"
        workspace_manager.git_layer.create_tag.assert_called_once()
        # Verify database record
        db_version = test_db.get_version("test_workspace", "v1")
        assert db_version["git_sha"] == "abc123"

    def test_auto_version_increments_version_number(
        self, test_db, test_config
    ):
        """Should increment version number for subsequent runs."""
        # Setup: Create existing versions
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "tag-v1", "sha1", "run")
        test_db.create_version("test_workspace", "v2", "tag-v2", "sha2", "run")

        pipeline_manager = MagicMock()

        # Mock git operations
        workspace_manager = MagicMock()
        workspace_manager.git_layer.get_current_sha.return_value = "abc123"
        workspace_manager.git_layer.create_tag = MagicMock()

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            dataset_registry=MagicMock()
        )

        # Execute
        version = executor._auto_version("test_workspace", "preprocess", "Third run")

        # Verify
        assert version == "v3"


class TestStageRunRecords:
    """Test stage run database records."""

    def test_create_stage_run_record(self, test_db, test_config):
        """Should create stage_run record with all metadata."""
        # Setup
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "tag-v1", "sha123", "run")

        pipeline_manager = MagicMock()
        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=pipeline_manager,
            dataset_registry=MagicMock()
        )

        # Execute
        stage_run_id = "stage-abc123"
        inputs = {"raw_data": "gs://bucket/datasets/data.csv"}
        config_override = {"BATCH_SIZE": "128"}

        executor._create_stage_run_record(
            stage_run_id=stage_run_id,
            workspace="test_workspace",
            version="v1",
            stage_name="preprocess",
            inputs=inputs,
            config_override=config_override,
            reason="Testing larger batch size"
        )

        # Verify
        record = test_db.get_stage_run(stage_run_id)
        assert record["workspace_name"] == "test_workspace"
        assert record["version"] == "v1"
        assert record["stage_name"] == "preprocess"
        assert record["status"] == "pending"
        assert json.loads(record["config_override"]) == config_override


class TestStageExecution:
    """Test full stage execution flow (mocked)."""

    def test_run_stage_with_local_backend(
        self, test_db, test_config, temp_dir
    ):
        """Should orchestrate full stage run with local backend."""
        # Setup workspace and pipeline
        test_db.create_workspace_lineage("test_workspace", description="Test")

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(
                    name="preprocess",
                    inputs={
                        "raw_data": SignalDef(
                            name="raw_data",
                            type="dataset",
                            dataset="test_data"
                        )
                    },
                    outputs={
                        "features": SignalDef(
                            name="features",
                            type="npy"
                        )
                    }
                )
            ]
        )

        dataset_registry = MagicMock()
        dataset_registry.get_dataset.return_value = MagicMock(
            gcs_location="gs://bucket/datasets/test_data"
        )

        # Mock git operations
        workspace_manager = MagicMock()
        workspace_manager.git_layer.get_current_sha.return_value = "abc123"
        workspace_manager.git_layer.create_tag = MagicMock()

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            dataset_registry=dataset_registry
        )

        # Mock stage execution methods
        executor._build_docker_image = MagicMock(return_value="goldfish-test-v1")
        executor._launch_container = MagicMock()

        # Execute
        result = executor.run_stage(
            workspace="test_workspace",
            stage_name="preprocess",
            reason="Test run"
        )

        # Verify
        assert result.workspace == "test_workspace"
        assert result.stage == "preprocess"
        assert result.status == "running"
        assert result.version == "v1"

        # Verify Docker image was built
        executor._build_docker_image.assert_called_once()

        # Verify container was launched
        executor._launch_container.assert_called_once()
