"""Tests for StageExecutor - TDD Phase 4."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from goldfish.errors import GoldfishError
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import PipelineDef, SignalDef, StageDef, StageRunStatus


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
                    inputs={"raw_data": SignalDef(name="raw_data", type="dataset", dataset="eurusd_raw_v3")},
                    outputs={},
                )
            ],
        )

        dataset_registry = MagicMock()
        dataset_registry.get_dataset.return_value = MagicMock(gcs_location="gs://bucket/datasets/eurusd_raw_v3")

        workspace_manager = MagicMock()

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=dataset_registry,
        )

        # Execute
        stage = pipeline_manager.get_pipeline.return_value.stages[0]
        inputs, sources = executor._resolve_inputs("test_workspace", stage)

        # Verify inputs
        assert inputs["raw_data"] == "gs://bucket/datasets/eurusd_raw_v3"
        dataset_registry.get_dataset.assert_called_once_with("eurusd_raw_v3")

        # Verify source metadata
        assert sources["raw_data"]["source_type"] == "dataset"
        assert sources["raw_data"]["dataset_name"] == "eurusd_raw_v3"

    def test_resolve_signal_input_from_previous_stage(self, test_db, test_config):
        """Should resolve signal inputs from previous stage runs."""
        # Setup: Create a previous stage run with output signal
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "tag-v1", "sha123", "manual")
        stage_run_id = "stage-abc123"
        test_db.create_stage_run(
            stage_run_id=stage_run_id, workspace_name="test_workspace", version="v1", stage_name="preprocess"
        )
        test_db.update_stage_run_status(stage_run_id, StageRunStatus.COMPLETED)
        test_db.add_signal(
            stage_run_id=stage_run_id,
            signal_name="features",
            signal_type="npy",
            storage_location="gs://bucket/runs/stage-abc123/features",
        )

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(name="preprocess", inputs={}, outputs={}),
                StageDef(
                    name="tokenize",
                    inputs={"features": SignalDef(name="features", type="npy", from_stage="preprocess")},
                    outputs={},
                ),
            ],
        )

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )

        # Execute
        stage = pipeline_manager.get_pipeline.return_value.stages[1]
        inputs, sources = executor._resolve_inputs("test_workspace", stage)

        # Verify inputs
        assert inputs["features"] == "gs://bucket/runs/stage-abc123/features"

        # Verify source metadata tracks upstream lineage
        assert sources["features"]["source_type"] == "stage"
        assert sources["features"]["source_stage_run_id"] == "stage-abc123"

    def test_resolve_input_with_override(self, test_db, test_config):
        """Should use override when provided."""
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(
                    name="preprocess",
                    inputs={"raw_data": SignalDef(name="raw_data", type="dataset", dataset="eurusd_raw_v3")},
                    outputs={},
                )
            ],
        )

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )

        # Execute with override
        stage = pipeline_manager.get_pipeline.return_value.stages[0]
        inputs, sources = executor._resolve_inputs(
            "test_workspace", stage, inputs_override={"raw_data": "gs://bucket/debug/test_data.csv"}
        )

        # Verify override was used
        assert inputs["raw_data"] == "gs://bucket/debug/test_data.csv"

        # Verify source metadata marks as override
        assert sources["raw_data"]["source_type"] == "override"

    def test_resolve_signal_raises_when_previous_stage_not_run(self, test_db, test_config):
        """Should raise error when signal source stage hasn't been run."""
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(name="preprocess", inputs={}, outputs={}),
                StageDef(
                    name="tokenize",
                    inputs={"features": SignalDef(name="features", type="npy", from_stage="preprocess")},
                    outputs={},
                ),
            ],
        )

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )

        # Execute - should raise because preprocess hasn't been run
        stage = pipeline_manager.get_pipeline.return_value.stages[1]
        with pytest.raises(GoldfishError, match="No successful run found"):
            executor._resolve_inputs("test_workspace", stage)


class TestAutoVersioning:
    """Test automatic workspace versioning on stage runs."""

    def test_auto_version_creates_git_tag(self, test_db, test_config):
        """Should create git tag and database version record via sync_and_version."""
        # Setup workspace lineage
        test_db.create_workspace_lineage("test_workspace", description="Test")

        pipeline_manager = MagicMock()

        # Mock workspace_manager with copy-based mounting
        # sync_and_version is the new provenance guard method
        workspace_manager = MagicMock()
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )

        # Execute
        version, git_sha = executor._auto_version("test_workspace", "preprocess", "Testing stage")

        # Verify
        assert version == "v1"
        assert git_sha == "abc123"
        workspace_manager.sync_and_version.assert_called_once_with("w1", "preprocess", "Testing stage")

    def test_auto_version_increments_version_number(self, test_db, test_config):
        """Should increment version number for subsequent runs via sync_and_version."""
        # Setup: Create existing versions
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "tag-v1", "sha1", "run")
        test_db.create_version("test_workspace", "v2", "tag-v2", "sha2", "run")

        pipeline_manager = MagicMock()

        # Mock workspace_manager with copy-based mounting
        workspace_manager = MagicMock()
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]
        # sync_and_version handles version numbering internally
        workspace_manager.sync_and_version.return_value = ("v3", "abc123")

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )

        # Execute
        version, git_sha = executor._auto_version("test_workspace", "preprocess", "Third run")

        # Verify
        assert version == "v3"
        assert git_sha == "abc123"


class TestStageRunRecords:
    """Test stage run database records."""

    def test_create_stage_run_record(self, test_db, test_config):
        """Should create stage_run record with all metadata."""
        # Setup
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "tag-v1", "sha123", "run")

        # Create stage version
        stage_version_id, _, _ = test_db.get_or_create_stage_version("test_workspace", "preprocess", "sha123", "a" * 64)

        pipeline_manager = MagicMock()
        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )

        # Execute
        stage_run_id = "stage-abc123"
        inputs = {"raw_data": "gs://bucket/datasets/data.csv"}
        input_sources = {"raw_data": {"source_type": "dataset", "dataset_name": "test_data"}}
        config_override = {"BATCH_SIZE": "128"}

        executor._create_stage_run_record(
            stage_run_id=stage_run_id,
            workspace="test_workspace",
            version="v1",
            stage_name="preprocess",
            stage_version_id=stage_version_id,
            inputs=inputs,
            input_sources=input_sources,
            config_override=config_override,
            reason="Testing larger batch size",
            pipeline_run_id=None,
            pipeline_name=None,
            profile=None,
            hints=None,
            config=config_override,
        )

        # Verify
        record = test_db.get_stage_run(stage_run_id)
        assert record["workspace_name"] == "test_workspace"
        assert record["version"] == "v1"
        assert record["stage_name"] == "preprocess"
        assert record["status"] == StageRunStatus.PENDING
        assert json.loads(record["config_json"]) == config_override
        assert record["stage_version_id"] == stage_version_id


class TestStageExecution:
    """Test full stage execution flow (mocked)."""

    def test_run_stage_with_local_backend(self, test_db, test_config, temp_dir):
        """Should orchestrate full stage run with local backend."""
        # Setup workspace and pipeline (with version for FK constraint)
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "abc123", "run")

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

        # Mock workspace_manager with copy-based mounting (sync_and_version)
        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = Path("/tmp/test_workspace")
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=dataset_registry,
        )

        # Mock stage execution methods
        executor._build_docker_image = MagicMock(return_value="goldfish-test-v1")
        executor._launch_container = MagicMock()

        # Execute
        result = executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="Test run")

        # Verify
        assert result.workspace == "test_workspace"
        assert result.stage == "preprocess"
        assert result.status == StageRunStatus.RUNNING
        assert result.version == "v1"

        # Verify Docker image was built
        executor._build_docker_image.assert_called_once()

        # Verify container was launched
        executor._launch_container.assert_called_once()


class TestStageVersioning:
    """Test stage versioning integration in StageExecutor."""

    def test_run_creates_stage_version(self, test_db, test_config, temp_dir):
        """run_stage() should create a stage version record."""
        # Setup workspace and pipeline
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "abc123", "run")

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
        workspace_manager.get_workspace_path.return_value = Path(temp_dir)
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=dataset_registry,
        )

        # Mock execution methods
        executor._build_docker_image = MagicMock(return_value="goldfish-test-v1")
        executor._launch_container = MagicMock()

        # Execute
        executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="Test run")

        # Verify stage version was created
        versions = test_db.list_stage_versions("test_workspace", stage="preprocess")
        assert len(versions) == 1
        assert versions[0]["git_sha"] == "abc123"
        assert versions[0]["version_num"] == 1

    def test_same_code_same_config_reuses_stage_version(self, test_db, test_config, temp_dir):
        """Running same code + config should reuse existing stage version."""
        # Setup
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "abc123", "run")
        test_db.create_version("test_workspace", "v2", "test_workspace-v2", "abc123", "run")

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(
                    name="preprocess",
                    inputs={},
                    outputs={},
                )
            ],
        )

        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = Path(temp_dir)
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )
        executor._build_docker_image = MagicMock(return_value="goldfish-test-v1")
        executor._launch_container = MagicMock()

        # Run twice with same SHA
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")
        executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="First run")

        workspace_manager.sync_and_version.return_value = ("v2", "abc123")
        executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="Second run")

        # Verify only one stage version created
        versions = test_db.list_stage_versions("test_workspace", stage="preprocess")
        assert len(versions) == 1

    def test_different_code_creates_new_stage_version(self, test_db, test_config, temp_dir):
        """Different git SHA should create new stage version."""
        # Setup
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "sha1", "run")
        test_db.create_version("test_workspace", "v2", "test_workspace-v2", "sha2", "run")

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[StageDef(name="preprocess", inputs={}, outputs={})],
        )

        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = Path(temp_dir)
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )
        executor._build_docker_image = MagicMock(return_value="goldfish-test-v1")
        executor._launch_container = MagicMock()

        # Run with different SHAs
        workspace_manager.sync_and_version.return_value = ("v1", "sha1")
        executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="First run")

        workspace_manager.sync_and_version.return_value = ("v2", "sha2")
        executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="Second run")

        # Verify two stage versions created
        versions = test_db.list_stage_versions("test_workspace", stage="preprocess")
        assert len(versions) == 2
        assert versions[0]["version_num"] == 1
        assert versions[1]["version_num"] == 2

    def test_config_override_changes_stage_version(self, test_db, test_config, temp_dir):
        """Different config_override should create new stage version."""
        # Setup
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "abc123", "run")
        test_db.create_version("test_workspace", "v2", "test_workspace-v2", "abc123", "run")

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[StageDef(name="preprocess", inputs={}, outputs={})],
        )

        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = Path(temp_dir)
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )
        executor._build_docker_image = MagicMock(return_value="goldfish-test-v1")
        executor._launch_container = MagicMock()

        # Run with same SHA but different configs
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")
        executor.run_stage(
            workspace="test_workspace",
            stage_name="preprocess",
            reason="First run",
            config_override={"BATCH_SIZE": "32"},
        )

        workspace_manager.sync_and_version.return_value = ("v2", "abc123")
        executor.run_stage(
            workspace="test_workspace",
            stage_name="preprocess",
            reason="Second run",
            config_override={"BATCH_SIZE": "64"},
        )

        # Verify two stage versions (different configs)
        versions = test_db.list_stage_versions("test_workspace", stage="preprocess")
        assert len(versions) == 2

    def test_stage_run_links_to_stage_version(self, test_db, test_config, temp_dir):
        """Stage run record should have stage_version_id set."""
        # Setup
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "abc123", "run")

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[StageDef(name="preprocess", inputs={}, outputs={})],
        )

        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = Path(temp_dir)
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )
        executor._build_docker_image = MagicMock(return_value="goldfish-test-v1")
        executor._launch_container = MagicMock()

        # Execute
        result = executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="Test run")

        # Verify stage run has stage_version_id
        stage_run = test_db.get_stage_run(result.stage_run_id)
        assert stage_run is not None
        assert stage_run["stage_version_id"] is not None

        # Verify it links to correct stage version
        stage_version = test_db.get_stage_version_for_run(result.stage_run_id)
        assert stage_version is not None
        assert stage_version["git_sha"] == "abc123"

    def test_stage_version_returns_in_result(self, test_db, test_config, temp_dir):
        """StageRunInfo should include stage_version info."""
        # Setup
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "abc123", "run")

        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[StageDef(name="preprocess", inputs={}, outputs={})],
        )

        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = Path(temp_dir)
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )
        executor._build_docker_image = MagicMock(return_value="goldfish-test-v1")
        executor._launch_container = MagicMock()

        # Execute
        result = executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="Test run")

        # Verify stage_version info is in result
        assert result.stage_version is not None
        assert result.stage_version_num == 1


class TestLineageTracking:
    """Test upstream lineage tracking in signal_lineage table."""

    def test_lineage_tracks_upstream_stage_run(self, test_db, test_config, temp_dir):
        """Downstream stage inputs should track upstream stage_run_id and stage_version_id.

        This is a critical end-to-end test for lineage provenance.
        """
        # Setup workspace
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "sha1", "run")
        test_db.create_version("test_workspace", "v2", "test_workspace-v2", "sha2", "run")

        # Create stage version for upstream (preprocess)
        upstream_version_id, _, _ = test_db.get_or_create_stage_version(
            "test_workspace", "preprocess", "sha1", "a" * 64
        )

        # Create upstream stage run (preprocess) and mark completed
        upstream_run_id = "stage-upstream1"
        test_db.create_stage_run(
            stage_run_id=upstream_run_id,
            workspace_name="test_workspace",
            version="v1",
            stage_name="preprocess",
        )
        test_db.update_stage_run_version(upstream_run_id, upstream_version_id)
        test_db.update_stage_run_status(upstream_run_id, StageRunStatus.COMPLETED)

        # Record output signal from upstream stage
        test_db.add_signal(
            stage_run_id=upstream_run_id,
            signal_name="features",
            signal_type="npy",
            storage_location="gs://bucket/runs/stage-upstream1/features",
        )

        # Setup pipeline with downstream stage that depends on upstream
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(name="preprocess", inputs={}, outputs={}),
                StageDef(
                    name="train",
                    inputs={"features": SignalDef(name="features", type="npy", from_stage="preprocess")},
                    outputs={},
                ),
            ],
        )

        # Setup workspace manager
        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = Path(temp_dir)
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]
        workspace_manager.sync_and_version.return_value = ("v2", "sha2")

        # Create executor
        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )
        executor._build_docker_image = MagicMock(return_value="goldfish-test-v1")
        executor._launch_container = MagicMock()

        # Run downstream stage (train)
        result = executor.run_stage(workspace="test_workspace", stage_name="train", reason="Test run")

        # Verify: Check signal_lineage has correct source tracking
        signals = test_db.list_signals(stage_run_id=result.stage_run_id)
        assert len(signals) == 1

        input_signal = signals[0]
        assert input_signal["signal_name"] == "features"
        assert input_signal["signal_type"] == "input"
        assert input_signal["storage_location"] == "gs://bucket/runs/stage-upstream1/features"

        # CRITICAL: These should NOT be NULL - this is what the fix addresses
        assert input_signal["source_stage_run_id"] == upstream_run_id
        assert input_signal["source_stage_version_id"] == upstream_version_id

    def test_lineage_dataset_input_has_no_upstream_run(self, test_db, test_config, temp_dir):
        """Dataset inputs should have NULL source_stage_run_id (no upstream stage)."""
        # Setup workspace
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "abc123", "run")

        # Setup pipeline with dataset input
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(
                    name="preprocess",
                    inputs={"raw_data": SignalDef(name="raw_data", type="dataset", dataset="test_data")},
                    outputs={},
                )
            ],
        )

        # Setup dataset registry
        dataset_registry = MagicMock()
        dataset_registry.get_dataset.return_value = MagicMock(gcs_location="gs://bucket/datasets/test_data")

        # Setup workspace manager
        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = Path(temp_dir)
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")

        # Create executor
        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=dataset_registry,
        )
        executor._build_docker_image = MagicMock(return_value="goldfish-test-v1")
        executor._launch_container = MagicMock()

        # Run stage
        result = executor.run_stage(workspace="test_workspace", stage_name="preprocess", reason="Test run")

        # Verify: Dataset input should have NULL source_stage_run_id
        signals = test_db.list_signals(stage_run_id=result.stage_run_id)
        assert len(signals) == 1

        input_signal = signals[0]
        assert input_signal["signal_name"] == "raw_data"
        assert input_signal["signal_type"] == "input"
        # Datasets don't have upstream stage runs
        assert input_signal["source_stage_run_id"] is None
        assert input_signal["source_stage_version_id"] is None

    def test_lineage_override_input_has_no_upstream_run(self, test_db, test_config, temp_dir):
        """Override inputs should have NULL source_stage_run_id."""
        # Setup workspace
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "abc123", "run")

        # Setup pipeline
        pipeline_manager = MagicMock()
        pipeline_manager.get_pipeline.return_value = PipelineDef(
            name="test_pipeline",
            stages=[
                StageDef(
                    name="preprocess",
                    inputs={"raw_data": SignalDef(name="raw_data", type="dataset", dataset="test_data")},
                    outputs={},
                )
            ],
        )

        # Setup workspace manager
        workspace_manager = MagicMock()
        workspace_manager.get_workspace_path.return_value = Path(temp_dir)
        workspace_manager.get_all_slots.return_value = [
            MagicMock(workspace="test_workspace", slot="w1"),
        ]
        workspace_manager.sync_and_version.return_value = ("v1", "abc123")

        # Create executor
        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=MagicMock(),
        )
        executor._build_docker_image = MagicMock(return_value="goldfish-test-v1")
        executor._launch_container = MagicMock()

        # Run stage with override
        result = executor.run_stage(
            workspace="test_workspace",
            stage_name="preprocess",
            reason="Test run",
            inputs_override={"raw_data": "gs://debug/test.csv"},
        )

        # Verify: Override input should have NULL source_stage_run_id
        signals = test_db.list_signals(stage_run_id=result.stage_run_id)
        assert len(signals) == 1

        input_signal = signals[0]
        assert input_signal["signal_name"] == "raw_data"
        assert input_signal["storage_location"] == "gs://debug/test.csv"
        # Overrides don't have upstream stage runs
        assert input_signal["source_stage_run_id"] is None
        assert input_signal["source_stage_version_id"] is None


# =============================================================================
# Regression Tests - Source name resolution in inputs_override
# =============================================================================


class TestSourceNameResolution:
    """Regression: inputs_override with source names must resolve to GCS paths."""

    def test_source_name_resolves_to_gcs_via_db_lookup(self, test_db):
        """Source name is resolved to GCS location via database lookup.

        This tests the core resolution logic: when a source name is provided,
        it should be looked up in the database and resolved to its GCS location.
        """
        # Register a source in the database
        test_db.create_source(
            source_id="src-test-001",
            name="my-tokens-v1",
            gcs_location="gs://my-bucket/datasets/tokens-v1",
            created_by="test",
            description="Test tokens dataset",
        )

        # Verify the source can be retrieved
        source = test_db.get_source("my-tokens-v1")
        assert source is not None
        assert source["gcs_location"] == "gs://my-bucket/datasets/tokens-v1"

        # Verify non-existent source returns None (falls through to literal)
        non_source = test_db.get_source("gs://literal/path")
        assert non_source is None

    def test_literal_gcs_path_not_resolved_as_source(self, test_db):
        """Literal GCS paths should not be found as sources.

        When a value like 'gs://bucket/path' is passed, it should not match
        any registered source and should be used as-is.
        """
        # Register a source with a different name
        test_db.create_source(
            source_id="src-test-002",
            name="my-dataset",
            gcs_location="gs://bucket/my-dataset",
            created_by="test",
        )

        # A literal GCS path should not be found as a source
        result = test_db.get_source("gs://some-bucket/some-path")
        assert result is None

        # But the registered source should be found by name
        result = test_db.get_source("my-dataset")
        assert result is not None
        assert result["gcs_location"] == "gs://bucket/my-dataset"
