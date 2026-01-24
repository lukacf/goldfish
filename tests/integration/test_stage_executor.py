"""Tests for StageExecutor - TDD Phase 4."""

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from goldfish.cloud.contracts import BackendStatus, RunStatus
from goldfish.db.database import Database
from goldfish.errors import GoldfishError, NotFoundError
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.models import PipelineDef, SignalDef, StageDef
from goldfish.state_machine import EventContext, StageEvent, transition
from goldfish.state_machine.types import StageState


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


def _transition_to_failed(db: Database, stage_run_id: str) -> None:
    """Transition a stage run to FAILED state via state machine."""
    ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
    transition(db, stage_run_id, StageEvent.BUILD_START, ctx)
    transition(db, stage_run_id, StageEvent.BUILD_OK, ctx)
    transition(db, stage_run_id, StageEvent.LAUNCH_OK, ctx)
    fail_ctx = EventContext(timestamp=datetime.now(UTC), source="executor", exit_code=1, exit_code_exists=True)
    transition(db, stage_run_id, StageEvent.EXIT_FAILURE, fail_ctx)


def _tensor_dataset_metadata(array_name: str, shape: list[int], dtype: str) -> dict:
    return {
        "schema_version": 1,
        "description": "Tensor dataset metadata for stage executor tests.",
        "source": {
            "format": "npz",
            "size_bytes": 1234,
            "created_at": "2025-12-24T12:00:00Z",
        },
        "schema": {
            "kind": "tensor",
            "arrays": {
                array_name: {
                    "role": "features",
                    "shape": shape,
                    "dtype": dtype,
                    "feature_names": {"kind": "list", "values": ["f1", "f2", "f3"]},
                }
            },
            "primary_array": array_name,
        },
    }


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
        inputs, sources, _ = executor._resolve_inputs("test_workspace", stage)

        # Verify inputs
        assert inputs["raw_data"] == "gs://bucket/datasets/eurusd_raw_v3"
        dataset_registry.get_dataset.assert_called_once_with("eurusd_raw_v3")

        # Verify source metadata
        assert sources["raw_data"]["source_type"] == "dataset"
        assert sources["raw_data"]["dataset_name"] == "eurusd_raw_v3"


class TestInputResolutionOutcome:
    """Test outcome-aware input resolution (success vs bad_results)."""

    def test_resolve_input_prefers_newest_non_bad(self, test_db, test_config):
        """Should prefer newest COMPLETED run that isn't marked 'bad_results'.

        Freshness matters more than explicit 'success' marking - unreviewed runs
        are valid and should be used if they're newer. Only 'bad_results' is skipped.
        """
        # 1. Setup runs for stage 'preprocess'
        with test_db._conn() as conn:
            conn.execute("PRAGMA foreign_keys = OFF")
            # Old success
            conn.execute(
                "INSERT INTO stage_runs (id, workspace_name, stage_name, version, state, outcome, started_at)"
                "VALUES (?, ?, ?, ?, ?, ?, datetime('now', '-10 minutes'))",
                ("run-success", "w1", "preprocess", "v1", "completed", "success"),
            )
            # New unreviewed
            conn.execute(
                "INSERT INTO stage_runs (id, workspace_name, stage_name, version, state, outcome, started_at)"
                "VALUES (?, ?, ?, ?, ?, ?, datetime('now', '-5 minutes'))",
                ("run-new", "w1", "preprocess", "v1", "completed", None),
            )
            # Add signal to success run
            conn.execute(
                "INSERT INTO signal_lineage (stage_run_id, signal_name, signal_type, storage_location) "
                "VALUES (?, ?, ?, ?)",
                ("run-success", "data", "output", "gs://bucket/success"),
            )
            # Add signal to new run
            conn.execute(
                "INSERT INTO signal_lineage (stage_run_id, signal_name, signal_type, storage_location) "
                "VALUES (?, ?, ?, ?)",
                ("run-new", "data", "output", "gs://bucket/new"),
            )

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=Path("/tmp"),
        )

        stage_def = StageDef(
            name="train",
            inputs={"input_data": SignalDef(name="input_data", type="signal", from_stage="preprocess", signal="data")},
            outputs={},
        )

        # Execute
        inputs, sources, _ = executor._resolve_inputs("w1", stage_def)

        # Verify: should pick the newer unreviewed run (freshness > explicit success)
        assert inputs["input_data"] == "gs://bucket/new"
        assert sources["input_data"]["source_stage_run_id"] == "run-new"

    def test_resolve_input_skips_bad_results(self, test_db, test_config):
        """Should skip runs marked as 'bad_results'."""
        with test_db._conn() as conn:
            conn.execute("PRAGMA foreign_keys = OFF")
            # Newer but bad
            conn.execute(
                "INSERT INTO stage_runs (id, workspace_name, stage_name, version, state, outcome, started_at)"
                "VALUES (?, ?, ?, ?, ?, ?, datetime('now', '-5 minutes'))",
                ("run-bad", "w1", "preprocess", "v1", "completed", "bad_results"),
            )
            # Older success
            conn.execute(
                "INSERT INTO stage_runs (id, workspace_name, stage_name, version, state, outcome, started_at)"
                "VALUES (?, ?, ?, ?, ?, ?, datetime('now', '-10 minutes'))",
                ("run-good", "w1", "preprocess", "v1", "completed", "success"),
            )
            conn.execute(
                "INSERT INTO signal_lineage (stage_run_id, signal_name, signal_type, storage_location) "
                "VALUES (?, ?, ?, ?)",
                ("run-good", "data", "output", "gs://bucket/good"),
            )

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=Path("/tmp"),
        )
        stage_def = StageDef(
            name="train",
            inputs={"input_data": SignalDef(name="input_data", type="signal", from_stage="preprocess", signal="data")},
            outputs={},
        )

        inputs, sources, _ = executor._resolve_inputs("w1", stage_def)

        assert inputs["input_data"] == "gs://bucket/good"
        assert sources["input_data"]["source_stage_run_id"] == "run-good"

    def test_resolve_input_falls_back_to_null(self, test_db, test_config):
        """Should fall back to most recent unreviewed run if no 'success' exists."""
        with test_db._conn() as conn:
            conn.execute("PRAGMA foreign_keys = OFF")
            # Most recent unreviewed
            conn.execute(
                "INSERT INTO stage_runs (id, workspace_name, stage_name, version, state, outcome, started_at)"
                "VALUES (?, ?, ?, ?, ?, ?, datetime('now', '-5 minutes'))",
                ("run-latest", "w1", "preprocess", "v1", "completed", None),
            )
            # Older unreviewed
            conn.execute(
                "INSERT INTO stage_runs (id, workspace_name, stage_name, version, state, outcome, started_at)"
                "VALUES (?, ?, ?, ?, ?, ?, datetime('now', '-10 minutes'))",
                ("run-older", "w1", "preprocess", "v1", "completed", None),
            )
            conn.execute(
                "INSERT INTO signal_lineage (stage_run_id, signal_name, signal_type, storage_location) "
                "VALUES (?, ?, ?, ?)",
                ("run-latest", "data", "output", "gs://bucket/latest"),
            )

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=Path("/tmp"),
        )
        stage_def = StageDef(
            name="train",
            inputs={"input_data": SignalDef(name="input_data", type="signal", from_stage="preprocess", signal="data")},
            outputs={},
        )

        inputs, sources, _ = executor._resolve_inputs("w1", stage_def)

        assert inputs["input_data"] == "gs://bucket/latest"
        assert sources["input_data"]["source_stage_run_id"] == "run-latest"

    def test_resolve_dataset_input_rejects_schema_mismatch(self, test_db, test_config):
        """Should reject dataset input when contract schema mismatches metadata."""
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
                            dataset="tokens_v1",
                            schema={
                                "kind": "tensor",
                                "arrays": {
                                    "X_train": {"shape": [10, 3], "dtype": "float32"},
                                },
                                "primary_array": "X_train",
                            },
                        )
                    },
                    outputs={},
                )
            ],
        )

        dataset_registry = MagicMock()
        dataset_registry.get_dataset.return_value = MagicMock(
            gcs_location="gs://bucket/datasets/tokens_v1",
            metadata=_tensor_dataset_metadata("price_changes_train", [10, 3], "float32"),
            metadata_status="ok",
        )

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=dataset_registry,
        )

        stage = pipeline_manager.get_pipeline.return_value.stages[0]
        with pytest.raises(GoldfishError, match="schema mismatch"):
            executor._resolve_inputs("test_workspace", stage)

    def test_resolve_dataset_input_allows_schema_match(self, test_db, test_config):
        """Should allow dataset input when contract schema matches metadata."""
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
                            dataset="tokens_v1",
                            schema={
                                "kind": "tensor",
                                "arrays": {
                                    "price_changes_train": {"shape": [None, 3], "dtype": "float32"},
                                },
                                "primary_array": "price_changes_train",
                            },
                        )
                    },
                    outputs={},
                )
            ],
        )

        dataset_registry = MagicMock()
        dataset_registry.get_dataset.return_value = MagicMock(
            gcs_location="gs://bucket/datasets/tokens_v1",
            metadata=_tensor_dataset_metadata("price_changes_train", [10, 3], "float32"),
            metadata_status="ok",
        )

        executor = StageExecutor(
            db=test_db,
            config=test_config,
            workspace_manager=MagicMock(),
            pipeline_manager=pipeline_manager,
            project_root=Path("/tmp"),
            dataset_registry=dataset_registry,
        )

        stage = pipeline_manager.get_pipeline.return_value.stages[0]
        inputs, sources, _ = executor._resolve_inputs("test_workspace", stage)
        assert inputs["raw_data"] == "gs://bucket/datasets/tokens_v1"
        assert sources["raw_data"]["source_type"] == "dataset"

    def test_resolve_signal_input_from_previous_stage(self, test_db, test_config):
        """Should resolve signal inputs from previous stage runs."""
        # Setup: Create a previous stage run with output signal
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "tag-v1", "sha123", "manual")
        stage_run_id = "stage-abc123"
        test_db.create_stage_run(
            stage_run_id=stage_run_id, workspace_name="test_workspace", version="v1", stage_name="preprocess"
        )
        _transition_to_completed(test_db, stage_run_id)
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
        inputs, sources, _ = executor._resolve_inputs("test_workspace", stage)

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
        inputs, sources, _ = executor._resolve_inputs(
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
        with pytest.raises(GoldfishError, match="No successful or unreviewed COMPLETED run found"):
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
            reason_structured=None,
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
        assert record["state"] == StageState.PREPARING.value
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
        assert result.status == StageState.RUNNING
        assert result.version == "v1"

        # Verify Docker image was built
        executor._build_docker_image.assert_called_once()

        # Verify container was launched
        executor._launch_container.assert_called_once()

    def test_launch_container_uses_svs_bootstrap_wrapper(self, test_db, test_config, temp_dir):
        """Entry point should run module via SVS bootstrap wrapper."""
        config = test_config.model_copy(deep=True)
        config.jobs.backend = "local"

        # Inject mock run_backend to capture launch arguments
        mock_backend = MagicMock()
        mock_backend.capabilities = MagicMock(
            has_launch_delay=False,
            supports_gpu=False,
            supports_spot=False,
        )
        mock_backend.launch.return_value = MagicMock(
            stage_run_id="stage-boot123",
            backend_type="local",
            backend_handle="container-123",
            zone=None,
        )

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
            dataset_registry=None,
            run_backend=mock_backend,
        )

        executor._launch_container(
            stage_run_id="stage-boot123",
            workspace="test_workspace",
            stage_name="train",
            image_tag="goldfish-test",
            inputs={},
            input_configs={},
            output_configs={},
            user_config={},
        )

        # Capture the RunSpec passed to run_backend.launch()
        mock_backend.launch.assert_called_once()
        run_spec = mock_backend.launch.call_args[0][0]

        # RunSpec.command contains the entrypoint script as ["sh", "-c", script]
        assert run_spec.command is not None
        script = run_spec.command[2] if len(run_spec.command) > 2 else ""
        assert "goldfish.io.bootstrap" in script
        assert "run_module_with_svs" in script

    def test_launch_container_uses_rust_entrypoint(self, test_db, test_config, temp_dir):
        """Rust runtime should execute the native entrypoint binary."""
        config = test_config.model_copy(deep=True)
        config.jobs.backend = "local"

        # Inject mock run_backend to capture launch arguments
        mock_backend = MagicMock()
        mock_backend.capabilities = MagicMock(
            has_launch_delay=False,
            supports_gpu=False,
            supports_spot=False,
        )
        mock_backend.launch.return_value = MagicMock(
            stage_run_id="stage-rust123",
            backend_type="local",
            backend_handle="container-123",
            zone=None,
        )

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
            dataset_registry=None,
            run_backend=mock_backend,
        )

        executor._launch_container(
            stage_run_id="stage-rust123",
            workspace="test_workspace",
            stage_name="encode",
            image_tag="goldfish-test",
            inputs={},
            input_configs={},
            output_configs={},
            user_config={},
            runtime="rust",
            entrypoint="entrypoints/encode",
        )

        # Capture the RunSpec passed to run_backend.launch()
        mock_backend.launch.assert_called_once()
        run_spec = mock_backend.launch.call_args[0][0]

        # RunSpec.command contains the entrypoint script as ["sh", "-c", script]
        assert run_spec.command is not None
        script = run_spec.command[2] if len(run_spec.command) > 2 else ""
        assert "/app/entrypoints/encode" in script
        assert "modules/encode.rs" in script
        assert "run_module_with_svs" not in script

    def test_launch_container_sets_home_env_for_agent_cli(self, test_db, test_config, temp_dir):
        """Goldfish should set HOME/XDG paths so agent CLIs can write config."""
        config = test_config.model_copy(deep=True)
        config.jobs.backend = "local"

        # Inject mock run_backend to capture launch arguments
        mock_backend = MagicMock()
        mock_backend.capabilities = MagicMock(
            has_launch_delay=False,
            supports_gpu=False,
            supports_spot=False,
        )
        mock_backend.launch.return_value = MagicMock(
            stage_run_id="stage-home123",
            backend_type="local",
            backend_handle="container-123",
            zone=None,
        )

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
            dataset_registry=None,
            run_backend=mock_backend,
        )

        executor._launch_container(
            stage_run_id="stage-home123",
            workspace="test_workspace",
            stage_name="train",
            image_tag="goldfish-test",
            inputs={},
            input_configs={},
            output_configs={},
            user_config={},
        )

        # Capture the RunSpec passed to run_backend.launch()
        mock_backend.launch.assert_called_once()
        run_spec = mock_backend.launch.call_args[0][0]

        # RunSpec.env contains the environment variables
        env = run_spec.env
        assert env.get("HOME") == "/app"
        assert env.get("XDG_CONFIG_HOME") == "/app/.config"
        assert env.get("XDG_CACHE_HOME") == "/app/.cache"


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
        _transition_to_completed(test_db, upstream_run_id)

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


class TestArtifactRegistryRequirement:
    """Test artifact_registry configuration for GCE backend."""

    def test_gce_backend_auto_generates_artifact_registry(self, test_db, temp_dir):
        """GCE backend should auto-generate artifact_registry from project_id."""
        from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig

        # Config with GCE backend and project_id but no explicit artifact_registry
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(temp_dir),
            jobs=JobsConfig(backend="gce"),
            gcs=GCSConfig(bucket="test-bucket"),
            gce=GCEConfig(
                project_id="my-project",
                artifact_registry=None,  # Not configured - should auto-generate
                zones=["us-central1-a"],  # Required for GCE backend
            ),
        )

        # Should not raise - artifact_registry auto-generates from project_id
        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
        )
        assert executor.artifact_registry == "us-docker.pkg.dev/my-project/goldfish"

    def test_gce_backend_requires_artifact_registry_or_project_id(self, test_db, temp_dir):
        """GCE backend should error if neither artifact_registry nor project_id configured."""
        from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig

        # Config with GCE backend but no artifact_registry and no project_id
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(temp_dir),
            jobs=JobsConfig(backend="gce"),
            gcs=GCSConfig(bucket="test-bucket"),
            gce=GCEConfig(
                # No project_id, no artifact_registry - can't auto-generate
                zones=["us-central1-a"],
            ),
        )

        with pytest.raises(GoldfishError, match="artifact_registry"):
            StageExecutor(
                db=test_db,
                config=config,
                workspace_manager=MagicMock(),
                pipeline_manager=MagicMock(),
                project_root=temp_dir,
            )

    def test_gce_backend_accepts_configured_artifact_registry(self, test_db, temp_dir):
        """GCE backend should work when artifact_registry is configured."""
        from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(temp_dir),
            jobs=JobsConfig(backend="gce"),
            gcs=GCSConfig(bucket="test-bucket"),
            gce=GCEConfig(
                project_id="my-project",
                artifact_registry="europe-docker.pkg.dev/my-project/goldfish",
                zones=["europe-west4-a"],  # Required for GCE backend
            ),
        )

        # Should not raise
        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
        )
        assert executor.artifact_registry == "europe-docker.pkg.dev/my-project/goldfish"


class TestProfileResolverIntegration:
    """Test ProfileResolver receives global_zones from config."""

    def test_global_zones_passed_to_profile_resolver(self, test_db, temp_dir):
        """Global zones from gce.zones should be passed to ProfileResolver."""
        from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(temp_dir),
            jobs=JobsConfig(backend="gce"),
            gcs=GCSConfig(bucket="test-bucket"),
            gce=GCEConfig(
                project_id="my-project",
                artifact_registry="europe-docker.pkg.dev/my-project/goldfish",
                zones=["europe-west4-a", "europe-west4-b"],  # Global zones
            ),
        )

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
        )

        # Resolve a profile and check zones are from config
        profile = executor.profile_resolver.resolve("cpu-small")
        assert profile["zones"] == ["europe-west4-a", "europe-west4-b"]

    def test_profile_override_zones_take_precedence(self, test_db, temp_dir):
        """Profile-specific zone overrides should take precedence over global zones."""
        from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(temp_dir),
            jobs=JobsConfig(backend="gce"),
            gcs=GCSConfig(bucket="test-bucket"),
            gce=GCEConfig(
                project_id="my-project",
                artifact_registry="europe-docker.pkg.dev/my-project/goldfish",
                zones=["europe-west4-a"],  # Global zones
                profile_overrides={
                    "cpu-small": {"zones": ["asia-east1-a"]},  # Profile-specific
                },
            ),
        )

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
        )

        # cpu-small should use profile-specific zones
        profile = executor.profile_resolver.resolve("cpu-small")
        assert profile["zones"] == ["asia-east1-a"]

        # h100-spot should use global zones (no profile override)
        profile = executor.profile_resolver.resolve("h100-spot")
        assert profile["zones"] == ["europe-west4-a"]


class TestGCEZonesRequirement:
    """Test that GCE backend requires zones to be configured."""

    def test_gce_backend_requires_zones_configured(self, test_db, temp_dir):
        """GCE backend should error if zones not configured - no US default."""
        from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig

        # Config with GCE backend but no zones configured
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(temp_dir),
            jobs=JobsConfig(backend="gce"),
            gcs=GCSConfig(bucket="test-bucket"),
            gce=GCEConfig(
                project_id="my-project",
                artifact_registry="europe-docker.pkg.dev/my-project/goldfish",
                zones=None,  # Not configured!
            ),
        )

        with pytest.raises(GoldfishError, match="zones"):
            StageExecutor(
                db=test_db,
                config=config,
                workspace_manager=MagicMock(),
                pipeline_manager=MagicMock(),
                project_root=temp_dir,
            )

    def test_gce_backend_accepts_configured_zones(self, test_db, temp_dir):
        """GCE backend should work when zones are configured."""
        from goldfish.config import GCEConfig, GCSConfig, GoldfishConfig, JobsConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(temp_dir),
            jobs=JobsConfig(backend="gce"),
            gcs=GCSConfig(bucket="test-bucket"),
            gce=GCEConfig(
                project_id="my-project",
                artifact_registry="europe-docker.pkg.dev/my-project/goldfish",
                zones=["europe-west4-a", "europe-west4-b"],  # Configured!
            ),
        )

        # Should not raise
        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
        )
        # Verify run_backend is created with GCE capabilities (has_launch_delay=True)
        assert executor.run_backend is not None
        assert executor.run_backend.capabilities.has_launch_delay is True


class TestLocalBackendRequirements:
    """Test local backend configuration requirements."""

    def test_local_backend_does_not_require_artifact_registry(self, test_db, temp_dir):
        """Local backend should work without artifact_registry configured."""
        from goldfish.config import GoldfishConfig, JobsConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(temp_dir),
            jobs=JobsConfig(backend="local"),
            # No GCE config, no artifact_registry
        )

        # Should not raise
        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
        )
        assert executor.artifact_registry is None

    def test_local_backend_does_not_require_zones(self, test_db, temp_dir):
        """Local backend should work without zones configured."""
        from goldfish.config import GoldfishConfig, JobsConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(temp_dir),
            jobs=JobsConfig(backend="local"),
            # No GCE config, no zones
        )

        # Should not raise
        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
        )
        # Run backend should be created successfully
        assert executor.run_backend is not None


class TestLocalExecutorConfigFromYaml:
    """Test LocalRunBackend is created with correct config."""

    def test_local_backend_created_with_memory_config(self, test_db, temp_dir):
        """LocalRunBackend should be created when memory_limit is configured."""
        from goldfish.config import GoldfishConfig, JobsConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(temp_dir),
            jobs=JobsConfig(
                backend="local",
                container_memory="8g",  # Custom memory limit
            ),
        )

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
        )

        # Verify run_backend is created and has local backend capabilities
        assert executor.run_backend is not None
        assert executor.run_backend.capabilities.has_launch_delay is False  # Local backend

    def test_local_backend_created_with_cpu_config(self, test_db, temp_dir):
        """LocalRunBackend should be created when cpu_limit is configured."""
        from goldfish.config import GoldfishConfig, JobsConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(temp_dir),
            jobs=JobsConfig(
                backend="local",
                container_cpus="4.0",  # Custom CPU limit
            ),
        )

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
        )

        # Verify run_backend is created and has local backend capabilities
        assert executor.run_backend is not None
        assert executor.run_backend.capabilities.has_launch_delay is False  # Local backend

    def test_local_backend_created_with_pids_config(self, test_db, temp_dir):
        """LocalRunBackend should be created when pids_limit is configured."""
        from goldfish.config import GoldfishConfig, JobsConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(temp_dir),
            jobs=JobsConfig(
                backend="local",
                container_pids=200,  # Custom pids limit
            ),
        )

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
        )

        # Verify run_backend is created and has local backend capabilities
        assert executor.run_backend is not None
        assert executor.run_backend.capabilities.has_launch_delay is False  # Local backend

    def test_local_backend_created_with_defaults(self, test_db, temp_dir):
        """LocalRunBackend should be created with defaults when not configured."""
        from goldfish.config import GoldfishConfig, JobsConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path=str(temp_dir),
            jobs=JobsConfig(backend="local"),  # No container limits specified
        )

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
        )

        # Verify run_backend is created and has local backend capabilities
        assert executor.run_backend is not None
        assert executor.run_backend.capabilities.has_launch_delay is False  # Local backend
        assert executor.run_backend.capabilities.supports_preemption is True  # Local supports SIGTERM


class TestGCEPreemptionHandling:
    """Regression tests for GCE spot instance preemption handling.

    These tests verify that when a GCE spot instance gets preempted:
    1. wait_for_completion() properly finalizes the run instead of throwing
    2. The status is correctly updated based on exit code in GCS
    3. The dashboard shows correct status (not stuck on "launching")

    These tests use mock run_backend injection to simulate GCE behavior.
    """

    def _create_mock_gce_backend(self) -> MagicMock:
        """Create a mock run_backend with GCE capabilities."""
        mock_backend = MagicMock()
        mock_backend.capabilities = MagicMock(
            has_launch_delay=True,  # GCE has launch delay
            supports_gpu=True,
            supports_spot=True,
            supports_preemption=True,
            ack_timeout_seconds=3.0,
            timeout_becomes_pending=True,
        )
        return mock_backend

    def test_preempted_instance_with_running_state_finalizes_as_failed(
        self, test_db, test_config, temp_dir, monkeypatch
    ):
        """Preempted instance that was RUNNING should finalize based on exit code.

        Regression test: Previously, preempted instances would throw GoldfishError
        and remain stuck in LAUNCHING/RUNNING state forever.
        """
        from datetime import UTC, datetime, timedelta

        # Setup workspace and stage run
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "sha123", "run")

        run_id = f"stage-{uuid4().hex[:8]}"
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test_workspace",
            version="v1",
            stage_name="train",
            pipeline_run_id=None,
            pipeline_name=None,
            config={},
            inputs={},
            profile="h100-spot",
            hints=None,
            backend_type="gce",
            backend_handle=run_id,
        )
        # Set to RUNNING state (instance was running before preemption)
        started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE stage_runs SET state=?, started_at=? WHERE id=?",
                (StageState.RUNNING.value, started_at, run_id),
            )

        # Create executor with mock GCE run_backend
        config = test_config.model_copy(deep=True)
        config.jobs.backend = "gce"

        mock_backend = self._create_mock_gce_backend()
        # Simulate preemption: backend returns FAILED with exit code 137 (SIGKILL)
        mock_backend.get_status.return_value = BackendStatus(
            status=RunStatus.FAILED,
            exit_code=137,
            termination_cause="preemption",
        )

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
            dataset_registry=None,
            run_backend=mock_backend,
        )

        executor._finalize_stage_run = MagicMock()
        monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

        # Execute - should NOT throw, should finalize
        status = executor.wait_for_completion(run_id)

        # Verify finalization happened
        assert status == StageState.FAILED
        executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageState.FAILED)

    def test_preempted_instance_with_launching_state_but_exit_code_finalizes(
        self, test_db, test_config, temp_dir, monkeypatch
    ):
        """Preemption between polls may leave state=LAUNCHING but exit code exists.

        Regression test: Instance could run and be preempted before status poll
        updated state to RUNNING. The exit code in GCS proves it ran.
        """
        from datetime import UTC, datetime, timedelta

        # Setup
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "sha123", "run")

        run_id = f"stage-{uuid4().hex[:8]}"
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test_workspace",
            version="v1",
            stage_name="train",
            pipeline_run_id=None,
            pipeline_name=None,
            config={},
            inputs={},
            profile="h100-spot",
            hints=None,
            backend_type="gce",
            backend_handle=run_id,
        )
        # Still at LAUNCHING state (polls never caught it running)
        started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE stage_runs SET state=?, started_at=? WHERE id=?",
                (StageState.LAUNCHING.value, started_at, run_id),
            )

        config = test_config.model_copy(deep=True)
        config.jobs.backend = "gce"

        mock_backend = self._create_mock_gce_backend()
        # Instance completed OK before preemption: backend recovered exit code from GCS
        mock_backend.get_status.return_value = BackendStatus(
            status=RunStatus.COMPLETED,
            exit_code=0,
        )

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
            dataset_registry=None,
            run_backend=mock_backend,
        )

        executor._finalize_stage_run = MagicMock()
        monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

        status = executor.wait_for_completion(run_id)

        # Should finalize as COMPLETED because exit_code=0
        assert status == StageState.COMPLETED
        executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageState.COMPLETED)

    def test_launch_failure_without_exit_code_marks_failed(self, test_db, test_config, temp_dir, monkeypatch):
        """Instance that never ran (no exit code) should be marked TERMINATED.

        Regression test: Previously this would throw GoldfishError.
        """
        from datetime import UTC, datetime, timedelta

        # Setup
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "sha123", "run")

        run_id = f"stage-{uuid4().hex[:8]}"
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test_workspace",
            version="v1",
            stage_name="train",
            pipeline_run_id=None,
            pipeline_name=None,
            config={},
            inputs={},
            profile="h100-spot",
            hints=None,
            backend_type="gce",
            backend_handle=run_id,
        )
        started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE stage_runs SET state=?, started_at=? WHERE id=?",
                (StageState.LAUNCHING.value, started_at, run_id),
            )

        config = test_config.model_copy(deep=True)
        config.jobs.backend = "gce"

        mock_backend = self._create_mock_gce_backend()
        # Instance not found and no exit code: backend raises NotFoundError
        mock_backend.get_status.side_effect = NotFoundError(f"instance:{run_id}")

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
            dataset_registry=None,
            run_backend=mock_backend,
        )

        executor._finalize_stage_run = MagicMock()
        monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")
        monkeypatch.setenv("GOLDFISH_GCE_LAUNCH_TIMEOUT", "0")

        status = executor.wait_for_completion(run_id)

        # Should mark as TERMINATED without calling finalize (never ran)
        assert status == StageState.TERMINATED
        executor._finalize_stage_run.assert_not_called()

        # Verify error message in DB
        row = test_db.get_stage_run(run_id)
        assert row is not None
        # State machine transitions to TERMINATED via INSTANCE_LOST (never ran)
        assert row["state"] == StageState.TERMINATED.value
        assert "not found" in row["error"].lower() or "failed to launch" in row["error"].lower()

    def test_refresh_status_recovers_preempted_run_with_launching_state(
        self, test_db, test_config, temp_dir, monkeypatch
    ):
        """refresh_status_once should recover preempted runs even at LAUNCHING state.

        Regression test: _refresh_status_once_unlocked would skip recovery when
        state was BUILDING/LAUNCHING, even if exit code in GCS proved it ran.
        """
        from datetime import UTC, datetime, timedelta

        # Setup
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "sha123", "run")

        run_id = f"stage-{uuid4().hex[:8]}"
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test_workspace",
            version="v1",
            stage_name="train",
            pipeline_run_id=None,
            pipeline_name=None,
            config={},
            inputs={},
            profile="h100-spot",
            hints=None,
            backend_type="gce",
            backend_handle=run_id,
        )
        started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE stage_runs SET state=?, started_at=? WHERE id=?",
                (StageState.LAUNCHING.value, started_at, run_id),
            )

        config = test_config.model_copy(deep=True)
        config.jobs.backend = "gce"

        mock_backend = self._create_mock_gce_backend()
        # Backend recovered exit code from GCS showing failure
        mock_backend.get_status.return_value = BackendStatus(
            status=RunStatus.FAILED,
            exit_code=1,
        )

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
            dataset_registry=None,
            run_backend=mock_backend,
        )

        executor._finalize_stage_run = MagicMock()
        monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

        # Use refresh_status_once (async path)
        status = executor.refresh_status_once(run_id)

        # Should finalize as FAILED
        assert status == StageState.FAILED
        executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageState.FAILED)

    def test_dashboard_shows_correct_state_after_preemption(self, test_db, test_config, temp_dir, monkeypatch):
        """After preemption recovery, list_runs should show correct state.

        Regression test: Dashboard was showing 'launching' for runs that had
        actually run for 9 epochs and been preempted.
        """
        from datetime import UTC, datetime, timedelta

        # Setup
        test_db.create_workspace_lineage("test_workspace", description="Test")
        test_db.create_version("test_workspace", "v1", "test_workspace-v1", "sha123", "run")

        run_id = f"stage-{uuid4().hex[:8]}"
        test_db.create_stage_run(
            stage_run_id=run_id,
            workspace_name="test_workspace",
            version="v1",
            stage_name="train",
            pipeline_run_id=None,
            pipeline_name=None,
            config={},
            inputs={},
            profile="h100-spot",
            hints=None,
            backend_type="gce",
            backend_handle=run_id,
        )
        started_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        with test_db._conn() as conn:
            conn.execute(
                "UPDATE stage_runs SET state=?, started_at=? WHERE id=?",
                (StageState.RUNNING.value, started_at, run_id),
            )

        config = test_config.model_copy(deep=True)
        config.jobs.backend = "gce"

        mock_backend = self._create_mock_gce_backend()
        # Simulate preemption: backend returns FAILED with exit code 137
        mock_backend.get_status.return_value = BackendStatus(
            status=RunStatus.FAILED,
            exit_code=137,
            termination_cause="preemption",
        )

        executor = StageExecutor(
            db=test_db,
            config=config,
            workspace_manager=MagicMock(),
            pipeline_manager=MagicMock(),
            project_root=temp_dir,
            dataset_registry=None,
            run_backend=mock_backend,
        )

        executor._finalize_stage_run = MagicMock()
        monkeypatch.setenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "0")

        # Recover the preempted run
        status = executor.wait_for_completion(run_id)

        # Verify finalization was called correctly (error message and state
        # transition happen inside _finalize_stage_run which handles the state machine)
        assert status == StageState.FAILED
        executor._finalize_stage_run.assert_called_once_with(run_id, "gce", StageState.FAILED)

        # Verify list_runs returns the run (state is managed by state machine inside finalize)
        runs = test_db.list_stage_runs(workspace_name="test_workspace")
        assert len(runs) == 1
        assert runs[0]["id"] == run_id
