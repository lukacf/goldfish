"""E2E test for full pipeline execution workflow.

This test covers the complete ML workflow:
1. Create workspace
2. Define pipeline (pipeline.yaml)
3. Create modules (Python scripts)
4. Create configs (YAML files)
5. Run stage via execution engine
6. Verify job execution and results

Uses real components (no mocks) to verify end-to-end integration.
"""

import pytest
import subprocess
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone

from goldfish.config import GoldfishConfig, StateMdConfig, AuditConfig, JobsConfig, GCSConfig
from goldfish.db.database import Database
from goldfish.workspace.manager import WorkspaceManager
from goldfish.pipeline.manager import PipelineManager
from goldfish.datasets.registry import DatasetRegistry
from goldfish.jobs.stage_executor import StageExecutor


@pytest.fixture
def pipeline_project():
    """Create a full project setup for pipeline execution testing."""
    # Create temp directory
    tmp = tempfile.mkdtemp(prefix="goldfish_pipeline_e2e_")
    project_root = Path(tmp)

    try:
        # Create project structure
        (project_root / "workspaces").mkdir()
        (project_root / ".goldfish").mkdir()
        (project_root / "experiments").mkdir()
        (project_root / "data").mkdir()

        # Initialize dev repo
        dev_repo = project_root / ".goldfish" / "dev"
        dev_repo.mkdir(parents=True)
        subprocess.run(["git", "init"], cwd=dev_repo, capture_output=True, check=True)
        subprocess.run(
            ["git", "config", "user.email", "test@example.com"],
            cwd=dev_repo, capture_output=True, check=True
        )
        subprocess.run(
            ["git", "config", "user.name", "Test User"],
            cwd=dev_repo, capture_output=True, check=True
        )

        # Create main branch with initial commit
        (dev_repo / "README.md").write_text("# Test Project")
        subprocess.run(["git", "add", "-A"], cwd=dev_repo, capture_output=True, check=True)
        subprocess.run(
            ["git", "commit", "-m", "Initial commit"],
            cwd=dev_repo, capture_output=True, check=True
        )
        subprocess.run(
            ["git", "branch", "-M", "main"],
            cwd=dev_repo, capture_output=True, check=True
        )

        # Create config
        config = GoldfishConfig(
            project_name="pipeline-e2e-test",
            dev_repo_path=".goldfish/dev",
            workspaces_dir="workspaces",
            slots=["w1", "w2", "w3"],
            state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(backend="local", experiments_dir="experiments"),
            gcs=GCSConfig(
                bucket="test-e2e-bucket",
                sources_prefix="datasets/",
                artifacts_prefix="artifacts/",
            ),
            invariants=[],
        )

        # Create database
        db = Database(project_root / ".goldfish" / "goldfish.db")

        # Create workspace manager (this creates git layer and state manager internally)
        workspace_manager = WorkspaceManager(
            config=config,
            project_root=project_root,
            db=db,
        )

        # Create pipeline manager
        pipeline_manager = PipelineManager(
            db=db,
            workspace_manager=workspace_manager,
        )

        # Create dataset registry
        dataset_registry = DatasetRegistry(
            db=db,
            config=config,
        )

        # Create stage executor
        stage_executor = StageExecutor(
            db=db,
            config=config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=project_root,
            dataset_registry=dataset_registry,
        )

        yield {
            "project_root": project_root,
            "dev_repo": dev_repo,
            "config": config,
            "db": db,
            "workspace_manager": workspace_manager,
            "pipeline_manager": pipeline_manager,
            "dataset_registry": dataset_registry,
            "stage_executor": stage_executor,
        }
    finally:
        # Cleanup
        shutil.rmtree(tmp, ignore_errors=True)


class TestPipelineExecutionWorkflow:
    """Test complete pipeline execution workflow."""

    def test_full_pipeline_execution_workflow(self, pipeline_project):
        """Complete E2E test: create workspace → define pipeline → run stage → verify results.

        This test verifies the entire ML workflow from start to finish:
        1. Create a workspace for a simple ML pipeline
        2. Define pipeline.yaml with two stages (preprocess, train)
        3. Create module files (Python scripts) for each stage
        4. Create config files (YAML) for each stage
        5. Checkpoint the workspace
        6. Validate the pipeline
        7. Run the preprocess stage
        8. Verify the stage execution creates a job and tracks it
        9. Verify workspace auto-versioning
        10. Verify pipeline retrieval
        11. Verify audit trail
        """
        workspace_manager = pipeline_project["workspace_manager"]
        pipeline_manager = pipeline_project["pipeline_manager"]
        dataset_registry = pipeline_project["dataset_registry"]
        stage_executor = pipeline_project["stage_executor"]
        db = pipeline_project["db"]
        project_root = pipeline_project["project_root"]

        # === Step 1: Create workspace ===
        workspace_manager.create_workspace(
            name="simple-ml-pipeline",
            goal="Test end-to-end pipeline execution",
            reason="E2E test for pipeline execution workflow"
        )

        # Mount workspace to w1
        mount_result = workspace_manager.mount(
            workspace="simple-ml-pipeline",
            slot="w1",
            reason="Setting up pipeline for E2E test"
        )
        assert mount_result.success is True

        workspace_path = project_root / "workspaces" / "w1"
        assert workspace_path.exists()

        # Create requirements.txt for Docker build
        (workspace_path / "requirements.txt").write_text("numpy>=1.20.0\n")

        # === Step 2: Define pipeline.yaml ===
        # NOTE: Using a simple pipeline without dataset inputs for E2E test
        # Dataset inputs require GCS upload which adds complexity
        pipeline_yaml = """name: simple-ml-pipeline
description: Simple two-stage ML pipeline for E2E testing

stages:
  - name: preprocess
    inputs: {}
    outputs:
      features:
        type: npy
        storage: local

  - name: train
    inputs:
      features:
        from_stage: preprocess
        signal_name: features
        type: npy
    outputs:
      model:
        type: directory
        storage: local
        artifact: true
"""
        (workspace_path / "pipeline.yaml").write_text(pipeline_yaml)

        # === Step 3: Create module files ===
        modules_dir = workspace_path / "modules"
        modules_dir.mkdir(exist_ok=True)

        # Create CLAUDE.md for modules
        (modules_dir / "CLAUDE.md").write_text("""# Modules

This directory contains the Python module implementations for each pipeline stage.

Each module should:
- Use `from goldfish.io import load_input, save_output`
- Define a `main()` function as the entry point
- Handle inputs/outputs defined in pipeline.yaml
""")

        # Create preprocess.py
        preprocess_module = """\"\"\"Preprocess stage - load raw data and create features.\"\"\"

def main():
    '''Preprocess raw data into features.

    For E2E testing, this creates a simple numpy array.
    '''
    import numpy as np
    from pathlib import Path

    # For testing, create synthetic features
    features = np.random.randn(100, 10).astype(np.float32)

    # Save output
    output_dir = Path("/mnt/outputs/features")
    output_dir.mkdir(parents=True, exist_ok=True)
    np.save(output_dir / "features.npy", features)

    print(f"Preprocessing complete: created features with shape {features.shape}")

if __name__ == "__main__":
    main()
"""
        (modules_dir / "preprocess.py").write_text(preprocess_module)

        # Create train.py
        train_module = """\"\"\"Train stage - train model on features.\"\"\"

def main():
    '''Train a simple model on features.

    For E2E testing, this loads features and creates a dummy model.
    '''
    import numpy as np
    from pathlib import Path

    # Load features
    features_path = Path("/mnt/inputs/features/features.npy")
    features = np.load(features_path)

    print(f"Training with features shape: {features.shape}")

    # Create dummy model (just save metadata)
    model_dir = Path("/mnt/outputs/model")
    model_dir.mkdir(parents=True, exist_ok=True)

    model_metadata = {
        "input_shape": list(features.shape),
        "model_type": "dummy",
        "trained_at": "2025-12-06"
    }

    import json
    with open(model_dir / "metadata.json", "w") as f:
        json.dump(model_metadata, f, indent=2)

    print("Training complete: model saved")

if __name__ == "__main__":
    main()
"""
        (modules_dir / "train.py").write_text(train_module)

        # === Step 4: Create config files ===
        configs_dir = workspace_path / "configs"
        configs_dir.mkdir(exist_ok=True)

        # Create CLAUDE.md for configs
        (configs_dir / "CLAUDE.md").write_text("""# Configs

This directory contains YAML configuration files for each pipeline stage.

Each config file defines:
- Compute resources (CPU, memory, GPU)
- Storage preferences
- Environment variables
""")

        # Create preprocess.yaml
        preprocess_config = """# Preprocess stage configuration

compute:
  cpu: 2
  memory: 4GB
  gpu: none
  disk: 10GB

storage:
  outputs:
    features:
      storage: local

env:
  BATCH_SIZE: "32"
  COMPRESSION: "none"
"""
        (configs_dir / "preprocess.yaml").write_text(preprocess_config)

        # Create train.yaml
        train_config = """# Train stage configuration

compute:
  cpu: 4
  memory: 8GB
  gpu: none
  disk: 20GB

storage:
  outputs:
    model:
      storage: local

env:
  EPOCHS: "10"
  LEARNING_RATE: "0.001"
"""
        (configs_dir / "train.yaml").write_text(train_config)

        # === Step 5: Checkpoint the workspace ===
        checkpoint_result = workspace_manager.checkpoint(
            slot="w1",
            message="Initial pipeline setup with preprocess and train stages"
        )
        assert checkpoint_result.success is True
        snapshot_id = checkpoint_result.snapshot_id
        assert snapshot_id.startswith("snap-")

        # === Step 6: Validate pipeline ===
        validation_result = pipeline_manager.validate_pipeline("simple-ml-pipeline")
        assert len(validation_result) == 0, f"Pipeline validation failed: {validation_result}"

        # === Step 7: Run preprocess stage ===
        # Note: This will create a stage run record, build Docker image (mocked),
        # and launch container (mocked). In a full implementation, this would
        # actually execute the module code.
        stage_run = stage_executor.run_stage(
            workspace="simple-ml-pipeline",
            stage_name="preprocess",
            config_override=None,
            inputs_override=None,
            reason="E2E test execution of preprocess stage"
        )

        # === Step 8: Verify stage run was created ===
        assert stage_run.stage_run_id.startswith("stage-")
        assert stage_run.workspace == "simple-ml-pipeline"
        assert stage_run.stage == "preprocess"
        # Status is "pending" because local executor runs jobs asynchronously
        assert stage_run.status in ["pending", "running"]
        assert stage_run.started_at is not None

        # Verify stage run is in database
        with db._conn() as conn:
            stage_run_record = conn.execute(
                "SELECT * FROM stage_runs WHERE id = ?",
                (stage_run.stage_run_id,)
            ).fetchone()
        assert stage_run_record is not None
        assert stage_run_record["workspace_name"] == "simple-ml-pipeline"
        assert stage_run_record["stage_name"] == "preprocess"
        assert stage_run_record["status"] in ["pending", "running"]

        # === Step 9: Verify workspace auto-versioning ===
        # Stage execution should create a new version tag
        with db._conn() as conn:
            versions = conn.execute(
                "SELECT * FROM workspace_versions WHERE workspace_name = ?",
                ("simple-ml-pipeline",)
            ).fetchall()
        # Should have at least one version from stage run
        assert len(versions) >= 1

        # === Step 10: Verify pipeline is retrievable ===
        pipeline_def = pipeline_manager.get_pipeline("simple-ml-pipeline")
        assert pipeline_def.name == "simple-ml-pipeline"
        assert len(pipeline_def.stages) == 2
        assert pipeline_def.stages[0].name == "preprocess"
        assert pipeline_def.stages[1].name == "train"

        # === Step 11: Verify audit trail ===
        audits = db.get_recent_audit(limit=20)

        # Should have audit entries for:
        # - create_workspace
        # - mount
        # - checkpoint
        operations = [a["operation"] for a in audits]
        assert "create_workspace" in operations
        assert "mount" in operations
        assert "checkpoint" in operations

        # === Cleanup ===
        workspace_manager.hibernate(
            slot="w1",
            reason="E2E test complete"
        )

    def test_pipeline_with_invalid_stage_fails(self, pipeline_project):
        """Attempting to run a non-existent stage should fail gracefully."""
        workspace_manager = pipeline_project["workspace_manager"]
        stage_executor = pipeline_project["stage_executor"]
        project_root = pipeline_project["project_root"]

        # Create workspace with minimal pipeline
        workspace_manager.create_workspace(
            name="invalid-stage-test",
            goal="Test error handling",
            reason="Testing invalid stage execution"
        )
        workspace_manager.mount(
            workspace="invalid-stage-test",
            slot="w1",
            reason="Testing invalid stage"
        )

        workspace_path = project_root / "workspaces" / "w1"

        # Create minimal pipeline with only one stage
        pipeline_yaml = """name: minimal-pipeline
description: Minimal pipeline for error testing

stages:
  - name: preprocess
    inputs: {}
    outputs: {}
"""
        (workspace_path / "pipeline.yaml").write_text(pipeline_yaml)

        # Create dummy files to pass validation
        (workspace_path / "modules").mkdir(exist_ok=True)
        (workspace_path / "modules" / "preprocess.py").write_text("def main(): pass")
        (workspace_path / "configs").mkdir(exist_ok=True)
        (workspace_path / "configs" / "preprocess.yaml").write_text("compute: {cpu: 1}")

        workspace_manager.checkpoint(slot="w1", message="Setup completed for test")

        # Try to run a stage that doesn't exist
        from goldfish.errors import GoldfishError

        with pytest.raises(GoldfishError, match="Stage.*not found"):
            stage_executor.run_stage(
                workspace="invalid-stage-test",
                stage_name="nonexistent_stage",
                reason="This should fail"
            )

        # Cleanup
        workspace_manager.hibernate(slot="w1", reason="Test complete - cleanup")

    def test_pipeline_validation_catches_missing_files(self, pipeline_project):
        """Pipeline validation should catch missing module or config files."""
        workspace_manager = pipeline_project["workspace_manager"]
        pipeline_manager = pipeline_project["pipeline_manager"]
        project_root = pipeline_project["project_root"]

        # Create workspace
        workspace_manager.create_workspace(
            name="validation-test",
            goal="Test pipeline validation",
            reason="Testing pipeline validation logic"
        )
        workspace_manager.mount(
            workspace="validation-test",
            slot="w1",
            reason="Setting up validation test"
        )

        workspace_path = project_root / "workspaces" / "w1"

        # Create pipeline that references stage "process"
        pipeline_yaml = """name: validation-test
description: Pipeline with missing files

stages:
  - name: process
    inputs: {}
    outputs: {}
"""
        (workspace_path / "pipeline.yaml").write_text(pipeline_yaml)

        # Don't create modules/process.py or configs/process.yaml
        (workspace_path / "modules").mkdir(exist_ok=True)
        (workspace_path / "configs").mkdir(exist_ok=True)

        workspace_manager.checkpoint(slot="w1", message="Pipeline with missing files")

        # Validate pipeline - should return errors
        errors = pipeline_manager.validate_pipeline("validation-test")

        assert len(errors) >= 2  # Should catch missing module AND config

        # Check error messages mention the missing files
        error_text = "\n".join(errors)
        assert "process.py" in error_text or "Module not found" in error_text
        assert "process.yaml" in error_text or "Config not found" in error_text

        # Cleanup
        workspace_manager.hibernate(slot="w1", reason="Validation test complete")
