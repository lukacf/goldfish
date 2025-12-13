"""Tests for config validation tools - TDD for new validation features.

Tests validate_config() tool and improved error messages.
"""

import subprocess
from pathlib import Path

import pytest

from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
from goldfish.db.database import Database
from goldfish.errors import GoldfishError
from goldfish.workspace.manager import WorkspaceManager


def run_git(cmd: list[str], cwd: Path) -> str:
    """Run a git command and return stdout."""
    result = subprocess.run(
        ["git"] + cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


@pytest.fixture
def e2e_setup(temp_dir):
    """Create a full project setup for e2e testing.

    Returns:
        Dict with project_root, dev_repo, db, git, and manager
    """
    # Create dev repo
    dev_repo = temp_dir / "project-dev"
    dev_repo.mkdir()
    run_git(["init"], dev_repo)
    run_git(["config", "user.email", "test@example.com"], dev_repo)
    run_git(["config", "user.name", "Test User"], dev_repo)
    (dev_repo / "README.md").write_text("# Test Project")
    (dev_repo / "code.py").write_text("# Initial code")
    run_git(["add", "."], dev_repo)
    run_git(["commit", "-m", "Initial commit"], dev_repo)

    # Create project structure
    project_root = temp_dir / "project"
    project_root.mkdir()
    (project_root / "workspaces").mkdir()
    (project_root / ".goldfish").mkdir()
    (project_root / "experiments").mkdir()

    # Create database
    db = Database(project_root / ".goldfish" / "goldfish.db")

    # Create config - dev_repo_path is relative to project_root.parent
    config = GoldfishConfig(
        project_name="test-project",
        dev_repo_path="project-dev",  # Sibling of project_root (relative to parent)
        workspaces_dir="workspaces",
        slots=["w1", "w2", "w3"],
        state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
        audit=AuditConfig(min_reason_length=15),
        jobs=JobsConfig(backend="local", experiments_dir="experiments"),
        invariants=[],
    )

    # Create WorkspaceManager
    manager = WorkspaceManager(
        config=config,
        project_root=project_root,
        db=db,
    )

    # Also expose git layer for direct testing
    git = manager.git

    return {
        "project_root": project_root,
        "dev_repo": dev_repo,
        "config": config,
        "db": db,
        "git": git,
        "manager": manager,
    }


class TestGoldfishYamlValidation:
    """Test goldfish.yaml validation with better error messages."""

    def test_catches_unknown_top_level_field(self, temp_dir):
        """Unknown fields like 'projeect_name' should be caught with suggestions."""
        config_path = temp_dir / "goldfish.yaml"
        config_path.write_text("""
project_name: test
dev_repo_path: ./dev
projeect_name: typo  # Should be caught!
""")

        with pytest.raises(GoldfishError) as exc_info:
            GoldfishConfig.load(temp_dir)

        error_msg = str(exc_info.value)
        # Should mention the unknown field
        assert "projeect_name" in error_msg or "unknown" in error_msg.lower()
        # Should suggest the correct field
        assert "project_name" in error_msg or "Did you mean" in error_msg

    def test_catches_unknown_nested_field(self, temp_dir):
        """Unknown nested fields like gce.projeect should be caught."""
        config_path = temp_dir / "goldfish.yaml"
        config_path.write_text("""
project_name: test
dev_repo_path: ./dev
gce:
  project_id: my-project
  projeect: typo  # Should be caught!
""")

        with pytest.raises(GoldfishError) as exc_info:
            GoldfishConfig.load(temp_dir)

        error_msg = str(exc_info.value)
        assert "projeect" in error_msg or "unknown" in error_msg.lower()

    def test_lists_valid_fields_on_error(self, temp_dir):
        """Error messages should list valid field names."""
        config_path = temp_dir / "goldfish.yaml"
        config_path.write_text("""
project_name: test
dev_repo_path: ./dev
invalid_field: value
""")

        with pytest.raises(GoldfishError) as exc_info:
            GoldfishConfig.load(temp_dir)

        error_msg = str(exc_info.value)
        # Should provide helpful context about valid fields
        # (exact format TBD during implementation)
        assert "invalid_field" in error_msg or "unknown" in error_msg.lower()


class TestStageConfigValidation:
    """Test stage config (configs/train.yaml) validation."""

    @pytest.mark.skip(reason="Stage config validation not yet implemented")
    def test_catches_unknown_stage_config_field(self, e2e_setup):
        """Unknown stage config fields like 'freeze_backone' should be caught."""
        manager = e2e_setup["manager"]
        project_root = e2e_setup["project_root"]

        # Create workspace and mount
        manager.create_workspace(
            name="config-test", goal="Test config validation", reason="Testing stage config validation"
        )
        manager.mount(workspace="config-test", slot="w1", reason="Testing config")

        slot_path = project_root / "workspaces" / "w1"

        # Create a pipeline.yaml
        (slot_path / "pipeline.yaml").write_text("""
stages:
  - name: train
    inputs:
      data: {type: dataset, dataset: test_data}
    outputs:
      model: {type: directory}
""")

        # Create configs dir and a stage config with typo
        (slot_path / "configs").mkdir(exist_ok=True)
        (slot_path / "configs" / "train.yaml").write_text("""
# Stage config with typo
freeze_backone: true  # Should be freeze_backbone!
learning_rate: 0.001
""")

        # Create minimal module
        (slot_path / "modules").mkdir(exist_ok=True)
        (slot_path / "modules" / "train.py").write_text("""
from goldfish.io import load_input, save_output
# Train stage
""")

        # Checkpoint to save changes
        manager.checkpoint(slot="w1", message="Add config with typo")

        # Now validate_config should catch the typo
        # (This is the feature we're implementing)
        from goldfish.config_validation import validate_stage_config

        result = validate_stage_config(slot_path / "configs" / "train.yaml")

        # Should warn about unknown field (not fail hard - stage configs are flexible)
        assert not result.valid or len(result.warnings) > 0
        warning_text = " ".join(result.warnings)
        assert "freeze_backone" in warning_text or "unknown" in warning_text.lower()

        # Cleanup
        manager.hibernate(slot="w1", reason="Done with config test")


@pytest.mark.skip(reason="validate_config MCP tool not yet implemented")
class TestValidateConfigTool:
    """Test the validate_config MCP tool."""

    def test_validate_config_returns_all_issues(self, e2e_setup):
        """validate_config should return all validation issues at once."""
        # This tests the MCP tool directly
        pass  # Implement after creating the tool

    def test_validate_config_validates_goldfish_yaml(self, e2e_setup):
        """validate_config should validate goldfish.yaml."""
        pass

    def test_validate_config_validates_pipeline_yaml(self, e2e_setup):
        """validate_config should validate pipeline.yaml."""
        pass

    def test_validate_config_validates_stage_configs(self, e2e_setup):
        """validate_config should validate configs/*.yaml."""
        pass


class TestDryRunMode:
    """Test dry_run mode for run() tool."""

    def test_dry_run_validates_without_launching(self, e2e_setup):
        """dry_run=True should validate everything without launching."""
        from goldfish.pipeline.validator import validate_pipeline_run

        manager = e2e_setup["manager"]
        project_root = e2e_setup["project_root"]
        db = e2e_setup["db"]

        # Create workspace with pipeline
        manager.create_workspace(name="dry-run-test", goal="Test dry run", reason="Testing dry run mode")
        manager.mount(workspace="dry-run-test", slot="w1", reason="Testing dry run mode")

        slot_path = project_root / "workspaces" / "w1"

        # Create pipeline referencing nonexistent dataset
        (slot_path / "pipeline.yaml").write_text("""
name: dry-run-test
stages:
  - name: train
    inputs:
      data: {type: dataset, dataset: nonexistent_dataset}
    outputs:
      model: {type: directory}
""")

        # Create module
        (slot_path / "modules").mkdir(exist_ok=True)
        (slot_path / "modules" / "train.py").write_text("# Train module")

        manager.save_version(slot="w1", message="Add pipeline for dry run test")

        # dry_run should report the missing dataset without launching
        result = validate_pipeline_run(
            workspace_name="dry-run-test",
            workspace_path=slot_path,
            db=db,
            stages=["train"],
            pipeline_name=None,
            inputs_override={},
        )

        assert result["valid"] is False
        assert "nonexistent_dataset" in str(result["validation_errors"])
        assert "train" in result["stages_to_run"]

        manager.hibernate(slot="w1", reason="Done with dry run validation test")

    def test_dry_run_catches_missing_module(self, e2e_setup):
        """dry_run should catch missing stage modules."""
        from goldfish.pipeline.validator import validate_pipeline_run

        manager = e2e_setup["manager"]
        project_root = e2e_setup["project_root"]
        db = e2e_setup["db"]

        # Create workspace with pipeline
        manager.create_workspace(
            name="dry-run-module", goal="Test missing module", reason="Testing dry run catches missing module"
        )
        manager.mount(workspace="dry-run-module", slot="w1", reason="Testing missing module detection")

        slot_path = project_root / "workspaces" / "w1"

        # Create pipeline but NO module file
        (slot_path / "pipeline.yaml").write_text("""
name: dry-run-module-test
stages:
  - name: preprocess
    outputs:
      features: {type: npy}
""")

        manager.save_version(slot="w1", message="Add pipeline without module for test")

        # dry_run should catch missing module
        result = validate_pipeline_run(
            workspace_name="dry-run-module",
            workspace_path=slot_path,
            db=db,
            stages=None,  # All stages
            pipeline_name=None,
            inputs_override={},
        )

        assert result["valid"] is False
        assert any("module not found" in err for err in result["validation_errors"])

        manager.hibernate(slot="w1", reason="Done with module detection test")

    def test_dry_run_valid_pipeline(self, e2e_setup):
        """dry_run returns valid=True for correct pipeline."""
        from goldfish.pipeline.validator import validate_pipeline_run

        manager = e2e_setup["manager"]
        project_root = e2e_setup["project_root"]
        db = e2e_setup["db"]

        # Create workspace with valid pipeline
        manager.create_workspace(
            name="dry-run-valid", goal="Test valid pipeline", reason="Testing dry run with valid pipeline"
        )
        manager.mount(workspace="dry-run-valid", slot="w1", reason="Testing valid pipeline")

        slot_path = project_root / "workspaces" / "w1"

        # Create valid pipeline with no external dependencies
        (slot_path / "pipeline.yaml").write_text("""
name: dry-run-valid-test
stages:
  - name: generate
    outputs:
      data: {type: npy}
  - name: process
    inputs:
      data: {type: npy, from_stage: generate}
    outputs:
      result: {type: csv}
""")

        # Create modules
        (slot_path / "modules").mkdir(exist_ok=True)
        (slot_path / "modules" / "generate.py").write_text("# Generate stage")
        (slot_path / "modules" / "process.py").write_text("# Process stage")

        manager.save_version(slot="w1", message="Add valid pipeline for test")

        # dry_run should pass
        result = validate_pipeline_run(
            workspace_name="dry-run-valid",
            workspace_path=slot_path,
            db=db,
            stages=None,
            pipeline_name=None,
            inputs_override={},
        )

        assert result["valid"] is True
        assert result["stages_to_run"] == ["generate", "process"]
        assert len(result["validation_errors"]) == 0

        manager.hibernate(slot="w1", reason="Done with valid pipeline test")


class TestImprovedErrorMessages:
    """Test improved error messages with suggestions."""

    def test_suggests_similar_field_names(self):
        """Should suggest similar field names for typos."""
        from goldfish.validation import suggest_similar_field

        # Test the suggestion function
        valid_fields = ["project_name", "dev_repo_path", "workspaces_dir", "slots"]

        suggestion = suggest_similar_field("projeect_name", valid_fields)
        assert suggestion == "project_name"

        suggestion = suggest_similar_field("project_nam", valid_fields)
        assert suggestion == "project_name"

        # dev_repo_paht is a typo for dev_repo_path (2 char swap)
        suggestion = suggest_similar_field("dev_repo_paht", valid_fields)
        assert suggestion == "dev_repo_path"

        # workspaces (missing _dir) should suggest workspaces_dir
        suggestion = suggest_similar_field("workspace_dir", valid_fields)
        assert suggestion == "workspaces_dir"

    def test_no_suggestion_for_completely_different_field(self):
        """Should not suggest if field is too different."""
        from goldfish.validation import suggest_similar_field

        valid_fields = ["project_name", "dev_repo_path"]

        suggestion = suggest_similar_field("xyz_completely_different", valid_fields)
        assert suggestion is None

    def test_error_message_includes_valid_fields(self):
        """Error messages should list valid field names."""
        from goldfish.validation import format_unknown_field_error

        error = format_unknown_field_error(
            unknown_field="projeect_name",
            valid_fields=["project_name", "dev_repo_path", "slots"],
            suggested_field="project_name",
        )

        assert "projeect_name" in error
        assert "project_name" in error  # Suggestion
        assert "Did you mean" in error or "Similar:" in error
