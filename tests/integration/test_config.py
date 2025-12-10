"""Tests for config.py - configuration loading and saving."""

import pytest
import yaml
from pydantic import ValidationError

from goldfish.config import (
    AuditConfig,
    GCSConfig,
    GoldfishConfig,
    JobsConfig,
    StateMdConfig,
    generate_default_config,
)
from goldfish.errors import GoldfishError, ProjectNotInitializedError


class TestGoldfishConfigLoad:
    """Tests for GoldfishConfig.load() method."""

    def test_load_valid_config(self, temp_dir):
        """Should load a valid configuration file."""
        config_data = {
            "project_name": "my-project",
            "dev_repo_path": "../my-project-dev",
            "workspaces_dir": "workspaces",
            "slots": ["w1", "w2", "w3"],
        }
        config_path = temp_dir / "goldfish.yaml"
        with open(config_path, "w") as f:
            yaml.safe_dump(config_data, f)

        config = GoldfishConfig.load(temp_dir)

        assert config.project_name == "my-project"
        assert config.dev_repo_path == "../my-project-dev"
        assert config.workspaces_dir == "workspaces"
        assert config.slots == ["w1", "w2", "w3"]

    def test_load_with_all_options(self, temp_dir):
        """Should load config with all optional fields."""
        config_data = {
            "project_name": "full-project",
            "dev_repo_path": "../dev",
            "workspaces_dir": "ws",
            "slots": ["a", "b"],
            "state_md": {"path": "CUSTOM.md", "max_recent_actions": 20},
            "audit": {"min_reason_length": 25},
            "jobs": {
                "backend": "gce",
                "infra_path": "../infra",
                "experiments_dir": "exp",
            },
            "gcs": {
                "bucket": "my-bucket",
                "sources_prefix": "src/",
                "artifacts_prefix": "art/",
                "snapshots_prefix": "snap/",
            },
            "invariants": ["Don't change X", "Keep Y constant"],
        }
        config_path = temp_dir / "goldfish.yaml"
        with open(config_path, "w") as f:
            yaml.safe_dump(config_data, f)

        config = GoldfishConfig.load(temp_dir)

        assert config.project_name == "full-project"
        assert config.state_md.path == "CUSTOM.md"
        assert config.state_md.max_recent_actions == 20
        assert config.audit.min_reason_length == 25
        assert config.jobs.backend == "gce"
        assert config.jobs.infra_path == "../infra"
        assert config.gcs.bucket == "my-bucket"
        assert config.gcs.sources_prefix == "src/"
        assert len(config.invariants) == 2

    def test_load_missing_file_raises_project_not_initialized(self, temp_dir):
        """Should raise ProjectNotInitializedError when config file missing."""
        with pytest.raises(ProjectNotInitializedError) as exc_info:
            GoldfishConfig.load(temp_dir)

        assert "goldfish.yaml" in str(exc_info.value)
        assert "goldfish init" in str(exc_info.value)

    def test_load_empty_file_raises_error(self, temp_dir):
        """Should raise GoldfishError when config file is empty."""
        config_path = temp_dir / "goldfish.yaml"
        config_path.write_text("")

        with pytest.raises(GoldfishError) as exc_info:
            GoldfishConfig.load(temp_dir)

        assert "empty" in str(exc_info.value).lower()

    def test_load_invalid_yaml_raises_error(self, temp_dir):
        """Should raise GoldfishError on invalid YAML syntax."""
        config_path = temp_dir / "goldfish.yaml"
        config_path.write_text("invalid: yaml: syntax: [")

        with pytest.raises(GoldfishError) as exc_info:
            GoldfishConfig.load(temp_dir)

        assert "yaml" in str(exc_info.value).lower()

    def test_load_missing_required_field_raises_error(self, temp_dir):
        """Should raise GoldfishError when required field is missing."""
        config_data = {
            # Missing project_name
            "dev_repo_path": "../dev",
        }
        config_path = temp_dir / "goldfish.yaml"
        with open(config_path, "w") as f:
            yaml.safe_dump(config_data, f)

        with pytest.raises(GoldfishError) as exc_info:
            GoldfishConfig.load(temp_dir)

        assert "project_name" in str(exc_info.value)

    def test_load_invalid_field_type_raises_error(self, temp_dir):
        """Should raise GoldfishError when field has wrong type."""
        config_data = {
            "project_name": "test",
            "dev_repo_path": "../dev",
            "slots": "not-a-list",  # Should be list
        }
        config_path = temp_dir / "goldfish.yaml"
        with open(config_path, "w") as f:
            yaml.safe_dump(config_data, f)

        with pytest.raises(GoldfishError) as exc_info:
            GoldfishConfig.load(temp_dir)

        assert "slots" in str(exc_info.value)

    def test_load_uses_defaults_for_optional_fields(self, temp_dir):
        """Should use default values for optional fields."""
        config_data = {
            "project_name": "minimal",
            "dev_repo_path": "../dev",
        }
        config_path = temp_dir / "goldfish.yaml"
        with open(config_path, "w") as f:
            yaml.safe_dump(config_data, f)

        config = GoldfishConfig.load(temp_dir)

        assert config.workspaces_dir == "workspaces"
        assert config.slots == ["w1", "w2", "w3"]
        assert config.state_md.path == "STATE.md"
        assert config.state_md.max_recent_actions == 15
        assert config.audit.min_reason_length == 15
        assert config.jobs.backend == "gce"
        assert config.gcs is None
        assert config.invariants == []


class TestGoldfishConfigSave:
    """Tests for GoldfishConfig.save() method."""

    def test_save_creates_config_file(self, temp_dir):
        """Should create goldfish.yaml file."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../dev",
        )

        config.save(temp_dir)

        config_path = temp_dir / "goldfish.yaml"
        assert config_path.exists()

    def test_save_writes_correct_content(self, temp_dir):
        """Should write config data correctly."""
        config = GoldfishConfig(
            project_name="my-project",
            dev_repo_path="../my-dev",
            slots=["slot1", "slot2"],
            invariants=["Keep this", "And this"],
        )

        config.save(temp_dir)

        config_path = temp_dir / "goldfish.yaml"
        with open(config_path) as f:
            loaded = yaml.safe_load(f)

        assert loaded["project_name"] == "my-project"
        assert loaded["dev_repo_path"] == "../my-dev"
        assert loaded["slots"] == ["slot1", "slot2"]
        assert loaded["invariants"] == ["Keep this", "And this"]

    def test_save_excludes_none_values(self, temp_dir):
        """Should not write None values to file."""
        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="../dev",
            gcs=None,  # Should not appear in output
        )

        config.save(temp_dir)

        config_path = temp_dir / "goldfish.yaml"
        with open(config_path) as f:
            loaded = yaml.safe_load(f)

        assert "gcs" not in loaded

    def test_save_roundtrip(self, temp_dir):
        """Should be able to save and reload config."""
        original = GoldfishConfig(
            project_name="roundtrip-test",
            dev_repo_path="../dev",
            slots=["a", "b", "c"],
            gcs=GCSConfig(bucket="bucket", sources_prefix="s/"),
            invariants=["inv1"],
        )

        original.save(temp_dir)
        loaded = GoldfishConfig.load(temp_dir)

        assert loaded.project_name == original.project_name
        assert loaded.dev_repo_path == original.dev_repo_path
        assert loaded.slots == original.slots
        assert loaded.gcs.bucket == original.gcs.bucket
        assert loaded.invariants == original.invariants


class TestGenerateDefaultConfig:
    """Tests for generate_default_config() function."""

    def test_generates_dev_repo_path_from_project_name(self):
        """Should generate dev_repo_path using project name."""
        config = generate_default_config("ml-experiment")

        assert config.dev_repo_path == "../ml-experiment-dev"

    def test_custom_dev_repo_path_template(self):
        """Should allow custom dev_repo_path template."""
        config = generate_default_config("test", dev_repo_path="../custom-{project}")

        assert config.dev_repo_path == "../custom-test"

    def test_includes_default_slots(self):
        """Should include default w1, w2, w3 slots."""
        config = generate_default_config("test")

        assert config.slots == ["w1", "w2", "w3"]

    def test_includes_default_sub_configs(self):
        """Should include default sub-configurations."""
        config = generate_default_config("test")

        assert isinstance(config.state_md, StateMdConfig)
        assert isinstance(config.audit, AuditConfig)
        assert isinstance(config.jobs, JobsConfig)

    def test_invariants_empty_by_default(self):
        """Should have empty invariants by default."""
        config = generate_default_config("test")

        assert config.invariants == []


class TestSubConfigs:
    """Tests for sub-configuration classes."""

    def test_state_md_config_defaults(self):
        """StateMdConfig should have sensible defaults."""
        config = StateMdConfig()

        assert config.path == "STATE.md"
        assert config.max_recent_actions == 15

    def test_audit_config_defaults(self):
        """AuditConfig should have sensible defaults."""
        config = AuditConfig()

        assert config.min_reason_length == 15

    def test_jobs_config_defaults(self):
        """JobsConfig should have sensible defaults."""
        config = JobsConfig()

        assert config.backend == "gce"
        assert config.infra_path is None
        assert config.experiments_dir == "experiments"

    def test_gcs_config_requires_bucket(self):
        """GCSConfig should require bucket field."""
        with pytest.raises(ValidationError):
            GCSConfig()

    def test_gcs_config_has_prefix_defaults(self):
        """GCSConfig should have default prefixes."""
        config = GCSConfig(bucket="test-bucket")

        assert config.sources_prefix == "sources/"
        assert config.artifacts_prefix == "artifacts/"
        assert config.snapshots_prefix == "snapshots/"
