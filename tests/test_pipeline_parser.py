"""Tests for pipeline parser and validator."""

import pytest
from pathlib import Path

from goldfish.pipeline.parser import PipelineParser, PipelineNotFoundError, PipelineValidationError
from goldfish.models import PipelineDef, StageDef, SignalDef


class TestPipelineParser:
    """Test pipeline YAML parsing."""

    def test_parse_minimal_pipeline(self, temp_dir):
        """Should parse minimal pipeline with just names."""
        pipeline_yaml = temp_dir / "pipeline.yaml"
        pipeline_yaml.write_text("""
name: test_pipeline
description: Test pipeline
stages:
  - name: stage1
  - name: stage2
""")

        parser = PipelineParser()
        pipeline = parser.parse(pipeline_yaml)

        assert pipeline.name == "test_pipeline"
        assert pipeline.description == "Test pipeline"
        assert len(pipeline.stages) == 2
        assert pipeline.stages[0].name == "stage1"
        assert pipeline.stages[1].name == "stage2"

    def test_parse_pipeline_with_signals(self, temp_dir):
        """Should parse pipeline with input/output signals."""
        pipeline_yaml = temp_dir / "pipeline.yaml"
        pipeline_yaml.write_text("""
name: test_pipeline
stages:
  - name: preprocess
    inputs:
      raw_data:
        type: dataset
        dataset: eurusd_raw
    outputs:
      features:
        type: npy
  - name: train
    inputs:
      features:
        from_stage: preprocess
        type: npy
    outputs:
      model:
        type: directory
""")

        parser = PipelineParser()
        pipeline = parser.parse(pipeline_yaml)

        # Check first stage
        assert pipeline.stages[0].name == "preprocess"
        assert "raw_data" in pipeline.stages[0].inputs
        assert pipeline.stages[0].inputs["raw_data"].type == "dataset"
        assert pipeline.stages[0].inputs["raw_data"].dataset == "eurusd_raw"

        # Check second stage
        assert pipeline.stages[1].name == "train"
        assert "features" in pipeline.stages[1].inputs
        assert pipeline.stages[1].inputs["features"].from_stage == "preprocess"
        assert pipeline.stages[1].inputs["features"].type == "npy"

    def test_parse_invalid_yaml(self, temp_dir):
        """Should raise error for invalid YAML."""
        pipeline_yaml = temp_dir / "pipeline.yaml"
        pipeline_yaml.write_text("invalid: yaml: syntax: [")

        parser = PipelineParser()
        with pytest.raises(Exception):  # YAML parsing error
            parser.parse(pipeline_yaml)

    def test_parse_missing_required_field(self, temp_dir):
        """Should raise error if name is missing."""
        pipeline_yaml = temp_dir / "pipeline.yaml"
        pipeline_yaml.write_text("""
description: Test pipeline
stages:
  - name: stage1
""")

        parser = PipelineParser()
        with pytest.raises(Exception):  # Pydantic validation error
            parser.parse(pipeline_yaml)


class TestPipelineValidator:
    """Test pipeline validation logic."""

    def test_validate_checks_module_exists(self, temp_dir):
        """Should validate that module files exist."""
        # Create pipeline
        pipeline_yaml = temp_dir / "pipeline.yaml"
        pipeline_yaml.write_text("""
name: test_pipeline
stages:
  - name: preprocess
""")

        # Create workspace structure (missing module)
        modules_dir = temp_dir / "modules"
        configs_dir = temp_dir / "configs"
        modules_dir.mkdir()
        configs_dir.mkdir()

        # Create config but not module
        (configs_dir / "preprocess.yaml").write_text("env: {}")

        parser = PipelineParser()
        pipeline = parser.parse(pipeline_yaml)
        errors = parser.validate(pipeline, temp_dir, dataset_exists_fn=None)

        assert len(errors) > 0
        assert any("preprocess.py" in err for err in errors)

    def test_validate_checks_config_exists(self, temp_dir):
        """Should validate that config files exist."""
        # Create pipeline
        pipeline_yaml = temp_dir / "pipeline.yaml"
        pipeline_yaml.write_text("""
name: test_pipeline
stages:
  - name: preprocess
""")

        # Create workspace structure (missing config)
        modules_dir = temp_dir / "modules"
        configs_dir = temp_dir / "configs"
        modules_dir.mkdir()
        configs_dir.mkdir()

        # Create module but not config
        (modules_dir / "preprocess.py").write_text("def main(): pass")

        parser = PipelineParser()
        pipeline = parser.parse(pipeline_yaml)
        errors = parser.validate(pipeline, temp_dir, dataset_exists_fn=None)

        assert len(errors) > 0
        assert any("preprocess.yaml" in err for err in errors)

    def test_validate_passes_when_files_exist(self, temp_dir):
        """Should pass validation when all files exist."""
        # Create pipeline
        pipeline_yaml = temp_dir / "pipeline.yaml"
        pipeline_yaml.write_text("""
name: test_pipeline
stages:
  - name: preprocess
""")

        # Create all required files
        modules_dir = temp_dir / "modules"
        configs_dir = temp_dir / "configs"
        modules_dir.mkdir()
        configs_dir.mkdir()

        (modules_dir / "preprocess.py").write_text("def main(): pass")
        (configs_dir / "preprocess.yaml").write_text("env: {}")

        parser = PipelineParser()
        pipeline = parser.parse(pipeline_yaml)
        errors = parser.validate(pipeline, temp_dir, dataset_exists_fn=None)

        assert len(errors) == 0

    def test_validate_checks_signal_connections(self, temp_dir):
        """Should validate that signal connections are valid."""
        # Create pipeline with invalid signal reference
        pipeline_yaml = temp_dir / "pipeline.yaml"
        pipeline_yaml.write_text("""
name: test_pipeline
stages:
  - name: train
    inputs:
      features:
        from_stage: nonexistent_stage
        signal_name: features
        type: npy
""")

        # Create files
        modules_dir = temp_dir / "modules"
        configs_dir = temp_dir / "configs"
        modules_dir.mkdir()
        configs_dir.mkdir()
        (modules_dir / "train.py").write_text("def main(): pass")
        (configs_dir / "train.yaml").write_text("env: {}")

        parser = PipelineParser()
        pipeline = parser.parse(pipeline_yaml)
        errors = parser.validate(pipeline, temp_dir, dataset_exists_fn=None)

        assert len(errors) > 0
        assert any("nonexistent_stage" in err for err in errors)

    def test_validate_checks_signal_type_compatibility(self, temp_dir):
        """Should validate that signal types match between stages."""
        # Create pipeline with type mismatch
        pipeline_yaml = temp_dir / "pipeline.yaml"
        pipeline_yaml.write_text("""
name: test_pipeline
stages:
  - name: preprocess
    outputs:
      features:
        type: npy
  - name: train
    inputs:
      features:
        from_stage: preprocess
        type: csv
""")

        # Create files
        modules_dir = temp_dir / "modules"
        configs_dir = temp_dir / "configs"
        modules_dir.mkdir()
        configs_dir.mkdir()
        (modules_dir / "preprocess.py").write_text("def main(): pass")
        (configs_dir / "preprocess.yaml").write_text("env: {}")
        (modules_dir / "train.py").write_text("def main(): pass")
        (configs_dir / "train.yaml").write_text("env: {}")

        parser = PipelineParser()
        pipeline = parser.parse(pipeline_yaml)
        errors = parser.validate(pipeline, temp_dir, dataset_exists_fn=None)

        assert len(errors) > 0
        assert any("type mismatch" in err.lower() for err in errors)

    def test_validate_allows_matching_types(self, temp_dir):
        """Should pass validation when signal types match."""
        # Create pipeline with matching types
        pipeline_yaml = temp_dir / "pipeline.yaml"
        pipeline_yaml.write_text("""
name: test_pipeline
stages:
  - name: preprocess
    outputs:
      features:
        type: npy
  - name: train
    inputs:
      features:
        from_stage: preprocess
        type: npy
""")

        # Create files
        modules_dir = temp_dir / "modules"
        configs_dir = temp_dir / "configs"
        modules_dir.mkdir()
        configs_dir.mkdir()
        (modules_dir / "preprocess.py").write_text("def main(): pass")
        (configs_dir / "preprocess.yaml").write_text("env: {}")
        (modules_dir / "train.py").write_text("def main(): pass")
        (configs_dir / "train.yaml").write_text("env: {}")

        parser = PipelineParser()
        pipeline = parser.parse(pipeline_yaml)
        errors = parser.validate(pipeline, temp_dir, dataset_exists_fn=None)

        # Should pass validation with matching types
        assert len([e for e in errors if "type mismatch" in e.lower()]) == 0
