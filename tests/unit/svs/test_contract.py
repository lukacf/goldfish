"""Unit tests for SVS contract validation - schema as law, config resolution.

Tests for three core functions:
1. resolve_config_params() - {param} substitution
2. merge_stage_config() - Precedence: defaults → file → runtime
3. validate_stage_contracts() - Schema validation with config resolution
"""

from pathlib import Path

import pytest
import yaml


class TestResolveConfigParams:
    """Tests for resolve_config_params() - single authority for {param} substitution."""

    def test_resolves_simple_param(self):
        """Should substitute {param} with config value."""
        from goldfish.svs.contract import resolve_config_params

        result = resolve_config_params("{vocab_size}", {"vocab_size": 15034})
        assert result == 15034

    def test_resolves_string_param(self):
        """Should substitute {param} with string config value."""
        from goldfish.svs.contract import resolve_config_params

        result = resolve_config_params("{model_type}", {"model_type": "lstm"})
        assert result == "lstm"

    def test_raises_for_missing_param(self):
        """Should raise ConfigParamNotFoundError for missing param."""
        from goldfish.errors import ConfigParamNotFoundError
        from goldfish.svs.contract import resolve_config_params

        with pytest.raises(ConfigParamNotFoundError) as exc_info:
            resolve_config_params("{missing}", {})

        error = exc_info.value
        assert "missing" in error.message.lower()
        assert error.details is not None
        assert "available" in error.details

    def test_raises_with_available_params_list(self):
        """Should include available params in error details."""
        from goldfish.errors import ConfigParamNotFoundError
        from goldfish.svs.contract import resolve_config_params

        with pytest.raises(ConfigParamNotFoundError) as exc_info:
            resolve_config_params("{missing}", {"vocab_size": 15034, "hidden_dim": 256})

        error = exc_info.value
        available = error.details["available"]
        assert "vocab_size" in available
        assert "hidden_dim" in available

    def test_leaves_non_params_unchanged(self):
        """Should not modify strings that are not params."""
        from goldfish.svs.contract import resolve_config_params

        result = resolve_config_params("regular_string", {"vocab_size": 15034})
        assert result == "regular_string"

    def test_leaves_partial_braces_unchanged(self):
        """Should not modify strings with only opening or closing brace."""
        from goldfish.svs.contract import resolve_config_params

        result = resolve_config_params("{incomplete", {"vocab_size": 15034})
        assert result == "{incomplete"

        result = resolve_config_params("incomplete}", {"vocab_size": 15034})
        assert result == "incomplete}"

    def test_passes_through_non_strings(self):
        """Should pass through non-string values unchanged."""
        from goldfish.svs.contract import resolve_config_params

        assert resolve_config_params(42, {"vocab_size": 15034}) == 42
        assert resolve_config_params(3.14, {"vocab_size": 15034}) == 3.14
        assert resolve_config_params(True, {"vocab_size": 15034}) is True
        assert resolve_config_params(None, {"vocab_size": 15034}) is None

    def test_resolves_nested_dict(self):
        """Should resolve params in nested dictionaries."""
        from goldfish.svs.contract import resolve_config_params

        nested = {"shape": ["{batch_size}", "{seq_len}"], "dtype": "float32"}
        config = {"batch_size": 32, "seq_len": 128}

        result = resolve_config_params(nested, config)
        assert result["shape"] == [32, 128]
        assert result["dtype"] == "float32"

    def test_resolves_list_with_params(self):
        """Should resolve params in lists."""
        from goldfish.svs.contract import resolve_config_params

        values = ["{dim1}", "{dim2}", 64]
        config = {"dim1": 256, "dim2": 512}

        result = resolve_config_params(values, config)
        assert result == [256, 512, 64]

    def test_resolves_deeply_nested_structures(self):
        """Should resolve params in arbitrarily nested structures."""
        from goldfish.svs.contract import resolve_config_params

        structure = {
            "outer": {
                "inner": ["{param1}", {"nested": "{param2}"}],
                "value": 42,
            }
        }
        config = {"param1": "value1", "param2": "value2"}

        result = resolve_config_params(structure, config)
        assert result["outer"]["inner"][0] == "value1"
        assert result["outer"]["inner"][1]["nested"] == "value2"
        assert result["outer"]["value"] == 42


class TestMergeStageConfig:
    """Tests for merge_stage_config() - explicit precedence: defaults → file → runtime."""

    def test_defaults_only(self, tmp_path: Path):
        """Should return stage defaults when no config file or overrides."""
        from goldfish.svs.contract import merge_stage_config

        # Create minimal pipeline.yaml
        pipeline_path = tmp_path / "pipeline.yaml"
        pipeline_path.write_text(
            yaml.dump(
                {
                    "stages": {
                        "train": {
                            "defaults": {"batch_size": 32, "epochs": 10},
                        }
                    }
                }
            )
        )

        result = merge_stage_config("train", tmp_path)
        assert result == {"batch_size": 32, "epochs": 10}

    def test_config_file_overrides_defaults(self, tmp_path: Path):
        """Config file should override stage defaults."""
        from goldfish.svs.contract import merge_stage_config

        # Create pipeline.yaml with defaults
        pipeline_path = tmp_path / "pipeline.yaml"
        pipeline_path.write_text(
            yaml.dump(
                {
                    "stages": {
                        "train": {
                            "defaults": {"batch_size": 32, "epochs": 10, "lr": 0.001},
                        }
                    }
                }
            )
        )

        # Create configs/train.yaml that overrides some values
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        config_file = configs_dir / "train.yaml"
        config_file.write_text(yaml.dump({"batch_size": 64, "epochs": 20}))

        result = merge_stage_config("train", tmp_path)
        assert result == {"batch_size": 64, "epochs": 20, "lr": 0.001}

    def test_runtime_overrides_config_file(self, tmp_path: Path):
        """Runtime overrides should win over config file."""
        from goldfish.svs.contract import merge_stage_config

        # Create pipeline.yaml with defaults
        pipeline_path = tmp_path / "pipeline.yaml"
        pipeline_path.write_text(
            yaml.dump(
                {
                    "stages": {
                        "train": {
                            "defaults": {"batch_size": 32, "epochs": 10, "lr": 0.001},
                        }
                    }
                }
            )
        )

        # Create configs/train.yaml
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        config_file = configs_dir / "train.yaml"
        config_file.write_text(yaml.dump({"batch_size": 64, "lr": 0.01}))

        # Runtime overrides
        runtime = {"batch_size": 128}

        result = merge_stage_config("train", tmp_path, runtime_overrides=runtime)
        assert result == {"batch_size": 128, "epochs": 10, "lr": 0.01}

    def test_full_precedence_chain(self, tmp_path: Path):
        """Should apply full precedence: defaults → file → runtime (later wins)."""
        from goldfish.svs.contract import merge_stage_config

        # Pipeline defaults
        pipeline_path = tmp_path / "pipeline.yaml"
        pipeline_path.write_text(
            yaml.dump(
                {
                    "stages": {
                        "train": {
                            "defaults": {
                                "batch_size": 32,
                                "epochs": 10,
                                "lr": 0.001,
                                "optimizer": "adam",
                            },
                        }
                    }
                }
            )
        )

        # Config file overrides
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        config_file = configs_dir / "train.yaml"
        config_file.write_text(yaml.dump({"batch_size": 64, "lr": 0.01}))

        # Runtime overrides
        runtime = {"epochs": 5}

        result = merge_stage_config("train", tmp_path, runtime_overrides=runtime)

        # Verify precedence
        assert result["batch_size"] == 64  # From config file
        assert result["epochs"] == 5  # From runtime
        assert result["lr"] == 0.01  # From config file
        assert result["optimizer"] == "adam"  # From defaults

    def test_missing_config_file_ok(self, tmp_path: Path):
        """Should work when config file doesn't exist."""
        from goldfish.svs.contract import merge_stage_config

        # Create pipeline.yaml
        pipeline_path = tmp_path / "pipeline.yaml"
        pipeline_path.write_text(yaml.dump({"stages": {"train": {"defaults": {"batch_size": 32}}}}))

        # No configs/ directory at all
        result = merge_stage_config("train", tmp_path)
        assert result == {"batch_size": 32}

    def test_missing_stage_in_pipeline(self, tmp_path: Path):
        """Should return empty config if stage not found in pipeline."""
        from goldfish.svs.contract import merge_stage_config

        # Create pipeline.yaml without the stage
        pipeline_path = tmp_path / "pipeline.yaml"
        pipeline_path.write_text(yaml.dump({"stages": {}}))

        result = merge_stage_config("nonexistent", tmp_path)
        assert result == {}

    def test_empty_config_file(self, tmp_path: Path):
        """Should handle empty config file gracefully."""
        from goldfish.svs.contract import merge_stage_config

        # Create pipeline.yaml
        pipeline_path = tmp_path / "pipeline.yaml"
        pipeline_path.write_text(yaml.dump({"stages": {"train": {"defaults": {"batch_size": 32}}}}))

        # Create empty config file
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir()
        config_file = configs_dir / "train.yaml"
        config_file.write_text("")

        result = merge_stage_config("train", tmp_path)
        assert result == {"batch_size": 32}


class TestValidateStageContracts:
    """Tests for validate_stage_contracts() - schema validation with config resolution."""

    def test_valid_schema_passes(self):
        """Should return empty list for valid schema."""
        from goldfish.svs.contract import validate_stage_contracts

        stage_def = {
            "outputs": {
                "embeddings": {
                    "type": "npy",
                    "schema": {"shape": [100, 256], "dtype": "float32", "rank": 2},
                }
            }
        }
        config = {}

        errors = validate_stage_contracts(stage_def, config)
        assert errors == []

    def test_shape_rank_mismatch_fails(self):
        """Should detect shape/rank mismatch."""
        from goldfish.svs.contract import validate_stage_contracts

        stage_def = {
            "outputs": {
                "embeddings": {
                    "type": "npy",
                    "schema": {
                        "shape": [100, 256],  # 2D
                        "dtype": "float32",
                        "rank": 3,  # Says 3D - MISMATCH
                    },
                }
            }
        }
        config = {}

        errors = validate_stage_contracts(stage_def, config)
        assert len(errors) == 1
        assert "embeddings" in errors[0]
        assert "shape/rank" in errors[0].lower() or "mismatch" in errors[0].lower()

    def test_resolves_params_in_schema(self):
        """Should resolve {param} in schema before validation."""
        from goldfish.svs.contract import validate_stage_contracts

        stage_def = {
            "outputs": {
                "embeddings": {
                    "type": "npy",
                    "schema": {
                        "shape": ["{vocab_size}", "{embedding_dim}"],
                        "dtype": "float32",
                        "rank": 2,
                    },
                }
            }
        }
        config = {"vocab_size": 15034, "embedding_dim": 256}

        errors = validate_stage_contracts(stage_def, config)
        assert errors == []

    def test_resolves_params_with_rank_check(self):
        """Should resolve params and then check shape/rank consistency."""
        from goldfish.svs.contract import validate_stage_contracts

        stage_def = {
            "outputs": {
                "embeddings": {
                    "type": "npy",
                    "schema": {
                        "shape": ["{dim1}", "{dim2}"],
                        "dtype": "float32",
                        "rank": 3,  # Mismatch after resolution
                    },
                }
            }
        }
        config = {"dim1": 100, "dim2": 256}

        errors = validate_stage_contracts(stage_def, config)
        assert len(errors) == 1
        assert "embeddings" in errors[0]

    def test_multiple_outputs_validated(self):
        """Should validate all outputs in stage."""
        from goldfish.svs.contract import validate_stage_contracts

        stage_def = {
            "outputs": {
                "embeddings": {
                    "type": "npy",
                    "schema": {"shape": [100, 256], "rank": 2},  # Valid
                },
                "labels": {
                    "type": "npy",
                    "schema": {"shape": [100], "rank": 2},  # Invalid
                },
                "metadata": {
                    "type": "csv",
                    "schema": {"shape": [100, 10], "rank": 2},  # Valid
                },
            }
        }
        config = {}

        errors = validate_stage_contracts(stage_def, config)
        assert len(errors) == 1
        assert "labels" in errors[0]

    def test_missing_rank_ok(self):
        """Should not error if rank is not specified."""
        from goldfish.svs.contract import validate_stage_contracts

        stage_def = {
            "outputs": {
                "embeddings": {
                    "type": "npy",
                    "schema": {"shape": [100, 256], "dtype": "float32"},
                }
            }
        }
        config = {}

        errors = validate_stage_contracts(stage_def, config)
        assert errors == []

    def test_missing_shape_ok(self):
        """Should not error if shape is not specified."""
        from goldfish.svs.contract import validate_stage_contracts

        stage_def = {
            "outputs": {
                "embeddings": {
                    "type": "npy",
                    "schema": {"dtype": "float32", "rank": 2},
                }
            }
        }
        config = {}

        errors = validate_stage_contracts(stage_def, config)
        assert errors == []

    def test_no_outputs_ok(self):
        """Should handle stage with no outputs."""
        from goldfish.svs.contract import validate_stage_contracts

        stage_def = {}
        config = {}

        errors = validate_stage_contracts(stage_def, config)
        assert errors == []

    def test_no_schema_ok(self):
        """Should handle output without schema."""
        from goldfish.svs.contract import validate_stage_contracts

        stage_def = {
            "outputs": {
                "embeddings": {
                    "type": "npy",
                }
            }
        }
        config = {}

        errors = validate_stage_contracts(stage_def, config)
        assert errors == []

    def test_missing_config_param_in_schema_fails(self):
        """Should fail if schema references missing config param."""
        from goldfish.svs.contract import validate_stage_contracts

        stage_def = {
            "outputs": {
                "embeddings": {
                    "type": "npy",
                    "schema": {
                        "shape": ["{vocab_size}", "{embedding_dim}"],
                        "rank": 2,
                    },
                }
            }
        }
        config = {"vocab_size": 15034}  # Missing embedding_dim

        errors = validate_stage_contracts(stage_def, config)
        assert len(errors) >= 1
        # Should mention the missing param or resolution failure
        assert any("embedding_dim" in err.lower() or "param" in err.lower() for err in errors)


class TestValidateOutputDataAgainstSchema:
    """Tests for validate_output_data_against_schema with JSON outputs."""

    def test_accepts_json_dict(self):
        """JSON schema should accept dict outputs."""
        from goldfish.svs.contract import validate_output_data_against_schema

        schema = {"kind": "json"}
        data = {"key": "value"}

        errors = validate_output_data_against_schema("output", schema, data)
        assert errors == []

    def test_accepts_json_list(self):
        """JSON schema should accept list outputs (e.g., list of dicts)."""
        from goldfish.svs.contract import validate_output_data_against_schema

        schema = {"kind": "json"}
        data = [{"id": 1}, {"id": 2}]

        errors = validate_output_data_against_schema("output", schema, data)
        assert errors == []

    def test_rejects_json_scalar(self):
        """JSON schema should reject non-dict/list outputs."""
        from goldfish.svs.contract import validate_output_data_against_schema

        schema = {"kind": "json"}
        data = "not-json-object"

        errors = validate_output_data_against_schema("output", schema, data)
        assert errors
