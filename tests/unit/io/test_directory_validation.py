"""Tests for directory output validation with tensor schemas.

Tests verify:
- NPZ files in directories are loaded for validation
- Individual NPY files in directories are loaded for validation
- Multi-array tensor schemas are validated correctly
- Missing arrays are detected
- Shape/dtype mismatches are detected
- Non-tensor schemas skip validation gracefully
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from goldfish.io import _load_directory_for_validation
from goldfish.svs.contract import validate_output_data_against_schema


class TestLoadDirectoryForValidation:
    """Tests for _load_directory_for_validation helper."""

    def test_loads_npz_file_from_directory(self, tmp_path: Path):
        """Should load NPZ file and return dict-like object."""
        # Create NPZ with multiple arrays
        train_events = np.array([1, 2, 3], dtype=np.int32)
        train_sigma = np.array([0.1, 0.2, 0.3], dtype=np.float32)
        npz_path = tmp_path / "events.npz"
        np.savez(npz_path, train_events=train_events, train_sigma=train_sigma)

        schema = {
            "kind": "tensor",
            "arrays": {
                "train_events": {"shape": [None], "dtype": "int32"},
                "train_sigma": {"shape": [None], "dtype": "float32"},
            },
        }

        result = _load_directory_for_validation(tmp_path, schema)

        assert result is not None
        assert "train_events" in result.keys()
        assert "train_sigma" in result.keys()
        np.testing.assert_array_equal(result["train_events"], train_events)
        np.testing.assert_array_equal(result["train_sigma"], train_sigma)

    def test_loads_individual_npy_files(self, tmp_path: Path):
        """Should load individual NPY files when no NPZ exists."""
        train_events = np.array([1, 2, 3], dtype=np.int32)
        train_sigma = np.array([0.1, 0.2, 0.3], dtype=np.float32)
        np.save(tmp_path / "train_events.npy", train_events)
        np.save(tmp_path / "train_sigma.npy", train_sigma)

        schema = {
            "kind": "tensor",
            "arrays": {
                "train_events": {"shape": [None], "dtype": "int32"},
                "train_sigma": {"shape": [None], "dtype": "float32"},
            },
        }

        result = _load_directory_for_validation(tmp_path, schema)

        assert result is not None
        assert isinstance(result, dict)
        assert "train_events" in result
        assert "train_sigma" in result
        np.testing.assert_array_equal(result["train_events"], train_events)

    def test_returns_none_for_non_tensor_schema(self, tmp_path: Path):
        """Should skip validation for non-tensor schemas."""
        schema = {"kind": "tabular", "columns": ["a", "b"]}

        result = _load_directory_for_validation(tmp_path, schema)

        assert result is None

    def test_returns_none_for_tensor_without_arrays(self, tmp_path: Path):
        """Should skip validation for tensor schema without arrays definition."""
        schema = {"kind": "tensor", "shape": [100, 10], "dtype": "float32"}

        result = _load_directory_for_validation(tmp_path, schema)

        assert result is None

    def test_returns_none_when_no_npz_or_npy_files(self, tmp_path: Path):
        """Should return None when directory has no NPZ or NPY files."""
        (tmp_path / "some_file.txt").write_text("hello")

        schema = {
            "kind": "tensor",
            "arrays": {"data": {"shape": [None], "dtype": "float32"}},
        }

        result = _load_directory_for_validation(tmp_path, schema)

        assert result is None

    def test_prefers_npz_over_npy(self, tmp_path: Path):
        """Should use NPZ file when both NPZ and NPY exist."""
        # Create both NPZ and NPY files with different data
        npz_data = np.array([10, 20, 30], dtype=np.int32)
        npy_data = np.array([1, 2, 3], dtype=np.int32)
        np.savez(tmp_path / "data.npz", train_events=npz_data)
        np.save(tmp_path / "train_events.npy", npy_data)

        schema = {
            "kind": "tensor",
            "arrays": {"train_events": {"shape": [None], "dtype": "int32"}},
        }

        result = _load_directory_for_validation(tmp_path, schema)

        # Should load from NPZ (10, 20, 30), not NPY (1, 2, 3)
        assert result is not None
        np.testing.assert_array_equal(result["train_events"], npz_data)


class TestMultiArraySchemaValidation:
    """Tests for validate_output_data_against_schema with multi-array data."""

    def test_validates_all_arrays_in_schema(self):
        """Should validate each array defined in schema."""
        schema = {
            "kind": "tensor",
            "arrays": {
                "train_events": {"shape": [100], "dtype": "int32"},
                "train_sigma": {"shape": [100], "dtype": "float32"},
            },
        }
        data = {
            "train_events": np.zeros(100, dtype=np.int32),
            "train_sigma": np.zeros(100, dtype=np.float32),
        }

        errors = validate_output_data_against_schema("tokens", schema, data)

        assert errors == []

    def test_detects_missing_array(self):
        """Should report error when array in schema is missing from data."""
        schema = {
            "kind": "tensor",
            "arrays": {
                "train_events": {"shape": [100], "dtype": "int32"},
                "train_sigma": {"shape": [100], "dtype": "float32"},
            },
        }
        data = {
            "train_events": np.zeros(100, dtype=np.int32),
            # Missing train_sigma
        }

        errors = validate_output_data_against_schema("tokens", schema, data)

        assert len(errors) == 1
        assert "train_sigma" in errors[0]
        assert "missing from output" in errors[0]

    def test_detects_dtype_mismatch(self):
        """Should report error when array dtype doesn't match schema."""
        schema = {
            "kind": "tensor",
            "arrays": {
                "train_events": {"shape": [100], "dtype": "int32"},
            },
        }
        data = {
            "train_events": np.zeros(100, dtype=np.float64),  # Wrong dtype
        }

        errors = validate_output_data_against_schema("tokens", schema, data)

        assert len(errors) == 1
        assert "dtype mismatch" in errors[0]
        assert "int32" in errors[0]
        assert "float64" in errors[0]

    def test_detects_shape_mismatch(self):
        """Should report error when array shape doesn't match schema."""
        schema = {
            "kind": "tensor",
            "arrays": {
                "features": {"shape": [100, 64], "dtype": "float32"},
            },
        }
        data = {
            "features": np.zeros((100, 32), dtype=np.float32),  # Wrong dim
        }

        errors = validate_output_data_against_schema("output", schema, data)

        assert len(errors) == 1
        assert "shape" in errors[0]

    def test_wildcard_shape_dimension_passes(self):
        """Should allow any value for null/wildcard shape dimensions."""
        schema = {
            "kind": "tensor",
            "arrays": {
                "events": {"shape": [None, 64], "dtype": "float32"},
            },
        }
        data = {
            "events": np.zeros((12345, 64), dtype=np.float32),  # Any first dim
        }

        errors = validate_output_data_against_schema("output", schema, data)

        assert errors == []

    def test_works_with_npz_file_object(self, tmp_path: Path):
        """Should work with actual NpzFile from np.load."""
        # Create real NPZ file
        train_events = np.array([1, 2, 3], dtype=np.int32)
        train_sigma = np.array([0.1, 0.2], dtype=np.float32)
        npz_path = tmp_path / "data.npz"
        np.savez(npz_path, train_events=train_events, train_sigma=train_sigma)

        schema = {
            "kind": "tensor",
            "arrays": {
                "train_events": {"shape": [3], "dtype": "int32"},
                "train_sigma": {"shape": [2], "dtype": "float32"},
            },
        }

        # Load actual NpzFile
        npz_data = np.load(npz_path)

        errors = validate_output_data_against_schema("output", schema, npz_data)

        assert errors == []

    def test_multiple_errors_reported(self):
        """Should report all errors, not just first one."""
        schema = {
            "kind": "tensor",
            "arrays": {
                "a": {"shape": [10], "dtype": "int32"},
                "b": {"shape": [10], "dtype": "float32"},
                "c": {"shape": [10], "dtype": "int64"},
            },
        }
        data = {
            "a": np.zeros(5, dtype=np.float64),  # Wrong shape AND dtype
            # b is missing
            "c": np.zeros(10, dtype=np.int64),  # Correct
        }

        errors = validate_output_data_against_schema("output", schema, data)

        assert len(errors) >= 2  # At least shape error for 'a' and missing 'b'
        error_text = " ".join(errors)
        assert "a" in error_text or "shape" in error_text
        assert "b" in error_text or "missing" in error_text


class TestSaveOutputDirectoryValidation:
    """Integration tests for save_output with directory + tensor schema."""

    def test_save_output_validates_directory_with_npz(self, tmp_path: Path):
        """save_output should validate directory outputs against tensor schema."""
        # Setup: create output directory with NPZ
        output_dir = tmp_path / "outputs" / "tokens"
        output_dir.mkdir(parents=True)
        train_data = np.array([1, 2, 3], dtype=np.int32)
        np.savez(output_dir / "events.npz", train_events=train_data)

        # Mock stage config
        config = {
            "outputs": {
                "tokens": {
                    "format": "directory",
                    "schema": {
                        "kind": "tensor",
                        "arrays": {
                            "train_events": {"shape": [3], "dtype": "int32"},
                        },
                    },
                }
            }
        }

        with (
            patch("goldfish.io.os.environ.get") as mock_env,
            patch("goldfish.io._get_stage_config", return_value=config),
            patch("goldfish.io.bootstrap._load_svs_config") as mock_svs,
        ):
            mock_env.side_effect = lambda k, d=None: str(tmp_path / "outputs") if k == "GOLDFISH_OUTPUTS_DIR" else d
            mock_svs.return_value = MagicMock(enabled=True, default_enforcement="warning")

            from goldfish.io import save_output

            # Should not raise - validation should pass
            save_output("tokens", output_dir)

    def test_save_output_catches_schema_mismatch_in_directory(self, tmp_path: Path):
        """save_output should catch schema mismatches in directory outputs."""
        # Setup: create output directory with wrong dtype
        output_dir = tmp_path / "outputs" / "tokens"
        output_dir.mkdir(parents=True)
        train_data = np.array([1.0, 2.0, 3.0], dtype=np.float64)  # Wrong dtype
        np.savez(output_dir / "events.npz", train_events=train_data)

        config = {
            "outputs": {
                "tokens": {
                    "format": "directory",
                    "schema": {
                        "kind": "tensor",
                        "arrays": {
                            "train_events": {"shape": [3], "dtype": "int32"},  # Expects int32
                        },
                    },
                }
            }
        }

        with (
            patch("goldfish.io.os.environ.get") as mock_env,
            patch("goldfish.io._get_stage_config", return_value=config),
            patch("goldfish.io.bootstrap._load_svs_config") as mock_svs,
        ):
            mock_env.side_effect = lambda k, d=None: str(tmp_path / "outputs") if k == "GOLDFISH_OUTPUTS_DIR" else d
            mock_svs.return_value = MagicMock(enabled=True, default_enforcement="blocking")

            from goldfish.errors import GoldfishError
            from goldfish.io import save_output

            # Should raise due to dtype mismatch
            with pytest.raises(GoldfishError, match="schema mismatch"):
                save_output("tokens", output_dir)
