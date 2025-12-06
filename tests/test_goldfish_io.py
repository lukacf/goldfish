"""Tests for Goldfish IO library (storage abstraction for modules)."""

import json
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import numpy as np
import pandas as pd

# We'll be testing the goldfish.io module that doesn't exist yet
# This will fail initially (TDD red phase)


class TestLoadInput:
    """Test load_input() function."""

    def test_load_npy_file(self, temp_dir):
        """load_input should auto-load NPY files."""
        from goldfish.io import load_input

        # Create test NPY file
        input_path = temp_dir / "inputs" / "features"
        input_path.parent.mkdir(parents=True)
        test_array = np.array([1, 2, 3, 4, 5])
        np.save(input_path, test_array)

        # Mock config
        config = {
            "inputs": {
                "features": {
                    "format": "npy",
                }
            }
        }

        env = {
            "GOLDFISH_STAGE_CONFIG": json.dumps(config),
            "GOLDFISH_INPUTS_DIR": str(temp_dir / "inputs"),
        }
        with patch.dict(os.environ, env):
            result = load_input("features")

        assert isinstance(result, np.ndarray)
        np.testing.assert_array_equal(result, test_array)

    def test_load_csv_file(self, temp_dir):
        """load_input should auto-load CSV files."""
        from goldfish.io import load_input

        # Create test CSV file
        input_path = temp_dir / "inputs" / "data"
        input_path.parent.mkdir(parents=True)
        test_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        test_df.to_csv(input_path, index=False)

        # Mock config
        config = {
            "inputs": {
                "data": {
                    "format": "csv",
                }
            }
        }

        env = {
            "GOLDFISH_STAGE_CONFIG": json.dumps(config),
            "GOLDFISH_INPUTS_DIR": str(temp_dir / "inputs"),
        }
        with patch.dict(os.environ, env):
            result = load_input("data")

        assert isinstance(result, pd.DataFrame)
        pd.testing.assert_frame_equal(result, test_df)

    def test_load_returns_path_for_directory(self, temp_dir):
        """load_input should return Path for directory inputs."""
        from goldfish.io import load_input

        # Create test directory
        input_dir = temp_dir / "inputs" / "model_dir"
        input_dir.mkdir(parents=True)
        (input_dir / "file.txt").write_text("test")

        config = {
            "inputs": {
                "model_dir": {
                    "format": "directory",
                }
            }
        }

        env = {
            "GOLDFISH_STAGE_CONFIG": json.dumps(config),
            "GOLDFISH_INPUTS_DIR": str(temp_dir / "inputs"),
        }
        with patch.dict(os.environ, env):
            result = load_input("model_dir")

        assert isinstance(result, Path)
        assert result.exists()

    def test_load_raises_on_missing_input(self):
        """load_input should raise error if input not in config."""
        from goldfish.io import load_input
        from goldfish.errors import GoldfishError

        config = {"inputs": {}}

        with patch.dict(os.environ, {"GOLDFISH_STAGE_CONFIG": json.dumps(config)}):
            with pytest.raises(ValueError, match="not defined"):
                load_input("nonexistent")


class TestSaveOutput:
    """Test save_output() function."""

    def test_save_npy_array(self, temp_dir):
        """save_output should auto-save NumPy arrays."""
        from goldfish.io import save_output

        test_array = np.array([1, 2, 3, 4, 5])

        config = {
            "outputs": {
                "results": {
                    "format": "npy",
                }
            }
        }

        env = {
            "GOLDFISH_STAGE_CONFIG": json.dumps(config),
            "GOLDFISH_OUTPUTS_DIR": str(temp_dir / "outputs"),
        }
        with patch.dict(os.environ, env):
            save_output("results", test_array)

        # Verify file was saved
        output_path = temp_dir / "outputs" / "results.npy"
        assert output_path.exists()
        loaded = np.load(output_path)
        np.testing.assert_array_equal(loaded, test_array)

    def test_save_csv_dataframe(self, temp_dir):
        """save_output should auto-save DataFrames."""
        from goldfish.io import save_output

        test_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        config = {
            "outputs": {
                "results": {
                    "format": "csv",
                }
            }
        }

        env = {
            "GOLDFISH_STAGE_CONFIG": json.dumps(config),
            "GOLDFISH_OUTPUTS_DIR": str(temp_dir / "outputs"),
        }
        with patch.dict(os.environ, env):
            save_output("results", test_df)

        # Verify file was saved
        output_path = temp_dir / "outputs" / "results.csv"
        assert output_path.exists()
        loaded = pd.read_csv(output_path)
        pd.testing.assert_frame_equal(loaded, test_df)

    def test_save_marks_artifact(self, temp_dir):
        """save_output with artifact=True should create marker file."""
        from goldfish.io import save_output

        test_array = np.array([1, 2, 3])

        config = {
            "outputs": {
                "model": {
                    "format": "npy",
                }
            }
        }

        env = {
            "GOLDFISH_STAGE_CONFIG": json.dumps(config),
            "GOLDFISH_OUTPUTS_DIR": str(temp_dir / "outputs"),
        }
        with patch.dict(os.environ, env):
            save_output("model", test_array, artifact=True)

        # Verify marker was created
        marker_path = temp_dir / "outputs" / ".artifacts" / "model"
        assert marker_path.exists()


class TestGetPaths:
    """Test get_input_path() and get_output_path()."""

    def test_get_input_path_returns_path(self):
        """get_input_path should return Path to input."""
        from goldfish.io import get_input_path

        path = get_input_path("features")
        assert isinstance(path, Path)
        assert "inputs" in str(path)
        assert "features" in str(path)

    def test_get_output_path_returns_path(self, temp_dir):
        """get_output_path should return Path to output."""
        from goldfish.io import get_output_path

        env = {
            "GOLDFISH_OUTPUTS_DIR": str(temp_dir / "outputs"),
        }
        with patch.dict(os.environ, env):
            path = get_output_path("results")

        assert isinstance(path, Path)
        assert "results" in str(path)


class TestCustomLoader:
    """Test custom loader functionality."""

    def test_custom_loader_is_called(self, temp_dir):
        """load_input should use custom loader if specified."""
        from goldfish.io import load_input

        # Create test file
        input_path = temp_dir / "inputs" / "compressed"
        input_path.parent.mkdir(parents=True)
        input_path.write_text("compressed data")

        # Create custom loader
        loader_path = temp_dir / "loader.py"
        loader_path.write_text("""
def decompress(path):
    return "decompressed: " + path.read_text()
""")

        config = {
            "inputs": {
                "compressed": {
                    "format": "file",
                    "loader": {
                        "script": str(loader_path),
                        "function": "decompress",
                    }
                }
            }
        }

        with patch.dict(os.environ, {"GOLDFISH_STAGE_CONFIG": json.dumps(config)}):
            with patch('goldfish.io.get_input_path', return_value=input_path):
                result = load_input("compressed")

        assert result == "decompressed: compressed data"
