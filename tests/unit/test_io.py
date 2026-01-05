"""Tests for goldfish.io - Container IO library."""

import json
from pathlib import Path

import pytest


class TestLoadInputFormats:
    """Test load_input format handling."""

    def test_load_input_dataset_format_returns_path(self, tmp_path, monkeypatch):
        """Regression: 'dataset' format must be recognized and return Path.

        Goldfish registered sources have format='dataset'. The IO library
        must handle this like 'directory' - return the path for manual loading.
        """
        from goldfish.io import load_input

        # Setup inputs directory
        inputs_dir = tmp_path / "inputs"
        inputs_dir.mkdir()

        # Create a dataset directory
        dataset_path = inputs_dir / "my_dataset"
        dataset_path.mkdir()
        (dataset_path / "data.csv").write_text("a,b,c\n1,2,3")

        # Setup environment
        monkeypatch.setenv("GOLDFISH_INPUTS_DIR", str(inputs_dir))
        monkeypatch.setenv(
            "GOLDFISH_STAGE_CONFIG",
            json.dumps({"inputs": {"my_dataset": {"format": "dataset", "location": "gs://bucket/datasets/v1"}}}),
        )

        # Load the input
        result = load_input("my_dataset")

        # Should return Path, not raise ValueError
        assert isinstance(result, Path)
        assert result == dataset_path

    def test_load_input_directory_format_returns_path(self, tmp_path, monkeypatch):
        """Test that 'directory' format returns Path."""
        from goldfish.io import load_input

        inputs_dir = tmp_path / "inputs"
        inputs_dir.mkdir()

        dir_path = inputs_dir / "model_dir"
        dir_path.mkdir()
        (dir_path / "model.pt").write_text("fake model")

        monkeypatch.setenv("GOLDFISH_INPUTS_DIR", str(inputs_dir))
        monkeypatch.setenv(
            "GOLDFISH_STAGE_CONFIG",
            json.dumps({"inputs": {"model_dir": {"format": "directory"}}}),
        )

        result = load_input("model_dir")
        assert isinstance(result, Path)
        assert result == dir_path

    def test_load_input_file_format_returns_path(self, tmp_path, monkeypatch):
        """Test that 'file' format returns Path."""
        from goldfish.io import load_input

        inputs_dir = tmp_path / "inputs"
        inputs_dir.mkdir()

        file_path = inputs_dir / "config"
        file_path.write_text("config content")

        monkeypatch.setenv("GOLDFISH_INPUTS_DIR", str(inputs_dir))
        monkeypatch.setenv(
            "GOLDFISH_STAGE_CONFIG",
            json.dumps({"inputs": {"config": {"format": "file"}}}),
        )

        result = load_input("config")
        assert isinstance(result, Path)
        assert result == file_path

    def test_load_input_unknown_format_raises(self, tmp_path, monkeypatch):
        """Test that unknown formats raise ValueError."""
        from goldfish.io import load_input

        inputs_dir = tmp_path / "inputs"
        inputs_dir.mkdir()
        (inputs_dir / "data").write_text("data")

        monkeypatch.setenv("GOLDFISH_INPUTS_DIR", str(inputs_dir))
        monkeypatch.setenv(
            "GOLDFISH_STAGE_CONFIG",
            json.dumps({"inputs": {"data": {"format": "unknown_format"}}}),
        )

        with pytest.raises(ValueError, match="Unknown format"):
            load_input("data")


class TestLoadInputWithNpy:
    """Test NPY format loading (requires numpy)."""

    def test_load_input_npy_format(self, tmp_path, monkeypatch):
        """Test that 'npy' format loads numpy array."""
        pytest.importorskip("numpy")
        import numpy as np

        from goldfish.io import load_input

        inputs_dir = tmp_path / "inputs"
        inputs_dir.mkdir()

        # Save test array
        test_array = np.array([1, 2, 3])
        np.save(inputs_dir / "features.npy", test_array)

        monkeypatch.setenv("GOLDFISH_INPUTS_DIR", str(inputs_dir))
        monkeypatch.setenv(
            "GOLDFISH_STAGE_CONFIG",
            json.dumps({"inputs": {"features": {"format": "npy"}}}),
        )

        result = load_input("features")
        assert isinstance(result, np.ndarray)
        np.testing.assert_array_equal(result, test_array)


class TestLoadInputWithCsv:
    """Test CSV format loading (requires pandas)."""

    def test_load_input_csv_format(self, tmp_path, monkeypatch):
        """Test that 'csv' format loads pandas DataFrame."""
        pytest.importorskip("pandas")
        import pandas as pd

        from goldfish.io import load_input

        inputs_dir = tmp_path / "inputs"
        inputs_dir.mkdir()

        # Save test CSV
        (inputs_dir / "data.csv").write_text("a,b,c\n1,2,3\n4,5,6")

        monkeypatch.setenv("GOLDFISH_INPUTS_DIR", str(inputs_dir))
        monkeypatch.setenv(
            "GOLDFISH_STAGE_CONFIG",
            json.dumps({"inputs": {"data": {"format": "csv"}}}),
        )

        result = load_input("data")
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["a", "b", "c"]
        assert len(result) == 2


class TestRuntimeLog:
    """Test runtime_log function."""

    def test_runtime_log_prints_to_stdout(self, tmp_path, monkeypatch, capsys):
        """Regression: runtime_log must print to stdout for visibility in logs() tool.

        Bug: runtime_log only wrote to .goldfish/logs.txt, making logs invisible
        during execution since logs() tool reads from stdout.
        Fix: runtime_log now also prints to stdout with flush=True.
        """
        from goldfish.io import runtime_log

        # Setup outputs directory
        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))

        # Call runtime_log
        runtime_log("Test message", level="INFO")

        # Verify it printed to stdout
        captured = capsys.readouterr()
        assert "INFO: Test message" in captured.out
        assert captured.out.endswith("\n")

    def test_runtime_log_writes_to_file(self, tmp_path, monkeypatch):
        """Test that runtime_log still writes to .goldfish/logs.txt for AI monitoring."""
        from goldfish.io import runtime_log

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))

        runtime_log("Test message for AI", level="WARN")

        # Verify file was created and contains the message
        logs_file = outputs_dir / ".goldfish" / "logs.txt"
        assert logs_file.exists()
        content = logs_file.read_text()
        assert "WARN: Test message for AI" in content

    def test_runtime_log_includes_timestamp(self, tmp_path, monkeypatch, capsys):
        """Test that runtime_log includes timestamp in output."""
        from goldfish.io import runtime_log

        outputs_dir = tmp_path / "outputs"
        outputs_dir.mkdir()
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(outputs_dir))

        runtime_log("Timestamped message")

        captured = capsys.readouterr()
        # Should have format: [YYYY-MM-DD HH:MM:SS] LEVEL: message
        assert "[" in captured.out
        assert "]" in captured.out
        assert "INFO: Timestamped message" in captured.out
