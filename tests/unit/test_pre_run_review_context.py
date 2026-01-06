"""Unit tests for Pre-Run Review context enhancements."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from goldfish.config import PreRunReviewConfig, SVSConfig
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.pre_run_review import PreRunReviewer


@pytest.fixture
def reviewer():
    config = PreRunReviewConfig()
    svs_config = SVSConfig()
    workspace_path = Path("/tmp/workspace")
    dev_repo_path = Path("/tmp/dev_repo")
    return PreRunReviewer(config, svs_config, workspace_path, dev_repo_path)


def test_format_input_resolution_includes_storage_and_contents(reviewer):
    """Test that input resolution formatting includes storage location and contents."""
    input_context = [
        {
            "input": "test_input",
            "source_type": "stage",
            "from_stage": "upstream",
            "signal": "data",
            "storage_location": "gs://bucket/path/to/data/",
            "contents": ["file1.bin", "subdir/file2.txt"],
        }
    ]

    formatted = reviewer._format_input_resolution(input_context)

    assert "- input: test_input" in formatted
    assert "source_type: stage" in formatted
    assert "storage_location: gs://bucket/path/to/data/" in formatted
    assert "contents:" in formatted
    assert "  - file1.bin" in formatted
    assert "  - subdir/file2.txt" in formatted


def test_format_input_resolution_truncates_contents(reviewer):
    """Test that content listing is truncated if too long."""
    long_contents = [f"file_{i}.txt" for i in range(60)]
    input_context = [
        {"input": "test_input", "source_type": "stage", "storage_location": "/local/path", "contents": long_contents}
    ]

    formatted = reviewer._format_input_resolution(input_context)

    assert "file_0.txt" in formatted
    assert "file_49.txt" in formatted
    assert "file_50.txt" not in formatted
    assert "... (10 more items)" in formatted


@patch("goldfish.jobs.stage_executor.subprocess.run")
def test_stage_executor_list_storage_contents_gsutil(mock_run):
    """Test _list_storage_contents with gsutil fallback."""
    # Setup mock
    mock_executor = MagicMock(spec=StageExecutor)
    # Bind the method from the class to the mock instance
    mock_executor._list_storage_contents = StageExecutor._list_storage_contents.__get__(mock_executor, StageExecutor)
    # Mock _get_gcs_client to return None (triggering gsutil fallback)
    mock_executor._get_gcs_client = MagicMock(return_value=(None, "error"))

    # Mock subprocess result
    mock_run.return_value = MagicMock(
        returncode=0, stdout="gs://bucket/path/file1.txt\ngs://bucket/path/subdir/file2.txt\n"
    )

    results = mock_executor._list_storage_contents("gs://bucket/path/")

    assert results == ["file1.txt", "subdir/file2.txt"]
    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert cmd[:3] == ["gsutil", "ls", "-r"]
    assert cmd[3] == "gs://bucket/path/"


def test_stage_executor_list_storage_contents_local():
    """Test _list_storage_contents with local path."""
    import tempfile

    # Setup temp directory structure
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        (base / "file1.txt").touch()
        (base / "subdir").mkdir()
        (base / "subdir" / "file2.txt").touch()

        mock_executor = MagicMock(spec=StageExecutor)
        mock_executor._list_storage_contents = StageExecutor._list_storage_contents.__get__(
            mock_executor, StageExecutor
        )

        results = mock_executor._list_storage_contents(str(base))

        # Results usually come back sorted from the method implementation
        assert "file1.txt" in results
        assert "subdir/file2.txt" in results
        # _list_storage_contents includes directories in its output
        assert "subdir/" in results
        assert len(results) == 3
