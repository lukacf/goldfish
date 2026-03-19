"""Unit tests for Pre-Run Review context enhancements."""

from pathlib import Path
from unittest.mock import MagicMock

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


def test_format_input_resolution_shows_all_contents(reviewer):
    """All contents must be listed — truncation caused SVS false positives."""
    long_contents = [f"file_{i}.txt" for i in range(83)]
    input_context = [
        {"input": "test_input", "source_type": "stage", "storage_location": "/local/path", "contents": long_contents}
    ]

    formatted = reviewer._format_input_resolution(input_context)

    # ALL files must be listed, not just the first 50
    assert "file_0.txt" in formatted
    assert "file_49.txt" in formatted
    assert "file_50.txt" in formatted
    assert "file_82.txt" in formatted
    # Must NOT truncate
    assert "more items" not in formatted
    # Should show total count
    assert "(83 items)" in formatted


def test_format_input_resolution_no_false_positive_on_subdirectory_files(reviewer):
    """Regression: files in subdirectories must be visible to SVS reviewer.

    Bug: With 83+ files, SVS only saw the first 50 (all train shards),
    missed tokenizer files in a subdirectory, and blocked with a false ERROR.
    """
    # Simulate the actual bug scenario: 80 train shards + tokenizer in subdirectory
    contents = [f"fineweb_train_{i:03d}.bin" for i in range(80)]
    contents.extend(["fineweb_val_000.bin", "fineweb_val_001.bin"])
    contents.extend(["tokenizers/tokenizer.model", "tokenizers/tokenizer.vocab"])

    input_context = [
        {
            "input": "fineweb",
            "source_type": "stage",
            "from_stage": "data",
            "signal": "fineweb",
            "storage_location": "gs://bucket/runs/stage-xxx/outputs/fineweb/",
            "contents": contents,
        }
    ]

    formatted = reviewer._format_input_resolution(input_context)

    # The tokenizer files (which were beyond index 50) MUST be visible
    assert "tokenizers/tokenizer.model" in formatted
    assert "tokenizers/tokenizer.vocab" in formatted


def test_format_input_resolution_caps_at_max_contents(reviewer):
    """Contents beyond MAX_CONTENTS_ITEMS are truncated with a warning."""
    from goldfish.pre_run_review import MAX_CONTENTS_ITEMS

    contents = [f"shard_{i:05d}.bin" for i in range(MAX_CONTENTS_ITEMS + 200)]
    input_context = [{"input": "big", "source_type": "stage", "storage_location": "/path", "contents": contents}]

    formatted = reviewer._format_input_resolution(input_context)

    # Items within the cap are shown
    assert "shard_00000.bin" in formatted
    assert f"shard_{MAX_CONTENTS_ITEMS - 1:05d}.bin" in formatted
    # Items beyond the cap are NOT shown
    assert f"shard_{MAX_CONTENTS_ITEMS:05d}.bin" not in formatted
    # Warning message present
    assert "NOT SHOWN" in formatted
    assert "do NOT treat unlisted files as absent" in formatted
    # Total count still shown
    assert f"({MAX_CONTENTS_ITEMS + 200} items)" in formatted


def test_stage_executor_list_storage_contents_cloud():
    """Test _list_storage_contents with cloud storage adapter."""
    from goldfish.cloud.contracts import StorageURI

    # Setup mock
    mock_executor = MagicMock(spec=StageExecutor)
    # Bind the method from the class to the mock instance
    mock_executor._list_storage_contents = StageExecutor._list_storage_contents.__get__(mock_executor, StageExecutor)

    # Mock storage property to return a mock storage adapter
    mock_storage = MagicMock()
    mock_storage.list_prefix = MagicMock(
        return_value=[
            StorageURI("gs", "bucket", "path/file1.txt"),
            StorageURI("gs", "bucket", "path/subdir/file2.txt"),
        ]
    )
    type(mock_executor).storage = property(lambda self: mock_storage)

    results = mock_executor._list_storage_contents("gs://bucket/path/")

    assert results == ["file1.txt", "subdir/file2.txt"]
    mock_storage.list_prefix.assert_called_once()


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


def test_build_stage_sections_reads_rust_module():
    """Test that _build_stage_sections reads .rs files and sets module_lang to rust."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        workspace = Path(tmpdir)
        (workspace / "modules").mkdir()
        (workspace / "configs").mkdir()

        # Create a Rust module file
        rust_code = """use goldfish_rust::prelude::*;

fn main() {
    let input = load_input::<f32>("data").unwrap();
    save_output("result", &input).unwrap();
}
"""
        (workspace / "modules" / "preprocess.rs").write_text(rust_code)

        # Create PreRunReviewer instance
        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(config, svs_config, workspace, workspace)

        # Build sections for the rust stage
        result = reviewer._build_stage_sections(["preprocess"])

        # Verify Rust module is read with correct language tag
        # _build_stage_sections returns a joined string of all sections
        assert isinstance(result, str)
        assert "```rust" in result  # module_lang should be "rust"
        assert "goldfish_rust::prelude" in result
        assert "load_input" in result
        assert "modules/preprocess.rs" in result


def test_build_stage_sections_prefers_python_over_rust():
    """Test that Python module is preferred when both .py and .rs exist."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        workspace = Path(tmpdir)
        (workspace / "modules").mkdir()
        (workspace / "configs").mkdir()

        # Create both Python and Rust modules
        (workspace / "modules" / "train.py").write_text("def main(): pass")
        (workspace / "modules" / "train.rs").write_text("fn main() {}")

        config = PreRunReviewConfig()
        svs_config = SVSConfig()
        reviewer = PreRunReviewer(config, svs_config, workspace, workspace)

        result = reviewer._build_stage_sections(["train"])

        # Should prefer Python
        assert isinstance(result, str)
        assert "```python" in result
        assert "def main()" in result
        assert "modules/train.py" in result
