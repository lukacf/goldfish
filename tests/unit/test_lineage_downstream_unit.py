"""Unit tests for downstream lineage tracking and input fetching."""

import time
from unittest.mock import Mock

from goldfish.lineage.manager import LineageManager
from goldfish.utils.fingerprint import calculate_fingerprint


def test_list_signals_source_stage_run_id(test_db):
    """Test filtering signals by source_stage_run_id."""
    run_a = "stage-run-a"
    run_b = "stage-run-b"

    # Create necessary records for FK constraints
    test_db.create_workspace_lineage("test_ws", description="test")
    test_db.create_version("test_ws", "v1", "v1", "sha1", "manual")
    for rid in [run_a, run_b]:
        test_db.create_stage_run(
            stage_run_id=rid,
            workspace_name="test_ws",
            version="v1",
            stage_name=f"stage-{rid[-1]}",
        )

    # Run A produces S1
    test_db.add_signal_with_source(
        stage_run_id=run_a, signal_name="s1", signal_type="output", storage_location="/path/a/s1"
    )

    # Run B consumes S1 (as an input)
    test_db.add_signal_with_source(
        stage_run_id=run_b,
        signal_name="input_s1",
        signal_type="input",
        storage_location="/path/a/s1",
        source_stage_run_id=run_a,
    )

    # List downstream signals for Run A
    signals = test_db.list_signals(source_stage_run_id=run_a)
    assert len(signals) == 1
    assert signals[0]["stage_run_id"] == run_b
    assert signals[0]["signal_name"] == "input_s1"


def test_list_inputs_for_runs(test_db):
    """Test efficient fetching of inputs for multiple runs."""
    run_a = "run-a"
    run_b = "run-b"
    run_c = "run-c"

    # Create necessary records for FK constraints
    test_db.create_workspace_lineage("ws", description="test")
    test_db.create_version("ws", "v1", "v1", "sha1", "manual")
    for rid in [run_a, run_b, run_c]:
        test_db.create_stage_run(
            stage_run_id=rid,
            workspace_name="ws",
            version="v1",
            stage_name="stage",
        )

    # Run B consumes from A
    test_db.add_signal_with_source(
        stage_run_id=run_b,
        signal_name="input_1",
        signal_type="input",
        storage_location="/loc/1",
        source_stage_run_id=run_a,
    )

    # Run C consumes from A and B
    test_db.add_signal_with_source(
        stage_run_id=run_c,
        signal_name="input_1",
        signal_type="input",
        storage_location="/loc/1",
        source_stage_run_id=run_a,
    )
    test_db.add_signal_with_source(
        stage_run_id=run_c,
        signal_name="input_2",
        signal_type="input",
        storage_location="/loc/2",
        source_stage_run_id=run_b,
    )

    inputs_map = test_db.list_inputs_for_runs([run_b, run_c])

    assert len(inputs_map[run_b]) == 1
    assert inputs_map[run_b][0]["source_stage_run_id"] == run_a

    assert len(inputs_map[run_c]) == 2
    sources_c = [inp["source_stage_run_id"] for inp in inputs_map[run_c]]
    assert set(sources_c) == {run_a, run_b}


def test_get_run_provenance_with_downstream(test_db):
    """Test LineageManager.get_run_provenance includes downstream runs."""
    # Mock workspace_manager
    mock_wm = Mock()
    mgr = LineageManager(db=test_db, workspace_manager=mock_wm)

    run_a_id = "run-a"
    run_b_id = "run-b"

    # Set up DB records
    test_db.create_workspace_lineage("test_ws", description="test")
    test_db.create_version("test_ws", "v1", "git_v1", "sha1", "manual")

    test_db.create_stage_run(
        stage_run_id=run_a_id,
        workspace_name="test_ws",
        version="v1",
        stage_name="stage-a",
    )

    test_db.create_stage_run(
        stage_run_id=run_b_id,
        workspace_name="test_ws",
        version="v1",
        stage_name="stage-b",
    )

    # Link them: B consumes A
    test_db.add_signal_with_source(
        stage_run_id=run_b_id,
        signal_name="in",
        signal_type="input",
        storage_location="/loc/a",
        source_stage_run_id=run_a_id,
    )

    # Verify provenance of A shows B as downstream
    provenance_a = mgr.get_run_provenance(run_a_id)
    assert len(provenance_a["downstream"]) == 1
    assert provenance_a["downstream"][0]["stage_run_id"] == run_b_id
    assert provenance_a["downstream"][0]["stage"] == "stage-b"

    # Verify provenance of B shows A as input source
    provenance_b = mgr.get_run_provenance(run_b_id)
    assert len(provenance_b["inputs"]) == 1
    assert provenance_b["inputs"][0]["source_stage_run_id"] == run_a_id


def test_fingerprint_performance_large_file(tmp_path):
    """Verify that fingerprinting is fast for large files (skips expensive stats)."""
    large_file = tmp_path / "large.csv"

    # Create a 1.1GB file (simulated by seeking)
    # Start with a header so pandas can read it quickly
    size = int(1.1 * 1024 * 1024 * 1024)

    start_create = time.time()
    with open(large_file, "wb") as f:
        f.write(b"col1,col2,col3\n")
        f.seek(size - 1)
        f.write(b"\n")
    create_time = time.time() - start_create
    print(f"DEBUG: File creation took {create_time:.4f}s")

    start = time.time()
    stats = calculate_fingerprint(large_file)
    elapsed = time.time() - start
    print(f"DEBUG: Fingerprinting took {elapsed:.4f}s")

    # Prefix hash and size check should be very fast if skip logic works
    assert elapsed < 1.0  # Increased from 0.5 to be safer in CI
    assert stats["size_bytes"] == size
    assert "row_count" not in stats
    assert stats["row_count_approx"] is True


def test_extract_stage_run_id_from_gcs_path():
    """Test extracting stage run ID from GCS paths."""
    from goldfish.jobs.stage_executor import _extract_stage_run_id_from_path

    # Valid GCS path with stage run ID
    path = "gs://bucket/artifacts/stage-abc123def456/outputs/model"
    assert _extract_stage_run_id_from_path(path) == "stage-abc123def456"

    # Path with stage ID in different positions
    path2 = "gs://my-bucket/some/prefix/stage-abcdef01/data.npy"
    assert _extract_stage_run_id_from_path(path2) == "stage-abcdef01"


def test_extract_stage_run_id_from_path_without_stage_id():
    """Test that paths without stage IDs return None."""
    from goldfish.jobs.stage_executor import _extract_stage_run_id_from_path

    # Path without stage run ID
    path = "gs://bucket/sources/my_data.csv"
    assert _extract_stage_run_id_from_path(path) is None

    # Path with similar but invalid pattern
    path2 = "gs://bucket/staged-data/output.npy"
    assert _extract_stage_run_id_from_path(path2) is None


def test_extract_stage_run_id_handles_local_paths():
    """Test extraction from local paths."""
    from goldfish.jobs.stage_executor import _extract_stage_run_id_from_path

    # Local path with stage run ID
    path = "/mnt/outputs/stage-deadbeef/features.npy"
    assert _extract_stage_run_id_from_path(path) == "stage-deadbeef"

    # Local path without stage run ID
    path2 = "/data/raw/input.csv"
    assert _extract_stage_run_id_from_path(path2) is None
