"""Tests that GCE startup script writes exit code on early failure.

Bug: When the startup script fails before Docker runs (e.g., apt-get fails,
Docker pull fails), no exit code is written to GCS. Goldfish then has to wait
the full not_found_timeout (300s) before giving up, and has no error details.

Fix: The EXIT trap writes the exit code to GCS before self-deleting, so goldfish
can detect the failure immediately.
"""

from __future__ import annotations


def test_exit_trap_captures_exit_code() -> None:
    """EXIT trap should capture $? into GOLDFISH_TRAP_EXIT_CODE before self_delete."""
    from goldfish.cloud.adapters.gcp.startup_builder import self_deletion_section

    script = self_deletion_section()
    assert "GOLDFISH_TRAP_EXIT_CODE=$?" in script


def test_self_delete_writes_exit_code_to_gcs() -> None:
    """self_delete() should write exit code to GCS before deleting the instance."""
    from goldfish.cloud.adapters.gcp.startup_builder import self_deletion_section

    script = self_deletion_section()
    # Should reference the exit code file and GCS path
    assert "EXIT_CODE_FILE" in script
    assert "GCS_EXIT_CODE_PATH" in script
    # Should write exit code before deleting
    assert script.index("GCS_EXIT_CODE_PATH") < script.index("gcloud compute instances delete")


def test_self_delete_sets_metadata_exit_code() -> None:
    """self_delete() should set exit code in instance metadata as fallback."""
    from goldfish.cloud.adapters.gcp.startup_builder import self_deletion_section

    script = self_deletion_section()
    assert "goldfish_exit_code=$trap_exit_code" in script


def test_startup_script_sets_exit_code_paths_early() -> None:
    """EXIT_CODE_FILE and GCS_EXIT_CODE_PATH must be set before any apt-get/docker commands."""
    from goldfish.cloud.adapters.gcp.startup_builder import build_startup_script

    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="runs",
        run_path="stage-abc123",
        image="us-docker.pkg.dev/proj/repo/img:v1",
        entrypoint="/bin/bash",
        env_map={"FOO": "bar"},
    )
    # EXIT_CODE_FILE must appear before apt-get install
    exit_code_pos = script.index("EXIT_CODE_FILE=")
    apt_get_pos = script.index("apt-get install")
    assert exit_code_pos < apt_get_pos, "EXIT_CODE_FILE must be set before apt-get"

    # GCS_EXIT_CODE_PATH must also appear early
    gcs_path_pos = script.index("GCS_EXIT_CODE_PATH=")
    assert gcs_path_pos < apt_get_pos, "GCS_EXIT_CODE_PATH must be set before apt-get"
