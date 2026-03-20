"""Unit tests for warm pool startup script idle loop (Phase 2)."""

from __future__ import annotations


def test_idle_loop_section_contains_timeout() -> None:
    """Idle loop must use the configured timeout."""
    from goldfish.cloud.adapters.gcp.startup_builder import idle_loop_section

    script = idle_loop_section(idle_timeout_seconds=1800, gcs_pool_path="gs://bucket/warm_pool/test")
    assert "IDLE_TIMEOUT=1800" in script


def test_idle_loop_section_handles_sigterm() -> None:
    """Idle loop must handle SIGTERM for spot preemption."""
    from goldfish.cloud.adapters.gcp.startup_builder import idle_loop_section

    script = idle_loop_section(idle_timeout_seconds=900, gcs_pool_path="gs://bucket/warm_pool/test")
    assert "PREEMPTED during idle" in script
    assert "preempted" in script


def test_idle_loop_section_clears_exit_trap() -> None:
    """Idle loop must clear self-delete EXIT trap during idle."""
    from goldfish.cloud.adapters.gcp.startup_builder import idle_loop_section

    script = idle_loop_section(idle_timeout_seconds=900, gcs_pool_path="gs://bucket/warm_pool/test")
    assert "trap '' EXIT" in script


def test_idle_loop_section_cleans_workspace() -> None:
    """Idle loop must clean workspace between runs."""
    from goldfish.cloud.adapters.gcp.startup_builder import idle_loop_section

    script = idle_loop_section(idle_timeout_seconds=900, gcs_pool_path="gs://bucket/warm_pool/test")
    assert "/mnt/outputs/*" in script
    assert "/tmp/triton*" in script


def test_idle_loop_section_acks_new_job() -> None:
    """Idle loop must ACK new_job signal immediately."""
    from goldfish.cloud.adapters.gcp.startup_builder import idle_loop_section

    script = idle_loop_section(idle_timeout_seconds=900, gcs_pool_path="gs://bucket/warm_pool/test")
    assert "goldfish_ack=$req_id" in script


def test_build_startup_script_default_no_idle_loop() -> None:
    """Without warm_pool, script should NOT contain idle loop."""
    from goldfish.cloud.adapters.gcp.startup_builder import build_startup_script

    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="runs",
        run_path="stage-abc",
        image="us-docker.pkg.dev/proj/repo/img:v1",
        entrypoint="/bin/bash",
        env_map={"FOO": "bar"},
    )
    assert "warm_pool_idle_loop" not in script
    assert "IDLE_TIMEOUT" not in script


def test_build_startup_script_with_warm_pool_includes_idle_loop() -> None:
    """With warm_pool_idle_timeout_seconds, script should contain idle loop."""
    from goldfish.cloud.adapters.gcp.startup_builder import build_startup_script

    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="runs",
        run_path="stage-abc",
        image="us-docker.pkg.dev/proj/repo/img:v1",
        entrypoint="/bin/bash",
        env_map={"FOO": "bar"},
        warm_pool_idle_timeout_seconds=1800,
    )
    assert "warm_pool_idle_loop" in script
    assert "IDLE_TIMEOUT=1800" in script
    # Should only enter idle loop on success (exit code 0)
    assert 'EXIT_CODE" == "0"' in script


def test_build_startup_script_warm_pool_sets_current_image() -> None:
    """Warm pool script must track the current image for cache hit detection."""
    from goldfish.cloud.adapters.gcp.startup_builder import build_startup_script

    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="runs",
        run_path="stage-abc",
        image="us-docker.pkg.dev/proj/repo/img:v5",
        entrypoint="/bin/bash",
        env_map={},
        warm_pool_idle_timeout_seconds=900,
    )
    assert 'CURRENT_IMAGE="us-docker.pkg.dev/proj/repo/img:v5"' in script


def test_idle_loop_updates_gcs_paths_for_new_run() -> None:
    """Idle loop must update GCS log/exit paths for each new job.

    Bug: Without this, logs from the second job would overwrite the first
    job's logs in GCS because the paths were set once at script generation.
    """
    from goldfish.cloud.adapters.gcp.startup_builder import idle_loop_section

    script = idle_loop_section(idle_timeout_seconds=900, gcs_pool_path="gs://bucket/warm_pool/test")
    # Must update GCS paths from the new run_path
    assert "GCS_STDOUT_PATH=" in script
    assert "GCS_STDERR_PATH=" in script
    assert "GCS_EXIT_CODE_PATH=" in script
    assert "new_run_path" in script


def test_idle_loop_writes_exit_code_to_gcs_directly() -> None:
    """Exit code must be written to GCS directly, not via stale local path.

    Bug: EXIT_CODE_FILE pointed to the first run's gcsfuse path. On subsequent
    runs the exit code would go to the wrong location.
    """
    from goldfish.cloud.adapters.gcp.startup_builder import idle_loop_section

    script = idle_loop_section(idle_timeout_seconds=900, gcs_pool_path="gs://bucket/warm_pool/test")
    assert "gsutil cp -" in script  # Direct GCS upload, not local file write


def test_build_startup_script_warm_pool_exports_bucket() -> None:
    """Warm pool script must export GCS_BUCKET for idle loop path construction."""
    from goldfish.cloud.adapters.gcp.startup_builder import build_startup_script

    script = build_startup_script(
        bucket="my-artifacts",
        bucket_prefix="runs",
        run_path="stage-abc",
        image="img:v1",
        entrypoint="/bin/bash",
        env_map={},
        warm_pool_idle_timeout_seconds=900,
    )
    assert 'GCS_BUCKET="my-artifacts"' in script
