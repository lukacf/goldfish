"""Tests for warm pool startup script generation (Phase 2).

Tests for idle_loop_section() and warm pool mode in build_startup_script().
"""

from __future__ import annotations

from goldfish.cloud.adapters.gcp.startup_builder import (
    build_startup_script,
    idle_loop_section,
)

# =============================================================================
# idle_loop_section() tests
# =============================================================================


class TestIdleLoopSection:
    """Tests for idle_loop_section() shell script fragment."""

    def test_idle_loop_section_contains_key_constructs(self) -> None:
        """Verify the output contains all critical shell constructs."""
        script = idle_loop_section(idle_timeout_seconds=1800)

        # Function definition
        assert "warm_pool_idle_loop" in script

        # EXIT trap suspension during idle
        assert "trap '' EXIT" in script

        # SIGTERM handler for spot preemption during idle
        assert "SIGTERM" in script

        # Metadata poll URL (Overdrive pattern)
        assert "metadata.google.internal/computeMetadata/v1/instance/attributes/goldfish" in script

        # Idle timeout check
        assert "IDLE_TIMEOUT=1800" in script
        assert "IDLE TIMEOUT REACHED" in script

        # PID guards (NEVER kill ${PID:-0})
        assert '[[ -n "$PID_VAL" && "$PID_VAL" != "0" ]]' in script

        # GCS path reset section
        assert "GCS_STDOUT_PATH=" in script
        assert "GCS_STDERR_PATH=" in script
        assert "GCS_EXIT_CODE_PATH=" in script
        assert "GCS_METRICS_PATH=" in script
        assert "GCS_SVS_DURING_PATH=" in script
        assert "GCS_TERMINATION_CAUSE_PATH=" in script
        assert "EXIT_CODE_FILE=" in script
        assert "LOCAL_STDOUT=" in script
        assert "LOCAL_STDERR=" in script

    def test_idle_loop_section_preserve_paths(self) -> None:
        """With preserve_paths, cleanup should skip those paths."""
        script = idle_loop_section(
            idle_timeout_seconds=1800,
            preserve_paths=["/tmp/triton*", "/mnt/cache/model*"],
        )

        # Should have targeted cleanup that excludes preserve_paths
        assert "/tmp/triton*" in script
        assert "/mnt/cache/model*" in script
        # Should use find with exclusion or conditional rm
        assert "find" in script or "!" in script

    def test_idle_loop_section_no_preserve_paths(self) -> None:
        """Without preserve_paths, cleanup should be simple rm -rf."""
        script = idle_loop_section(idle_timeout_seconds=1800)

        # Default cleanup without preserve_paths exclusions
        assert "rm -rf" in script
        # Should NOT have find-based exclusion logic
        assert "/tmp/triton" not in script

    def test_idle_loop_section_custom_timeout(self) -> None:
        """Verify custom timeout value is embedded."""
        script = idle_loop_section(idle_timeout_seconds=3600)
        assert "IDLE_TIMEOUT=3600" in script

    def test_idle_loop_section_new_job_ack(self) -> None:
        """Verify ACK is sent immediately on new_job receipt."""
        script = idle_loop_section(idle_timeout_seconds=1800)
        assert "goldfish_ack=$REQ_ID" in script

    def test_idle_loop_section_docker_run(self) -> None:
        """Verify Docker run command is present for new jobs."""
        script = idle_loop_section(idle_timeout_seconds=1800)
        assert "docker run" in script
        assert "--rm" in script
        assert "--ipc=host" in script

    def test_idle_loop_section_exit_trap_restored_for_job(self) -> None:
        """Verify EXIT trap is restored during job execution."""
        script = idle_loop_section(idle_timeout_seconds=1800)
        # Should restore self-delete trap during job
        assert "self_delete" in script

    def test_idle_loop_section_job_complete_returns_to_idle(self) -> None:
        """Verify idle timer is reset after job completion."""
        script = idle_loop_section(idle_timeout_seconds=1800)
        assert "RETURNING TO IDLE" in script

    def test_idle_loop_section_spec_download(self) -> None:
        """Verify job spec is downloaded from GCS."""
        script = idle_loop_section(idle_timeout_seconds=1800)
        assert "gsutil cp" in script
        assert "job_spec.json" in script

    def test_idle_loop_section_image_pull_conditional(self) -> None:
        """Verify warm-pool jobs always refresh the requested image tag."""
        script = idle_loop_section(idle_timeout_seconds=1800)
        assert "CURRENT_IMAGE" in script
        assert "docker pull" in script
        assert 'if [[ "$NEW_IMAGE" != "$CURRENT_IMAGE" ]]' not in script


# =============================================================================
# build_startup_script() warm pool mode tests
# =============================================================================


class TestBuildStartupScriptWarmPool:
    """Tests for build_startup_script() with warm pool parameters."""

    def _build_default_kwargs(self) -> dict:
        """Return default kwargs for build_startup_script."""
        return {
            "bucket": "test-bucket",
            "bucket_prefix": "prefix",
            "run_path": "runs/stage-abc123",
            "image": "us-docker.pkg.dev/proj/repo/image:v1",
            "entrypoint": "/bin/bash",
            "env_map": {"GOLDFISH_RUN_ID": "stage-abc123"},
            "mounts": [("/mnt/inputs", "/mnt/inputs"), ("/mnt/outputs", "/mnt/outputs")],
            "gpu_count": 0,
        }

    def test_build_startup_script_warm_pool_mode(self) -> None:
        """With warm_pool_idle_timeout_seconds, idle loop is included and exit is replaced."""
        kwargs = self._build_default_kwargs()
        kwargs["warm_pool_idle_timeout_seconds"] = 1800

        script = build_startup_script(**kwargs)

        # Idle loop should be present
        assert "warm_pool_idle_loop" in script
        # Final exit should call idle loop, NOT exit $EXIT_CODE
        # The script should end with the idle loop call, not a bare exit
        lines = script.strip().splitlines()
        # Find the last substantive lines — should call warm_pool_idle_loop
        # and NOT have "exit $EXIT_CODE" as the final action
        tail = "\n".join(lines[-20:])
        assert "warm_pool_idle_loop" in tail

    def test_build_startup_script_no_warm_pool(self) -> None:
        """Without warm pool params, no idle loop and exit $EXIT_CODE is present."""
        kwargs = self._build_default_kwargs()

        script = build_startup_script(**kwargs)

        # No idle loop
        assert "warm_pool_idle_loop" not in script
        # Normal exit
        assert "exit $EXIT_CODE" in script

    def test_build_startup_script_warm_pool_exports_bucket(self) -> None:
        """Verify GCS_BUCKET is exported for the idle loop to use."""
        kwargs = self._build_default_kwargs()
        kwargs["warm_pool_idle_timeout_seconds"] = 1800

        script = build_startup_script(**kwargs)

        assert 'GCS_BUCKET="test-bucket"' in script

    def test_build_startup_script_warm_pool_tracks_image(self) -> None:
        """Verify CURRENT_IMAGE is set after Docker pull."""
        kwargs = self._build_default_kwargs()
        kwargs["warm_pool_idle_timeout_seconds"] = 1800

        script = build_startup_script(**kwargs)

        assert "CURRENT_IMAGE=" in script
        # Should track the image that was pulled
        assert "us-docker.pkg.dev/proj/repo/image:v1" in script

    def test_build_startup_script_warm_pool_last_job_req_id(self) -> None:
        """Verify LAST_JOB_REQ_ID is initialized."""
        kwargs = self._build_default_kwargs()
        kwargs["warm_pool_idle_timeout_seconds"] = 1800

        script = build_startup_script(**kwargs)

        assert 'LAST_JOB_REQ_ID=""' in script

    def test_build_startup_script_warm_pool_no_final_exit(self) -> None:
        """In warm pool mode, the script ends with warm_pool_idle_loop (not exit $EXIT_CODE)."""
        kwargs = self._build_default_kwargs()
        kwargs["warm_pool_idle_timeout_seconds"] = 1800

        script = build_startup_script(**kwargs)

        # The script should call warm_pool_idle_loop as the final command
        lines = [line.strip() for line in script.strip().splitlines() if line.strip()]
        assert lines[-1] == "warm_pool_idle_loop"
        # There should also be a metadata check that can fall back to exit $EXIT_CODE
        # (for overflow VMs where registration failed), but that's in the guard block
        assert "goldfish_warm_pool_disabled" in script

    def test_build_startup_script_warm_pool_with_preserve_paths(self) -> None:
        """Verify preserve_paths are passed through to idle loop."""
        kwargs = self._build_default_kwargs()
        kwargs["warm_pool_idle_timeout_seconds"] = 1800
        kwargs["warm_pool_preserve_paths"] = ["/tmp/triton*"]

        script = build_startup_script(**kwargs)

        assert "/tmp/triton*" in script
