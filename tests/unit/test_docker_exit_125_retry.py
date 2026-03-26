"""Tests for Docker exit 125 retry logic and GPU runtime readiness gate.

Bug: Docker exit 125 = "daemon failed to start the container". On GPU VMs this
happens when nvidia-container-toolkit is installed and docker restarted, but the
nvidia runtime isn't fully registered before `docker run --gpus all` executes.
nvidia-smi passes (driver level) but Docker's runtime plugin isn't loaded yet.

Fix: Two layers of defense:
1. GPU readiness gate: after nvidia-smi passes, verify `docker info` shows nvidia
2. Exit 125 retry: if docker run exits 125, restart docker daemon and retry
"""

from __future__ import annotations

# =============================================================================
# Docker GPU readiness gate tests
# =============================================================================


class TestDockerGpuReadinessSection:
    """Tests for docker_gpu_readiness_section()."""

    def test_checks_docker_info_for_nvidia(self) -> None:
        """Must check docker info for nvidia runtime, not just nvidia-smi."""
        from goldfish.cloud.adapters.gcp.startup_builder import (
            docker_gpu_readiness_section,
        )

        script = docker_gpu_readiness_section()
        assert "docker info" in script
        assert "nvidia" in script.lower()

    def test_has_retry_loop(self) -> None:
        """Must retry multiple times waiting for nvidia runtime."""
        from goldfish.cloud.adapters.gcp.startup_builder import (
            DOCKER_GPU_READINESS_MAX_ATTEMPTS,
            docker_gpu_readiness_section,
        )

        script = docker_gpu_readiness_section()
        assert f"seq 1 {DOCKER_GPU_READINESS_MAX_ATTEMPTS}" in script
        assert "DOCKER_GPU_READY" in script

    def test_restarts_docker_on_retry(self) -> None:
        """Must restart docker daemon when nvidia runtime isn't detected."""
        from goldfish.cloud.adapters.gcp.startup_builder import (
            docker_gpu_readiness_section,
        )

        script = docker_gpu_readiness_section()
        assert "systemctl restart docker" in script

    def test_only_runs_on_gpu_nodes(self) -> None:
        """Must only run when GPU_PRESENT is set (reuses flag from gpu_driver_section)."""
        from goldfish.cloud.adapters.gcp.startup_builder import (
            docker_gpu_readiness_section,
        )

        script = docker_gpu_readiness_section()
        assert "GPU_PRESENT" in script
        # Should be guarded by GPU_PRESENT check
        assert 'if [[ "$GPU_PRESENT" == "1" ]]' in script

    def test_logs_stage_transitions(self) -> None:
        """Must log stage transitions for observability."""
        from goldfish.cloud.adapters.gcp.startup_builder import (
            docker_gpu_readiness_section,
        )

        script = docker_gpu_readiness_section()
        assert "docker_gpu_check" in script
        assert "docker_gpu_ready" in script

    def test_warns_but_proceeds_on_failure(self) -> None:
        """Must not hard-fail if nvidia runtime never appears — proceed and let docker run decide."""
        from goldfish.cloud.adapters.gcp.startup_builder import (
            docker_gpu_readiness_section,
        )

        script = docker_gpu_readiness_section()
        assert "WARNING" in script
        # Should NOT hard-exit on failure (fail-open design)
        # Check that no line is just "exit 1" (exclude "exit 125" in echo messages)
        lines = script.strip().split("\n")
        hard_exit_lines = [line.strip() for line in lines if line.strip() == "exit 1"]
        assert len(hard_exit_lines) == 0


# =============================================================================
# Docker run exit 125 retry tests
# =============================================================================


class TestDockerRunExit125Retry:
    """Tests for exit 125 retry logic in build_startup_script."""

    def _build_script(self, gpu_count: int = 8) -> str:
        from goldfish.cloud.adapters.gcp.startup_builder import build_startup_script

        return build_startup_script(
            bucket="test-bucket",
            bucket_prefix="runs",
            run_path="stage-abc",
            image="us-docker.pkg.dev/proj/repo/img:v1",
            entrypoint="/bin/bash",
            env_map={"FOO": "bar"},
            gpu_count=gpu_count,
        )

    def test_has_exit_125_retry_loop(self) -> None:
        """Assembled script must contain retry logic for exit code 125."""
        script = self._build_script()
        assert "EXIT_CODE" in script
        assert "125" in script
        # Must have a retry loop
        assert "DOCKER_RUN_ATTEMPT" in script

    def test_retry_restarts_docker_daemon(self) -> None:
        """Retry must restart docker daemon before re-attempting container launch."""
        script = self._build_script()
        # Find the retry block (contains both 125 and systemctl restart)
        assert "systemctl restart docker" in script
        # The restart in the retry block should come after the 125 check
        idx_125 = script.index('"$EXIT_CODE" -eq 125')
        idx_restart = script.index("systemctl restart docker", idx_125)
        assert idx_restart > idx_125

    def test_retry_re_runs_docker_cmd(self) -> None:
        """Retry must re-execute the DOCKER_CMD array."""
        script = self._build_script()
        # After the retry check, DOCKER_CMD must appear again
        idx_125 = script.index('"$EXIT_CODE" -eq 125')
        remaining = script[idx_125:]
        assert "DOCKER_CMD" in remaining

    def test_retry_logs_stage_transitions(self) -> None:
        """Retry attempts must be logged for observability."""
        script = self._build_script()
        assert "docker_retry" in script

    def test_retry_has_max_attempts(self) -> None:
        """Retry must have a bounded number of attempts."""
        from goldfish.cloud.adapters.gcp.startup_builder import (
            DOCKER_RUN_125_MAX_RETRIES,
        )

        script = self._build_script()
        assert str(DOCKER_RUN_125_MAX_RETRIES) in script
        assert "DOCKER_RUN_ATTEMPT" in script

    def test_cpu_only_script_also_has_retry(self) -> None:
        """Exit 125 can happen on CPU nodes too — retry should always be present."""
        script = self._build_script(gpu_count=0)
        assert '"$EXIT_CODE" -eq 125' in script


# =============================================================================
# Integration: GPU readiness gate in assembled script
# =============================================================================


class TestGpuReadinessInAssembledScript:
    """Tests that GPU readiness gate is properly placed in assembled script."""

    def test_gpu_readiness_after_driver_install(self) -> None:
        """GPU readiness check must come after driver install section."""
        from goldfish.cloud.adapters.gcp.startup_builder import build_startup_script

        script = build_startup_script(
            bucket="test-bucket",
            bucket_prefix="runs",
            run_path="stage-abc",
            image="us-docker.pkg.dev/proj/repo/img:v1",
            entrypoint="/bin/bash",
            env_map={},
            gpu_count=8,
        )
        # driver_ready comes from gpu_driver_section
        # docker_gpu_check comes from docker_gpu_readiness_section
        idx_driver = script.index("driver_ready")
        idx_gpu_check = script.index("docker_gpu_check")
        assert idx_gpu_check > idx_driver

    def test_gpu_readiness_before_docker_run(self) -> None:
        """GPU readiness check must come before docker run."""
        from goldfish.cloud.adapters.gcp.startup_builder import build_startup_script

        script = build_startup_script(
            bucket="test-bucket",
            bucket_prefix="runs",
            run_path="stage-abc",
            image="us-docker.pkg.dev/proj/repo/img:v1",
            entrypoint="/bin/bash",
            env_map={},
            gpu_count=8,
        )
        idx_gpu_check = script.index("docker_gpu_check")
        idx_docker_run = script.index("docker_run_begin")
        assert idx_gpu_check < idx_docker_run
