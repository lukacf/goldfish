"""Regression test: GPU container must not rely on nvidia-smi runtime detection.

Bug: On a3-highgpu-8g VMs, NVIDIA drivers load asynchronously (~210s after boot).
The startup script's `command -v nvidia-smi` check runs earlier, fails, and launches
the container WITHOUT --gpus all. Result: CUDA error: invalid device ordinal.

Fix: When the profile specifies GPUs (gpu_count > 0), pass --gpus all unconditionally
at build time. Don't rely on runtime nvidia-smi detection.
"""

from __future__ import annotations


def test_docker_run_section_uses_gpus_all_when_gpu_requested() -> None:
    """When gpu_count > 0, --gpus all must be hardcoded, not runtime-detected."""
    from goldfish.cloud.adapters.gcp.startup_builder import docker_run_section

    script = docker_run_section(
        image="test:v1",
        env_keys=["FOO"],
        mounts=[],
        entrypoint="/bin/bash",
        gpu_count=8,
    )
    # Must unconditionally set --gpus all
    assert "--gpus all" in script
    # Must NOT use nvidia-smi runtime detection for GPU flag
    assert "command -v nvidia-smi" not in script or "DOCKER_GPU_ARGS" not in script


def test_docker_run_section_no_gpus_when_cpu_only() -> None:
    """When gpu_count is 0, no --gpus flag should be added."""
    from goldfish.cloud.adapters.gcp.startup_builder import docker_run_section

    script = docker_run_section(
        image="test:v1",
        env_keys=["FOO"],
        mounts=[],
        entrypoint="/bin/bash",
        gpu_count=0,
    )
    assert "--gpus all" not in script


def test_docker_run_section_default_no_gpus() -> None:
    """Default (no gpu_count) should not add --gpus."""
    from goldfish.cloud.adapters.gcp.startup_builder import docker_run_section

    script = docker_run_section(
        image="test:v1",
        env_keys=[],
        mounts=[],
        entrypoint="/bin/bash",
    )
    assert "--gpus all" not in script


def test_build_startup_script_passes_gpu_count_through() -> None:
    """build_startup_script must propagate gpu_count to docker_run_section."""
    from goldfish.cloud.adapters.gcp.startup_builder import build_startup_script

    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="runs",
        run_path="stage-abc",
        image="us-docker.pkg.dev/proj/repo/img:v1",
        entrypoint="/bin/bash",
        env_map={"FOO": "bar"},
        gpu_count=8,
    )
    assert "--gpus all" in script
