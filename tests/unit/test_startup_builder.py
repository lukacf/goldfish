"""Tests for startup_builder - GCE startup script generation."""

import shlex

from goldfish.infra.startup_builder import (
    build_startup_script,
    disk_mount_section,
    docker_run_section,
    gcsfuse_section,
    gpu_driver_section,
    stage_log_section,
)


def test_gpu_driver_section_contains_retry_logic():
    """Test that GPU driver section includes 160-attempt retry logic."""
    script = gpu_driver_section()

    assert "for attempt in $(seq 1 160)" in script
    assert "nvidia-smi" in script
    assert "sleep 15" in script
    assert "DRIVER_READY=1" in script


def test_gcsfuse_section_correct_mount_point():
    """Test that gcsfuse section uses correct bucket and mount point."""
    script = gcsfuse_section(bucket="my-bucket", mount_point="/mnt/gcs")

    assert "gcsfuse" in script
    assert "--implicit-dirs" in script
    assert "my-bucket /mnt/gcs" in script
    assert "mkdir -p /mnt/gcs" in script
    assert "for attempt in $(seq 1 5)" in script  # 5 retry attempts


def test_gcsfuse_section_retry_logic():
    """Test that gcsfuse includes retry with fallback."""
    script = gcsfuse_section(bucket="test-bucket", mount_point="/mnt/test")

    assert "for attempt in $(seq 1 5)" in script
    assert "fusermount -u /mnt/test" in script  # Cleanup on retry
    assert "sleep 2" in script


# =============================================================================
# Regression Tests - gcsfuse must use allow_other for Docker access
# =============================================================================


def test_gcsfuse_section_allow_other_for_docker():
    """Regression: gcsfuse must use -o allow_other for Docker containers to access mount.

    FUSE filesystems only allow the mounting user (root) by default. Docker containers
    run in a different process context and need allow_other to access the mount.
    """
    script = gcsfuse_section(bucket="test-bucket", mount_point="/mnt/gcs")

    # Must have allow_other FUSE option
    assert "-o allow_other" in script

    # Must enable user_allow_other in fuse.conf (required for allow_other)
    assert "user_allow_other" in script
    assert "/etc/fuse.conf" in script


def test_gcsfuse_section_uses_correct_flag_syntax():
    """Regression: gcsfuse uses -o for FUSE options, NOT --allow-other.

    gcsfuse 3.5.4+ doesn't recognize --allow-other flag. Must use -o allow_other.
    This was a bug where we used --allow-other which caused:
    'Error: unknown flag: --allow-other'
    """
    script = gcsfuse_section(bucket="test-bucket", mount_point="/mnt/gcs")

    # Must use -o syntax for FUSE mount options
    assert "-o allow_other" in script

    # Must NOT use the incorrect --allow-other syntax
    assert "--allow-other" not in script


def test_gcsfuse_section_uid_gid_for_container_user():
    """Regression: gcsfuse must set uid/gid=1000 for non-root container user.

    Container images like pytorch-notebook run as jovyan (UID 1000, GID 100).
    Files need to appear owned by this user for the container to read them.
    """
    script = gcsfuse_section(bucket="test-bucket", mount_point="/mnt/gcs")

    assert "--uid=1000" in script
    assert "--gid=100" in script


def test_gcsfuse_section_file_dir_modes():
    """Regression: gcsfuse must set readable file/dir modes."""
    script = gcsfuse_section(bucket="test-bucket", mount_point="/mnt/gcs")

    # Files should be readable (0644)
    assert "--file-mode=0644" in script
    # Directories should be traversable (0755)
    assert "--dir-mode=0755" in script


def test_disk_mount_section_device_candidates():
    """Test that disk mount tries multiple device paths."""
    script = disk_mount_section(disk_id="data-disk", mount_point="/mnt/data", mode="ro")

    # Should try multiple device candidates
    assert "/dev/disk/by-id/google-data-disk-part1" in script
    assert "/dev/disk/by-id/google-data-diskp1" in script
    assert "/dev/disk/by-id/google-data-disk" in script
    assert "mount -t ext4 -o ro" in script


def test_disk_mount_section_rw_mode():
    """Test that disk can be mounted in read-write mode."""
    script = disk_mount_section(disk_id="data-disk", mount_point="/mnt/data", mode="rw")

    assert "mount -t ext4 -o rw" in script


def test_docker_run_section_gpu_detection():
    """Test that Docker run section includes GPU detection."""
    script = docker_run_section(
        image="gcr.io/project/image:tag",
        env_keys=["VAR1", "VAR2"],
        mounts=[("/host/path", "/container/path")],
        entrypoint="/bin/bash",
    )

    assert "nvidia-smi" in script
    assert "--gpus all" in script
    assert "DOCKER_GPU_ARGS" in script


def test_docker_run_section_environment_variables():
    """Test that Docker run section passes environment variables."""
    script = docker_run_section(
        image="test-image",
        env_keys=["FOO", "BAR", "BAZ"],
        mounts=[],
        entrypoint="/bin/bash",
    )

    assert "-e FOO" in script
    assert "-e BAR" in script
    assert "-e BAZ" in script


def test_docker_run_section_volume_mounts():
    """Test that Docker run section includes volume mounts."""
    script = docker_run_section(
        image="test-image",
        env_keys=[],
        mounts=[("/host1", "/container1"), ("/host2", "/container2")],
        entrypoint="/bin/bash",
    )

    assert "-v /host1:/container1" in script
    assert "-v /host2:/container2" in script


def test_docker_run_section_shm_size():
    """Test that Docker run section sets shared memory size."""
    script = docker_run_section(
        image="test-image",
        env_keys=[],
        mounts=[],
        entrypoint="/bin/bash",
        shm_size="32g",
    )

    assert "--shm-size=32g" in script


def test_docker_run_section_cmd():
    """Test that Docker run section includes cmd as argument to entrypoint."""
    script = docker_run_section(
        image="test-image",
        env_keys=[],
        mounts=[],
        entrypoint="/bin/bash",
        cmd="/entrypoint.sh",
    )

    # The cmd should appear after the image name
    assert "test-image /entrypoint.sh" in script


def test_docker_run_section_no_cmd():
    """Test that Docker run section works without cmd (just entrypoint)."""
    script = docker_run_section(
        image="test-image",
        env_keys=[],
        mounts=[],
        entrypoint="/bin/bash",
    )

    # Image should be the last thing in the DOCKER_CMD array (no trailing space)
    assert "test-image\n)" in script


def test_stage_log_section_gcs_uri():
    """Test that stage log section logs to GCS."""
    script = stage_log_section(gcs_uri="gs://bucket/path/stage_times.log")

    assert "log_stage()" in script
    assert "gs://bucket/path/stage_times.log" in script
    assert "gsutil cp" in script


def test_build_startup_script_basic_structure():
    """Test that build_startup_script creates valid bash script."""
    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="",
        run_path="runs/test-run",
        image="test-image:latest",
        entrypoint="/bin/bash",
        env_map={"KEY": "value"},
    )

    # Should start with shebang
    assert script.startswith("#!/bin/bash")

    # Should set error handling
    assert "set -euxo pipefail" in script

    # Should include Docker installation
    assert "docker.io" in script

    # Should include image pull
    assert "docker pull test-image:latest" in script


def test_build_startup_script_environment_escaping():
    """Test that environment variables are properly escaped with shlex.quote."""
    dangerous_value = "'; whoami; echo '"

    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="",
        run_path="runs/test",
        image="test-image",
        entrypoint="/bin/bash",
        env_map={"DANGER": dangerous_value},
    )

    # Should use proper shell quoting (shlex.quote wraps and escapes properly)
    # The dangerous value should be safely quoted
    expected = f"export DANGER={shlex.quote(dangerous_value)}"
    assert expected in script

    # Verify shlex.quote actually did its job (produces safe shell string)
    # shlex.quote("'; whoami; echo '") -> ''"'"'; whoami; echo '"'"''
    # This ensures the value is treated as a literal string, not executable code
    safe_quoted = shlex.quote(dangerous_value)
    assert safe_quoted.startswith("'") or safe_quoted.startswith('"')  # Quoted
    assert "export DANGER=" + safe_quoted in script  # Full export statement present


def test_build_startup_script_with_gcsfuse():
    """Test that gcsfuse mounting is included when enabled."""
    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="",
        run_path="runs/test",
        image="test-image",
        entrypoint="/bin/bash",
        env_map={},
        gcsfuse=True,
    )

    assert "gcsfuse" in script
    assert "--implicit-dirs" in script
    assert "test-bucket" in script
    assert "gcsfuse_begin" in script
    assert "gcsfuse_ready" in script


def test_build_startup_script_without_gcsfuse():
    """Test that gcsfuse is excluded when disabled."""
    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="",
        run_path="runs/test",
        image="test-image",
        entrypoint="/bin/bash",
        env_map={},
        gcsfuse=False,
    )

    assert "gcsfuse" not in script


def test_build_startup_script_with_disk_mounts():
    """Test that disk mounts are included."""
    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="",
        run_path="runs/test",
        image="test-image",
        entrypoint="/bin/bash",
        env_map={},
        disk_mounts=[("data-disk", "/mnt/data", "ro")],
    )

    assert "/dev/disk/by-id/google-data-disk" in script
    assert "mount -t ext4 -o ro" in script


def test_build_startup_script_pre_run_commands():
    """Test that pre-run commands are executed before Docker."""
    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="",
        run_path="runs/test",
        image="test-image",
        entrypoint="/bin/bash",
        env_map={},
        pre_run_cmds=["echo 'before docker'", "ls /tmp"],
    )

    # Pre-run commands should appear before docker_run_begin
    pre_run_idx = script.index("echo 'before docker'")
    docker_idx = script.index("docker_run_begin")
    assert pre_run_idx < docker_idx


def test_build_startup_script_post_run_commands():
    """Test that post-run commands are executed after Docker."""
    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="",
        run_path="runs/test",
        image="test-image",
        entrypoint="/bin/bash",
        env_map={},
        post_run_cmds=["echo 'after docker'", "ls /tmp"],
    )

    # Post-run commands should appear after docker_run_end
    docker_idx = script.index("docker_run_end")
    post_run_idx = script.index("echo 'after docker'")
    assert docker_idx < post_run_idx


def test_build_startup_script_exit_code_upload():
    """Test that exit code is uploaded to GCS."""
    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="",
        run_path="runs/test-run",
        image="test-image",
        entrypoint="/bin/bash",
        env_map={},
    )

    assert "EXIT_CODE=$?" in script
    assert "wait" in script  # Wait for tee background processes
    assert "exit_code.txt" in script
    assert "gs://test-bucket/runs/test-run/logs/exit_code.txt" in script
    assert "stdout.log" in script
    assert "stderr.log" in script


def test_build_startup_script_shutdown():
    """Test that script shuts down instance after completion."""
    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="",
        run_path="runs/test",
        image="test-image",
        entrypoint="/bin/bash",
        env_map={},
    )

    assert "shutdown -h now" in script
    assert "exit $EXIT_CODE" in script


def test_build_startup_script_multiple_env_vars():
    """Test that multiple environment variables are properly exported."""
    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="",
        run_path="runs/test",
        image="test-image",
        entrypoint="/bin/bash",
        env_map={
            "VAR1": "value1",
            "VAR2": "value2",
            "VAR3": "complex 'value' with \"quotes\"",
        },
    )

    assert "export VAR1=" in script
    assert "export VAR2=" in script
    assert "export VAR3=" in script

    # All values should be properly quoted
    for key, value in [("VAR1", "value1"), ("VAR2", "value2")]:
        expected = f"export {key}={shlex.quote(value)}"
        assert expected in script


def test_build_startup_script_with_cmd():
    """Test that cmd is passed to docker_run_section for entrypoint script."""
    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="",
        run_path="runs/test",
        image="test-image:latest",
        entrypoint="/bin/bash",
        cmd="/entrypoint.sh",
        env_map={},
    )

    # The cmd should appear after the image in the DOCKER_CMD array
    assert "test-image:latest /entrypoint.sh" in script
