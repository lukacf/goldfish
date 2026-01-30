"""Tests for startup_builder - GCE startup script generation."""

import shlex

from goldfish.cloud.adapters.gcp.startup_builder import (
    build_startup_script,
    disk_mount_section,
    docker_run_section,
    gcsfuse_section,
    gpu_driver_section,
    log_syncer_section,
    metadata_syncer_section,
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


def test_build_startup_script_docker_pull_failure_diagnostics():
    """Docker pull failure diagnostics should be non-fatal and explicit."""
    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="",
        run_path="runs/test-run",
        image="test-image:latest",
        entrypoint="/bin/bash",
        env_map={},
    )

    # Must log a distinct failure stage for debugging
    assert 'log_stage "docker_pull_failed"' in script
    # Diagnostics must not exit the script prematurely under set -euo pipefail
    assert "set +e" in script
    assert "set -e" in script
    assert "docker info 2>&1 | head -20" in script
    # Fail fast after diagnostics so the run is marked failed
    assert "exit 1" in script


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


def test_metadata_syncer_section_contains_metadata_endpoint():
    """Metadata syncer should poll GCE metadata for goldfish signals."""
    script = metadata_syncer_section()

    assert "metadata.google.internal/computeMetadata/v1/instance/attributes/goldfish" in script
    assert "Metadata-Flavor: Google" in script


def test_metadata_syncer_section_sets_ack():
    """Metadata syncer should ack sync requests via instance metadata."""
    script = metadata_syncer_section()

    assert "goldfish_ack" in script
    assert "gcloud compute instances add-metadata" in script
    assert "sync_final_logs" in script
    assert "Failed to set goldfish_ack" in script
    assert "LAST_SEEN" in script
    assert '"$REQ_ID" != "$LAST_SEEN"' in script


def test_build_startup_script_starts_metadata_syncer():
    """Startup script should start metadata syncer in background."""
    script = build_startup_script(
        bucket="test-bucket",
        bucket_prefix="",
        run_path="runs/test",
        image="test-image",
        entrypoint="/bin/bash",
        env_map={},
    )

    assert "start_metadata_syncer" in script


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


# =============================================================================
# Log Syncer Tests - Real-time log streaming to GCS
# =============================================================================


class TestLogSyncer:
    """Tests for log_syncer_section - periodic log upload to GCS."""

    def test_log_syncer_section_generates_background_process(self):
        """Log syncer should create bash function that runs in background."""
        script = log_syncer_section(bucket="test-bucket", bucket_path="runs/test-run")

        # Should define start_log_syncer function
        assert "start_log_syncer()" in script or "start_log_syncer ()" in script

        # Should run in background with &
        assert "&" in script

        # Should track PID for cleanup
        assert "LOG_SYNCER_PID" in script

    def test_log_syncer_uses_correct_gcs_paths(self):
        """Log syncer should upload to correct gs://bucket/path/logs/ location."""
        script = log_syncer_section(bucket="my-bucket", bucket_path="runs/stage-abc123")

        # Should upload stdout and stderr to GCS
        assert "gs://my-bucket/runs/stage-abc123/logs/stdout.log" in script
        assert "gs://my-bucket/runs/stage-abc123/logs/stderr.log" in script

        # Should upload metrics.jsonl to GCS
        assert "gs://my-bucket/runs/stage-abc123/logs/metrics.jsonl" in script

        # Should use gcloud storage cp for uploads
        assert "gcloud storage cp" in script

    def test_log_syncer_respects_sync_interval(self):
        """Log syncer should sleep for configured interval between syncs."""
        script = log_syncer_section(bucket="test-bucket", bucket_path="runs/test", sync_interval=60)

        # Should have the configured sleep interval
        assert "sleep 60" in script or "LOG_SYNC_INTERVAL=60" in script

    def test_log_syncer_default_interval(self):
        """Log syncer should use 30 second default interval."""
        script = log_syncer_section(bucket="test-bucket", bucket_path="runs/test")

        # Default should be 30 seconds
        assert "30" in script

    def test_log_syncer_loops_while_docker_running(self):
        """Log syncer should continue until Docker process exits."""
        script = log_syncer_section(bucket="test-bucket", bucket_path="runs/test")

        # Should check if Docker is still running
        assert "DOCKER_PID" in script
        assert "kill -0" in script or "while" in script

    def test_log_syncer_final_sync_after_docker_exits(self):
        """Log syncer should do final sync after Docker exits."""
        script = log_syncer_section(bucket="test-bucket", bucket_path="runs/test")

        # Should have final upload after loop (uploads happen at least twice in script)
        # This ensures final logs are captured even if they came after last periodic sync
        stdout_count = script.count("stdout.log")
        stderr_count = script.count("stderr.log")

        # At least 2 occurrences: in loop + final sync
        assert stdout_count >= 2, "Should sync stdout both in loop and after"
        assert stderr_count >= 2, "Should sync stderr both in loop and after"

    def test_log_syncer_uses_local_tmp_paths(self):
        """Log syncer should read from local /tmp/ paths."""
        script = log_syncer_section(bucket="test-bucket", bucket_path="runs/test")

        # Should reference local temp paths
        assert "/tmp/stdout.log" in script or "LOCAL_STDOUT" in script
        assert "/tmp/stderr.log" in script or "LOCAL_STDERR" in script

    def test_log_syncer_ignores_upload_errors(self):
        """Log syncer should not fail if gsutil cp fails."""
        script = log_syncer_section(bucket="test-bucket", bucket_path="runs/test")

        # Should have || true or 2>/dev/null to ignore errors
        assert "|| true" in script or "2>/dev/null" in script


class TestBuildStartupScriptWithLogSyncer:
    """Tests for build_startup_script with log syncer integration."""

    def test_build_startup_script_uses_local_log_paths(self):
        """Startup script should write Docker output to /tmp/ not gcsfuse."""
        script = build_startup_script(
            bucket="test-bucket",
            bucket_prefix="",
            run_path="runs/test",
            image="test-image",
            entrypoint="/bin/bash",
            env_map={},
            log_sync_interval=30,
        )

        # Docker stdout/stderr should go to local /tmp/ paths
        assert 'STDOUT_LOG="/tmp/stdout.log"' in script or "LOCAL_STDOUT" in script
        # Should NOT write directly to gcsfuse mount for streaming
        # (gcsfuse mount is /mnt/gcs/... which doesn't work for real-time)

    def test_build_startup_script_starts_log_syncer(self):
        """Startup script should start log syncer after Docker begins."""
        script = build_startup_script(
            bucket="test-bucket",
            bucket_prefix="",
            run_path="runs/test",
            image="test-image",
            entrypoint="/bin/bash",
            env_map={},
            log_sync_interval=30,
        )

        # Should call start_log_syncer
        assert "start_log_syncer" in script

        # Log syncer should start AFTER Docker run begins
        # Find where DOCKER_PID is ASSIGNED (with $!), not just referenced
        docker_run_idx = script.find("DOCKER_PID=$!")

        # Find where start_log_syncer is CALLED (as a command, not in function definition)
        # The function definition has "start_log_syncer()" but the call has "start_log_syncer\n"
        import re

        calls = list(re.finditer(r"^start_log_syncer$", script, re.MULTILINE))
        assert len(calls) > 0, "start_log_syncer should be called"
        syncer_call_idx = calls[0].start()

        # Docker PID must be set before syncer starts (syncer needs it)
        assert docker_run_idx > 0, "DOCKER_PID should be set"
        assert docker_run_idx < syncer_call_idx, "DOCKER_PID must be set before syncer starts"

    def test_build_startup_script_custom_sync_interval(self):
        """Startup script should respect custom log_sync_interval."""
        script = build_startup_script(
            bucket="test-bucket",
            bucket_prefix="",
            run_path="runs/test",
            image="test-image",
            entrypoint="/bin/bash",
            env_map={},
            log_sync_interval=10,
        )

        # Should have the custom interval
        assert "10" in script
