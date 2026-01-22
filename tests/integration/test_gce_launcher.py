"""Tests for GCELauncher - GCE instance management."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from goldfish.errors import GoldfishError
from goldfish.infra.gce_launcher import GCELauncher


@pytest.fixture
def launcher():
    """Create GCELauncher instance for testing."""
    return GCELauncher(
        project_id="test-project",
        zone="us-central1-a",
        bucket="gs://test-bucket",
        service_account="svc@test.iam.gserviceaccount.com",
        resources=[
            {
                "name": "cpu-resource",
                "machine_type": "n1-standard-4",
                "zones": ["us-central1-a"],
                "gpu": {},
                "preemptible_allowed": True,
                "on_demand_allowed": True,
                "boot_disk": {"size_gb": 100, "type": "pd-ssd"},
            },
            {
                "name": "gpu-resource",
                "machine_type": "a2-highgpu-1g",
                "zones": ["us-central1-a"],
                "gpu": {"type": "a100", "count": 1},
                "preemptible_allowed": True,
                "on_demand_allowed": True,
                "boot_disk": {"size_gb": 100, "type": "pd-ssd"},
            },
        ],
    )


def test_gce_launcher_init():
    """Test GCELauncher initialization."""
    launcher = GCELauncher(
        project_id="test-project",
        zone="us-west1-a",
        bucket="gs://my-bucket",
        service_account="svc@test.iam.gserviceaccount.com",
    )

    assert launcher.project_id == "test-project"
    assert launcher.default_zone == "us-west1-a"
    assert launcher.bucket == "gs://my-bucket"
    assert launcher.resources == []
    assert launcher.service_account == "svc@test.iam.gserviceaccount.com"


@patch("goldfish.infra.gce_launcher.run_gcloud")
def test_resolve_service_account_default(mock_run_gcloud):
    """Should derive default compute service account when not provided."""
    launcher = GCELauncher(project_id="test-project")
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="123456789012\n", stderr="")

    assert launcher._resolve_service_account() == "123456789012-compute@developer.gserviceaccount.com"

    cmd = mock_run_gcloud.call_args[0][0]
    assert cmd[:3] == ["gcloud", "projects", "describe"]
    assert "test-project" in cmd


def test_gce_launcher_requires_bucket_for_launch(launcher):
    """Test that launch_instance raises error without bucket."""
    launcher_no_bucket = GCELauncher(project_id="test-project")

    with pytest.raises(GoldfishError, match="GCS bucket required"):
        launcher_no_bucket.launch_instance(
            image_tag="test-image",
            stage_run_id="test-run",
            entrypoint_script="#!/bin/bash\necho test",
            stage_config={},
            work_dir=Path("/tmp/work"),
        )


@patch("goldfish.infra.gce_launcher.ResourceLauncher")
@patch("goldfish.infra.gce_launcher.build_startup_script")
def test_launch_with_capacity_search(mock_build_startup, mock_resource_launcher_class, launcher):
    """Test launch_instance with capacity search."""
    # Mock startup script builder
    mock_build_startup.return_value = "#!/bin/bash\necho startup"

    # Mock ResourceLauncher
    mock_launcher = MagicMock()
    mock_result = Mock(instance_name="test-instance")
    mock_launcher.launch.return_value = mock_result
    mock_resource_launcher_class.return_value = mock_launcher

    # Launch with capacity search
    result = launcher.launch_instance(
        image_tag="test-image:latest",
        stage_run_id="test-run-123",
        entrypoint_script="#!/bin/bash\necho test",
        stage_config={"inputs": {}, "outputs": {}},
        work_dir=Path("/tmp/work"),
        use_capacity_search=True,
    )

    # Result is now GCELaunchResult with instance_name and zone
    assert result.instance_name == "test-instance"

    # Verify startup script was built with correct params
    mock_build_startup.assert_called_once()
    call_kwargs = mock_build_startup.call_args[1]
    assert call_kwargs["bucket"] == "test-bucket"
    assert call_kwargs["run_path"] == "runs/test-run-123"
    assert call_kwargs["image"] == "test-image:latest"

    # Verify ResourceLauncher was created with service account
    mock_resource_launcher_class.assert_called_once()
    _, rl_kwargs = mock_resource_launcher_class.call_args
    assert rl_kwargs["service_account"] == "svc@test.iam.gserviceaccount.com"


@patch("goldfish.infra.gce_launcher.ResourceLauncher")
@patch("goldfish.infra.gce_launcher.build_startup_script")
def test_launch_with_goldfish_env_vars(mock_build_startup, mock_resource_launcher_class, launcher):
    """Test launch_instance passes Goldfish environment variables."""
    # Mock startup script builder
    mock_build_startup.return_value = "#!/bin/bash\necho startup"

    # Mock ResourceLauncher
    mock_launcher = MagicMock()
    mock_result = Mock(instance_name="test-instance")
    mock_launcher.launch.return_value = mock_result
    mock_resource_launcher_class.return_value = mock_launcher

    # Goldfish environment variables for metrics and provenance
    goldfish_env = {
        "GOLDFISH_PROJECT_NAME": "my-ml-project",
        "GOLDFISH_WORKSPACE": "baseline_lstm",
        "GOLDFISH_STAGE": "train",
        "GOLDFISH_RUN_ID": "stage-abc123",
        "GOLDFISH_GIT_SHA": "abc123def456",
        "GOLDFISH_OUTPUTS_DIR": "/mnt/outputs",
        "GOLDFISH_METRICS_BACKEND": "wandb",
        "GOLDFISH_WANDB_PROJECT": "my-wandb-project",
        "GOLDFISH_WANDB_GROUP": "baseline_lstm",
        "GOLDFISH_WANDB_ENTITY": "my-team",
        "WANDB_API_KEY": "fake-wandb-key",
    }

    # Launch with goldfish_env
    result = launcher.launch_instance(
        image_tag="test-image:latest",
        stage_run_id="test-run-123",
        entrypoint_script="#!/bin/bash\necho test",
        stage_config={"inputs": {}, "outputs": {}},
        work_dir=Path("/tmp/work"),
        goldfish_env=goldfish_env,
        use_capacity_search=True,
    )

    # Result is now GCELaunchResult with instance_name and zone
    assert result.instance_name == "test-instance"

    # Verify env_map includes goldfish_env variables
    mock_build_startup.assert_called_once()
    call_kwargs = mock_build_startup.call_args[1]
    env_map = call_kwargs["env_map"]

    # Verify all Goldfish env vars are in env_map
    assert env_map["GOLDFISH_PROJECT_NAME"] == "my-ml-project"
    assert env_map["GOLDFISH_WORKSPACE"] == "baseline_lstm"
    assert env_map["GOLDFISH_STAGE"] == "train"
    assert env_map["GOLDFISH_RUN_ID"] == "stage-abc123"  # Note: may be overridden
    assert env_map["GOLDFISH_GIT_SHA"] == "abc123def456"
    assert env_map["GOLDFISH_OUTPUTS_DIR"] == "/mnt/outputs"
    assert env_map["GOLDFISH_METRICS_BACKEND"] == "wandb"
    assert env_map["GOLDFISH_WANDB_PROJECT"] == "my-wandb-project"
    assert env_map["GOLDFISH_WANDB_GROUP"] == "baseline_lstm"
    assert env_map["GOLDFISH_WANDB_ENTITY"] == "my-team"
    assert env_map["WANDB_API_KEY"] == "fake-wandb-key"
    assert call_kwargs["gcsfuse"] is True

    # Verify ResourceLauncher was created and used
    mock_resource_launcher_class.assert_called_once()
    _, call_kwargs = mock_resource_launcher_class.call_args
    assert call_kwargs["service_account"] == "svc@test.iam.gserviceaccount.com"
    mock_launcher.launch.assert_called_once()


@patch("goldfish.infra.resource_launcher.wait_for_instance_ready")
@patch("tempfile.NamedTemporaryFile")
@patch("goldfish.infra.gce_launcher.run_gcloud")
@patch("goldfish.infra.gce_launcher.build_startup_script")
def test_launch_simple_no_gpu(mock_build_startup, mock_run_gcloud, mock_tempfile, mock_wait, launcher):
    """Test simple launch without GPU."""
    # Mock temp file
    mock_temp = MagicMock()
    mock_temp.name = "/tmp/startup.sh"
    mock_tempfile.return_value.__enter__.return_value = mock_temp

    # Mock startup script
    mock_build_startup.return_value = "#!/bin/bash\necho startup"

    # Mock gcloud success
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="", stderr="")

    # Mock wait_for_instance_ready (called after instance create)
    mock_wait.return_value = None

    # Launch without capacity search (simple mode)
    result = launcher.launch_instance(
        image_tag="test-image",
        stage_run_id="test-run",
        entrypoint_script="#!/bin/bash\necho test",
        stage_config={},
        work_dir=Path("/tmp/work"),
        machine_type="n1-standard-8",
        use_capacity_search=False,
    )

    # Result is now GCELaunchResult with instance_name and zone
    assert result.instance_name == "test-run"
    assert result.zone == "us-central1-a"

    # Verify gcloud create was called without GPU flags
    mock_run_gcloud.assert_called_once()
    gcloud_cmd = mock_run_gcloud.call_args[0][0]
    assert "gcloud" in gcloud_cmd
    assert "compute" in gcloud_cmd
    assert "instances" in gcloud_cmd
    assert "create" in gcloud_cmd
    assert "test-run" in gcloud_cmd
    assert "--machine-type=n1-standard-8" in gcloud_cmd
    assert "--project=test-project" in gcloud_cmd
    assert "--service-account=svc@test.iam.gserviceaccount.com" in gcloud_cmd

    # GPU flags should NOT be present
    assert not any("--accelerator" in str(arg) for arg in gcloud_cmd)


@patch("goldfish.infra.resource_launcher.wait_for_instance_ready")
@patch("tempfile.NamedTemporaryFile")
@patch("goldfish.infra.gce_launcher.run_gcloud")
@patch("goldfish.infra.gce_launcher.build_startup_script")
def test_launch_simple_with_gpu(mock_build_startup, mock_run_gcloud, mock_tempfile, mock_wait, launcher):
    """Test simple launch with GPU."""
    # Mock temp file
    mock_temp = MagicMock()
    mock_temp.name = "/tmp/startup.sh"
    mock_tempfile.return_value.__enter__.return_value = mock_temp

    # Mock startup script
    mock_build_startup.return_value = "#!/bin/bash\necho startup"

    # Mock gcloud success
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="", stderr="")

    # Mock wait_for_instance_ready (called after instance create)
    mock_wait.return_value = None

    # Launch with GPU
    result = launcher.launch_instance(
        image_tag="test-image",
        stage_run_id="gpu-run",
        entrypoint_script="#!/bin/bash\necho test",
        stage_config={},
        work_dir=Path("/tmp/work"),
        machine_type="a2-highgpu-1g",
        gpu_type="nvidia-tesla-a100",
        gpu_count=1,
        use_capacity_search=False,
    )

    # Result is now GCELaunchResult with instance_name and zone
    assert result.instance_name == "gpu-run"
    assert result.zone == "us-central1-a"

    # Verify GPU flags are present
    mock_run_gcloud.assert_called_once()
    gcloud_cmd = mock_run_gcloud.call_args[0][0]
    assert "--accelerator" in gcloud_cmd
    assert "count=1,type=nvidia-tesla-a100" in "".join(gcloud_cmd)
    assert "--maintenance-policy=TERMINATE" in gcloud_cmd
    assert "--metadata=install-nvidia-driver=True" in gcloud_cmd


@patch("goldfish.infra.gce_launcher.run_gcloud")
def test_create_disk_basic(mock_run_gcloud, launcher):
    """Test basic disk creation."""
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="", stderr="")

    launcher.create_disk(disk_name="test-disk", zone="us-central1-a", size_gb=200, disk_type="pd-ssd")

    mock_run_gcloud.assert_called_once()
    cmd = mock_run_gcloud.call_args[0][0]
    assert "gcloud" in cmd
    assert "compute" in cmd
    assert "disks" in cmd
    assert "create" in cmd
    assert "test-disk" in cmd
    assert "--zone=us-central1-a" in cmd
    assert "--type=pd-ssd" in cmd
    assert "--size=200GB" in cmd
    assert "--project=test-project" in cmd


@patch("goldfish.infra.gce_launcher.run_gcloud")
def test_create_disk_with_snapshot(mock_run_gcloud, launcher):
    """Test disk creation from snapshot."""
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="", stderr="")

    launcher.create_disk(
        disk_name="restored-disk",
        zone="us-central1-a",
        size_gb=100,
        disk_type="pd-balanced",
        snapshot="my-snapshot",
    )

    cmd = mock_run_gcloud.call_args[0][0]
    assert "--source-snapshot=my-snapshot" in cmd


@patch("goldfish.infra.gce_launcher.run_gcloud")
def test_create_hyperdisk(mock_run_gcloud, launcher):
    """Test hyperdisk creation with IOPS/throughput."""
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="", stderr="")

    launcher.create_disk(
        disk_name="fast-disk",
        zone="us-central1-a",
        size_gb=500,
        disk_type="hyperdisk-balanced",
    )

    cmd = mock_run_gcloud.call_args[0][0]
    assert "--type=hyperdisk-balanced" in cmd
    assert "--provisioned-iops=80000" in cmd
    assert "--provisioned-throughput=2400" in cmd


@patch("goldfish.infra.gce_launcher.cleanup_disk")
def test_delete_disk(mock_cleanup, launcher):
    """Test disk deletion."""
    launcher.delete_disk("test-disk", "us-central1-a")

    mock_cleanup.assert_called_once_with("test-disk", "us-central1-a")


@patch("goldfish.infra.gce_launcher.run_gcloud")
def test_snapshot_disk(mock_run_gcloud, launcher):
    """Test disk snapshot creation."""
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="", stderr="")

    launcher.snapshot_disk(disk_name="my-disk", snapshot_name="my-snapshot", zone="us-central1-a")

    cmd = mock_run_gcloud.call_args[0][0]
    assert "gcloud" in cmd
    assert "compute" in cmd
    assert "disks" in cmd
    assert "snapshot" in cmd
    assert "my-disk" in cmd
    assert "--snapshot-names=my-snapshot" in cmd
    assert "--zone=us-central1-a" in cmd


@patch("goldfish.infra.gce_launcher.subprocess.run")
def test_sync_to_gcs_success(mock_run, launcher, tmp_path):
    """Test successful sync to GCS."""
    mock_run.return_value = Mock(returncode=0, stdout="", stderr="")

    local_dir = tmp_path / "data"
    local_dir.mkdir()

    launcher.sync_to_gcs(local_dir, "gs://test-bucket/path")

    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert cmd[0] == "gsutil"
    assert "-m" in cmd
    assert "rsync" in cmd
    assert "-r" in cmd
    assert str(local_dir) in cmd
    assert "gs://test-bucket/path" in cmd


@patch("goldfish.infra.gce_launcher.subprocess.run")
def test_sync_to_gcs_failure(mock_run, launcher, tmp_path):
    """Test failed sync to GCS."""
    mock_run.return_value = Mock(returncode=1, stdout="", stderr="Permission denied")

    local_dir = tmp_path / "data"
    local_dir.mkdir()

    with pytest.raises(GoldfishError, match="GCS sync failed"):
        launcher.sync_to_gcs(local_dir, "gs://test-bucket/path")


@patch("goldfish.infra.gce_launcher.subprocess.run")
def test_sync_from_gcs_success(mock_run, launcher, tmp_path):
    """Test successful sync from GCS."""
    mock_run.return_value = Mock(returncode=0, stdout="", stderr="")

    local_dir = tmp_path / "output"

    launcher.sync_from_gcs("gs://test-bucket/results", local_dir)

    # Directory should be created
    assert local_dir.exists()

    # gsutil command should be called
    cmd = mock_run.call_args[0][0]
    assert cmd[0] == "gsutil"
    assert "rsync" in cmd
    assert "gs://test-bucket/results" in cmd
    assert str(local_dir) in cmd


@patch("goldfish.infra.gce_launcher.subprocess.run")
def test_sync_from_gcs_failure(mock_run, launcher, tmp_path):
    """Test failed sync from GCS."""
    mock_run.return_value = Mock(returncode=1, stdout="", stderr="Not found")

    with pytest.raises(GoldfishError, match="GCS sync failed"):
        launcher.sync_from_gcs("gs://test-bucket/missing", tmp_path / "output")


@patch("goldfish.infra.gce_launcher.run_gcloud")
def test_get_instance_status_running(mock_run_gcloud, launcher):
    """Test get_instance_status for running instance."""
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="RUNNING\n", stderr="")

    status = launcher.get_instance_status("test-instance")

    assert status == "running"


@patch("goldfish.infra.gce_launcher.run_gcloud")
@patch("goldfish.infra.gce_launcher.get_exit_code_gce")
def test_get_instance_status_completed(mock_get_exit_code, mock_run_gcloud, launcher):
    """Test get_instance_status for completed instance."""
    from goldfish.state_machine.exit_code import ExitCodeResult

    # gcloud describe returns TERMINATED
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="TERMINATED\n", stderr="")

    # Exit code is 0 (success)
    mock_get_exit_code.return_value = ExitCodeResult.from_code(0)

    status = launcher.get_instance_status("test-instance")

    assert status == "completed"


@patch("goldfish.infra.gce_launcher.run_gcloud")
@patch("goldfish.infra.gce_launcher.get_exit_code_gce")
def test_get_instance_status_failed(mock_get_exit_code, mock_run_gcloud, launcher):
    """Test get_instance_status for failed instance."""
    from goldfish.state_machine.exit_code import ExitCodeResult

    # gcloud describe returns TERMINATED
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="TERMINATED\n", stderr="")

    # Exit code is 1 (failure)
    mock_get_exit_code.return_value = ExitCodeResult.from_code(1)

    status = launcher.get_instance_status("test-instance")

    assert status == "failed"


@patch("goldfish.infra.gce_launcher.run_gcloud")
def test_get_instance_status_not_found(mock_run_gcloud, launcher):
    """Test get_instance_status for non-existent instance."""
    mock_run_gcloud.return_value = Mock(returncode=1, stdout="", stderr="Instance not found")

    status = launcher.get_instance_status("nonexistent")

    assert status == "not_found"


@patch("goldfish.infra.gce_launcher.subprocess.Popen")
def test_get_instance_logs_from_gcs(mock_popen, launcher):
    """Test log retrieval from GCS."""
    import io

    # Mock for stdout fetch
    proc_stdout = Mock()
    proc_stdout.stdout = io.StringIO("Training started\nEpoch 1 complete\n")
    proc_stdout.stderr = io.StringIO("")
    proc_stdout.wait = Mock(return_value=0)
    proc_stdout.returncode = 0

    # Mock for stderr fetch
    proc_stderr = Mock()
    proc_stderr.stdout = io.StringIO("")
    proc_stderr.wait = Mock(return_value=0)
    proc_stderr.returncode = 0

    mock_popen.side_effect = [proc_stdout, proc_stderr]

    logs = launcher.get_instance_logs("test-instance")

    assert "Training started" in logs
    assert "Epoch 1 complete" in logs

    # Verify gcloud storage cat was called for stdout (new format)
    # Check the first call (stdout), there may be a second call for stderr
    first_call = mock_popen.call_args_list[0]
    cmd = first_call[0][0]
    assert cmd[0] == "gcloud"
    assert cmd[1] == "storage"
    assert cmd[2] == "cat"
    assert cmd[3] == "gs://test-bucket/runs/test-instance/logs/stdout.log"


@patch("goldfish.infra.gce_launcher.subprocess.Popen")
@patch("goldfish.infra.gce_launcher.run_gcloud")
def test_get_instance_logs_fallback_serial(mock_run_gcloud, mock_popen, launcher):
    """Test log retrieval falls back to serial console."""
    # GCS fetch fails
    mock_popen.side_effect = Exception("not found")

    # Serial console succeeds
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="Serial console output\n", stderr="")

    logs = launcher.get_instance_logs("test-instance")

    assert "Serial console output" in logs

    # Verify get-serial-port-output was called
    cmd = mock_run_gcloud.call_args[0][0]
    assert "get-serial-port-output" in cmd


@patch("goldfish.infra.gce_launcher.run_gcloud")
def test_stop_instance(mock_run_gcloud, launcher):
    """Test instance stop."""
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="", stderr="")

    launcher.stop_instance("test-instance")

    cmd = mock_run_gcloud.call_args[0][0]
    assert "gcloud" in cmd
    assert "compute" in cmd
    assert "instances" in cmd
    assert "stop" in cmd
    assert "test-instance" in cmd
    assert "--zone=us-central1-a" in cmd


@patch("goldfish.infra.gce_launcher.run_gcloud")
def test_delete_instance(mock_run_gcloud, launcher):
    """Test instance deletion."""
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="", stderr="")

    launcher.delete_instance("test-instance")

    cmd = mock_run_gcloud.call_args[0][0]
    assert "gcloud" in cmd
    assert "compute" in cmd
    assert "instances" in cmd
    assert "delete" in cmd
    assert "test-instance" in cmd


@patch("goldfish.infra.gce_launcher.run_gcloud")
@patch("goldfish.infra.gce_launcher.get_exit_code_gce")
@patch("goldfish.infra.gce_launcher.time.time")
@patch("goldfish.infra.gce_launcher.time.sleep")
def test_wait_for_termination_success(mock_sleep, mock_time, mock_get_exit_code, mock_run_gcloud, launcher):
    """Test wait_for_termination succeeds."""
    from goldfish.state_machine.exit_code import ExitCodeResult

    # Mock time progression
    mock_time.side_effect = [0, 5, 10, 15]

    # First two calls: running, third call: completed
    mock_run_gcloud.side_effect = [
        Mock(returncode=0, stdout="RUNNING\n", stderr=""),
        Mock(returncode=0, stdout="RUNNING\n", stderr=""),
        Mock(returncode=0, stdout="TERMINATED\n", stderr=""),
    ]

    # Exit code is 0 (success)
    mock_get_exit_code.return_value = ExitCodeResult.from_code(0)

    status = launcher.wait_for_termination("test-instance", timeout_sec=3600)

    assert status == "completed"
    assert mock_sleep.call_count == 2  # Slept twice before completion


@patch("goldfish.infra.gce_launcher.run_gcloud")
@patch("goldfish.infra.gce_launcher.time.sleep")
@patch("goldfish.infra.gce_launcher.time.time")
def test_wait_for_termination_timeout(mock_time, mock_sleep, mock_run_gcloud, launcher):
    """Test wait_for_termination timeout."""
    # Mock time progression that exceeds timeout
    mock_time.side_effect = [0, 10, 20, 30, 40, 50, 60]

    # Always return RUNNING
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="RUNNING\n", stderr="")

    with pytest.raises(GoldfishError, match="did not terminate within 50s"):
        launcher.wait_for_termination("test-instance", timeout_sec=50)


def test_sanitize_name():
    """Test name sanitization for GCE."""
    # Underscores to hyphens
    assert GCELauncher._sanitize_name("test_run_123") == "test-run-123"

    # Uppercase to lowercase
    assert GCELauncher._sanitize_name("TestRun") == "testrun"

    # Special characters removed
    assert GCELauncher._sanitize_name("test@run#123") == "test-run-123"

    # Truncation to 60 chars
    long_name = "a" * 100
    assert len(GCELauncher._sanitize_name(long_name)) == 60

    # Mixed transformations
    assert GCELauncher._sanitize_name("Test_Run@2024") == "test-run-2024"


@patch("goldfish.infra.gce_launcher.ResourceLauncher")
@patch("goldfish.infra.gce_launcher.build_startup_script")
def test_launch_filters_gpu_resources(mock_build_startup, mock_resource_launcher_class, launcher):
    """Test that GPU filtering works when selecting resources."""
    mock_build_startup.return_value = "#!/bin/bash\necho startup"

    # Mock ResourceLauncher
    mock_launcher = MagicMock()
    mock_result = Mock(instance_name="gpu-instance")
    mock_launcher.launch.return_value = mock_result
    mock_resource_launcher_class.return_value = mock_launcher

    # Launch with GPU type
    launcher.launch_instance(
        image_tag="test-image",
        stage_run_id="gpu-run",
        entrypoint_script="#!/bin/bash\necho test",
        stage_config={},
        work_dir=Path("/tmp/work"),
        gpu_type="a100",
        use_capacity_search=True,
    )

    # Verify ResourceLauncher was created with filtered resources
    call_args = mock_resource_launcher_class.call_args[1]
    resources = call_args["resources"]

    # Should only have GPU resource
    assert len(resources) == 1
    assert resources[0]["name"] == "gpu-resource"
    assert resources[0]["gpu"]["type"] == "a100"


# =============================================================================
# Regression Tests - Container permission fixes
# =============================================================================


@patch("goldfish.infra.gce_launcher.ResourceLauncher")
@patch("goldfish.infra.gce_launcher.build_startup_script")
def test_launch_instance_chown_for_container_user(mock_build_startup, mock_resource_launcher_class, launcher):
    """Regression: pre_run_cmds must chown /mnt/inputs and /mnt/outputs for container user.

    Docker containers (like pytorch-notebook) run as non-root user (jovyan, UID 1000).
    The startup script runs as root, so we need to chown the input/output directories
    before Docker starts, otherwise the container gets PermissionError.
    """
    mock_build_startup.return_value = "#!/bin/bash\necho startup"

    # Mock ResourceLauncher
    mock_launcher = MagicMock()
    mock_result = Mock(instance_name="test-instance")
    mock_launcher.launch.return_value = mock_result
    mock_resource_launcher_class.return_value = mock_launcher

    # Launch instance
    launcher.launch_instance(
        image_tag="test-image:latest",
        stage_run_id="perm-test",
        entrypoint_script="#!/bin/bash\necho test",
        stage_config={"inputs": {}, "outputs": {}},
        work_dir=Path("/tmp/work"),
        use_capacity_search=True,
    )

    # Verify build_startup_script was called with pre_run_cmds containing chown
    call_kwargs = mock_build_startup.call_args[1]
    pre_run_cmds = call_kwargs.get("pre_run_cmds", [])

    # Must have chown commands for UID 1000 (jovyan) and GID 100 (users)
    pre_run_str = "\n".join(pre_run_cmds)
    assert (
        "chown 1000:100 /mnt/inputs /mnt/outputs" in pre_run_str
    ), "Missing chown command for container user (UID 1000, GID 100)"

    # Must also chown symlinks inside /mnt/inputs (using -h flag)
    assert "chown -h 1000:100 /mnt/inputs/*" in pre_run_str, "Missing chown -h for symlinks inside /mnt/inputs"
