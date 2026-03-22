"""Tests for ResourceLauncher - capacity-aware GCE instance launching."""

from unittest.mock import MagicMock, Mock, patch

import pytest

from goldfish.cloud.adapters.gcp.resource_launcher import (
    CAPACITY_PATTERNS,
    CapacityError,
    ResourceLauncher,
    mode_order,
    order_resources,
)
from goldfish.errors import GoldfishError


def test_capacity_error_patterns():
    """Test that capacity error patterns match expected GCE error messages."""
    test_cases = [
        "zone_resource_pool_exhausted in us-central1-a",
        "does not have sufficient resources available",
        "quota exceeded for resource 'GPUs'",
        "was not able to fulfil your request",
        "resource is not available in this zone",
        "insufficient capacity in zone us-west1-b",
    ]

    for error_msg in test_cases:
        lowered = error_msg.lower()
        assert any(pattern in lowered for pattern in CAPACITY_PATTERNS), f"Pattern should match: {error_msg}"


def test_capacity_error_patterns_match_actual_gce_messages():
    """Test that capacity patterns match ACTUAL GCE error messages.

    GCE uses 'enough resources' not 'sufficient resources' in zone
    resource pool exhaustion errors. Missing this pattern causes spot
    launches to fail on the first zone without retrying others.
    """
    actual_gce_messages = [
        # ZONE_RESOURCE_POOL_EXHAUSTED — actual gcloud stderr output
        "The zone 'projects/my-project/zones/us-central1-a' does not have enough resources available to fulfill the request. Try a different zone, or try again later.",
        # Quota exceeded — standard format
        "Quota 'NVIDIA_H100_80GB_GPUS' exceeded. Limit: 0.0 in region us-central1.",
        "Quota 'PREEMPTIBLE_NVIDIA_H100_80GB_GPUS' exceeded. Limit: 8.0 in region us-central1.",
        # ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS
        "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS: The zone 'us-central1-a' does not have enough resources.",
        # Spot-specific stockout
        "The zone does not have enough resources available to fulfill the request.",
        # A3 machine type capacity
        "resource 'a3-megagpu-8g' is not available in zone 'us-central1-a'",
    ]

    for error_msg in actual_gce_messages:
        lowered = error_msg.lower()
        assert any(
            pattern in lowered for pattern in CAPACITY_PATTERNS
        ), f"CAPACITY_PATTERNS should match actual GCE message: {error_msg}"


def test_order_resources_by_gpu_preference():
    """Test that resources are ordered by GPU preference."""
    resources = [
        {"name": "cpu-only", "gpu": {}},
        {"name": "a100-1", "gpu": {"type": "a100"}},
        {"name": "h100-1", "gpu": {"type": "h100"}},
        {"name": "a100-2", "gpu": {"type": "a100"}},
    ]

    # Prefer h100, then a100, then none
    ordered = order_resources(resources, gpu_preference=["h100", "a100", "none"], force_gpu=None)

    assert ordered[0]["name"] == "h100-1"
    assert ordered[1]["name"] == "a100-1"
    assert ordered[2]["name"] == "a100-2"
    assert ordered[3]["name"] == "cpu-only"


def test_order_resources_force_gpu():
    """Test that force_gpu filters to only that GPU type."""
    resources = [
        {"name": "cpu-only", "gpu": {}},
        {"name": "a100-1", "gpu": {"type": "a100"}},
        {"name": "h100-1", "gpu": {"type": "h100"}},
    ]

    # Force only a100
    ordered = order_resources(resources, gpu_preference=[], force_gpu="a100")

    assert len(ordered) == 1
    assert ordered[0]["name"] == "a100-1"


def test_order_resources_force_gpu_missing():
    """Test that force_gpu raises error if GPU type not in catalog."""
    resources = [
        {"name": "a100-1", "gpu": {"type": "a100"}},
    ]

    with pytest.raises(GoldfishError, match="not present in resource catalog"):
        order_resources(resources, gpu_preference=[], force_gpu="v100")


def test_order_resources_force_gpu_by_accelerator():
    """REGRESSION: force_gpu should match against gpu.accelerator, not just gpu.type.

    Bug: gce_launcher passes the accelerator name (e.g., "nvidia-h100-80gb") as force_gpu,
    but order_resources() groups by gpu.type (e.g., "h100"). The mismatch caused
    "force_gpu=nvidia-h100-80gb not present in resource catalog" error.

    Fix: order_resources should check both gpu.type AND gpu.accelerator when matching.
    """
    resources = [
        {
            "name": "h100-spot",
            "gpu": {
                "type": "h100",
                "accelerator": "nvidia-h100-80gb",
                "count": 1,
            },
        },
        {
            "name": "a100-spot",
            "gpu": {
                "type": "a100",
                "accelerator": "nvidia-tesla-a100",
                "count": 1,
            },
        },
        {
            "name": "cpu-small",
            "gpu": {
                "type": "none",
                "accelerator": None,
                "count": 0,
            },
        },
    ]

    # Force using accelerator name (as passed by gce_launcher)
    ordered = order_resources(resources, gpu_preference=[], force_gpu="nvidia-h100-80gb")

    assert len(ordered) == 1
    assert ordered[0]["name"] == "h100-spot"


def test_order_resources_force_gpu_by_accelerator_a100():
    """Verify A100 filtering also works with accelerator name."""
    resources = [
        {
            "name": "h100-spot",
            "gpu": {
                "type": "h100",
                "accelerator": "nvidia-h100-80gb",
                "count": 1,
            },
        },
        {
            "name": "a100-spot",
            "gpu": {
                "type": "a100",
                "accelerator": "nvidia-tesla-a100",
                "count": 1,
            },
        },
    ]

    # Force using A100 accelerator name
    ordered = order_resources(resources, gpu_preference=[], force_gpu="nvidia-tesla-a100")

    assert len(ordered) == 1
    assert ordered[0]["name"] == "a100-spot"


def test_order_resources_force_gpu_still_works_with_short_type():
    """Ensure backward compatibility: force_gpu with short type still works."""
    resources = [
        {
            "name": "h100-spot",
            "gpu": {
                "type": "h100",
                "accelerator": "nvidia-h100-80gb",
                "count": 1,
            },
        },
        {
            "name": "a100-spot",
            "gpu": {
                "type": "a100",
                "accelerator": "nvidia-tesla-a100",
                "count": 1,
            },
        },
    ]

    # Force using short type name (backward compatibility)
    ordered = order_resources(resources, gpu_preference=[], force_gpu="h100")

    assert len(ordered) == 1
    assert ordered[0]["name"] == "h100-spot"


def test_mode_order_spot_first():
    """Test mode ordering prefers spot over on-demand."""
    resource = {
        "preemptible_allowed": True,
        "on_demand_allowed": True,
    }

    modes = mode_order(resource, preference="spot_first", force_mode=None)
    assert modes == ["spot", "on_demand"]


def test_mode_order_on_demand_first():
    """Test mode ordering prefers on-demand over spot."""
    resource = {
        "preemptible_allowed": True,
        "on_demand_allowed": True,
    }

    modes = mode_order(resource, preference="on_demand_first", force_mode=None)
    assert modes == ["on_demand", "spot"]


def test_mode_order_force_spot():
    """Test forcing spot mode excludes on-demand."""
    resource = {
        "preemptible_allowed": True,
        "on_demand_allowed": True,
    }

    modes = mode_order(resource, preference="spot_first", force_mode="spot")
    assert modes == ["spot"]


def test_mode_order_force_spot_not_allowed():
    """Test forcing spot mode returns empty if not allowed."""
    resource = {
        "preemptible_allowed": False,
        "on_demand_allowed": True,
    }

    modes = mode_order(resource, preference="spot_first", force_mode="spot")
    assert modes == []


def test_resource_launcher_init_empty_resources():
    """Test that ResourceLauncher raises error with empty resources."""
    with pytest.raises(GoldfishError, match="resources list is empty"):
        ResourceLauncher(resources=[])


def test_resource_launcher_init_filters_resources():
    """Test that ResourceLauncher correctly filters and orders resources."""
    resources = [
        {
            "name": "a100-resource",
            "machine_type": "a2-highgpu-1g",
            "gpu": {"type": "a100", "count": 1},
            "zones": ["us-central1-a"],
            "preemptible_allowed": True,
            "on_demand_allowed": True,
        },
        {
            "name": "cpu-resource",
            "machine_type": "n1-standard-4",
            "gpu": {},
            "zones": ["us-central1-a"],
            "preemptible_allowed": False,
            "on_demand_allowed": True,
        },
    ]

    launcher = ResourceLauncher(
        resources=resources,
        gpu_preference=["a100", "none"],
    )

    assert launcher.ordered_resources[0]["name"] == "a100-resource"
    assert launcher.ordered_resources[1]["name"] == "cpu-resource"


@patch("goldfish.cloud.adapters.gcp.resource_launcher.wait_for_instance_ready")
@patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud")
@patch("goldfish.cloud.adapters.gcp.resource_launcher.tempfile.NamedTemporaryFile")
def test_resource_launcher_launch_success(mock_tempfile, mock_run_gcloud, mock_wait):
    """Test successful launch with capacity search."""
    # Mock temp file for startup script
    mock_temp = MagicMock()
    mock_temp.name = "/tmp/startup.sh"
    mock_tempfile.return_value.__enter__.return_value = mock_temp

    # Mock successful gcloud commands
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="", stderr="")

    # Mock wait_for_instance_ready (called after instance create)
    mock_wait.return_value = None

    resources = [
        {
            "name": "test-resource",
            "machine_type": "n1-standard-4",
            "zones": ["us-central1-a"],
            "gpu": {},
            "preemptible_allowed": True,
            "on_demand_allowed": True,
            "boot_disk": {"size_gb": 100, "type": "pd-ssd"},
        },
    ]

    launcher = ResourceLauncher(
        resources=resources,
        gpu_preference=["none"],
    )

    result = launcher.launch(
        instance_name="test-instance",
        startup_script="#!/bin/bash\necho test",
    )

    assert result.instance_name == "test-instance"
    assert result.selection.resource == "test-resource"
    assert result.selection.zone == "us-central1-a"
    assert "instance_create_sec" in result.timings


@patch("goldfish.cloud.adapters.gcp.resource_launcher.wait_for_instance_ready")
@patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud")
@patch("goldfish.cloud.adapters.gcp.resource_launcher.tempfile.NamedTemporaryFile")
def test_resource_launcher_uses_profile_boot_image(mock_tempfile, mock_run_gcloud, mock_wait):
    """REGRESSION: Boot image must always be specified to ensure bash is available.

    Bug: When boot_disk didn't have image or image_family, the gcloud command
    would have no --image* argument. GCE would use the project default, which
    could be a minimal COS image without bash. Startup scripts use bash features
    (set -o pipefail) and would fail with "sh: Illegal option -o pipefail".

    Fix: All GCP profiles must specify image_family and image_project in boot_disk.
    """
    mock_temp = MagicMock()
    mock_temp.name = "/tmp/startup.sh"
    mock_tempfile.return_value.__enter__.return_value = mock_temp
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="", stderr="")
    mock_wait.return_value = None

    # Resource WITH image_family (as all profiles should have)
    resources = [
        {
            "name": "h100-spot",
            "machine_type": "a3-highgpu-1g",
            "zones": ["us-central1-a"],
            "gpu": {"type": "h100", "accelerator": "nvidia-h100-80gb", "count": 1},
            "preemptible_allowed": True,
            "on_demand_allowed": False,
            "boot_disk": {
                "size_gb": 600,
                "type": "hyperdisk-balanced",
                "image_family": "debian-12",
                "image_project": "debian-cloud",
            },
        },
    ]

    launcher = ResourceLauncher(
        resources=resources,
        gpu_preference=["h100"],
    )

    launcher.launch(
        instance_name="test-instance",
        startup_script="#!/bin/bash\nset -euxo pipefail\necho test",
    )

    # Verify gcloud was called with boot image from profile
    cmd = mock_run_gcloud.call_args[0][0]
    assert "--image-family=debian-12" in cmd, (
        "Boot image must be specified to ensure bash is available. "
        "Without it, GCE may use COS which lacks bash, causing startup script failure."
    )
    assert "--image-project=debian-cloud" in cmd


@patch("goldfish.cloud.adapters.gcp.resource_launcher.wait_for_instance_ready")
@patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud")
@patch("goldfish.cloud.adapters.gcp.resource_launcher.tempfile.NamedTemporaryFile")
def test_resource_launcher_respects_custom_boot_image(mock_tempfile, mock_run_gcloud, mock_wait):
    """Profiles can override the default boot image."""
    mock_temp = MagicMock()
    mock_temp.name = "/tmp/startup.sh"
    mock_tempfile.return_value.__enter__.return_value = mock_temp
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="", stderr="")
    mock_wait.return_value = None

    # Resource with custom image_family
    resources = [
        {
            "name": "custom-resource",
            "machine_type": "n1-standard-4",
            "zones": ["us-central1-a"],
            "gpu": {},
            "preemptible_allowed": True,
            "on_demand_allowed": True,
            "boot_disk": {
                "size_gb": 100,
                "type": "pd-ssd",
                "image_family": "ubuntu-2204-lts",
                "image_project": "ubuntu-os-cloud",
            },
        },
    ]

    launcher = ResourceLauncher(
        resources=resources,
        gpu_preference=["none"],
    )

    launcher.launch(
        instance_name="test-instance",
        startup_script="#!/bin/bash\necho test",
    )

    # Verify custom image is used
    cmd = mock_run_gcloud.call_args[0][0]
    assert "--image-family=ubuntu-2204-lts" in cmd
    assert "--image-project=ubuntu-os-cloud" in cmd


@patch("goldfish.cloud.adapters.gcp.resource_launcher.wait_for_instance_ready")
@patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud")
@patch("goldfish.cloud.adapters.gcp.resource_launcher.tempfile.NamedTemporaryFile")
def test_resource_launcher_launch_sets_service_account(mock_tempfile, mock_run_gcloud, mock_wait):
    """Should set service account on instance creation when configured."""
    mock_temp = MagicMock()
    mock_temp.name = "/tmp/startup.sh"
    mock_tempfile.return_value.__enter__.return_value = mock_temp
    mock_run_gcloud.return_value = Mock(returncode=0, stdout="", stderr="")

    # Mock wait_for_instance_ready (called after instance create)
    mock_wait.return_value = None

    resources = [
        {
            "name": "test-resource",
            "machine_type": "n1-standard-4",
            "zones": ["us-central1-a"],
            "gpu": {},
            "preemptible_allowed": True,
            "on_demand_allowed": True,
            "boot_disk": {"size_gb": 100, "type": "pd-ssd"},
        },
    ]

    launcher = ResourceLauncher(
        resources=resources,
        gpu_preference=["none"],
        service_account="svc@test.iam.gserviceaccount.com",
    )

    launcher.launch(
        instance_name="test-instance",
        startup_script="#!/bin/bash\necho test",
    )

    cmd = mock_run_gcloud.call_args[0][0]
    assert "--service-account=svc@test.iam.gserviceaccount.com" in cmd


@patch("goldfish.cloud.adapters.gcp.resource_launcher.wait_for_instance_ready")
@patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud")
@patch("goldfish.cloud.adapters.gcp.resource_launcher.tempfile.NamedTemporaryFile")
@patch("goldfish.cloud.adapters.gcp.resource_launcher.time.time")
def test_resource_launcher_retry_on_capacity_error(mock_time, mock_tempfile, mock_run_gcloud, mock_wait):
    """Test that launcher retries on capacity errors."""
    # Mock temp file
    mock_temp = MagicMock()
    mock_temp.name = "/tmp/startup.sh"
    mock_tempfile.return_value.__enter__.return_value = mock_temp

    # Mock time for backoff testing - need enough values for all time.time() calls
    # including the ones in _attempt_launch after successful create
    mock_time.side_effect = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # First call raises CapacityError, second succeeds
    mock_run_gcloud.side_effect = [
        CapacityError("zone_resource_pool_exhausted"),
        Mock(returncode=0, stdout="", stderr=""),
    ]

    # Mock wait_for_instance_ready (called after successful instance create)
    mock_wait.return_value = None

    resources = [
        {
            "name": "test-resource",
            "machine_type": "n1-standard-4",
            "zones": ["us-central1-a", "us-central1-b"],  # Two zones
            "gpu": {},
            "preemptible_allowed": True,
            "on_demand_allowed": False,
            "boot_disk": {"size_gb": 100, "type": "pd-ssd"},
        },
    ]

    launcher = ResourceLauncher(
        resources=resources,
        gpu_preference=["none"],
        initial_backoff_sec=1,
    )

    result = launcher.launch(
        instance_name="test-instance",
        startup_script="#!/bin/bash\necho test",
    )

    # Should succeed on second zone
    assert result.instance_name == "test-instance"
    assert len(result.attempt_log) == 2
    assert result.attempt_log[0]["status"] == "capacity"
    assert result.attempt_log[1]["status"] == "success"


@patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud")
@patch("goldfish.cloud.adapters.gcp.resource_launcher.tempfile.NamedTemporaryFile")
def test_resource_launcher_timeout_exceeded(mock_tempfile, mock_run_gcloud):
    """Test that launcher fails after timeout."""
    # Mock temp file
    mock_temp = MagicMock()
    mock_temp.name = "/tmp/startup.sh"
    mock_tempfile.return_value.__enter__.return_value = mock_temp

    # Always raise capacity error
    mock_run_gcloud.side_effect = CapacityError("zone_resource_pool_exhausted")

    resources = [
        {
            "name": "test-resource",
            "machine_type": "n1-standard-4",
            "zones": ["us-central1-a"],
            "gpu": {},
            "preemptible_allowed": True,
            "on_demand_allowed": False,
            "boot_disk": {"size_gb": 100, "type": "pd-ssd"},
        },
    ]

    launcher = ResourceLauncher(
        resources=resources,
        gpu_preference=["none"],
        search_timeout_sec=1,  # Very short timeout
    )

    with pytest.raises(GoldfishError, match="Failed to acquire capacity"):
        launcher.launch(
            instance_name="test-instance",
            startup_script="#!/bin/bash\necho test",
        )


def test_resource_launcher_zones_override():
    """Test that zones_override filters zones."""
    resources = [
        {
            "name": "multi-zone",
            "machine_type": "n1-standard-4",
            "zones": ["us-central1-a", "us-west1-a", "us-east1-a"],
            "gpu": {},
            "preemptible_allowed": True,
            "on_demand_allowed": True,
            "boot_disk": {"size_gb": 100, "type": "pd-ssd"},
        },
    ]

    launcher = ResourceLauncher(
        resources=resources,
        zones_override=["us-west1-a"],  # Only try this zone
    )

    # The launcher should only search in us-west1-a
    assert launcher.zone_filter == {"us-west1-a"}
