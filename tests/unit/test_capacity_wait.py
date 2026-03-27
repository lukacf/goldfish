"""Tests for capacity_wait_seconds feature — persistent GPU capacity search."""

from unittest.mock import MagicMock, patch

import pytest

from goldfish.cloud.adapters.gcp.resource_launcher import (
    CapacityError,
    ResourceLauncher,
)
from goldfish.cloud.contracts import RunSpec


def _make_resource(name: str = "h100-spot", zone: str = "us-central1-a") -> dict:
    return {
        "name": name,
        "machine_type": "a3-highgpu-1g",
        "gpu": {"type": "h100", "accelerator": "nvidia-h100-80gb", "count": 1},
        "preemptible_allowed": True,
        "on_demand_allowed": False,
        "zones": [zone],
        "boot_disk": {"type": "pd-ssd", "size_gb": 100, "image_family": "debian-12", "image_project": "debian-cloud"},
    }


class TestZoneCycling:
    """ResourceLauncher should cycle back to zone A after exhausting all zones."""

    @patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud")
    @patch("goldfish.cloud.adapters.gcp.resource_launcher.time")
    def test_retries_zones_in_loop(self, mock_time: MagicMock, mock_gcloud: MagicMock) -> None:
        """When all zones fail with capacity, the loop should cycle back and retry."""
        resource = _make_resource()
        resource["zones"] = ["zone-a", "zone-b"]

        # Track attempts by zone
        attempts_by_zone: dict[str, int] = {"zone-a": 0, "zone-b": 0}

        # Simulate: fail first pass, succeed on zone-a second pass
        def side_effect(*args: object, **kwargs: object) -> None:
            # Extract zone from command args
            cmd = args[0] if args else kwargs.get("cmd", [])
            zone = ""
            for item in cmd:
                if isinstance(item, str) and item.startswith("--zone="):
                    zone = item.split("=")[1]
            attempts_by_zone[zone] = attempts_by_zone.get(zone, 0) + 1
            if zone == "zone-a" and attempts_by_zone["zone-a"] >= 2:
                return  # Success on second try
            raise CapacityError("zone_resource_pool_exhausted")

        mock_gcloud.side_effect = side_effect

        # Time: never expires (large deadline)
        call_count = 0

        def time_time() -> float:
            nonlocal call_count
            call_count += 1
            return 1000.0  # Always before deadline

        mock_time.time = time_time
        mock_time.sleep = MagicMock()

        launcher = ResourceLauncher(
            resources=[resource],
            search_timeout_sec=3600,
            max_attempts=100,
            initial_backoff_sec=1,
            force_preemptible="spot",
        )

        result = launcher.launch(
            instance_name="test-instance",
            startup_script="#!/bin/bash\necho hello",
        )

        assert result.selection.zone == "zone-a"
        # zone-a was tried twice (first pass fail, second pass success)
        assert attempts_by_zone["zone-a"] == 2
        # zone-b was tried once (first pass fail)
        assert attempts_by_zone["zone-b"] == 1

    @patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud")
    @patch("goldfish.cloud.adapters.gcp.resource_launcher.time")
    def test_stops_at_deadline(self, mock_time: MagicMock, mock_gcloud: MagicMock) -> None:
        """Search should stop when deadline is reached even if max_attempts not hit."""
        resource = _make_resource()
        mock_gcloud.side_effect = CapacityError("zone_resource_pool_exhausted")

        # Time advances past deadline after 3 calls
        times = iter([100.0, 100.0, 100.0, 100.0, 200.0, 200.0, 200.0, 999.0, 999.0, 999.0])
        mock_time.time = lambda: next(times, 999.0)
        mock_time.sleep = MagicMock()

        launcher = ResourceLauncher(
            resources=[resource],
            search_timeout_sec=5,  # deadline = 105
            max_attempts=1000,
            initial_backoff_sec=0.01,
        )

        with pytest.raises(Exception, match="Failed to acquire capacity"):
            launcher.launch(instance_name="test", startup_script="#!/bin/bash")

    @patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud")
    @patch("goldfish.cloud.adapters.gcp.resource_launcher.time")
    def test_no_valid_modes_breaks_immediately(self, mock_time: MagicMock, mock_gcloud: MagicMock) -> None:
        """If no resource has valid modes, don't spin forever."""
        resource = _make_resource()
        resource["preemptible_allowed"] = False  # No spot allowed

        mock_time.time = lambda: 100.0
        mock_time.sleep = MagicMock()

        launcher = ResourceLauncher(
            resources=[resource],
            search_timeout_sec=3600,
            max_attempts=1000,
            force_preemptible="spot",  # Force spot but resource doesn't allow it
        )

        with pytest.raises(Exception, match="Failed to acquire capacity"):
            launcher.launch(instance_name="test", startup_script="#!/bin/bash")

        # Should not have called gcloud at all
        mock_gcloud.assert_not_called()


class TestCapacityWaitInRunSpec:
    """capacity_wait_seconds should be part of RunSpec."""

    def test_default_is_none(self) -> None:
        """RunSpec.capacity_wait_seconds defaults to None."""
        spec = RunSpec(stage_run_id="stage-1", workspace_name="w1", stage_name="train", image="img:v1")
        assert spec.capacity_wait_seconds is None

    def test_can_set_value(self) -> None:
        """RunSpec.capacity_wait_seconds can be set to a value."""
        spec = RunSpec(
            stage_run_id="stage-1",
            workspace_name="w1",
            stage_name="train",
            image="img:v1",
            capacity_wait_seconds=3600,
        )
        assert spec.capacity_wait_seconds == 3600


class TestDefaultsConfig:
    """capacity_wait_seconds should be in DefaultsConfig."""

    def test_default_value(self) -> None:
        """DefaultsConfig.capacity_wait_seconds defaults to 600."""
        from goldfish.config import DefaultsConfig

        config = DefaultsConfig()
        assert config.capacity_wait_seconds == 600

    def test_custom_value(self) -> None:
        """DefaultsConfig.capacity_wait_seconds can be overridden."""
        from goldfish.config import DefaultsConfig

        config = DefaultsConfig(capacity_wait_seconds=3600, launch_timeout_seconds=3600)
        assert config.capacity_wait_seconds == 3600
