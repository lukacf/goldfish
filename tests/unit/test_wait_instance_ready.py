"""Tests for wait_for_instance_ready function."""

from unittest.mock import MagicMock, patch

import pytest

from goldfish.cloud.adapters.gcp.resource_launcher import wait_for_instance_ready
from goldfish.errors import GoldfishError


class TestWaitForInstanceReady:
    """Tests for wait_for_instance_ready."""

    def test_instance_already_running_returns_immediately(self):
        """If instance is already RUNNING, return immediately."""
        with patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud") as mock_gcloud:
            mock_gcloud.return_value = MagicMock(returncode=0, stdout="RUNNING\n", stderr="")

            # Should return without error
            wait_for_instance_ready(
                instance_name="test-instance",
                zone="us-central1-a",
                timeout_sec=10,
                poll_interval=0.1,
            )

            # Should have called gcloud once
            assert mock_gcloud.call_count == 1

    def test_instance_provisioning_waits_until_running(self):
        """Wait while instance is PROVISIONING, return when RUNNING."""
        call_count = 0

        def mock_gcloud(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return MagicMock(returncode=0, stdout="PROVISIONING\n", stderr="")
            else:
                return MagicMock(returncode=0, stdout="RUNNING\n", stderr="")

        with patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud", side_effect=mock_gcloud):
            wait_for_instance_ready(
                instance_name="test-instance",
                zone="us-central1-a",
                timeout_sec=10,
                poll_interval=0.1,
            )

            assert call_count == 3

    def test_instance_staging_waits_until_running(self):
        """Wait while instance is STAGING, return when RUNNING."""
        statuses = ["STAGING", "STAGING", "RUNNING"]
        call_count = 0

        def mock_gcloud(*args, **kwargs):
            nonlocal call_count
            status = statuses[min(call_count, len(statuses) - 1)]
            call_count += 1
            return MagicMock(returncode=0, stdout=f"{status}\n", stderr="")

        with patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud", side_effect=mock_gcloud):
            wait_for_instance_ready(
                instance_name="test-instance",
                zone="us-central1-a",
                timeout_sec=10,
                poll_interval=0.1,
            )

            assert call_count == 3

    def test_instance_terminated_raises_error(self):
        """Raise error if instance reaches TERMINATED state."""
        with patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud") as mock_gcloud:
            mock_gcloud.return_value = MagicMock(returncode=0, stdout="TERMINATED\n", stderr="")

            with pytest.raises(GoldfishError, match="unexpected state: TERMINATED"):
                wait_for_instance_ready(
                    instance_name="test-instance",
                    zone="us-central1-a",
                    timeout_sec=10,
                    poll_interval=0.1,
                )

    def test_instance_stopped_raises_error(self):
        """Raise error if instance reaches STOPPED state."""
        with patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud") as mock_gcloud:
            mock_gcloud.return_value = MagicMock(returncode=0, stdout="STOPPED\n", stderr="")

            with pytest.raises(GoldfishError, match="unexpected state: STOPPED"):
                wait_for_instance_ready(
                    instance_name="test-instance",
                    zone="us-central1-a",
                    timeout_sec=10,
                    poll_interval=0.1,
                )

    def test_timeout_raises_error(self):
        """Raise error if instance doesn't reach RUNNING within timeout."""
        with patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud") as mock_gcloud:
            # Always return PROVISIONING
            mock_gcloud.return_value = MagicMock(returncode=0, stdout="PROVISIONING\n", stderr="")

            with pytest.raises(
                GoldfishError,
                match="did not reach RUNNING state within",
            ):
                wait_for_instance_ready(
                    instance_name="test-instance",
                    zone="us-central1-a",
                    timeout_sec=0.3,  # Very short timeout
                    poll_interval=0.1,
                )

    def test_api_error_keeps_waiting(self):
        """Continue waiting if API returns error (instance not yet visible)."""
        call_count = 0

        def mock_gcloud(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                # Simulate API error - instance not yet visible
                return MagicMock(
                    returncode=1,
                    stdout="",
                    stderr="ERROR: Instance not found",
                )
            else:
                return MagicMock(returncode=0, stdout="RUNNING\n", stderr="")

        with patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud", side_effect=mock_gcloud):
            wait_for_instance_ready(
                instance_name="test-instance",
                zone="us-central1-a",
                timeout_sec=10,
                poll_interval=0.1,
            )

            assert call_count == 3

    def test_project_id_passed_to_gcloud(self):
        """Project ID is passed to gcloud command."""
        with patch("goldfish.cloud.adapters.gcp.resource_launcher.run_gcloud") as mock_gcloud:
            mock_gcloud.return_value = MagicMock(returncode=0, stdout="RUNNING\n", stderr="")

            wait_for_instance_ready(
                instance_name="test-instance",
                zone="us-central1-a",
                project_id="my-project",
                timeout_sec=10,
                poll_interval=0.1,
            )

            # Check the command includes project
            call_args = mock_gcloud.call_args
            cmd = call_args[0][0]
            assert "--project=my-project" in cmd
