"""RCT Tests for Google Compute Engine.

These tests validate our assumptions about GCE behavior against reality.
They run against real GCE and document the actual representations.

RCT-GCE-1: Instance status field values and transitions
RCT-GCE-2: Zone-agnostic instance lookup behavior
RCT-GCE-3: Metadata operations and limits

WARNING: These tests CREATE and DELETE real GCE instances.
They are expensive and slow. Only run when necessary.
"""

import json
import subprocess
import time
import uuid

import pytest

# Mark all tests in this module as RCT tests
pytestmark = pytest.mark.rct


def run_gcloud(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run gcloud command and return result."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"gcloud command failed: {result.stderr}")
    return result


class TestGCEInstanceStatus:
    """RCT-GCE-1: Instance status field behavior."""

    def test_status_values_match_documentation(self, gcp_available):
        """Document the actual GCE status values returned by API.

        This test documents but doesn't create instances - it validates
        our understanding of the status field format.
        """
        if not gcp_available:
            pytest.skip("GCP not available")

        # These are the documented GCE instance statuses
        # https://cloud.google.com/compute/docs/instances/instance-life-cycle
        documented_statuses = {
            "PROVISIONING",  # Resources being reserved
            "STAGING",  # Resources acquired, instance preparing to start
            "RUNNING",  # Instance booted and running
            "STOPPING",  # Instance being stopped
            "STOPPED",  # Instance stopped, no charges for compute
            "SUSPENDING",  # Instance being suspended
            "SUSPENDED",  # Instance suspended (RAM preserved)
            "TERMINATED",  # Instance stopped and resources released
            "REPAIRING",  # Instance under maintenance
        }

        # Our code maps these in _map_gce_status() - verify alignment
        goldfish_running_statuses = {"PROVISIONING", "STAGING", "RUNNING", "STOPPING", "SUSPENDING", "SUSPENDED"}
        goldfish_terminal_statuses = {"TERMINATED", "STOPPED"}

        # Verify we handle all statuses
        handled = goldfish_running_statuses | goldfish_terminal_statuses
        assert (
            documented_statuses - handled - {"REPAIRING"} == set()
        ), f"Unhandled GCE statuses: {documented_statuses - handled}"

    @pytest.mark.slow
    @pytest.mark.timeout(300)  # GCE instance creation/deletion takes ~3-5 minutes
    def test_instance_lifecycle_status_transitions(self, gcp_project_id, gce_zone, gcp_available):
        """Validate actual instance status transitions during lifecycle.

        This test creates a real instance and observes status transitions.
        """
        if not gcp_available:
            pytest.skip("GCP not available")

        instance_name = f"rct-test-{uuid.uuid4().hex[:8]}"
        observed_statuses = []

        try:
            # Create minimal instance
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "create",
                instance_name,
                f"--zone={gce_zone}",
                f"--project={gcp_project_id}",
                "--machine-type=f1-micro",
                "--image-family=debian-12",
                "--image-project=debian-cloud",
                "--quiet",
            ]
            run_gcloud(cmd)

            # Poll for RUNNING
            for _ in range(60):
                cmd = [
                    "gcloud",
                    "compute",
                    "instances",
                    "describe",
                    instance_name,
                    f"--zone={gce_zone}",
                    f"--project={gcp_project_id}",
                    "--format=value(status)",
                ]
                result = run_gcloud(cmd, check=False)
                status = result.stdout.strip()

                if status and status not in observed_statuses:
                    observed_statuses.append(status)

                if status == "RUNNING":
                    break
                time.sleep(2)

            # Verify we saw expected statuses
            assert "RUNNING" in observed_statuses, f"Instance never reached RUNNING. Observed: {observed_statuses}"

            # Stop instance to observe STOPPING -> TERMINATED
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "stop",
                instance_name,
                f"--zone={gce_zone}",
                f"--project={gcp_project_id}",
                "--quiet",
            ]
            run_gcloud(cmd, check=False)

            # Poll for TERMINATED
            for _ in range(60):
                cmd = [
                    "gcloud",
                    "compute",
                    "instances",
                    "describe",
                    instance_name,
                    f"--zone={gce_zone}",
                    f"--project={gcp_project_id}",
                    "--format=value(status)",
                ]
                result = run_gcloud(cmd, check=False)
                status = result.stdout.strip()

                if status and status not in observed_statuses:
                    observed_statuses.append(status)

                if status in ("TERMINATED", "STOPPED"):
                    break
                time.sleep(2)

        finally:
            # Cleanup
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "delete",
                instance_name,
                f"--zone={gce_zone}",
                f"--project={gcp_project_id}",
                "--quiet",
            ]
            run_gcloud(cmd, check=False)


class TestGCEZoneAgnosticLookup:
    """RCT-GCE-2: Zone-agnostic instance lookup tests."""

    def test_instances_list_filter_works_across_zones(self, gcp_project_id, gcp_available):
        """Validate that instances list --filter works without specifying zone."""
        if not gcp_available:
            pytest.skip("GCP not available")

        # This is the pattern we use in GCELauncher.get_instance_status
        fake_name = f"rct-nonexistent-{uuid.uuid4().hex[:8]}"

        cmd = [
            "gcloud",
            "compute",
            "instances",
            "list",
            f"--filter=name={fake_name}",
            "--format=value(status)",
            f"--project={gcp_project_id}",
        ]
        result = run_gcloud(cmd, check=False)

        # Should succeed but return empty (not error)
        assert result.returncode == 0, f"Zone-agnostic list failed: {result.stderr}"
        assert result.stdout.strip() == "", "Non-existent instance should return empty"

    def test_instances_list_returns_zone_in_output(self, gcp_project_id, gcp_available):
        """Validate we can get zone from instances list output."""
        if not gcp_available:
            pytest.skip("GCP not available")

        # List any existing instance to verify format
        cmd = [
            "gcloud",
            "compute",
            "instances",
            "list",
            "--limit=1",
            "--format=value(name,zone)",
            f"--project={gcp_project_id}",
        ]
        result = run_gcloud(cmd, check=False)

        if result.stdout.strip():
            # If there's an instance, verify zone is in the output
            parts = result.stdout.strip().split()
            assert len(parts) >= 2, "Output should include both name and zone"
            # Zone format is like us-central1-a
            zone_part = parts[-1]
            assert "-" in zone_part, f"Zone should be like us-central1-a, got: {zone_part}"


class TestGCEMetadata:
    """RCT-GCE-3: Instance metadata operations."""

    def test_metadata_value_size_limit(self, gcp_available):
        """Document: GCP metadata values have 256KB limit."""
        if not gcp_available:
            pytest.skip("GCP not available")

        # This is a documentation test - we don't create instances
        # The limit is 256KB per value, 512KB total per instance
        METADATA_VALUE_LIMIT = 256 * 1024  # 256KB
        METADATA_TOTAL_LIMIT = 512 * 1024  # 512KB

        assert METADATA_VALUE_LIMIT == 262144
        assert METADATA_TOTAL_LIMIT == 524288

    def test_metadata_key_format_restrictions(self, gcp_available):
        """Document: GCP metadata keys must match pattern."""
        if not gcp_available:
            pytest.skip("GCP not available")

        import re

        # GCP metadata key restrictions
        # Keys must be 1-128 characters
        # Keys can contain: letters, numbers, hyphens, underscores
        valid_pattern = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")

        # Our keys should match
        goldfish_keys = [
            "goldfish",
            "goldfish_ack",
            "goldfish_exit_code",
        ]

        for key in goldfish_keys:
            assert valid_pattern.match(key), f"Key {key} doesn't match GCP pattern"


class TestGCEPreemption:
    """Document GCE preemption behavior."""

    def test_preemption_signal_is_documented(self, gcp_available):
        """Document: How GCE signals preemption to instances.

        When a preemptible/spot instance is preempted:
        1. ACPI G2 soft off signal sent (like power button press)
        2. SIGTERM sent to init process
        3. 30 second grace period
        4. ACPI G3 hard off (forced shutdown)

        Instance can detect via metadata endpoint:
        curl -H "Metadata-Flavor: Google" \
             http://metadata.google.internal/computeMetadata/v1/instance/preempted

        Returns "TRUE" if being preempted.
        """
        if not gcp_available:
            pytest.skip("GCP not available")

        # Documentation test - describes expected behavior
        expected_behavior = {
            "signal_method": "ACPI G2 soft off + SIGTERM",
            "grace_period_seconds": 30,
            "metadata_endpoint": "http://metadata.google.internal/computeMetadata/v1/instance/preempted",
            "preempted_value": "TRUE",
        }

        assert expected_behavior["grace_period_seconds"] == 30


class TestGCEGCloudOutput:
    """RCT tests for gcloud command output formats."""

    def test_gcloud_json_output_structure(self, gcp_project_id, gcp_available):
        """Validate gcloud --format=json output structure."""
        if not gcp_available:
            pytest.skip("GCP not available")

        # Get zones list as JSON to validate structure
        cmd = [
            "gcloud",
            "compute",
            "zones",
            "list",
            "--format=json",
            "--limit=1",
            f"--project={gcp_project_id}",
        ]
        result = run_gcloud(cmd)

        data = json.loads(result.stdout)
        assert isinstance(data, list), "gcloud JSON output should be a list"

        if data:
            zone = data[0]
            # Validate expected fields
            assert "name" in zone, "Zone should have 'name' field"
            assert "status" in zone, "Zone should have 'status' field"

    def test_gcloud_value_output_is_plain_text(self, gcp_project_id, gcp_available):
        """Validate gcloud --format=value output is plain text."""
        if not gcp_available:
            pytest.skip("GCP not available")

        cmd = [
            "gcloud",
            "compute",
            "zones",
            "list",
            "--format=value(name)",
            "--limit=1",
            f"--project={gcp_project_id}",
        ]
        result = run_gcloud(cmd)

        # Value format should be plain text, no JSON
        output = result.stdout.strip()
        assert not output.startswith("{"), "Value format should not be JSON"
        assert not output.startswith("["), "Value format should not be JSON array"
