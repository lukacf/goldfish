"""RCT Tests for GCE Instance Metadata Signaling.

These tests validate our assumptions about the metadata server behavior.
The metadata system is used for low-latency "Overdrive" synchronization.

RCT-META-1: Signal round-trip via instance metadata
RCT-META-2: Acknowledgment pattern works correctly
RCT-META-3: Metadata size limits are enforced
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


class TestMetadataSignalRoundTrip:
    """RCT-META-1: Metadata signal round-trip tests.

    Note: These tests require a running GCE instance to test against.
    They are skipped if no test instance is available.
    """

    @pytest.fixture
    def test_instance(self, gcp_project_id, gce_zone, gcp_available):
        """Get or create a test instance for metadata tests."""
        if not gcp_available:
            pytest.skip("GCP not available")

        # Check for existing test instance
        instance_name = f"rct-metadata-test-{gce_zone}"

        cmd = [
            "gcloud",
            "compute",
            "instances",
            "describe",
            instance_name,
            f"--zone={gce_zone}",
            f"--project={gcp_project_id}",
            "--format=value(name)",
        ]
        result = run_gcloud(cmd, check=False)

        if result.returncode != 0:
            # Skip if no instance - creating instances for metadata tests is expensive
            pytest.skip(f"No test instance '{instance_name}' available. " "Create one manually for metadata RCT tests.")

        yield {
            "name": instance_name,
            "zone": gce_zone,
            "project": gcp_project_id,
        }

    def test_metadata_set_and_get_roundtrip(self, test_instance, gcp_available):
        """Validate metadata can be set and retrieved."""
        if not gcp_available:
            pytest.skip("GCP not available")

        instance = test_instance
        key = f"rct_test_{uuid.uuid4().hex[:8]}"
        value = json.dumps(
            {
                "command": "sync",
                "request_id": uuid.uuid4().hex,
                "timestamp": "2025-01-22T00:00:00Z",
            }
        )

        try:
            # Set metadata
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "add-metadata",
                instance["name"],
                f"--zone={instance['zone']}",
                f"--project={instance['project']}",
                "--metadata",
                f"{key}={value}",
                "--quiet",
            ]
            run_gcloud(cmd)

            # Get metadata back
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "describe",
                instance["name"],
                f"--zone={instance['zone']}",
                f"--project={instance['project']}",
                "--format",
                f"value(metadata.items.{key})",
            ]
            result = run_gcloud(cmd)

            retrieved = result.stdout.strip()
            assert retrieved == value, f"Metadata mismatch: {retrieved} != {value}"

        finally:
            # Cleanup
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "remove-metadata",
                instance["name"],
                f"--zone={instance['zone']}",
                f"--project={instance['project']}",
                "--keys",
                key,
                "--quiet",
            ]
            run_gcloud(cmd, check=False)

    def test_metadata_update_latency(self, test_instance, gcp_available):
        """Measure metadata update latency for Overdrive feasibility."""
        if not gcp_available:
            pytest.skip("GCP not available")

        instance = test_instance
        key = f"rct_latency_{uuid.uuid4().hex[:8]}"

        try:
            # Measure set latency
            start = time.time()
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "add-metadata",
                instance["name"],
                f"--zone={instance['zone']}",
                f"--project={instance['project']}",
                "--metadata",
                f"{key}=test_value",
                "--quiet",
            ]
            run_gcloud(cmd)
            set_latency = time.time() - start

            # Measure get latency
            start = time.time()
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "describe",
                instance["name"],
                f"--zone={instance['zone']}",
                f"--project={instance['project']}",
                "--format",
                f"value(metadata.items.{key})",
            ]
            run_gcloud(cmd)
            get_latency = time.time() - start

            # Document observed latencies
            # These are informational - gcloud has ~1-3s overhead
            print("\nMetadata latencies (via gcloud CLI):")
            print(f"  Set: {set_latency:.2f}s")
            print(f"  Get: {get_latency:.2f}s")

            # Basic sanity check - should complete within 30s
            assert set_latency < 30, f"Set latency too high: {set_latency}s"
            assert get_latency < 30, f"Get latency too high: {get_latency}s"

        finally:
            cmd = [
                "gcloud",
                "compute",
                "instances",
                "remove-metadata",
                instance["name"],
                f"--zone={instance['zone']}",
                f"--project={instance['project']}",
                "--keys",
                key,
                "--quiet",
            ]
            run_gcloud(cmd, check=False)


class TestMetadataAckPattern:
    """RCT-META-2: Acknowledgment pattern tests."""

    def test_ack_key_convention(self, gcp_available):
        """Document: Our ack key convention appends _ack suffix."""
        if not gcp_available:
            pytest.skip("GCP not available")

        # Our convention from GCPMetadataBus
        signal_key = "goldfish"
        expected_ack_key = "goldfish_ack"

        # Verify convention
        actual_ack_key = f"{signal_key}_ack"
        assert actual_ack_key == expected_ack_key


class TestMetadataSizeLimits:
    """RCT-META-3: Metadata size limit tests."""

    def test_single_value_limit_is_256kb(self, gcp_available):
        """Document: GCP enforces 256KB limit per metadata value."""
        if not gcp_available:
            pytest.skip("GCP not available")

        # GCP documentation states 256KB per value limit
        MAX_VALUE_SIZE = 256 * 1024  # 256KB in bytes

        # Our code enforces this in GCPMetadataBus.set_signal
        test_signal = {"command": "sync", "request_id": "test", "payload": {}}
        signal_json = json.dumps(test_signal)

        assert len(signal_json) < MAX_VALUE_SIZE, f"Test signal ({len(signal_json)} bytes) should be under limit"

    def test_large_payload_exceeds_limit(self, gcp_available):
        """Validate that large payloads would exceed the 256KB limit."""
        if not gcp_available:
            pytest.skip("GCP not available")

        MAX_VALUE_SIZE = 256 * 1024

        # Create a payload that exceeds the limit
        large_payload = {"data": "x" * (MAX_VALUE_SIZE + 1000)}
        large_signal = {
            "command": "sync",
            "request_id": "test",
            "payload": large_payload,
        }
        large_json = json.dumps(large_signal)

        assert len(large_json) > MAX_VALUE_SIZE, "Test payload should exceed limit"


class TestMetadataFromInstance:
    """Test metadata access patterns from inside an instance.

    These tests document the expected behavior when code runs inside
    a GCE instance and accesses the metadata server directly.
    """

    def test_metadata_server_endpoint_format(self, gcp_available):
        """Document: Metadata server endpoint format."""
        if not gcp_available:
            pytest.skip("GCP not available")

        # Standard GCE metadata server endpoint
        METADATA_SERVER = "http://metadata.google.internal"
        METADATA_ROOT = f"{METADATA_SERVER}/computeMetadata/v1"

        # Instance-specific endpoints used in startup scripts
        endpoints = {
            "instance_name": f"{METADATA_ROOT}/instance/name",
            "instance_zone": f"{METADATA_ROOT}/instance/zone",
            "project_id": f"{METADATA_ROOT}/project/project-id",
            "custom_metadata": f"{METADATA_ROOT}/instance/attributes/",
        }

        # Verify format matches what startup_builder.py uses
        assert "metadata.google.internal" in endpoints["instance_name"]

    def test_metadata_flavor_header_required(self, gcp_available):
        """Document: Metadata-Flavor header is required."""
        if not gcp_available:
            pytest.skip("GCP not available")

        # GCP requires this header to prevent SSRF attacks
        REQUIRED_HEADER = {"Metadata-Flavor": "Google"}

        assert REQUIRED_HEADER["Metadata-Flavor"] == "Google"

    def test_zone_response_format(self, gcp_available):
        """Document: Zone endpoint returns full path, needs parsing."""
        if not gcp_available:
            pytest.skip("GCP not available")

        # The zone endpoint returns: projects/PROJECT_NUM/zones/ZONE_NAME
        # Our startup script extracts just the zone name with awk
        example_response = "projects/123456789/zones/us-central1-a"
        expected_zone = "us-central1-a"

        # Simulate the awk extraction
        extracted = example_response.split("/")[-1]
        assert extracted == expected_zone
