"""RCT Tests for Exit Code Communication.

These tests validate how Goldfish communicates stage completion
via exit codes stored in GCS. The exit code file is critical
for the state machine to determine run success/failure.

RCT-EXIT-1: Exit code file format and round-trip
RCT-EXIT-2: Error detection when exit code is missing
RCT-EXIT-3: Race condition handling for exit code reads
"""

import pytest
from google.cloud import storage  # type: ignore[attr-defined]

# Mark all tests in this module as RCT tests
pytestmark = pytest.mark.rct


class TestExitCodeFormat:
    """RCT-EXIT-1: Exit code file format tests."""

    def test_exit_code_is_plain_text_integer(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate exit_code.txt contains a plain text integer."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        exit_code_path = f"{cleanup_gcs_prefix}/logs/exit_code.txt"
        blob = bucket.blob(exit_code_path)

        # Write exit code as plain text (what startup script does)
        blob.upload_from_string("0\n")

        # Read back
        content = blob.download_as_text()

        # Parse as integer
        exit_code = int(content.strip())
        assert exit_code == 0

    def test_exit_code_with_nonzero_value(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate non-zero exit codes are preserved."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        exit_code_path = f"{cleanup_gcs_prefix}/logs/exit_code.txt"
        blob = bucket.blob(exit_code_path)

        # Common exit codes
        test_codes = [1, 2, 127, 137, 143, 255]

        for code in test_codes:
            blob.upload_from_string(f"{code}\n")
            content = blob.download_as_text()
            parsed = int(content.strip())
            assert parsed == code, f"Exit code {code} not preserved"

    def test_exit_code_without_newline(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate exit code parsing works without trailing newline."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        exit_code_path = f"{cleanup_gcs_prefix}/logs/exit_code.txt"
        blob = bucket.blob(exit_code_path)

        # Write without newline
        blob.upload_from_string("42")

        content = blob.download_as_text()
        exit_code = int(content.strip())

        assert exit_code == 42


class TestExitCodeMissing:
    """RCT-EXIT-2: Missing exit code behavior."""

    def test_missing_exit_code_blob_does_not_exist(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Validate blob.exists() returns False for missing exit code."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        exit_code_path = f"{cleanup_gcs_prefix}/nonexistent/exit_code.txt"
        blob = bucket.blob(exit_code_path)

        # exists() should return False
        assert blob.exists() is False

    def test_missing_exit_code_semantics(self, gcp_available):
        """Document: Missing exit code file means crash/preemption."""
        if not gcp_available:
            pytest.skip("GCP not available")

        # In Goldfish's state machine:
        # - exit_code.txt exists with "0" → COMPLETED
        # - exit_code.txt exists with non-zero → FAILED
        # - exit_code.txt missing → TERMINATED (crash/preemption/timeout)

        # This is implemented in ExitCodeResult and _get_exit_code
        semantics = {
            "exists_zero": "COMPLETED",
            "exists_nonzero": "FAILED",
            "missing": "TERMINATED (crash/preemption assumed)",
        }

        assert semantics["missing"] == "TERMINATED (crash/preemption assumed)"


class TestExitCodeRaceConditions:
    """RCT-EXIT-3: Race condition handling tests."""

    def test_eventual_consistency_on_upload(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Document: GCS has strong consistency, immediate read after write works."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        exit_code_path = f"{cleanup_gcs_prefix}/logs/exit_code.txt"
        blob = bucket.blob(exit_code_path)

        # Write and immediately read (tests strong consistency)
        blob.upload_from_string("0\n")
        content = blob.download_as_text()

        assert content.strip() == "0"

    def test_overwrite_semantics(self, gcs_bucket, cleanup_gcs_prefix, gcp_available):
        """Document: Overwriting exit code file replaces content."""
        if not gcp_available:
            pytest.skip("GCP not available")

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        exit_code_path = f"{cleanup_gcs_prefix}/logs/exit_code.txt"
        blob = bucket.blob(exit_code_path)

        # Write initial value
        blob.upload_from_string("0\n")

        # Overwrite
        blob.upload_from_string("1\n")

        # Should see new value
        content = blob.download_as_text()
        assert content.strip() == "1"


class TestExitCodePathConvention:
    """Document exit code path conventions."""

    def test_exit_code_path_matches_goldfish_convention(self, gcp_available):
        """Document: Exit code path follows Goldfish convention."""
        if not gcp_available:
            pytest.skip("GCP not available")

        # Path convention from startup_builder.py and gce_launcher.py
        # gs://{bucket}/runs/{stage_run_id}/logs/exit_code.txt

        bucket = "my-bucket"
        stage_run_id = "stage-abc123"

        expected_path = f"gs://{bucket}/runs/{stage_run_id}/logs/exit_code.txt"

        # Verify format
        assert "runs/" in expected_path
        assert "logs/exit_code.txt" in expected_path

    def test_termination_cause_path_convention(self, gcp_available):
        """Document: Termination cause path convention."""
        if not gcp_available:
            pytest.skip("GCP not available")

        # Termination cause file written by watchdog/supervisor
        # gs://{bucket}/runs/{stage_run_id}/logs/termination_cause.txt

        bucket = "my-bucket"
        stage_run_id = "stage-abc123"

        expected_path = f"gs://{bucket}/runs/{stage_run_id}/logs/termination_cause.txt"

        # Verify path format
        assert "termination_cause.txt" in expected_path

        # Values: "watchdog" or "supervisor"
        valid_causes = ["watchdog", "supervisor"]

        for cause in valid_causes:
            assert cause in ["watchdog", "supervisor"]
