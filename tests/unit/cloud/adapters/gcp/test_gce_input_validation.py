"""Unit tests for GCE input scheme validation.

Tests that GCERunBackend rejects non-GCS (non-gs://) inputs with a clear
ValidationError, rather than silently skipping them during staging.
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest

from goldfish.cloud.adapters.gcp.run_backend import GCERunBackend
from goldfish.cloud.contracts import RunSpec, StorageURI
from goldfish.validation import ValidationError


@pytest.fixture
def mock_launcher():
    """Create a mock GCELauncher."""
    with patch("goldfish.cloud.adapters.gcp.gce_launcher.GCELauncher") as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        mock_instance.default_zone = "us-central1-a"
        yield mock_instance


@pytest.fixture
def backend(mock_launcher):
    """Create a GCERunBackend with mocked GCELauncher."""
    return GCERunBackend(
        project_id="test-project",
        zones=["us-central1-a", "us-central1-b"],
        bucket="test-bucket",
    )


@dataclass
class MockLaunchResult:
    """Mock result from GCELauncher.launch_instance."""

    instance_name: str
    zone: str


class TestGCEInputSchemeValidation:
    """Tests for input URI scheme validation in launch method.

    GCE backend only supports GCS (gs://) inputs. Non-GCS inputs like file://
    will be silently skipped during staging, causing confusing runtime failures.
    The backend should validate input schemes upfront and reject non-GCS inputs.
    """

    def test_launch_rejects_non_gcs_input_scheme(self, backend, mock_launcher):
        """Launch rejects inputs with non-gs:// scheme (file://, etc.).

        GCELauncher only stages gs:// inputs. Other schemes are silently skipped,
        causing the input to be missing at runtime. This validation catches the
        issue early with a clear error message.
        """
        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={
                "local_data": StorageURI("file", "", "/tmp/data.npy"),
            },
        )

        with pytest.raises(ValidationError) as exc_info:
            backend.launch(spec)

        assert "file://" in str(exc_info.value) or "file" in str(exc_info.value).lower()
        assert "local_data" in str(exc_info.value) or exc_info.value.details.get("field") == "local_data"

    def test_launch_rejects_multiple_non_gcs_inputs(self, backend, mock_launcher):
        """Launch rejects when any input has non-GCS scheme."""
        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={
                "valid_input": StorageURI("gs", "bucket", "data/valid.npy"),
                "invalid_input": StorageURI("file", "", "/tmp/invalid.npy"),
            },
        )

        with pytest.raises(ValidationError) as exc_info:
            backend.launch(spec)

        assert "invalid_input" in str(exc_info.value) or exc_info.value.details.get("field") == "invalid_input"

    def test_launch_accepts_all_gcs_inputs(self, backend, mock_launcher):
        """Launch accepts inputs when all have gs:// scheme."""
        mock_launcher.launch_instance.return_value = MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={
                "input1": StorageURI("gs", "bucket-a", "data/input1.npy"),
                "input2": StorageURI("gs", "bucket-b", "data/input2.csv"),
            },
        )

        # Should not raise
        handle = backend.launch(spec)
        assert handle.stage_run_id == "stage-abc123"

    def test_launch_accepts_empty_inputs(self, backend, mock_launcher):
        """Launch accepts empty inputs dict (no validation needed)."""
        mock_launcher.launch_instance.return_value = MockLaunchResult(
            instance_name="goldfish-stage-abc123",
            zone="us-central1-a",
        )

        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={},
        )

        # Should not raise
        handle = backend.launch(spec)
        assert handle.stage_run_id == "stage-abc123"

    def test_launch_rejects_http_scheme(self, backend, mock_launcher):
        """Launch rejects http:// inputs."""
        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={
                "web_data": StorageURI("http", "example.com", "data.csv"),
            },
        )

        with pytest.raises(ValidationError):
            backend.launch(spec)

    def test_launch_rejects_s3_scheme(self, backend, mock_launcher):
        """Launch rejects s3:// inputs (AWS, not GCP)."""
        spec = RunSpec(
            stage_run_id="stage-abc123",
            workspace_name="test-workspace",
            stage_name="train",
            image="gcr.io/test-project/test-image:latest",
            inputs={
                "s3_data": StorageURI("s3", "my-bucket", "data.npy"),
            },
        )

        with pytest.raises(ValidationError):
            backend.launch(spec)
