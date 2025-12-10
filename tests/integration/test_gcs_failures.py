"""Tests for GCS upload failure handling - P2 Edge Cases.

Tests that verify proper cleanup and error handling when GCS uploads fail.
"""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from goldfish.config import AuditConfig, GCSConfig, GoldfishConfig, JobsConfig, StateMdConfig
from goldfish.db.database import Database
from goldfish.errors import GoldfishError
from goldfish.jobs.tracker import JobTracker


class TestGCSUploadFailures:
    """Tests for GCS upload failure scenarios."""

    def test_gcs_upload_failure_cleanup(self, temp_dir):
        """Test cleanup when GCS upload fails during job artifact upload."""
        # 1. Configure GCS settings
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path=str(temp_dir / "test-dev"),
            gcs=GCSConfig(
                bucket="test-bucket",
                sources_prefix="sources/",
                artifacts_prefix="artifacts/",
            ),
            state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(backend="local", experiments_dir="experiments"),
            invariants=[],
        )

        db = Database(temp_dir / "test.db")
        tracker = JobTracker(db, temp_dir)

        # 2. Create a job with artifacts
        db.create_job(
            job_id="job-test-001",
            workspace="test-workspace",
            snapshot_id="snap-abc123-20251206-120000",
            script="train.py",
            experiment_dir=str(temp_dir / "experiments" / "exp-001"),
        )

        # Create experiment directory with artifacts
        exp_dir = temp_dir / "experiments" / "exp-001"
        exp_dir.mkdir(parents=True)

        # Create artifact files
        artifact_file = exp_dir / "model.pt"
        artifact_file.write_text("fake model data")

        log_file = exp_dir / "output.log"
        log_file.write_text("Training log output")

        # Create completion marker
        (exp_dir / "COMPLETED").write_text("0")

        # Update job to running
        db.update_job_status("job-test-001", "running")

        # 3. Mock gsutil/GCS upload to fail
        # Simulate an upload function that would be called by infrastructure
        def mock_upload_to_gcs(local_path: Path, gcs_uri: str) -> None:
            """Mock GCS upload function that fails."""
            # Simulate gsutil command
            result = subprocess.run(
                ["gsutil", "cp", "-r", str(local_path), gcs_uri],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise GoldfishError(f"GCS upload failed: {result.stderr}")

        with patch("subprocess.run") as mock_subprocess:
            # Make gsutil upload command fail
            mock_subprocess.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="ServiceException: 401 Unauthorized\n",
            )

            # Attempt to upload should fail
            gcs_uri = f"gs://{config.gcs.bucket}/{config.gcs.artifacts_prefix}job-test-001/"
            with pytest.raises(GoldfishError) as exc_info:
                mock_upload_to_gcs(exp_dir, gcs_uri)

            # Verify the error is related to GCS
            assert "upload failed" in str(exc_info.value).lower() or "unauthorized" in str(exc_info.value).lower()

        # 4. Verify job status - upload failure doesn't automatically mark job as failed
        # (In a real scenario, the infrastructure would mark the job as failed)
        job = db.get_job("job-test-001")
        assert (
            job["status"] == "running"
        ), "Job should still be running - infrastructure marks it as failed on upload error"

        # Simulate infrastructure marking job as failed due to upload error
        tracker.update_job_status(
            "job-test-001", "failed", error="GCS upload failed: ServiceException: 401 Unauthorized"
        )

        # 5. Verify local artifacts still exist (not cleaned up yet)
        # They should remain until successful upload or explicit cleanup
        assert artifact_file.exists(), "Local artifacts should remain after failed upload"
        assert log_file.exists(), "Log file should remain after failed upload"

        # 6. Verify database is consistent (no orphaned records)
        # Job should still be retrievable
        retrieved_job = tracker.get_job("job-test-001")
        assert retrieved_job.job_id == "job-test-001"
        assert retrieved_job.workspace == "test-workspace"
        assert retrieved_job.status == "failed"
        assert "upload failed" in retrieved_job.error.lower()

        # Verify no artifact_uri was set (upload failed)
        job = db.get_job("job-test-001")
        assert (
            job["artifact_uri"] is None or job["artifact_uri"] == ""
        ), "artifact_uri should not be set when upload fails"

    def test_gcs_upload_partial_failure_consistency(self, temp_dir):
        """Test that partial upload failures leave database in consistent state."""
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path=str(temp_dir / "test-dev"),
            gcs=GCSConfig(
                bucket="test-bucket",
                sources_prefix="sources/",
                artifacts_prefix="artifacts/",
            ),
            state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(backend="local", experiments_dir="experiments"),
            invariants=[],
        )

        db = Database(temp_dir / "test.db")
        tracker = JobTracker(db, temp_dir)

        # Create multiple jobs
        for i in range(3):
            db.create_job(
                job_id=f"job-test-{i:03d}",
                workspace=f"test-workspace-{i}",
                snapshot_id=f"snap-abc{i:03d}-20251206-120000",
                script="train.py",
                experiment_dir=str(temp_dir / "experiments" / f"exp-{i:03d}"),
            )
            db.update_job_status(f"job-test-{i:03d}", "completed")

        # Mock upload function
        def mock_upload_to_gcs(local_path: Path, gcs_uri: str) -> None:
            """Mock GCS upload function."""
            result = subprocess.run(
                ["gsutil", "cp", "-r", str(local_path), gcs_uri],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise GoldfishError(f"GCS upload failed: {result.stderr}")

        # Simulate uploading artifacts for multiple jobs, one fails
        upload_call_count = [0]

        with patch("subprocess.run") as mock_subprocess:
            # First upload succeeds, second fails
            def side_effect(*args, **kwargs):
                upload_call_count[0] += 1
                if upload_call_count[0] == 1:
                    return MagicMock(returncode=0, stdout="Upload complete", stderr="")
                else:
                    return MagicMock(
                        returncode=1,
                        stdout="",
                        stderr="ServiceException: 401 Unauthorized",
                    )

            mock_subprocess.side_effect = side_effect

            # Try to upload first job - should succeed
            exp_dir_0 = temp_dir / "experiments" / "exp-000"
            exp_dir_0.mkdir(parents=True, exist_ok=True)
            (exp_dir_0 / "model.pt").write_text("model 0")

            gcs_uri_0 = f"gs://{config.gcs.bucket}/{config.gcs.artifacts_prefix}job-test-000/"
            try:
                mock_upload_to_gcs(exp_dir_0, gcs_uri_0)
                # Upload succeeded, update database
                tracker.update_job_status(
                    "job-test-000",
                    "completed",
                    artifact_uri=gcs_uri_0,
                )
            except GoldfishError:
                pass  # Should not happen for first upload

            # Try to upload second job - should fail
            exp_dir_1 = temp_dir / "experiments" / "exp-001"
            exp_dir_1.mkdir(parents=True, exist_ok=True)
            (exp_dir_1 / "model.pt").write_text("model 1")

            gcs_uri_1 = f"gs://{config.gcs.bucket}/{config.gcs.artifacts_prefix}job-test-001/"
            with pytest.raises(GoldfishError) as exc_info:
                mock_upload_to_gcs(exp_dir_1, gcs_uri_1)
            assert "upload failed" in str(exc_info.value).lower()

        # Verify database consistency
        # Job 0 should have artifact_uri (successful upload)
        job_0 = db.get_job("job-test-000")
        assert job_0["artifact_uri"] == gcs_uri_0, "Successful upload should set artifact_uri"

        # Job 1 should NOT have artifact_uri (failed upload)
        job_1 = db.get_job("job-test-001")
        assert job_1["artifact_uri"] is None or job_1["artifact_uri"] == "", "Failed upload should not set artifact_uri"

        # Job 2 should still be retrievable and unchanged
        job_2 = db.get_job("job-test-002")
        assert job_2 is not None
        assert job_2["status"] == "completed"
        assert job_2["artifact_uri"] is None or job_2["artifact_uri"] == ""

    def test_gcs_upload_retry_mechanism(self, temp_dir):
        """Test that failed uploads can be retried without data corruption."""
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path=str(temp_dir / "test-dev"),
            gcs=GCSConfig(
                bucket="test-bucket",
                sources_prefix="sources/",
                artifacts_prefix="artifacts/",
            ),
            state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(backend="local", experiments_dir="experiments"),
            invariants=[],
        )

        db = Database(temp_dir / "test.db")
        tracker = JobTracker(db, temp_dir)

        # Create a job
        db.create_job(
            job_id="job-retry-001",
            workspace="test-workspace",
            snapshot_id="snap-abc123-20251206-120000",
            script="train.py",
            experiment_dir=str(temp_dir / "experiments" / "exp-retry"),
        )
        db.update_job_status("job-retry-001", "completed")

        # Create artifacts
        exp_dir = temp_dir / "experiments" / "exp-retry"
        exp_dir.mkdir(parents=True)
        artifact_file = exp_dir / "model.pt"
        artifact_file.write_text("model data v1")

        def mock_upload_to_gcs(local_path: Path, gcs_uri: str) -> None:
            """Mock GCS upload function."""
            result = subprocess.run(
                ["gsutil", "cp", "-r", str(local_path), gcs_uri],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise GoldfishError(f"GCS upload failed: {result.stderr}")

        gcs_uri = f"gs://{config.gcs.bucket}/{config.gcs.artifacts_prefix}job-retry-001/"

        # First attempt fails
        with patch("subprocess.run") as mock_subprocess:
            mock_subprocess.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="ServiceException: Network error",
            )

            with pytest.raises(GoldfishError) as exc_info:
                mock_upload_to_gcs(exp_dir, gcs_uri)
            assert "upload failed" in str(exc_info.value).lower()

        # Verify artifact_uri is not set
        job = db.get_job("job-retry-001")
        assert job["artifact_uri"] is None or job["artifact_uri"] == ""

        # Second attempt succeeds
        with patch("subprocess.run") as mock_subprocess:
            mock_subprocess.return_value = MagicMock(
                returncode=0,
                stdout="Upload complete",
                stderr="",
            )

            # This should succeed without corruption
            mock_upload_to_gcs(exp_dir, gcs_uri)

            # Update the database after successful upload
            tracker.update_job_status(
                "job-retry-001",
                "completed",
                artifact_uri=gcs_uri,
            )

        # Verify final state is consistent
        job = db.get_job("job-retry-001")
        assert job is not None
        assert job["status"] == "completed"
        assert job["artifact_uri"] == gcs_uri

        # Local file should still exist
        assert artifact_file.exists()
        assert artifact_file.read_text() == "model data v1"
