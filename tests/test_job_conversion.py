"""Tests for job conversion utility - P2.

Tests cover conversion of database job dictionaries to JobInfo model objects.
"""

import pytest


class TestJobDictToInfo:
    """Tests for job_dict_to_info() function."""

    def test_job_dict_to_info_all_fields(self, temp_dir):
        """Test conversion with all fields present."""
        from goldfish.db.database import Database
        from goldfish.jobs.conversion import job_dict_to_info

        db = Database(temp_dir / "test.db")

        # Create a source for inputs
        db.create_source(
            source_id="test-source",
            name="test-source",
            gcs_location="gs://bucket/source",
            created_by="external",
        )

        # Create job with all fields
        db.create_job_with_inputs(
            job_id="job-test123",
            workspace="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="train.py",
            experiment_dir="/path/to/exp",
            inputs={"data": "test-source"},
            metadata={"reason": "Testing conversion"},
        )

        # Update with completion info
        db.update_job_status(
            job_id="job-test123",
            status="completed",
            completed_at="2025-12-05T15:00:00+00:00",
            log_uri="gs://bucket/logs/job-test123.log",
            artifact_uri="gs://bucket/artifacts/job-test123",
            error=None,
        )

        # Get job dict and convert
        job_dict = db.get_job("job-test123")
        job_info = job_dict_to_info(job_dict, db)

        # Verify all fields
        assert job_info.job_id == "job-test123"
        assert job_info.status == "completed"
        assert job_info.workspace == "test-ws"
        assert job_info.snapshot_id == "snap-abc1234-20251205-120000"
        assert job_info.script == "train.py"
        assert job_info.started_at is not None
        assert job_info.completed_at is not None
        assert job_info.log_uri == "gs://bucket/logs/job-test123.log"
        assert job_info.artifact_uri == "gs://bucket/artifacts/job-test123"
        assert job_info.error is None
        assert len(job_info.input_sources) == 1
        assert job_info.input_sources[0] == "test-source"

    def test_job_dict_to_info_optional_fields_missing(self, temp_dir):
        """Test conversion with optional fields missing."""
        from goldfish.db.database import Database
        from goldfish.jobs.conversion import job_dict_to_info

        db = Database(temp_dir / "test.db")

        # Create job with minimal fields (no inputs)
        db.create_job(
            job_id="job-minimal",
            workspace="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="script.py",
        )

        # Get job dict and convert
        job_dict = db.get_job("job-minimal")
        job_info = job_dict_to_info(job_dict, db)

        # Verify required fields present, optional fields None
        assert job_info.job_id == "job-minimal"
        assert job_info.status == "pending"
        assert job_info.completed_at is None
        assert job_info.log_uri is None
        assert job_info.artifact_uri is None
        assert job_info.error is None
        assert len(job_info.input_sources) == 0

    def test_job_dict_to_info_with_inputs(self, temp_dir):
        """Test that get_job_inputs is called and results included."""
        from goldfish.db.database import Database
        from goldfish.jobs.conversion import job_dict_to_info

        db = Database(temp_dir / "test.db")

        # Create multiple sources
        for i in range(3):
            db.create_source(
                source_id=f"source-{i}",
                name=f"source-{i}",
                gcs_location=f"gs://bucket/source-{i}",
                created_by="external",
            )

        # Create job with multiple inputs
        db.create_job_with_inputs(
            job_id="job-multi-input",
            workspace="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="process.py",
            inputs={
                "train_data": "source-0",
                "val_data": "source-1",
                "test_data": "source-2",
            },
        )

        # Get job dict and convert
        job_dict = db.get_job("job-multi-input")
        job_info = job_dict_to_info(job_dict, db)

        # Verify inputs are included
        assert len(job_info.input_sources) == 3
        assert "source-0" in job_info.input_sources
        assert "source-1" in job_info.input_sources
        assert "source-2" in job_info.input_sources

    def test_job_dict_to_info_with_error(self, temp_dir):
        """Test conversion of failed job with error message."""
        from goldfish.db.database import Database
        from goldfish.jobs.conversion import job_dict_to_info

        db = Database(temp_dir / "test.db")

        db.create_job(
            job_id="job-failed",
            workspace="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="fail.py",
        )

        # Mark as failed with error
        db.update_job_status(
            job_id="job-failed",
            status="failed",
            completed_at="2025-12-05T15:00:00+00:00",
            error="Something went wrong",
        )

        # Get job dict and convert
        job_dict = db.get_job("job-failed")
        job_info = job_dict_to_info(job_dict, db)

        # Verify error is preserved
        assert job_info.status == "failed"
        assert job_info.error == "Something went wrong"
        assert job_info.completed_at is not None

    def test_job_dict_to_info_datetime_parsing(self, temp_dir):
        """Test that datetime fields are parsed correctly."""
        from goldfish.db.database import Database
        from goldfish.jobs.conversion import job_dict_to_info
        from datetime import datetime

        db = Database(temp_dir / "test.db")

        db.create_job(
            job_id="job-datetime",
            workspace="test-ws",
            snapshot_id="snap-abc1234-20251205-120000",
            script="script.py",
        )

        db.update_job_status(
            job_id="job-datetime",
            status="completed",
            completed_at="2025-12-05T15:30:45.123456+00:00",
        )

        # Get job dict and convert
        job_dict = db.get_job("job-datetime")
        job_info = job_dict_to_info(job_dict, db)

        # Verify datetime objects are returned
        assert isinstance(job_info.started_at, datetime)
        assert isinstance(job_info.completed_at, datetime)
        assert job_info.completed_at.microsecond == 123456
