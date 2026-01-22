"""Tests for Phase 4 fixes - TDD.

These tests verify the fixes identified by review agents:
1. Database.update_stage_run_gcs_outage() method
2. Container ID and instance name validation
3. Input validation in subprocess calls
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    pass


class TestDatabaseUpdateGcsOutage:
    """Tests for Database.update_stage_run_gcs_outage() method."""

    def test_update_gcs_outage_sets_timestamp(self, test_db) -> None:
        """update_stage_run_gcs_outage() must set gcs_outage_started."""
        from datetime import UTC, datetime

        # Create workspace lineage and version first
        test_db.create_workspace_lineage("test_ws", description="Test")
        test_db.create_version("test_ws", "v1", "tag-1", "abc123", "run")

        # Create a stage run
        test_db.create_stage_run(
            stage_run_id="stage-abc123",
            workspace_name="test_ws",
            version="v1",
            stage_name="train",
        )

        # Set GCS outage started
        now = datetime.now(UTC)
        test_db.update_stage_run_gcs_outage("stage-abc123", now.isoformat())

        # Verify it was set
        run = test_db.get_stage_run("stage-abc123")
        assert run is not None
        assert run["gcs_outage_started"] == now.isoformat()

    def test_update_gcs_outage_clears_timestamp(self, test_db) -> None:
        """update_stage_run_gcs_outage(None) must clear gcs_outage_started."""
        from datetime import UTC, datetime

        # Create workspace lineage and version first
        test_db.create_workspace_lineage("test_ws", description="Test")
        test_db.create_version("test_ws", "v1", "tag-1", "abc123", "run")

        # Create a stage run
        test_db.create_stage_run(
            stage_run_id="stage-abc123",
            workspace_name="test_ws",
            version="v1",
            stage_name="train",
        )

        now = datetime.now(UTC)
        test_db.update_stage_run_gcs_outage("stage-abc123", now.isoformat())

        # Clear it
        test_db.update_stage_run_gcs_outage("stage-abc123", None)

        # Verify it was cleared
        run = test_db.get_stage_run("stage-abc123")
        assert run is not None
        assert run["gcs_outage_started"] is None

    def test_update_gcs_outage_nonexistent_run(self, test_db) -> None:
        """update_stage_run_gcs_outage() on nonexistent run should not raise."""
        from datetime import UTC, datetime

        # Should not raise - just a no-op
        now = datetime.now(UTC)
        test_db.update_stage_run_gcs_outage("stage-nonexistent", now.isoformat())


class TestContainerIdValidation:
    """Tests for container ID validation."""

    def test_valid_container_id_short_hash(self) -> None:
        """Valid short container ID (12 chars hex)."""
        from goldfish.validation import validate_container_id

        # Valid 12-char hex
        validate_container_id("abc123def456")

    def test_valid_container_id_full_hash(self) -> None:
        """Valid full container ID (64 chars hex)."""
        from goldfish.validation import validate_container_id

        # Valid 64-char hex
        validate_container_id("a" * 64)

    def test_valid_container_id_name(self) -> None:
        """Valid container name with allowed characters."""
        from goldfish.validation import validate_container_id

        validate_container_id("my-container_name.1")
        validate_container_id("goldfish-stage-abc123")

    def test_invalid_container_id_shell_metachar(self) -> None:
        """Container ID with shell metacharacters must fail."""
        from goldfish.validation import InvalidContainerIdError, validate_container_id

        with pytest.raises(InvalidContainerIdError):
            validate_container_id("container;rm -rf /")

        with pytest.raises(InvalidContainerIdError):
            validate_container_id("container$(whoami)")

        with pytest.raises(InvalidContainerIdError):
            validate_container_id("container`id`")

    def test_invalid_container_id_empty(self) -> None:
        """Empty container ID must fail."""
        from goldfish.validation import InvalidContainerIdError, validate_container_id

        with pytest.raises(InvalidContainerIdError):
            validate_container_id("")

    def test_invalid_container_id_too_long(self) -> None:
        """Container ID over 128 chars must fail."""
        from goldfish.validation import InvalidContainerIdError, validate_container_id

        with pytest.raises(InvalidContainerIdError):
            validate_container_id("a" * 129)


class TestInstanceNameValidation:
    """Tests for GCE instance name validation."""

    def test_valid_instance_name(self) -> None:
        """Valid GCE instance names."""
        from goldfish.validation import validate_instance_name

        validate_instance_name("my-instance")
        validate_instance_name("instance-123")
        validate_instance_name("goldfish-stage-abc123")

    def test_valid_instance_name_with_numbers(self) -> None:
        """Instance names with numbers are valid."""
        from goldfish.validation import validate_instance_name

        validate_instance_name("instance1")
        validate_instance_name("instance-1-2-3")

    def test_invalid_instance_name_shell_metachar(self) -> None:
        """Instance name with shell metacharacters must fail."""
        from goldfish.validation import InvalidInstanceNameError, validate_instance_name

        with pytest.raises(InvalidInstanceNameError):
            validate_instance_name("instance;rm -rf /")

        with pytest.raises(InvalidInstanceNameError):
            validate_instance_name("instance$(whoami)")

    def test_invalid_instance_name_uppercase(self) -> None:
        """Instance names must be lowercase (GCE requirement)."""
        from goldfish.validation import InvalidInstanceNameError, validate_instance_name

        with pytest.raises(InvalidInstanceNameError):
            validate_instance_name("MyInstance")

    def test_invalid_instance_name_empty(self) -> None:
        """Empty instance name must fail."""
        from goldfish.validation import InvalidInstanceNameError, validate_instance_name

        with pytest.raises(InvalidInstanceNameError):
            validate_instance_name("")

    def test_invalid_instance_name_too_long(self) -> None:
        """Instance name over 63 chars must fail."""
        from goldfish.validation import InvalidInstanceNameError, validate_instance_name

        with pytest.raises(InvalidInstanceNameError):
            validate_instance_name("a" * 64)


class TestZoneValidation:
    """Tests for GCE zone validation."""

    def test_valid_zone(self) -> None:
        """Valid GCE zone names."""
        from goldfish.validation import validate_zone

        validate_zone("us-central1-a")
        validate_zone("europe-west1-b")
        validate_zone("asia-east1-c")

    def test_invalid_zone_shell_metachar(self) -> None:
        """Zone with shell metacharacters must fail."""
        from goldfish.validation import InvalidZoneError, validate_zone

        with pytest.raises(InvalidZoneError):
            validate_zone("us-central1-a;rm -rf /")

    def test_invalid_zone_empty(self) -> None:
        """Empty zone must fail."""
        from goldfish.validation import InvalidZoneError, validate_zone

        with pytest.raises(InvalidZoneError):
            validate_zone("")


class TestProjectIdValidation:
    """Tests for GCP project ID validation."""

    def test_valid_project_id(self) -> None:
        """Valid GCP project IDs."""
        from goldfish.validation import validate_project_id

        validate_project_id("my-project")
        validate_project_id("my-project-123")
        validate_project_id("project123")

    def test_invalid_project_id_shell_metachar(self) -> None:
        """Project ID with shell metacharacters must fail."""
        from goldfish.validation import InvalidProjectIdError, validate_project_id

        with pytest.raises(InvalidProjectIdError):
            validate_project_id("project;rm -rf /")

    def test_invalid_project_id_empty(self) -> None:
        """Empty project ID must fail."""
        from goldfish.validation import InvalidProjectIdError, validate_project_id

        with pytest.raises(InvalidProjectIdError):
            validate_project_id("")


class TestEventEmissionWithValidation:
    """Tests for event emission with input validation."""

    def test_verify_instance_stopped_validates_instance_name(self) -> None:
        """verify_instance_stopped must validate instance_name."""
        from goldfish.state_machine.event_emission import verify_instance_stopped
        from goldfish.validation import InvalidInstanceNameError

        with pytest.raises(InvalidInstanceNameError):
            verify_instance_stopped(
                run_id="stage-abc123",
                backend_type="gce",
                backend_handle="instance;rm -rf /",
                project_id="my-project",
            )

    def test_verify_docker_stopped_validates_container_id(self) -> None:
        """verify_instance_stopped must validate container_id for Docker."""
        from goldfish.state_machine.event_emission import verify_instance_stopped
        from goldfish.validation import InvalidContainerIdError

        with pytest.raises(InvalidContainerIdError):
            verify_instance_stopped(
                run_id="stage-abc123",
                backend_type="local",
                backend_handle="container$(whoami)",
            )

    def test_detect_termination_cause_validates_inputs(self) -> None:
        """detect_termination_cause must validate inputs."""
        from goldfish.state_machine.event_emission import detect_termination_cause
        from goldfish.validation import InvalidInstanceNameError

        with pytest.raises(InvalidInstanceNameError):
            detect_termination_cause(
                run_id="stage-abc123",
                backend_type="gce",
                backend_handle="instance`id`",
                project_id="my-project",
            )

    def test_get_exit_code_gce_validates_inputs(self) -> None:
        """get_exit_code_gce must validate stage_run_id."""
        from goldfish.state_machine.exit_code import get_exit_code_gce
        from goldfish.validation import InvalidStageRunIdError

        with pytest.raises(InvalidStageRunIdError):
            get_exit_code_gce(
                bucket_uri="gs://bucket",
                stage_run_id="invalid;rm -rf /",
                project_id="my-project",
            )

    def test_get_exit_code_docker_validates_container_id(self) -> None:
        """get_exit_code_docker must validate container_id."""
        from goldfish.state_machine.exit_code import get_exit_code_docker
        from goldfish.validation import InvalidContainerIdError

        with pytest.raises(InvalidContainerIdError):
            get_exit_code_docker(container_id="container$(whoami)")
