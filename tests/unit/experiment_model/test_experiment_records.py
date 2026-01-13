"""Unit tests for experiment record creation and management.

TDD: Tests for experiment_records creation on run and save_version.
Tests should fail initially (RED), then pass after implementation (GREEN).
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from goldfish.experiment_model.records import (
    ExperimentRecordManager,
    generate_record_id,
)
from goldfish.validation import InvalidWorkspaceNameError


class TestGenerateRecordId:
    """Tests for record_id generation."""

    def test_generate_record_id_is_ulid_format(self) -> None:
        """Generated ID follows ULID format (26 chars, alphanumeric)."""
        record_id = generate_record_id()
        assert len(record_id) == 26
        # ULID uses Crockford's Base32 (excludes I, L, O, U)
        assert record_id.isalnum()

    def test_generate_record_id_is_unique(self) -> None:
        """Each call generates a unique ID."""
        ids = [generate_record_id() for _ in range(100)]
        assert len(set(ids)) == 100

    def test_generate_record_id_is_sortable(self) -> None:
        """IDs generated later sort after earlier ones when timestamps differ."""
        id1 = generate_record_id()
        time.sleep(0.015)  # Delay ensures different millisecond timestamps
        id2 = generate_record_id()
        assert id1 < id2


class TestExperimentRecordManagerCreate:
    """Tests for creating experiment records."""

    def test_create_run_record(self) -> None:
        """Can create a run record linking to a stage_run."""
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        record_id = manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-abc123",
        )

        assert record_id is not None
        assert len(record_id) == 26  # ULID format

    def test_create_checkpoint_record(self) -> None:
        """Can create a checkpoint record (no stage_run_id)."""
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        record_id = manager.create_checkpoint_record(
            workspace_name="test_ws",
            version="v2",
        )

        assert record_id is not None
        assert len(record_id) == 26

    def test_create_run_record_persists_to_database(self) -> None:
        """Run record is persisted via database insert."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-abc123",
        )

        # Verify database insert was called (2 calls: experiment_records + run_results)
        assert mock_conn.execute.call_count == 2
        first_call = mock_conn.execute.call_args_list[0]
        assert "INSERT INTO experiment_records" in first_call[0][0]
        # Check the values tuple
        values = first_call[0][1]
        assert values[1] == "test_ws"  # workspace_name
        assert values[2] == "run"  # type
        assert values[3] == "stage-abc123"  # stage_run_id
        assert values[4] == "v1"  # version

    def test_create_checkpoint_record_persists_with_null_stage_run_id(self) -> None:
        """Checkpoint record has NULL stage_run_id."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.create_checkpoint_record(
            workspace_name="test_ws",
            version="v2",
        )

        call_args = mock_conn.execute.call_args
        values = call_args[0][1]
        assert values[2] == "checkpoint"  # type
        assert values[3] is None  # stage_run_id


class TestExperimentRecordManagerGet:
    """Tests for retrieving experiment records."""

    def test_get_record_by_id(self) -> None:
        """Can retrieve a record by its ID."""
        mock_row = {
            "record_id": "01HXYZ1234567890ABCDEF",
            "workspace_name": "test_ws",
            "type": "run",
            "stage_run_id": "stage-abc123",
            "version": "v1",
            "created_at": "2025-01-13T12:00:00Z",
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_row
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        record = manager.get_record("01HXYZ1234567890ABCDEF")

        assert record is not None
        assert record["record_id"] == "01HXYZ1234567890ABCDEF"
        assert record["type"] == "run"

    def test_get_record_by_id_not_found(self) -> None:
        """Returns None when record not found."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        record = manager.get_record("nonexistent")

        assert record is None

    def test_get_record_by_stage_run_id(self) -> None:
        """Can retrieve a record by its stage_run_id."""
        mock_row = {
            "record_id": "01HXYZ1234567890ABCDEF",
            "workspace_name": "test_ws",
            "type": "run",
            "stage_run_id": "stage-abc123",
            "version": "v1",
            "created_at": "2025-01-13T12:00:00Z",
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_row
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        record = manager.get_record_by_stage_run("stage-abc123")

        assert record is not None
        assert record["stage_run_id"] == "stage-abc123"

    def test_get_record_by_stage_run_not_found(self) -> None:
        """Returns None when stage_run_id not found."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        record = manager.get_record_by_stage_run("nonexistent-run")

        assert record is None


class TestExperimentRecordManagerList:
    """Tests for listing experiment records."""

    def test_list_records_by_workspace(self) -> None:
        """Can list all records for a workspace."""
        mock_rows = [
            {
                "record_id": "01HXYZ0001",
                "workspace_name": "test_ws",
                "type": "run",
                "stage_run_id": "stage-001",
                "version": "v1",
                "created_at": "2025-01-13T12:00:00Z",
            },
            {
                "record_id": "01HXYZ0002",
                "workspace_name": "test_ws",
                "type": "checkpoint",
                "stage_run_id": None,
                "version": "v2",
                "created_at": "2025-01-13T13:00:00Z",
            },
        ]
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = mock_rows
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        records = manager.list_records("test_ws")

        assert len(records) == 2
        assert records[0]["type"] == "run"
        assert records[1]["type"] == "checkpoint"

    def test_list_records_by_type(self) -> None:
        """Can filter records by type."""
        mock_rows = [
            {
                "record_id": "01HXYZ0001",
                "workspace_name": "test_ws",
                "type": "run",
                "stage_run_id": "stage-001",
                "version": "v1",
                "created_at": "2025-01-13T12:00:00Z",
            },
        ]
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = mock_rows
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        records = manager.list_records("test_ws", record_type="run")

        assert len(records) == 1
        # Verify type filter was passed to query
        call_args = mock_conn.execute.call_args
        assert "type = ?" in call_args[0][0] or "run" in call_args[0][1]

    def test_list_records_ordered_by_record_id_desc(self) -> None:
        """Records are ordered by record_id descending (newest first via ULID)."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.list_records("test_ws")

        call_args = mock_conn.execute.call_args
        assert "ORDER BY record_id DESC" in call_args[0][0]

    def test_list_records_by_version(self) -> None:
        """Can filter records by version."""
        mock_rows = [
            {
                "record_id": "01HXYZ0001",
                "workspace_name": "test_ws",
                "type": "run",
                "stage_run_id": "stage-001",
                "version": "v1",
                "created_at": "2025-01-13T12:00:00Z",
            },
        ]
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = mock_rows
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        records = manager.list_records("test_ws", version="v1")

        assert len(records) == 1
        # Verify version filter was passed to query
        call_args = mock_conn.execute.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        assert "version = ?" in query
        assert "v1" in params

    def test_list_records_pagination_params(self) -> None:
        """Pagination parameters are passed to query."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.list_records("test_ws", limit=10, offset=5)

        call_args = mock_conn.execute.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        assert "LIMIT ? OFFSET ?" in query
        # params should end with [limit, offset]
        assert params[-2] == 10
        assert params[-1] == 5


class TestExperimentRecordManagerWithRunResults:
    """Tests for creating run records with associated run_results."""

    def test_create_run_record_initializes_run_results(self) -> None:
        """Creating a run record also initializes run_results with missing status."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.create_run_record(
            workspace_name="test_ws",
            version="v1",
            stage_run_id="stage-abc123",
        )

        # Should have 2 execute calls: one for experiment_records, one for run_results
        assert mock_conn.execute.call_count == 2

        # Check second call is for run_results
        second_call = mock_conn.execute.call_args_list[1]
        assert "INSERT INTO run_results" in second_call[0][0]
        values = second_call[0][1]
        assert values[0] == "stage-abc123"  # stage_run_id
        assert values[2] == "missing"  # results_status
        assert values[3] == "unknown"  # infra_outcome
        assert values[4] == "unknown"  # ml_outcome

    def test_create_checkpoint_record_does_not_create_run_results(self) -> None:
        """Checkpoint records do not have run_results."""
        mock_conn = MagicMock()
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        manager.create_checkpoint_record(
            workspace_name="test_ws",
            version="v2",
        )

        # Should only have 1 execute call for experiment_records
        assert mock_conn.execute.call_count == 1
        call_args = mock_conn.execute.call_args
        assert "INSERT INTO experiment_records" in call_args[0][0]
        assert "run_results" not in call_args[0][0]


class TestExperimentRecordManagerRunResults:
    """Tests for get_run_results methods."""

    def test_get_run_results_by_stage_run_id(self) -> None:
        """Can retrieve run results by stage_run_id."""
        mock_row = {
            "stage_run_id": "stage-abc123",
            "record_id": "01HXYZ1234567890ABCDEF",
            "results_status": "missing",
            "infra_outcome": "unknown",
            "ml_outcome": "unknown",
            "results_auto": None,
            "results_final": None,
            "comparison": None,
            "finalized_by": None,
            "finalized_at": None,
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_row
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        results = manager.get_run_results("stage-abc123")

        assert results is not None
        assert results["stage_run_id"] == "stage-abc123"
        assert results["results_status"] == "missing"

    def test_get_run_results_not_found(self) -> None:
        """Returns None when run results not found."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        results = manager.get_run_results("nonexistent")

        assert results is None

    def test_get_run_results_by_record_id(self) -> None:
        """Can retrieve run results by record_id."""
        mock_row = {
            "stage_run_id": "stage-abc123",
            "record_id": "01HXYZ1234567890ABCDEF",
            "results_status": "finalized",
            "infra_outcome": "completed",
            "ml_outcome": "success",
            "results_auto": '{"metric": 0.95}',
            "results_final": '{"metric": 0.95}',
            "comparison": None,
            "finalized_by": "ml_claude",
            "finalized_at": "2025-01-13T14:00:00Z",
        }
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = mock_row
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        results = manager.get_run_results_by_record("01HXYZ1234567890ABCDEF")

        assert results is not None
        assert results["record_id"] == "01HXYZ1234567890ABCDEF"
        assert results["ml_outcome"] == "success"

    def test_get_run_results_by_record_not_found(self) -> None:
        """Returns None when record_id not found."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)
        results = manager.get_run_results_by_record("nonexistent-record-id")

        assert results is None


class TestExperimentRecordManagerValidation:
    """Tests for input validation."""

    def test_create_run_record_validates_workspace_name(self) -> None:
        """create_run_record validates workspace_name format."""
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        with pytest.raises(InvalidWorkspaceNameError):
            manager.create_run_record(
                workspace_name="invalid workspace!",  # spaces and ! not allowed
                version="v1",
                stage_run_id="stage-abc123",
            )

    def test_create_checkpoint_record_validates_workspace_name(self) -> None:
        """create_checkpoint_record validates workspace_name format."""
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        with pytest.raises(InvalidWorkspaceNameError):
            manager.create_checkpoint_record(
                workspace_name="../traversal",  # path traversal not allowed
                version="v1",
            )

    def test_list_records_validates_workspace_name(self) -> None:
        """list_records validates workspace_name format."""
        mock_db = MagicMock()
        mock_db._conn.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

        manager = ExperimentRecordManager(mock_db)

        with pytest.raises(InvalidWorkspaceNameError):
            manager.list_records(workspace_name="invalid workspace!")


class TestCrockfordAlphabet:
    """Tests for Crockford's Base32 alphabet compliance."""

    def test_generated_ids_use_only_valid_crockford_chars(self) -> None:
        """Generated IDs only contain valid Crockford's Base32 characters."""
        # Full Crockford's Base32 alphabet (excludes I, L, O, U)
        valid_chars = set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")

        for _ in range(100):
            record_id = generate_record_id()
            for char in record_id:
                assert char in valid_chars, f"ID {record_id} contains invalid char '{char}'"

    def test_generated_ids_exclude_forbidden_chars(self) -> None:
        """Generated IDs never contain I, L, O, U (Crockford's excluded chars)."""
        forbidden_chars = set("ILOU")

        for _ in range(100):
            record_id = generate_record_id()
            id_chars = set(record_id.upper())
            assert not id_chars & forbidden_chars, f"ID {record_id} contains forbidden chars"

    def test_generated_ids_are_uppercase(self) -> None:
        """Generated IDs are uppercase."""
        for _ in range(10):
            record_id = generate_record_id()
            assert record_id == record_id.upper()
