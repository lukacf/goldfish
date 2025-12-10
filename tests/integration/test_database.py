"""Tests for database transaction management - P1.

TDD: Write failing tests first, then implement.
"""

import sqlite3

import pytest

from goldfish.db.database import Database


class TestTransactionManagement:
    """Tests for database transaction context manager."""

    def test_transaction_context_manager_exists(self, temp_dir):
        """Database should have a transaction context manager."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # Should have transaction method
        assert hasattr(db, "transaction")

    def test_transaction_commits_on_success(self, temp_dir):
        """Transaction should commit when block completes normally."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        with db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sources (id, name, created_at, created_by, gcs_location, status)
                VALUES ('test-source', 'Test', '2024-01-01', 'test', 'gs://bucket', 'available')
                """
            )

        # Should be persisted
        source = db.get_source("test-source")
        assert source is not None
        assert source["name"] == "Test"

    def test_transaction_rolls_back_on_exception(self, temp_dir):
        """Transaction should rollback when exception is raised."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        with pytest.raises(ValueError):
            with db.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO sources (id, name, created_at, created_by, gcs_location, status)
                    VALUES ('test-source', 'Test', '2024-01-01', 'test', 'gs://bucket', 'available')
                    """
                )
                raise ValueError("Intentional failure")

        # Should NOT be persisted
        source = db.get_source("test-source")
        assert source is None

    def test_transaction_allows_multiple_operations(self, temp_dir):
        """Multiple operations in transaction should all succeed or all fail."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        with db.transaction() as conn:
            # Create a source
            conn.execute(
                """
                INSERT INTO sources (id, name, created_at, created_by, gcs_location, status)
                VALUES ('source-1', 'Source 1', '2024-01-01', 'test', 'gs://bucket/1', 'available')
                """
            )
            # Create another source
            conn.execute(
                """
                INSERT INTO sources (id, name, created_at, created_by, gcs_location, status)
                VALUES ('source-2', 'Source 2', '2024-01-01', 'test', 'gs://bucket/2', 'available')
                """
            )
            # Add lineage
            conn.execute(
                """
                INSERT INTO source_lineage (source_id, parent_source_id, created_at)
                VALUES ('source-2', 'source-1', '2024-01-01')
                """
            )

        # All should be persisted
        assert db.get_source("source-1") is not None
        assert db.get_source("source-2") is not None
        lineage = db.get_lineage("source-2")
        assert len(lineage) == 1
        assert lineage[0]["parent_source_id"] == "source-1"

    def test_partial_failure_rolls_back_all(self, temp_dir):
        """If later operation fails, earlier ones should be rolled back."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # First, create source-1 outside transaction to test rollback
        db.create_source(
            source_id="existing-source",
            name="Existing",
            gcs_location="gs://bucket/existing",
            created_by="test",
        )

        with pytest.raises(sqlite3.IntegrityError):
            with db.transaction() as conn:
                # Create a new source
                conn.execute(
                    """
                    INSERT INTO sources (id, name, created_at, created_by, gcs_location, status)
                    VALUES ('new-source', 'New', '2024-01-01', 'test', 'gs://bucket/new', 'available')
                    """
                )
                # Try to create duplicate (should fail)
                conn.execute(
                    """
                    INSERT INTO sources (id, name, created_at, created_by, gcs_location, status)
                    VALUES ('existing-source', 'Dupe', '2024-01-01', 'test', 'gs://bucket/dupe', 'available')
                    """
                )

        # new-source should NOT exist due to rollback
        source = db.get_source("new-source")
        assert source is None

        # existing-source should still have original values
        existing = db.get_source("existing-source")
        assert existing["name"] == "Existing"


class TestDatabaseOperationHelpers:
    """Tests for helper methods that use transactions internally."""

    def test_create_job_with_inputs_atomic(self, temp_dir):
        """Creating job and adding inputs should be atomic."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # Create a source first
        db.create_source(
            source_id="input-source",
            name="Input",
            gcs_location="gs://bucket/input",
            created_by="test",
        )

        # Test atomic creation
        db.create_job_with_inputs(
            job_id="job-a1b2c3d4",
            workspace="test-ws",
            snapshot_id="snap-abc",
            script="run.py",
            experiment_dir="/exp/test",
            inputs={"raw": "input-source"},
        )

        job = db.get_job("job-a1b2c3d4")
        assert job is not None

        inputs = db.get_job_inputs("job-a1b2c3d4")
        assert len(inputs) == 1
        assert inputs[0]["source_id"] == "input-source"
        assert inputs[0]["input_name"] == "raw"

    def test_create_job_with_inputs_rolls_back_on_failure(self, temp_dir):
        """If adding inputs fails, job creation should be rolled back."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # Don't create the source - this should cause failure (FK constraint)
        with pytest.raises(sqlite3.IntegrityError):
            db.create_job_with_inputs(
                job_id="job-b5c6d7e8",
                workspace="test-ws",
                snapshot_id="snap-def",
                script="run.py",
                experiment_dir="/exp/test",
                inputs={"raw": "nonexistent-source"},  # Should fail FK constraint
            )

        # Job should NOT exist
        job = db.get_job("job-b5c6d7e8")
        assert job is None


class TestDatabaseInitialization:
    """Tests for database initialization."""

    def test_creates_directory_if_not_exists(self, temp_dir):
        """Should create parent directory for database file."""
        db_path = temp_dir / "nested" / "deep" / "test.db"
        db = Database(db_path)

        assert db_path.parent.exists()

    def test_initializes_schema(self, temp_dir):
        """Should initialize schema on creation."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # Should have expected tables
        with db._conn() as conn:
            tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            table_names = {t[0] for t in tables}

            assert "audit" in table_names
            assert "sources" in table_names
            assert "jobs" in table_names
            assert "source_lineage" in table_names
            assert "job_inputs" in table_names

    def test_foreign_keys_enabled(self, temp_dir):
        """Foreign key constraints should be enabled."""
        db_path = temp_dir / "test.db"
        db = Database(db_path)

        # Try to add job_input for non-existent job (should fail)
        with pytest.raises(sqlite3.IntegrityError):
            with db._conn() as conn:
                conn.execute(
                    "INSERT INTO job_inputs (job_id, source_id, input_name) VALUES ('fake-job', 'fake-source', 'test')"
                )
