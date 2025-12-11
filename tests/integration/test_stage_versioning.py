"""Tests for stage versioning - tracking unique (code + config) per stage.

TDD: Write failing tests first, then implement.
"""

import threading
import time

from goldfish.db.database import Database


class TestStageVersionSchema:
    """Tests for stage_versions table and schema migrations."""

    def test_stage_versions_table_exists(self, test_db):
        """stage_versions table should be created on DB init."""
        with test_db._conn() as conn:
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='stage_versions'"
            ).fetchone()
        assert result is not None

    def test_stage_runs_has_stage_version_id_column(self, test_db):
        """stage_runs table should have stage_version_id column."""
        with test_db._conn() as conn:
            columns = [row[1] for row in conn.execute("PRAGMA table_info(stage_runs)")]
        assert "stage_version_id" in columns

    def test_signal_lineage_has_source_columns(self, test_db):
        """signal_lineage table should have source tracking columns."""
        with test_db._conn() as conn:
            columns = [row[1] for row in conn.execute("PRAGMA table_info(signal_lineage)")]
        assert "source_stage_run_id" in columns
        assert "source_stage_version_id" in columns


class TestStageVersionCreation:
    """Tests for get_or_create_stage_version."""

    def test_create_stage_version_record(self, test_db):
        """Should create a new stage version record."""
        # Need workspace first (FK constraint)
        test_db.create_workspace_lineage("test-ws", description="Test workspace")

        stage_version_id, version_num, is_new = test_db.get_or_create_stage_version(
            workspace="test-ws",
            stage="preprocess",
            git_sha="abc123def456",
            config_hash="fedcba987654321fedcba987654321fedcba987654321fedcba987654321aaaa",
        )

        assert stage_version_id > 0
        assert version_num == 1
        assert is_new is True

    def test_stage_version_auto_increments_per_stage(self, test_db):
        """Version numbers should auto-increment per stage."""
        test_db.create_workspace_lineage("test-ws", description="Test")

        # First version of preprocess
        _, v1, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha1", "config1" + "0" * 56)

        # Second version of preprocess (different sha)
        _, v2, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha2", "config1" + "0" * 56)

        # First version of train (different stage)
        _, v3, _ = test_db.get_or_create_stage_version("test-ws", "train", "sha1", "config1" + "0" * 56)

        assert v1 == 1
        assert v2 == 2  # Incremented for preprocess
        assert v3 == 1  # Separate sequence for train

    def test_same_sha_and_config_reuses_version(self, test_db):
        """Same git_sha + config_hash should return existing version."""
        test_db.create_workspace_lineage("test-ws", description="Test")

        config_hash = "a" * 64

        # Create first version
        id1, v1, is_new1 = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha123", config_hash)

        # Same inputs should return same version
        id2, v2, is_new2 = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha123", config_hash)

        assert id1 == id2
        assert v1 == v2
        assert is_new1 is True
        assert is_new2 is False

    def test_different_config_creates_new_version(self, test_db):
        """Different config_hash with same sha should create new version."""
        test_db.create_workspace_lineage("test-ws", description="Test")

        id1, v1, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "same-sha", "a" * 64)

        id2, v2, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "same-sha", "b" * 64)

        assert id1 != id2
        assert v1 == 1
        assert v2 == 2

    def test_different_sha_creates_new_version(self, test_db):
        """Different git_sha with same config should create new version."""
        test_db.create_workspace_lineage("test-ws", description="Test")

        config = "c" * 64

        id1, v1, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha-1", config)

        id2, v2, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha-2", config)

        assert id1 != id2
        assert v1 == 1
        assert v2 == 2


class TestStageVersionQueries:
    """Tests for stage version query methods."""

    def test_get_stage_version_by_run_id(self, test_db):
        """Should get stage version info for a run."""
        test_db.create_workspace_lineage("test-ws", description="Test")
        test_db.create_version("test-ws", "v1", "test-ws-v1", "sha123", "run")

        # Create stage version
        sv_id, sv_num, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha123", "d" * 64)

        # Create stage run linked to version
        test_db.create_stage_run(
            stage_run_id="stage-abc",
            workspace_name="test-ws",
            version="v1",
            stage_name="preprocess",
        )
        test_db.update_stage_run_version("stage-abc", sv_id)

        # Query
        stage_version = test_db.get_stage_version_for_run("stage-abc")
        assert stage_version is not None
        assert stage_version["version_num"] == sv_num
        assert stage_version["git_sha"] == "sha123"

    def test_list_stage_versions_for_workspace(self, test_db):
        """Should list all stage versions in a workspace."""
        test_db.create_workspace_lineage("test-ws", description="Test")

        # Create versions for different stages
        test_db.get_or_create_stage_version("test-ws", "preprocess", "sha1", "e" * 64)
        test_db.get_or_create_stage_version("test-ws", "preprocess", "sha2", "f" * 64)
        test_db.get_or_create_stage_version("test-ws", "train", "sha1", "g" * 64)

        # List all
        versions = test_db.list_stage_versions("test-ws")
        assert len(versions) == 3

        # List filtered by stage
        preprocess_versions = test_db.list_stage_versions("test-ws", stage="preprocess")
        assert len(preprocess_versions) == 2

        train_versions = test_db.list_stage_versions("test-ws", stage="train")
        assert len(train_versions) == 1

    def test_get_stage_version_by_number(self, test_db):
        """Should get specific stage version by number."""
        test_db.create_workspace_lineage("test-ws", description="Test")

        test_db.get_or_create_stage_version("test-ws", "preprocess", "sha1", "h" * 64)
        test_db.get_or_create_stage_version("test-ws", "preprocess", "sha2", "i" * 64)

        # Get v1
        v1 = test_db.get_stage_version("test-ws", "preprocess", 1)
        assert v1 is not None
        assert v1["git_sha"] == "sha1"

        # Get v2
        v2 = test_db.get_stage_version("test-ws", "preprocess", 2)
        assert v2 is not None
        assert v2["git_sha"] == "sha2"

        # Non-existent
        v3 = test_db.get_stage_version("test-ws", "preprocess", 99)
        assert v3 is None


class TestConcurrentVersionCreation:
    """Tests for race condition handling."""

    def test_concurrent_version_creation_handles_race(self, temp_dir):
        """Concurrent creation of same version should not fail."""
        db_path = temp_dir / "concurrent.db"
        db = Database(db_path)
        db.create_workspace_lineage("test-ws", description="Test")

        results = []
        errors = []

        def create_version():
            try:
                # Small random delay to increase race likelihood
                time.sleep(0.001)
                result = db.get_or_create_stage_version("test-ws", "preprocess", "same-sha", "j" * 64)
                results.append(result)
            except Exception as e:
                errors.append(e)

        # Launch multiple threads
        threads = [threading.Thread(target=create_version) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # No errors
        assert len(errors) == 0, f"Errors: {errors}"

        # All should get same ID and version
        ids = [r[0] for r in results]
        versions = [r[1] for r in results]
        assert len(set(ids)) == 1, "All threads should get same ID"
        assert len(set(versions)) == 1, "All threads should get same version"
        assert versions[0] == 1


class TestMigrationPreservesData:
    """Tests for schema migration safety."""

    def test_migration_is_idempotent(self, temp_dir):
        """Multiple DB initializations should not break schema."""
        db_path = temp_dir / "migrate.db"

        # First init
        db1 = Database(db_path)
        db1.create_workspace_lineage("test-ws", description="Test")
        db1.get_or_create_stage_version("test-ws", "preprocess", "sha1", "k" * 64)

        # Reinitialize (simulates restart/upgrade)
        db2 = Database(db_path)

        # Data should still be there
        versions = db2.list_stage_versions("test-ws")
        assert len(versions) == 1
        assert versions[0]["git_sha"] == "sha1"
