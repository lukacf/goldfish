"""Unit tests for backup database CRUD methods."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest


@pytest.fixture
def test_db(tmp_path: Path):
    """Create a test database with schema."""
    from goldfish.db.database import Database

    db_path = tmp_path / "test.db"
    return Database(db_path)


class TestInsertBackup:
    """Tests for insert_backup."""

    def test_insert_backup_creates_record(self, test_db):
        """insert_backup should create a new record."""
        now = datetime.now(UTC)
        test_db.insert_backup(
            backup_id="backup-abc12345",
            tier="event",
            trigger="run",
            gcs_path="gs://test-bucket/backups/event/backup.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )

        result = test_db.get_backup("backup-abc12345")
        assert result is not None
        assert result["backup_id"] == "backup-abc12345"
        assert result["tier"] == "event"
        assert result["trigger"] == "run"

    def test_insert_backup_with_details(self, test_db):
        """insert_backup should store trigger details."""
        now = datetime.now(UTC)
        test_db.insert_backup(
            backup_id="backup-xyz12345",
            tier="daily",
            trigger="save_version",
            gcs_path="gs://test-bucket/backups/daily/2026-01-11.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(days=7)).isoformat(),
            trigger_details={"workspace": "baseline", "version": "v5"},
            size_bytes=1024000,
        )

        result = test_db.get_backup("backup-xyz12345")
        assert result is not None
        assert "baseline" in result["trigger_details_json"]
        assert result["size_bytes"] == 1024000


class TestGetBackup:
    """Tests for get_backup."""

    def test_get_nonexistent_returns_none(self, test_db):
        """get_backup should return None for unknown backup."""
        result = test_db.get_backup("backup-00000000")
        assert result is None

    def test_get_returns_all_fields(self, test_db):
        """get_backup should return all fields."""
        now = datetime.now(UTC)
        test_db.insert_backup(
            backup_id="backup-fields",
            tier="weekly",
            trigger="manual",
            gcs_path="gs://test-bucket/backups/weekly/2026-W02.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(days=30)).isoformat(),
        )

        result = test_db.get_backup("backup-fields")
        assert result is not None
        assert "backup_id" in result
        assert "tier" in result
        assert "trigger" in result
        assert "trigger_details_json" in result
        assert "gcs_path" in result
        assert "size_bytes" in result
        assert "created_at" in result
        assert "expires_at" in result
        assert "deleted_at" in result


class TestListBackups:
    """Tests for list_backups."""

    def test_list_empty(self, test_db):
        """list_backups should return empty list when no backups."""
        result = test_db.list_backups()
        assert result == []

    def test_list_returns_all(self, test_db):
        """list_backups should return all backups."""
        now = datetime.now(UTC)
        for i in range(3):
            test_db.insert_backup(
                backup_id=f"backup-list{i:04d}",
                tier="event",
                trigger="run",
                gcs_path=f"gs://test/event/backup-{i}.db.gz",
                created_at=now.isoformat(),
                expires_at=(now + timedelta(hours=24)).isoformat(),
            )

        result = test_db.list_backups()
        assert len(result) == 3

    def test_list_filter_by_tier(self, test_db):
        """list_backups should filter by tier."""
        now = datetime.now(UTC)
        test_db.insert_backup(
            backup_id="backup-event",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/backup.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )
        test_db.insert_backup(
            backup_id="backup-daily",
            tier="daily",
            trigger="run",
            gcs_path="gs://test/daily/backup.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(days=7)).isoformat(),
        )

        events = test_db.list_backups(tier="event")
        assert len(events) == 1
        assert events[0]["tier"] == "event"

        daily = test_db.list_backups(tier="daily")
        assert len(daily) == 1
        assert daily[0]["tier"] == "daily"

    def test_list_excludes_deleted_by_default(self, test_db):
        """list_backups should exclude deleted backups by default."""
        now = datetime.now(UTC)
        test_db.insert_backup(
            backup_id="backup-active",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/active.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )
        test_db.insert_backup(
            backup_id="backup-deleted",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/deleted.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )
        test_db.mark_backup_deleted("backup-deleted")

        result = test_db.list_backups()
        assert len(result) == 1
        assert result[0]["backup_id"] == "backup-active"

    def test_list_includes_deleted_when_requested(self, test_db):
        """list_backups should include deleted when include_deleted=True."""
        now = datetime.now(UTC)
        test_db.insert_backup(
            backup_id="backup-active",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/active.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )
        test_db.insert_backup(
            backup_id="backup-deleted",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/deleted.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )
        test_db.mark_backup_deleted("backup-deleted")

        result = test_db.list_backups(include_deleted=True)
        assert len(result) == 2

    def test_list_orders_by_created_at_desc(self, test_db):
        """list_backups should order by created_at descending."""
        now = datetime.now(UTC)
        for i in range(3):
            created = now - timedelta(hours=i)
            test_db.insert_backup(
                backup_id=f"backup-order{i}",
                tier="event",
                trigger="run",
                gcs_path=f"gs://test/event/backup-{i}.db.gz",
                created_at=created.isoformat(),
                expires_at=(now + timedelta(hours=24)).isoformat(),
            )

        result = test_db.list_backups()
        assert result[0]["backup_id"] == "backup-order0"  # Most recent
        assert result[2]["backup_id"] == "backup-order2"  # Oldest

    def test_list_with_limit(self, test_db):
        """list_backups should respect limit parameter."""
        now = datetime.now(UTC)
        for i in range(5):
            test_db.insert_backup(
                backup_id=f"backup-limit{i}",
                tier="event",
                trigger="run",
                gcs_path=f"gs://test/event/backup-{i}.db.gz",
                created_at=now.isoformat(),
                expires_at=(now + timedelta(hours=24)).isoformat(),
            )

        result = test_db.list_backups(limit=3)
        assert len(result) == 3


class TestMarkBackupDeleted:
    """Tests for mark_backup_deleted."""

    def test_mark_deleted_sets_deleted_at(self, test_db):
        """mark_backup_deleted should set deleted_at timestamp."""
        now = datetime.now(UTC)
        test_db.insert_backup(
            backup_id="backup-todelete",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/todelete.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )

        result = test_db.mark_backup_deleted("backup-todelete")
        assert result is True

        backup = test_db.get_backup("backup-todelete")
        assert backup is not None
        assert backup["deleted_at"] is not None

    def test_mark_deleted_nonexistent_returns_false(self, test_db):
        """mark_backup_deleted should return False for unknown backup."""
        result = test_db.mark_backup_deleted("backup-00000000")
        assert result is False


class TestGetLastBackup:
    """Tests for get_last_backup."""

    def test_get_last_backup_returns_most_recent(self, test_db):
        """get_last_backup should return the most recent backup."""
        now = datetime.now(UTC)
        for i in range(3):
            created = now - timedelta(hours=i)
            test_db.insert_backup(
                backup_id=f"backup-last{i}",
                tier="event",
                trigger="run",
                gcs_path=f"gs://test/event/backup-{i}.db.gz",
                created_at=created.isoformat(),
                expires_at=(now + timedelta(hours=24)).isoformat(),
            )

        result = test_db.get_last_backup()
        assert result is not None
        assert result["backup_id"] == "backup-last0"

    def test_get_last_backup_filters_by_tier(self, test_db):
        """get_last_backup should filter by tier."""
        now = datetime.now(UTC)
        test_db.insert_backup(
            backup_id="backup-event",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/backup.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )
        test_db.insert_backup(
            backup_id="backup-daily",
            tier="daily",
            trigger="run",
            gcs_path="gs://test/daily/backup.db.gz",
            created_at=(now - timedelta(hours=1)).isoformat(),
            expires_at=(now + timedelta(days=7)).isoformat(),
        )

        result = test_db.get_last_backup(tier="daily")
        assert result is not None
        assert result["backup_id"] == "backup-daily"

    def test_get_last_backup_empty_returns_none(self, test_db):
        """get_last_backup should return None when no backups exist."""
        result = test_db.get_last_backup()
        assert result is None

    def test_get_last_backup_excludes_deleted(self, test_db):
        """get_last_backup should exclude deleted backups."""
        now = datetime.now(UTC)
        test_db.insert_backup(
            backup_id="backup-deleted",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/deleted.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )
        test_db.insert_backup(
            backup_id="backup-active",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/active.db.gz",
            created_at=(now - timedelta(hours=1)).isoformat(),
            expires_at=(now + timedelta(hours=23)).isoformat(),
        )
        test_db.mark_backup_deleted("backup-deleted")

        result = test_db.get_last_backup()
        assert result is not None
        assert result["backup_id"] == "backup-active"


class TestGetExpiredBackups:
    """Tests for get_expired_backups."""

    def test_get_expired_returns_expired_only(self, test_db):
        """get_expired_backups should return only expired backups."""
        now = datetime.now(UTC)
        # Expired backup
        test_db.insert_backup(
            backup_id="backup-expired",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/expired.db.gz",
            created_at=(now - timedelta(days=2)).isoformat(),
            expires_at=(now - timedelta(hours=1)).isoformat(),
        )
        # Non-expired backup
        test_db.insert_backup(
            backup_id="backup-valid",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/valid.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )

        result = test_db.get_expired_backups()
        assert len(result) == 1
        assert result[0]["backup_id"] == "backup-expired"

    def test_get_expired_excludes_already_deleted(self, test_db):
        """get_expired_backups should exclude already deleted backups."""
        now = datetime.now(UTC)
        test_db.insert_backup(
            backup_id="backup-expired-deleted",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/expired-deleted.db.gz",
            created_at=(now - timedelta(days=2)).isoformat(),
            expires_at=(now - timedelta(hours=1)).isoformat(),
        )
        test_db.mark_backup_deleted("backup-expired-deleted")

        result = test_db.get_expired_backups()
        assert len(result) == 0


class TestCountBackupsByTier:
    """Tests for count_backups_by_tier."""

    def test_count_returns_correct_counts(self, test_db):
        """count_backups_by_tier should return correct counts per tier."""
        now = datetime.now(UTC)
        # Create 3 event, 2 daily, 1 weekly
        for i in range(3):
            test_db.insert_backup(
                backup_id=f"backup-event{i}",
                tier="event",
                trigger="run",
                gcs_path=f"gs://test/event/backup-{i}.db.gz",
                created_at=now.isoformat(),
                expires_at=(now + timedelta(hours=24)).isoformat(),
            )
        for i in range(2):
            test_db.insert_backup(
                backup_id=f"backup-daily{i}",
                tier="daily",
                trigger="run",
                gcs_path=f"gs://test/daily/backup-{i}.db.gz",
                created_at=now.isoformat(),
                expires_at=(now + timedelta(days=7)).isoformat(),
            )
        test_db.insert_backup(
            backup_id="backup-weekly0",
            tier="weekly",
            trigger="run",
            gcs_path="gs://test/weekly/backup.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(days=30)).isoformat(),
        )

        result = test_db.count_backups_by_tier()
        assert result["event"] == 3
        assert result["daily"] == 2
        assert result["weekly"] == 1
        assert result.get("monthly", 0) == 0

    def test_count_excludes_deleted(self, test_db):
        """count_backups_by_tier should exclude deleted backups."""
        now = datetime.now(UTC)
        test_db.insert_backup(
            backup_id="backup-active",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/active.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )
        test_db.insert_backup(
            backup_id="backup-deleted",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/deleted.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )
        test_db.mark_backup_deleted("backup-deleted")

        result = test_db.count_backups_by_tier()
        assert result["event"] == 1
