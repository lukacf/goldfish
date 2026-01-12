"""Integration tests for backup flow."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from goldfish.backup.manager import BackupManager
from goldfish.db.database import Database


@pytest.fixture
def test_db(tmp_path: Path) -> Database:
    """Create a test database with schema."""
    db_path = tmp_path / "goldfish.db"
    return Database(db_path)


@pytest.fixture
def backup_manager(test_db: Database, tmp_path: Path) -> BackupManager:
    """Create a BackupManager with test database."""
    # Create a separate file to backup (not the tracking database)
    backup_source = tmp_path / "backup_source.db"
    backup_source.write_bytes(b"test database content for backup")

    return BackupManager(
        db=test_db,
        db_path=backup_source,
        gcs_bucket="gs://test-bucket/backups",
    )


class TestBackupFlowIntegration:
    """Integration tests for backup creation and management."""

    @patch("goldfish.backup.manager.subprocess.run")
    def test_backup_tier_promotion_monthly(
        self, mock_run: MagicMock, backup_manager: BackupManager, test_db: Database
    ) -> None:
        """First backup should be promoted to monthly tier."""
        mock_run.return_value = MagicMock(returncode=0)

        result = backup_manager.create_backup(trigger="run")

        assert result is not None
        assert result["tier"] == "monthly"

        # Verify database record
        backup = test_db.get_backup(result["backup_id"])
        assert backup is not None
        assert backup["tier"] == "monthly"

    @patch("goldfish.backup.manager.subprocess.run")
    def test_backup_tier_promotion_weekly(
        self, mock_run: MagicMock, backup_manager: BackupManager, test_db: Database
    ) -> None:
        """Second backup (after monthly) should be weekly tier."""
        mock_run.return_value = MagicMock(returncode=0)
        now = datetime.now(UTC)

        # Create a monthly backup
        test_db.insert_backup(
            backup_id="backup-monthly-existing",
            tier="monthly",
            trigger="run",
            gcs_path="gs://test/monthly/2026-01.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(days=365)).isoformat(),
        )

        result = backup_manager.create_backup(trigger="run")

        assert result is not None
        assert result["tier"] == "weekly"

    @patch("goldfish.backup.manager.subprocess.run")
    def test_backup_tier_promotion_daily(
        self, mock_run: MagicMock, backup_manager: BackupManager, test_db: Database
    ) -> None:
        """Third backup (after monthly and weekly) should be daily tier."""
        mock_run.return_value = MagicMock(returncode=0)
        now = datetime.now(UTC)

        # Create monthly backup
        test_db.insert_backup(
            backup_id="backup-monthly",
            tier="monthly",
            trigger="run",
            gcs_path="gs://test/monthly/2026-01.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(days=365)).isoformat(),
        )

        # Create weekly backup
        test_db.insert_backup(
            backup_id="backup-weekly",
            tier="weekly",
            trigger="run",
            gcs_path="gs://test/weekly/2026-W02.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(days=30)).isoformat(),
        )

        result = backup_manager.create_backup(trigger="run")

        assert result is not None
        assert result["tier"] == "daily"

    @patch("goldfish.backup.manager.subprocess.run")
    def test_backup_tier_event_when_all_tiers_exist(
        self, mock_run: MagicMock, backup_manager: BackupManager, test_db: Database
    ) -> None:
        """Backup should be event tier when all tiers exist for current period."""
        mock_run.return_value = MagicMock(returncode=0)
        now = datetime.now(UTC)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        # Create monthly backup
        test_db.insert_backup(
            backup_id="backup-monthly",
            tier="monthly",
            trigger="run",
            gcs_path="gs://test/monthly/2026-01.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(days=365)).isoformat(),
        )

        # Create weekly backup
        test_db.insert_backup(
            backup_id="backup-weekly",
            tier="weekly",
            trigger="run",
            gcs_path="gs://test/weekly/2026-W02.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(days=30)).isoformat(),
        )

        # Create daily backup for today
        test_db.insert_backup(
            backup_id="backup-daily",
            tier="daily",
            trigger="run",
            gcs_path="gs://test/daily/2026-01-11.db.gz",
            created_at=(today_start + timedelta(hours=1)).isoformat(),
            expires_at=(now + timedelta(days=7)).isoformat(),
        )

        result = backup_manager.create_backup(trigger="run")

        assert result is not None
        assert result["tier"] == "event"

    @patch("goldfish.backup.manager.subprocess.run")
    def test_backup_gcs_path_format(self, mock_run: MagicMock, backup_manager: BackupManager) -> None:
        """Backup GCS path should match expected format."""
        mock_run.return_value = MagicMock(returncode=0)

        result = backup_manager.create_backup(trigger="run")

        assert result is not None
        gcs_path = result["gcs_path"]
        assert gcs_path.startswith("gs://test-bucket/backups/")
        assert gcs_path.endswith(".db.gz")

    @patch("goldfish.backup.manager.subprocess.run")
    def test_backup_records_size(self, mock_run: MagicMock, backup_manager: BackupManager, test_db: Database) -> None:
        """Backup should record compressed size."""
        mock_run.return_value = MagicMock(returncode=0)

        result = backup_manager.create_backup(trigger="run")

        assert result is not None
        assert "size_bytes" in result
        assert result["size_bytes"] > 0

        # Verify in database
        backup = test_db.get_backup(result["backup_id"])
        assert backup is not None
        assert backup["size_bytes"] is not None
        assert backup["size_bytes"] > 0


class TestBackupRateLimitingIntegration:
    """Integration tests for backup rate limiting."""

    @patch("goldfish.backup.manager.subprocess.run")
    def test_rate_limit_blocks_frequent_backups(
        self, mock_run: MagicMock, backup_manager: BackupManager, test_db: Database
    ) -> None:
        """maybe_backup should be rate limited."""
        mock_run.return_value = MagicMock(returncode=0)
        now = datetime.now(UTC)

        # Create a recent backup (1 minute ago)
        test_db.insert_backup(
            backup_id="backup-recent",
            tier="monthly",
            trigger="run",
            gcs_path="gs://test/event/recent.db.gz",
            created_at=(now - timedelta(minutes=1)).isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )

        result = backup_manager.maybe_backup(trigger="run")

        assert result is None  # Skipped due to rate limiting

    @patch("goldfish.backup.manager.subprocess.run")
    def test_rate_limit_allows_after_interval(
        self, mock_run: MagicMock, backup_manager: BackupManager, test_db: Database
    ) -> None:
        """maybe_backup should proceed after rate limit interval."""
        mock_run.return_value = MagicMock(returncode=0)
        now = datetime.now(UTC)

        # Create an old backup (10 minutes ago)
        test_db.insert_backup(
            backup_id="backup-old",
            tier="monthly",
            trigger="run",
            gcs_path="gs://test/event/old.db.gz",
            created_at=(now - timedelta(minutes=10)).isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )

        result = backup_manager.maybe_backup(trigger="run")

        assert result is not None  # Backup created

    @patch("goldfish.backup.manager.subprocess.run")
    def test_high_value_trigger_bypasses_rate_limit(
        self, mock_run: MagicMock, backup_manager: BackupManager, test_db: Database
    ) -> None:
        """High-value triggers should bypass rate limiting."""
        mock_run.return_value = MagicMock(returncode=0)
        now = datetime.now(UTC)

        # Create a recent backup (1 minute ago)
        test_db.insert_backup(
            backup_id="backup-recent",
            tier="monthly",
            trigger="run",
            gcs_path="gs://test/event/recent.db.gz",
            created_at=(now - timedelta(minutes=1)).isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )

        # High-value trigger should bypass rate limiting
        result = backup_manager.maybe_backup(trigger="create_workspace")

        assert result is not None  # Backup created despite recent backup


class TestBackupCleanupIntegration:
    """Integration tests for backup cleanup."""

    @patch("goldfish.backup.manager.subprocess.run")
    def test_cleanup_marks_expired_deleted(
        self, mock_run: MagicMock, backup_manager: BackupManager, test_db: Database
    ) -> None:
        """Cleanup should mark expired backups as deleted."""
        mock_run.return_value = MagicMock(returncode=0)
        now = datetime.now(UTC)

        # Create an expired backup
        test_db.insert_backup(
            backup_id="backup-expired",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/expired.db.gz",
            created_at=(now - timedelta(days=2)).isoformat(),
            expires_at=(now - timedelta(hours=1)).isoformat(),
        )

        # Create a non-expired backup
        test_db.insert_backup(
            backup_id="backup-valid",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/valid.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )

        result = backup_manager.cleanup()

        assert result["deleted_expired"] == 1

        # Verify expired backup is marked deleted
        expired = test_db.get_backup("backup-expired")
        assert expired is not None
        assert expired["deleted_at"] is not None

        # Verify valid backup is not marked deleted
        valid = test_db.get_backup("backup-valid")
        assert valid is not None
        assert valid["deleted_at"] is None

    @patch("goldfish.backup.manager.subprocess.run")
    def test_cleanup_enforces_event_limit(
        self, mock_run: MagicMock, backup_manager: BackupManager, test_db: Database
    ) -> None:
        """Cleanup should enforce max 20 event backups."""
        mock_run.return_value = MagicMock(returncode=0)
        now = datetime.now(UTC)

        # Create 25 event backups
        for i in range(25):
            created = now - timedelta(hours=i)
            test_db.insert_backup(
                backup_id=f"backup-event-{i:02d}",
                tier="event",
                trigger="run",
                gcs_path=f"gs://test/event/backup-{i:02d}.db.gz",
                created_at=created.isoformat(),
                expires_at=(now + timedelta(hours=24)).isoformat(),
            )

        result = backup_manager.cleanup()

        assert result["deleted_excess"] == 5  # 25 - 20 = 5

        # Verify only 20 active event backups remain
        active = test_db.list_backups(tier="event")
        assert len(active) == 20


class TestBackupListingIntegration:
    """Integration tests for backup listing."""

    def test_list_backups_by_tier(self, backup_manager: BackupManager, test_db: Database) -> None:
        """list_backups should filter by tier."""
        now = datetime.now(UTC)

        # Create backups of different tiers
        for tier in ["event", "daily", "weekly", "monthly"]:
            test_db.insert_backup(
                backup_id=f"backup-{tier}",
                tier=tier,
                trigger="run",
                gcs_path=f"gs://test/{tier}/backup.db.gz",
                created_at=now.isoformat(),
                expires_at=(now + timedelta(days=7)).isoformat(),
            )

        # Filter by tier
        events = backup_manager.list_backups(tier="event")
        assert len(events) == 1
        assert events[0]["tier"] == "event"

        # All tiers
        all_backups = backup_manager.list_backups()
        assert len(all_backups) == 4

    def test_list_backups_excludes_deleted(self, backup_manager: BackupManager, test_db: Database) -> None:
        """list_backups should exclude deleted by default."""
        now = datetime.now(UTC)

        test_db.insert_backup(
            backup_id="backup-active",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/active.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(days=1)).isoformat(),
        )
        test_db.insert_backup(
            backup_id="backup-deleted",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/deleted.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(days=1)).isoformat(),
        )
        test_db.mark_backup_deleted("backup-deleted")

        backups = backup_manager.list_backups()
        assert len(backups) == 1
        assert backups[0]["backup_id"] == "backup-active"

        # Include deleted
        all_backups = backup_manager.list_backups(include_deleted=True)
        assert len(all_backups) == 2
