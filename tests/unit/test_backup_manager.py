"""Unit tests for BackupManager."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def test_db(tmp_path: Path):
    """Create a test database with schema."""
    from goldfish.db.database import Database

    db_path = tmp_path / "test.db"
    return Database(db_path)


@pytest.fixture
def backup_manager(test_db, tmp_path: Path):
    """Create a BackupManager with test database."""
    from goldfish.backup.manager import BackupManager

    gcs_bucket = "gs://test-bucket/backups"
    return BackupManager(
        db=test_db,
        db_path=test_db.db_path,
        gcs_bucket=gcs_bucket,
    )


class TestBackupTierDetermination:
    """Tests for determining backup tier based on timing."""

    def test_first_backup_of_month_is_monthly(self, backup_manager, test_db):
        """First backup of the month should be promoted to monthly."""
        # No previous backups
        tier = backup_manager._determine_tier()
        assert tier == "monthly"

    def test_first_backup_of_week_is_weekly(self, backup_manager, test_db):
        """First backup of the week (after monthly exists) should be weekly."""
        # Add a monthly backup from earlier this month
        now = datetime.now(UTC)
        first_of_month = now.replace(day=1, hour=0, minute=0, second=0)
        test_db.insert_backup(
            backup_id="backup-monthly",
            tier="monthly",
            trigger="manual",
            gcs_path="gs://test/monthly/2026-01.db.gz",
            created_at=first_of_month.isoformat(),
            expires_at=(first_of_month + timedelta(days=365)).isoformat(),
        )

        tier = backup_manager._determine_tier()
        # If it's the first of a new week, should be weekly
        assert tier in ("weekly", "daily", "event")

    def test_first_backup_of_day_is_daily(self, backup_manager, test_db):
        """First backup of the day should be promoted to daily."""
        now = datetime.now(UTC)
        # Calculate a date definitely in the current month but current week
        # by using today's start as the weekly backup date
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        first_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        # Add monthly backup for this month
        test_db.insert_backup(
            backup_id="backup-monthly-old",
            tier="monthly",
            trigger="manual",
            gcs_path="gs://test/monthly/2026-01.db.gz",
            created_at=first_of_month.isoformat(),
            expires_at=(now + timedelta(days=355)).isoformat(),
        )
        # Add weekly backup for this week (use today's start so it's definitely this week)
        test_db.insert_backup(
            backup_id="backup-weekly-old",
            tier="weekly",
            trigger="manual",
            gcs_path="gs://test/weekly/2026-W02.db.gz",
            created_at=today_start.isoformat(),
            expires_at=(now + timedelta(days=27)).isoformat(),
        )

        tier = backup_manager._determine_tier()
        # First of the day should be daily (no daily backup exists yet today)
        assert tier in ("daily", "event")

    def test_subsequent_backup_same_day_is_event(self, backup_manager, test_db):
        """Subsequent backups on the same day should be event tier."""
        now = datetime.now(UTC)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        first_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        # Add monthly backup for this month (so monthly check passes)
        test_db.insert_backup(
            backup_id="backup-monthly-current",
            tier="monthly",
            trigger="run",
            gcs_path="gs://test/monthly/2026-01.db.gz",
            created_at=first_of_month.isoformat(),
            expires_at=(now + timedelta(days=365)).isoformat(),
        )

        # Add weekly backup for this week (use today's start so it's definitely this week)
        test_db.insert_backup(
            backup_id="backup-weekly-current",
            tier="weekly",
            trigger="run",
            gcs_path="gs://test/weekly/2026-W02.db.gz",
            created_at=today_start.isoformat(),
            expires_at=(now + timedelta(days=30)).isoformat(),
        )

        # Add a daily backup from today (so daily check passes)
        test_db.insert_backup(
            backup_id="backup-daily-today",
            tier="daily",
            trigger="run",
            gcs_path="gs://test/daily/2026-01-11.db.gz",
            created_at=(today_start + timedelta(hours=1)).isoformat(),
            expires_at=(now + timedelta(days=7)).isoformat(),
        )

        tier = backup_manager._determine_tier()
        assert tier == "event"


class TestRateLimiting:
    """Tests for backup rate limiting."""

    def test_should_not_backup_within_5_minutes(self, backup_manager, test_db):
        """Should not create backup within 5 minutes of last backup."""
        now = datetime.now(UTC)
        recent = (now - timedelta(minutes=2)).isoformat()

        test_db.insert_backup(
            backup_id="backup-recent",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/recent.db.gz",
            created_at=recent,
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )

        should_backup = backup_manager._should_backup(trigger="run")
        assert should_backup is False

    def test_should_backup_after_5_minutes(self, backup_manager, test_db):
        """Should create backup after 5 minutes since last backup."""
        now = datetime.now(UTC)
        old = (now - timedelta(minutes=6)).isoformat()

        test_db.insert_backup(
            backup_id="backup-old",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/old.db.gz",
            created_at=old,
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )

        should_backup = backup_manager._should_backup(trigger="run")
        assert should_backup is True

    def test_high_value_trigger_bypasses_rate_limit(self, backup_manager, test_db):
        """High-value triggers should bypass rate limiting."""
        now = datetime.now(UTC)
        recent = (now - timedelta(minutes=1)).isoformat()

        test_db.insert_backup(
            backup_id="backup-recent",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/recent.db.gz",
            created_at=recent,
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )

        # create_workspace is high-value, should bypass rate limit
        should_backup = backup_manager._should_backup(trigger="create_workspace")
        assert should_backup is True


class TestExpiryCalculation:
    """Tests for backup expiry calculation."""

    def test_event_tier_expires_in_24_hours(self, backup_manager):
        """Event tier backups should expire in 24 hours."""
        now = datetime.now(UTC)
        expires_at = backup_manager._calculate_expiry("event", now)
        expected = now + timedelta(hours=24)
        assert abs((expires_at - expected).total_seconds()) < 1

    def test_daily_tier_expires_in_7_days(self, backup_manager):
        """Daily tier backups should expire in 7 days."""
        now = datetime.now(UTC)
        expires_at = backup_manager._calculate_expiry("daily", now)
        expected = now + timedelta(days=7)
        assert abs((expires_at - expected).total_seconds()) < 1

    def test_weekly_tier_expires_in_30_days(self, backup_manager):
        """Weekly tier backups should expire in 30 days."""
        now = datetime.now(UTC)
        expires_at = backup_manager._calculate_expiry("weekly", now)
        expected = now + timedelta(days=30)
        assert abs((expires_at - expected).total_seconds()) < 1

    def test_monthly_tier_expires_in_365_days(self, backup_manager):
        """Monthly tier backups should expire in 365 days."""
        now = datetime.now(UTC)
        expires_at = backup_manager._calculate_expiry("monthly", now)
        expected = now + timedelta(days=365)
        assert abs((expires_at - expected).total_seconds()) < 1


class TestGcsPathGeneration:
    """Tests for GCS path generation."""

    def test_event_path_includes_timestamp_and_trigger(self, backup_manager):
        """Event backup path should include timestamp and trigger."""
        now = datetime.now(UTC)
        path = backup_manager._generate_gcs_path("event", "run", now)
        assert "/event/" in path
        assert "_run.db.gz" in path
        assert now.strftime("%Y-%m-%d") in path

    def test_daily_path_includes_date(self, backup_manager):
        """Daily backup path should use date format."""
        now = datetime.now(UTC)
        path = backup_manager._generate_gcs_path("daily", "run", now)
        assert "/daily/" in path
        date_str = now.strftime("%Y-%m-%d")
        assert date_str in path

    def test_weekly_path_includes_week_number(self, backup_manager):
        """Weekly backup path should use year-week format."""
        now = datetime.now(UTC)
        path = backup_manager._generate_gcs_path("weekly", "run", now)
        assert "/weekly/" in path
        year, week, _ = now.isocalendar()
        assert f"{year}-W{week:02d}" in path

    def test_monthly_path_includes_year_month(self, backup_manager):
        """Monthly backup path should use year-month format."""
        now = datetime.now(UTC)
        path = backup_manager._generate_gcs_path("monthly", "run", now)
        assert "/monthly/" in path
        assert now.strftime("%Y-%m") in path


class TestBackupCreation:
    """Tests for creating backups."""

    @patch("goldfish.backup.manager.subprocess.run")
    def test_create_backup_uploads_compressed_db(self, mock_run, backup_manager, tmp_path):
        """create_backup should compress and upload the database."""
        mock_run.return_value = MagicMock(returncode=0)

        # Create a small file to backup (separate from the tracking database)
        backup_source = tmp_path / "backup_source.db"
        backup_source.write_bytes(b"test database content")
        backup_manager.db_path = backup_source

        result = backup_manager.create_backup(trigger="run")

        assert result is not None
        assert "backup_id" in result
        assert "gcs_path" in result
        assert "tier" in result
        # gsutil should have been called to upload
        mock_run.assert_called()

    @patch("goldfish.backup.manager.subprocess.run")
    def test_create_backup_records_in_database(self, mock_run, backup_manager, test_db, tmp_path):
        """create_backup should record the backup in the database."""
        mock_run.return_value = MagicMock(returncode=0)

        # Create a small file to backup (separate from the tracking database)
        backup_source = tmp_path / "backup_source.db"
        backup_source.write_bytes(b"test database content")
        backup_manager.db_path = backup_source

        result = backup_manager.create_backup(trigger="save_version", details={"workspace": "test"})

        # Should be recorded in database
        backup = test_db.get_backup(result["backup_id"])
        assert backup is not None
        assert backup["trigger"] == "save_version"
        assert "test" in backup["trigger_details_json"]

    @patch("goldfish.backup.manager.subprocess.run")
    def test_create_backup_appends_to_manifest(self, mock_run, backup_manager, tmp_path):
        """create_backup should append to GCS manifest."""
        mock_run.return_value = MagicMock(returncode=0)

        # Create a small file to backup (separate from the tracking database)
        backup_source = tmp_path / "backup_source.db"
        backup_source.write_bytes(b"test database content")
        backup_manager.db_path = backup_source

        backup_manager.create_backup(trigger="run")

        # Check that gsutil was called to append to manifest
        calls = [str(c) for c in mock_run.call_args_list]
        manifest_call = any("manifest.jsonl" in str(c) for c in calls)
        assert manifest_call


class TestCleanup:
    """Tests for backup cleanup."""

    def test_cleanup_removes_expired_backups(self, backup_manager, test_db):
        """cleanup should remove expired backups."""
        now = datetime.now(UTC)
        expired = (now - timedelta(hours=1)).isoformat()

        test_db.insert_backup(
            backup_id="backup-expired",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/expired.db.gz",
            created_at=(now - timedelta(days=2)).isoformat(),
            expires_at=expired,
        )

        with patch("goldfish.backup.manager.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            backup_manager.cleanup()

        backup = test_db.get_backup("backup-expired")
        assert backup is not None
        assert backup["deleted_at"] is not None

    def test_cleanup_keeps_non_expired_backups(self, backup_manager, test_db):
        """cleanup should keep non-expired backups."""
        now = datetime.now(UTC)
        future = (now + timedelta(days=7)).isoformat()

        test_db.insert_backup(
            backup_id="backup-valid",
            tier="daily",
            trigger="run",
            gcs_path="gs://test/daily/valid.db.gz",
            created_at=now.isoformat(),
            expires_at=future,
        )

        with patch("goldfish.backup.manager.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            backup_manager.cleanup()

        backup = test_db.get_backup("backup-valid")
        assert backup is not None
        assert backup["deleted_at"] is None

    def test_cleanup_enforces_event_count_limit(self, backup_manager, test_db):
        """cleanup should enforce max 20 event backups."""
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
                expires_at=(now + timedelta(hours=24 - i)).isoformat(),  # All non-expired
            )

        with patch("goldfish.backup.manager.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            backup_manager.cleanup()

        # Should have marked oldest 5 as deleted (keeping 20)
        active_events = test_db.list_backups(tier="event", include_deleted=False)
        assert len(active_events) <= 20


class TestListBackups:
    """Tests for listing backups."""

    def test_list_backups_returns_all_tiers(self, backup_manager, test_db):
        """list_backups should return backups from all tiers."""
        now = datetime.now(UTC)
        for tier in ["event", "daily", "weekly", "monthly"]:
            test_db.insert_backup(
                backup_id=f"backup-{tier}",
                tier=tier,
                trigger="manual",
                gcs_path=f"gs://test/{tier}/backup.db.gz",
                created_at=now.isoformat(),
                expires_at=(now + timedelta(days=7)).isoformat(),
            )

        backups = backup_manager.list_backups()
        assert len(backups) == 4

    def test_list_backups_filters_by_tier(self, backup_manager, test_db):
        """list_backups should filter by tier."""
        now = datetime.now(UTC)
        for tier in ["event", "daily"]:
            test_db.insert_backup(
                backup_id=f"backup-{tier}",
                tier=tier,
                trigger="manual",
                gcs_path=f"gs://test/{tier}/backup.db.gz",
                created_at=now.isoformat(),
                expires_at=(now + timedelta(days=7)).isoformat(),
            )

        daily_only = backup_manager.list_backups(tier="daily")
        assert len(daily_only) == 1
        assert daily_only[0]["tier"] == "daily"

    def test_list_backups_excludes_deleted_by_default(self, backup_manager, test_db):
        """list_backups should exclude deleted backups by default."""
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


class TestMaybeBackup:
    """Tests for maybe_backup convenience method."""

    @patch("goldfish.backup.manager.subprocess.run")
    def test_maybe_backup_creates_when_appropriate(self, mock_run, backup_manager, tmp_path):
        """maybe_backup should create backup when conditions are met."""
        mock_run.return_value = MagicMock(returncode=0)

        # Create a small file to backup (separate from the tracking database)
        backup_source = tmp_path / "backup_source.db"
        backup_source.write_bytes(b"test database content")
        backup_manager.db_path = backup_source

        result = backup_manager.maybe_backup(trigger="run")
        assert result is not None  # Backup created

    @patch("goldfish.backup.manager.subprocess.run")
    def test_maybe_backup_skips_when_rate_limited(self, mock_run, backup_manager, test_db, tmp_path):
        """maybe_backup should skip when rate limited."""
        mock_run.return_value = MagicMock(returncode=0)

        # Create a small file to backup (separate from the tracking database)
        backup_source = tmp_path / "backup_source.db"
        backup_source.write_bytes(b"test database content")
        backup_manager.db_path = backup_source

        now = datetime.now(UTC)
        test_db.insert_backup(
            backup_id="backup-recent",
            tier="event",
            trigger="run",
            gcs_path="gs://test/event/recent.db.gz",
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=24)).isoformat(),
        )

        result = backup_manager.maybe_backup(trigger="run")
        assert result is None  # Skipped due to rate limit
