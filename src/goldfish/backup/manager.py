"""BackupManager - Tiered backup system with GFS retention.

Implements a Grandfather-Father-Son backup strategy:
- Event tier: Ephemeral backups (24h retention, max 20)
- Daily tier: First backup each day (7d retention)
- Weekly tier: First backup each week (30d retention)
- Monthly tier: First backup each month (365d retention)

Backups are triggered by Goldfish operations (run, save_version, create_workspace)
with rate limiting to avoid excessive backups.
"""

from __future__ import annotations

import gzip
import json
import logging
import subprocess
import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)

# High-value triggers that bypass rate limiting
HIGH_VALUE_TRIGGERS = frozenset({"create_workspace", "save_version", "delete_workspace"})

# Retention periods by tier
TIER_RETENTION = {
    "event": timedelta(hours=24),
    "daily": timedelta(days=7),
    "weekly": timedelta(days=30),
    "monthly": timedelta(days=365),
}

# Rate limit (minimum time between backups for normal triggers)
MIN_BACKUP_INTERVAL = timedelta(minutes=5)

# Maximum event backups to keep
MAX_EVENT_BACKUPS = 20


class BackupManager:
    """Manages database backups with tiered retention."""

    def __init__(
        self,
        db: Database,
        db_path: Path,
        gcs_bucket: str,
    ) -> None:
        """Initialize BackupManager.

        Args:
            db: Database instance for tracking backups
            db_path: Path to the database file to backup
            gcs_bucket: GCS bucket path (e.g., "gs://bucket/backups")
        """
        self.db = db
        self.db_path = db_path
        self.gcs_bucket = gcs_bucket.rstrip("/")

    def _determine_tier(self) -> str:
        """Determine which tier a new backup should be assigned to.

        Uses GFS strategy:
        - First backup of month → monthly
        - First backup of week → weekly
        - First backup of day → daily
        - Otherwise → event

        Returns:
            Tier name: "monthly", "weekly", "daily", or "event"
        """
        now = datetime.now(UTC)

        # Check for existing monthly backup this month
        monthly = self.db.get_last_backup(tier="monthly")
        if not monthly or not self._is_same_month(monthly["created_at"], now):
            return "monthly"

        # Check for existing weekly backup this week
        weekly = self.db.get_last_backup(tier="weekly")
        if not weekly or not self._is_same_week(weekly["created_at"], now):
            return "weekly"

        # Check for existing daily backup today
        daily = self.db.get_last_backup(tier="daily")
        if not daily or not self._is_same_day(daily["created_at"], now):
            return "daily"

        return "event"

    def _is_same_month(self, iso_timestamp: str, now: datetime) -> bool:
        """Check if timestamp is in the same month as now."""
        backup_time = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
        return backup_time.year == now.year and backup_time.month == now.month

    def _is_same_week(self, iso_timestamp: str, now: datetime) -> bool:
        """Check if timestamp is in the same ISO week as now."""
        backup_time = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
        return backup_time.isocalendar()[:2] == now.isocalendar()[:2]

    def _is_same_day(self, iso_timestamp: str, now: datetime) -> bool:
        """Check if timestamp is on the same day as now."""
        backup_time = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
        return backup_time.date() == now.date()

    def _should_backup(self, trigger: str) -> bool:
        """Check if a backup should be created (rate limiting).

        High-value triggers bypass rate limiting.
        Normal triggers must wait MIN_BACKUP_INTERVAL since last backup.

        Args:
            trigger: What triggered the backup request

        Returns:
            True if backup should proceed, False otherwise
        """
        if trigger in HIGH_VALUE_TRIGGERS:
            return True

        last_backup = self.db.get_last_backup()
        if not last_backup:
            return True

        last_time = datetime.fromisoformat(last_backup["created_at"].replace("Z", "+00:00"))
        elapsed = datetime.now(UTC) - last_time
        return elapsed >= MIN_BACKUP_INTERVAL

    def _calculate_expiry(self, tier: str, now: datetime) -> datetime:
        """Calculate when a backup should expire.

        Args:
            tier: Backup tier
            now: Current time

        Returns:
            Expiry datetime
        """
        return now + TIER_RETENTION[tier]

    def _generate_gcs_path(self, tier: str, trigger: str, now: datetime) -> str:
        """Generate GCS path for backup file.

        Args:
            tier: Backup tier
            trigger: What triggered the backup
            now: Current time

        Returns:
            Full GCS path
        """
        if tier == "event":
            timestamp = now.strftime("%Y-%m-%dT%H%M%S")
            return f"{self.gcs_bucket}/event/{timestamp}_{trigger}.db.gz"
        elif tier == "daily":
            date_str = now.strftime("%Y-%m-%d")
            return f"{self.gcs_bucket}/daily/{date_str}.db.gz"
        elif tier == "weekly":
            year, week, _ = now.isocalendar()
            return f"{self.gcs_bucket}/weekly/{year}-W{week:02d}.db.gz"
        else:  # monthly
            month_str = now.strftime("%Y-%m")
            return f"{self.gcs_bucket}/monthly/{month_str}.db.gz"

    def create_backup(
        self,
        trigger: str,
        details: dict | None = None,
    ) -> dict | None:
        """Create a backup of the database.

        Args:
            trigger: What triggered the backup (run, save_version, etc.)
            details: Optional dict with workspace, version, run_id, etc.

        Returns:
            Dict with backup info (backup_id, gcs_path, tier) or None if failed
        """
        now = datetime.now(UTC)
        tier = self._determine_tier()
        backup_id = f"backup-{uuid.uuid4().hex[:8]}"
        gcs_path = self._generate_gcs_path(tier, trigger, now)
        expires_at = self._calculate_expiry(tier, now)

        try:
            # Compress database to temp file
            with tempfile.NamedTemporaryFile(suffix=".db.gz", delete=False) as tmp:
                tmp_path = Path(tmp.name)
                with open(self.db_path, "rb") as f_in:
                    with gzip.open(tmp_path, "wb") as f_out:
                        f_out.write(f_in.read())

                size_bytes = tmp_path.stat().st_size

                # Upload to GCS
                result = subprocess.run(
                    ["gsutil", "-q", "cp", str(tmp_path), gcs_path],
                    capture_output=True,
                    text=True,
                )
                if result.returncode != 0:
                    logger.error(f"Failed to upload backup: {result.stderr}")
                    return None

                # Clean up temp file
                tmp_path.unlink()

            # Record in database
            self.db.insert_backup(
                backup_id=backup_id,
                tier=tier,
                trigger=trigger,
                gcs_path=gcs_path,
                created_at=now.isoformat(),
                expires_at=expires_at.isoformat(),
                trigger_details=details,
                size_bytes=size_bytes,
            )

            # Append to manifest (survives database restoration)
            self._append_to_manifest(backup_id, tier, trigger, gcs_path, now, size_bytes)

            logger.info(f"Created {tier} backup: {backup_id} → {gcs_path}")
            return {
                "backup_id": backup_id,
                "gcs_path": gcs_path,
                "tier": tier,
                "size_bytes": size_bytes,
            }

        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return None

    def _append_to_manifest(
        self,
        backup_id: str,
        tier: str,
        trigger: str,
        gcs_path: str,
        created_at: datetime,
        size_bytes: int,
    ) -> None:
        """Append backup record to GCS manifest.

        The manifest is an append-only log that survives database restoration.
        """
        manifest_path = f"{self.gcs_bucket}/manifest.jsonl"
        record = {
            "backup_id": backup_id,
            "tier": tier,
            "trigger": trigger,
            "gcs_path": gcs_path,
            "created_at": created_at.isoformat(),
            "size_bytes": size_bytes,
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as tmp:
            tmp.write(json.dumps(record) + "\n")
            tmp_path = tmp.name

        try:
            # Append to manifest using gsutil compose
            subprocess.run(
                ["gsutil", "-q", "cp", tmp_path, f"{manifest_path}.tmp"],
                capture_output=True,
            )
            subprocess.run(
                [
                    "gsutil",
                    "-q",
                    "compose",
                    manifest_path,
                    f"{manifest_path}.tmp",
                    manifest_path,
                ],
                capture_output=True,
            )
            subprocess.run(
                ["gsutil", "-q", "rm", f"{manifest_path}.tmp"],
                capture_output=True,
            )
        except Exception as e:
            logger.warning(f"Failed to update manifest: {e}")
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def cleanup(self) -> dict:
        """Clean up expired and excess backups.

        - Removes backups past their expiry date
        - Enforces MAX_EVENT_BACKUPS limit

        Returns:
            Dict with cleanup stats
        """
        deleted_expired = 0
        deleted_excess = 0

        # Delete expired backups
        expired = self.db.get_expired_backups()
        for backup in expired:
            self._delete_backup_file(backup["gcs_path"])
            self.db.mark_backup_deleted(backup["backup_id"])
            deleted_expired += 1

        # Enforce event backup count limit
        event_backups = self.db.list_backups(tier="event")
        if len(event_backups) > MAX_EVENT_BACKUPS:
            # Delete oldest event backups (list is ordered by created_at DESC)
            excess = event_backups[MAX_EVENT_BACKUPS:]
            for backup in excess:
                self._delete_backup_file(backup["gcs_path"])
                self.db.mark_backup_deleted(backup["backup_id"])
                deleted_excess += 1

        logger.info(f"Cleanup: {deleted_expired} expired, {deleted_excess} excess event backups")
        return {
            "deleted_expired": deleted_expired,
            "deleted_excess": deleted_excess,
        }

    def _delete_backup_file(self, gcs_path: str) -> bool:
        """Delete a backup file from GCS.

        Args:
            gcs_path: GCS path to delete

        Returns:
            True if deleted, False if failed
        """
        result = subprocess.run(
            ["gsutil", "-q", "rm", gcs_path],
            capture_output=True,
        )
        return result.returncode == 0

    def list_backups(
        self,
        tier: str | None = None,
        include_deleted: bool = False,
    ) -> list:
        """List backups.

        Args:
            tier: Optional filter by tier
            include_deleted: Include deleted backups

        Returns:
            List of backup dicts
        """
        return self.db.list_backups(tier=tier, include_deleted=include_deleted)

    def maybe_backup(
        self,
        trigger: str,
        details: dict | None = None,
    ) -> dict | None:
        """Create a backup if rate limiting allows.

        Convenience method that checks _should_backup before creating.

        Args:
            trigger: What triggered the backup
            details: Optional trigger details

        Returns:
            Backup info dict or None if skipped/failed
        """
        if not self._should_backup(trigger):
            logger.debug(f"Backup skipped (rate limited): {trigger}")
            return None

        return self.create_backup(trigger, details)
