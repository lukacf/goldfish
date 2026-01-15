"""Leader election for daemon instances.

This module implements leader election to prevent duplicate event emission
when multiple daemon instances might be running. Uses SQLite's BEGIN IMMEDIATE
for race-free lease acquisition.
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)

# Default lease duration: 60 seconds (per spec line 1913)
DEFAULT_LEASE_DURATION = 60

# Maximum lease duration: 1 hour (prevents unbounded leases)
MAX_LEASE_DURATION = 3600

# Length of UUID suffix in generated holder IDs
UUID_SUFFIX_LENGTH = 8

# Validation pattern for holder_id and lease_name.
# Accepts alphanumeric characters, dashes, and underscores.
# Length must be 1-128 characters.
# Examples: "daemon-123-abc", "stage_daemon", "holder_1"
_VALID_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")


def validate_holder_id(holder_id: str) -> None:
    """Validate a holder ID.

    Args:
        holder_id: The holder ID to validate.

    Raises:
        ValueError: If holder_id doesn't match the required pattern.
    """
    if not _VALID_ID_PATTERN.match(holder_id):
        raise ValueError(f"Invalid holder_id: must match {_VALID_ID_PATTERN.pattern}")


class DaemonLeaderElection:
    """Leader election for stage daemon.

    Uses a single-row lease table with optimistic locking to ensure
    only one daemon processes events at a time.

    The lease is time-limited, so if a daemon crashes, another can
    take over after the lease expires.
    """

    def __init__(
        self,
        db: Database,
        lease_name: str = "stage_daemon",
        lease_duration_seconds: int = DEFAULT_LEASE_DURATION,
    ) -> None:
        """Initialize leader election.

        Args:
            db: Database instance.
            lease_name: Name of the lease (allows multiple lease types).
            lease_duration_seconds: How long the lease is valid (1 to 3600 seconds).

        Raises:
            ValueError: If lease_name doesn't match validation pattern or
                lease_duration_seconds is out of range.
        """
        if not _VALID_ID_PATTERN.match(lease_name):
            raise ValueError(f"Invalid lease_name: must match {_VALID_ID_PATTERN.pattern}")
        if lease_duration_seconds <= 0 or lease_duration_seconds > MAX_LEASE_DURATION:
            raise ValueError(f"lease_duration_seconds must be between 1 and {MAX_LEASE_DURATION}")
        self._db = db
        self._lease_name = lease_name
        self._lease_duration = timedelta(seconds=lease_duration_seconds)

    def try_acquire_lease(self, holder_id: str) -> bool:
        """Try to acquire or renew the lease.

        Uses BEGIN IMMEDIATE to prevent race conditions between
        concurrent acquisition attempts.

        Args:
            holder_id: Unique identifier for this daemon instance.

        Returns:
            True if lease was acquired/renewed, False if held by another.

        Raises:
            ValueError: If holder_id doesn't match validation pattern.
        """
        if not _VALID_ID_PATTERN.match(holder_id):
            raise ValueError(f"Invalid holder_id: must match {_VALID_ID_PATTERN.pattern}")

        now = datetime.now(UTC)
        expires_at = now + self._lease_duration

        # Use raw connection for BEGIN IMMEDIATE
        conn = self._db._get_raw_conn()
        try:
            # BEGIN IMMEDIATE ensures we get a write lock immediately
            # This prevents TOCTOU race conditions
            conn.execute("BEGIN IMMEDIATE")

            # Check current lease state
            row = conn.execute(
                "SELECT holder_id, expires_at FROM daemon_leases WHERE lease_name = ?",
                (self._lease_name,),
            ).fetchone()

            if row is None:
                # No lease exists - acquire it
                conn.execute(
                    """INSERT INTO daemon_leases (lease_name, holder_id, acquired_at, expires_at)
                    VALUES (?, ?, ?, ?)""",
                    (self._lease_name, holder_id, now.isoformat(), expires_at.isoformat()),
                )
                conn.execute("COMMIT")
                logger.debug("Lease %s acquired by %s", self._lease_name, holder_id)
                return True

            current_holder = row["holder_id"]
            current_expires = datetime.fromisoformat(row["expires_at"])
            # Ensure timezone-aware comparison (handle legacy naive timestamps)
            if current_expires.tzinfo is None:
                current_expires = current_expires.replace(tzinfo=UTC)

            if current_holder == holder_id:
                # Same holder - renew the lease
                conn.execute(
                    """UPDATE daemon_leases SET acquired_at = ?, expires_at = ?
                    WHERE lease_name = ?""",
                    (now.isoformat(), expires_at.isoformat(), self._lease_name),
                )
                conn.execute("COMMIT")
                logger.debug("Lease %s renewed by %s", self._lease_name, holder_id)
                return True

            if current_expires < now:
                # Lease expired - take it over
                conn.execute(
                    """UPDATE daemon_leases SET holder_id = ?, acquired_at = ?, expires_at = ?
                    WHERE lease_name = ?""",
                    (holder_id, now.isoformat(), expires_at.isoformat(), self._lease_name),
                )
                conn.execute("COMMIT")
                logger.info(
                    "Lease %s taken over by %s (was held by %s, expired)",
                    self._lease_name,
                    holder_id,
                    current_holder,
                )
                return True

            # Lease held by another and not expired
            conn.execute("ROLLBACK")
            logger.debug(
                "Lease %s held by %s until %s",
                self._lease_name,
                current_holder,
                current_expires.isoformat(),
            )
            return False

        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass  # Best effort rollback - don't mask original exception
            raise
        finally:
            conn.close()

    def release_lease(self, holder_id: str) -> None:
        """Release the lease if held by this holder.

        Args:
            holder_id: Unique identifier for this daemon instance.

        Raises:
            ValueError: If holder_id doesn't match validation pattern.
        """
        if not _VALID_ID_PATTERN.match(holder_id):
            raise ValueError(f"Invalid holder_id: must match {_VALID_ID_PATTERN.pattern}")

        with self._db._conn() as conn:
            result = conn.execute(
                "DELETE FROM daemon_leases WHERE lease_name = ? AND holder_id = ?",
                (self._lease_name, holder_id),
            )
            if result.rowcount > 0:
                logger.debug("Lease %s released by %s", self._lease_name, holder_id)

    def is_leader(self, holder_id: str) -> bool:
        """Check if this holder is the current leader.

        Args:
            holder_id: Unique identifier to check.

        Returns:
            True if holder_id currently holds a valid lease.

        Raises:
            ValueError: If holder_id doesn't match validation pattern.
        """
        if not _VALID_ID_PATTERN.match(holder_id):
            raise ValueError(f"Invalid holder_id: must match {_VALID_ID_PATTERN.pattern}")

        now = datetime.now(UTC)

        with self._db._conn() as conn:
            row = conn.execute(
                "SELECT holder_id, expires_at FROM daemon_leases WHERE lease_name = ?",
                (self._lease_name,),
            ).fetchone()

            if row is None:
                return False

            current_holder = row["holder_id"]
            current_expires = datetime.fromisoformat(row["expires_at"])
            # Ensure timezone-aware comparison (handle legacy naive timestamps)
            if current_expires.tzinfo is None:
                current_expires = current_expires.replace(tzinfo=UTC)

            return current_holder == holder_id and current_expires > now

    @staticmethod
    def generate_holder_id() -> str:
        """Generate a unique holder ID for this daemon instance.

        Combines PID with a UUID to ensure uniqueness across restarts.

        Returns:
            Unique holder ID string.
        """
        return f"daemon-{os.getpid()}-{uuid.uuid4().hex[:UUID_SUFFIX_LENGTH]}"
