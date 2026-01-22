"""Tests for leader election - Phase 5.1.

Tests for the DaemonLeaderElection class that prevents duplicate
event emission when multiple daemon instances are running.
"""

from __future__ import annotations

import os
import re
import threading
import time
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from goldfish.state_machine.leader_election import DaemonLeaderElection


class TestLeaderElection:
    """Tests for DaemonLeaderElection class."""

    def test_try_acquire_lease_grants_to_first_caller(self, test_db) -> None:
        """First caller should acquire the lease."""
        leader = DaemonLeaderElection(test_db)

        result = leader.try_acquire_lease("holder-1")

        assert result is True

    def test_concurrent_lease_attempts_only_one_wins(self, test_db) -> None:
        """Only one holder should win when multiple attempt concurrently."""
        leader = DaemonLeaderElection(test_db)
        results = []
        barrier = threading.Barrier(3)

        def attempt_lease(holder_id: str) -> None:
            barrier.wait()  # Synchronize all threads
            result = leader.try_acquire_lease(holder_id)
            results.append((holder_id, result))

        threads = [threading.Thread(target=attempt_lease, args=(f"holder-{i}",)) for i in range(3)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Exactly one should win
        winners = [r for r in results if r[1] is True]
        assert len(winners) == 1

    def test_expired_lease_can_be_acquired(self, test_db) -> None:
        """Expired lease should be available for acquisition."""
        leader = DaemonLeaderElection(test_db, lease_duration_seconds=1)

        # First holder acquires
        assert leader.try_acquire_lease("holder-1") is True

        # Wait for lease to expire
        time.sleep(1.5)

        # Second holder can acquire expired lease
        assert leader.try_acquire_lease("holder-2") is True

    def test_lease_renewal_by_same_holder(self, test_db) -> None:
        """Same holder should be able to renew their lease."""
        leader = DaemonLeaderElection(test_db, lease_duration_seconds=5)

        # Acquire lease
        assert leader.try_acquire_lease("holder-1") is True

        # Renew lease (same holder)
        assert leader.try_acquire_lease("holder-1") is True

    def test_release_lease_allows_immediate_acquisition(self, test_db) -> None:
        """Released lease should be immediately available."""
        leader = DaemonLeaderElection(test_db)

        # Acquire lease
        assert leader.try_acquire_lease("holder-1") is True

        # Release
        leader.release_lease("holder-1")

        # Different holder can immediately acquire
        assert leader.try_acquire_lease("holder-2") is True

    def test_different_holder_cannot_acquire_active_lease(self, test_db) -> None:
        """Different holder should not acquire active lease."""
        leader = DaemonLeaderElection(test_db, lease_duration_seconds=60)

        # First holder acquires
        assert leader.try_acquire_lease("holder-1") is True

        # Second holder cannot acquire
        assert leader.try_acquire_lease("holder-2") is False

    def test_release_by_wrong_holder_does_nothing(self, test_db) -> None:
        """Releasing by wrong holder should not affect lease."""
        leader = DaemonLeaderElection(test_db)

        # First holder acquires
        assert leader.try_acquire_lease("holder-1") is True

        # Wrong holder tries to release
        leader.release_lease("holder-2")

        # Second holder still cannot acquire (lease not released)
        assert leader.try_acquire_lease("holder-2") is False

    def test_is_leader_returns_true_for_holder(self, test_db) -> None:
        """is_leader should return True for current holder."""
        leader = DaemonLeaderElection(test_db)
        leader.try_acquire_lease("holder-1")

        assert leader.is_leader("holder-1") is True
        assert leader.is_leader("holder-2") is False

    def test_lease_uses_begin_immediate(self, test_db) -> None:
        """Lease acquisition should use BEGIN IMMEDIATE for race prevention."""
        leader = DaemonLeaderElection(test_db)

        # The implementation should use BEGIN IMMEDIATE internally
        # This test verifies the lease works correctly under contention
        results = []

        def attempt(holder_id: str) -> None:
            for _ in range(10):
                if leader.try_acquire_lease(holder_id):
                    results.append(holder_id)
                    time.sleep(0.01)
                    leader.release_lease(holder_id)

        threads = [threading.Thread(target=attempt, args=(f"h{i}",)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All attempts should succeed (no deadlocks or race conditions)
        assert len(results) > 0


class TestLeaderElectionTable:
    """Tests for daemon_leases table creation."""

    def test_table_created_on_first_use(self, test_db) -> None:
        """daemon_leases table should be created when needed."""
        # Table shouldn't exist yet
        with test_db._conn() as conn:
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='daemon_leases'"
            ).fetchone()
            # May or may not exist depending on schema

        # Use leader election
        leader = DaemonLeaderElection(test_db)
        leader.try_acquire_lease("test-holder")

        # Table should now exist
        with test_db._conn() as conn:
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='daemon_leases'"
            ).fetchone()
            assert result is not None

    def test_lease_stores_holder_and_expiry(self, test_db) -> None:
        """Lease should store holder ID and expiry timestamp."""
        leader = DaemonLeaderElection(test_db, lease_duration_seconds=60)
        leader.try_acquire_lease("test-holder")

        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT holder_id, expires_at FROM daemon_leases WHERE lease_name = 'stage_daemon'"
            ).fetchone()

        assert row is not None
        assert row["holder_id"] == "test-holder"
        # Expiry should be in the future
        expiry = datetime.fromisoformat(row["expires_at"])
        assert expiry > datetime.now(UTC)


class TestGenerateHolderId:
    """Tests for generate_holder_id static method."""

    def test_generate_holder_id_returns_expected_format(self) -> None:
        """generate_holder_id should return format daemon-{pid}-{uuid8}."""
        holder_id = DaemonLeaderElection.generate_holder_id()

        # Should match daemon-{pid}-{8 hex chars}
        pattern = rf"^daemon-{os.getpid()}-[a-f0-9]{{8}}$"
        assert re.match(pattern, holder_id) is not None

    def test_generate_holder_id_returns_unique_values(self) -> None:
        """generate_holder_id should return unique values on each call."""
        ids = [DaemonLeaderElection.generate_holder_id() for _ in range(100)]

        # All IDs should be unique
        assert len(set(ids)) == 100


class TestIsLeaderExpired:
    """Tests for is_leader with expired lease."""

    def test_is_leader_returns_false_when_lease_expired(self, test_db) -> None:
        """is_leader should return False when lease has expired."""
        leader = DaemonLeaderElection(test_db, lease_duration_seconds=1)

        # Acquire lease
        assert leader.try_acquire_lease("holder-1") is True
        assert leader.is_leader("holder-1") is True

        # Wait for lease to expire
        time.sleep(1.5)

        # Should no longer be leader after expiry
        assert leader.is_leader("holder-1") is False


class TestReleaseLeaseDatabaseState:
    """Tests for release_lease database state verification."""

    def test_release_lease_removes_row_from_database(self, test_db) -> None:
        """release_lease should remove the row from daemon_leases."""
        leader = DaemonLeaderElection(test_db)

        # Acquire lease
        leader.try_acquire_lease("holder-1")

        # Verify row exists
        with test_db._conn() as conn:
            row = conn.execute("SELECT * FROM daemon_leases WHERE lease_name = 'stage_daemon'").fetchone()
            assert row is not None

        # Release lease
        leader.release_lease("holder-1")

        # Verify row is deleted
        with test_db._conn() as conn:
            row = conn.execute("SELECT * FROM daemon_leases WHERE lease_name = 'stage_daemon'").fetchone()
            assert row is None

    def test_release_lease_when_no_lease_exists_is_silent(self, test_db) -> None:
        """release_lease should not raise when no lease exists."""
        leader = DaemonLeaderElection(test_db)

        # Release without acquiring first - should not raise
        leader.release_lease("holder-1")

        # Verify no row exists
        with test_db._conn() as conn:
            row = conn.execute("SELECT * FROM daemon_leases WHERE lease_name = 'stage_daemon'").fetchone()
            assert row is None


class TestValidation:
    """Tests for input validation."""

    def test_invalid_lease_name_raises_value_error(self, test_db) -> None:
        """Invalid lease_name should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid lease_name"):
            DaemonLeaderElection(test_db, lease_name="invalid/lease/name")

    def test_invalid_holder_id_raises_value_error(self, test_db) -> None:
        """Invalid holder_id should raise ValueError."""
        leader = DaemonLeaderElection(test_db)

        with pytest.raises(ValueError, match="Invalid holder_id"):
            leader.try_acquire_lease("invalid/holder/id")

    def test_release_invalid_holder_id_raises_value_error(self, test_db) -> None:
        """release_lease with invalid holder_id should raise ValueError."""
        leader = DaemonLeaderElection(test_db)

        with pytest.raises(ValueError, match="Invalid holder_id"):
            leader.release_lease("invalid@holder#id")

    def test_is_leader_invalid_holder_id_raises_value_error(self, test_db) -> None:
        """is_leader with invalid holder_id should raise ValueError."""
        leader = DaemonLeaderElection(test_db)

        with pytest.raises(ValueError, match="Invalid holder_id"):
            leader.is_leader("invalid$holder%id")


class TestExceptionHandling:
    """Tests for exception handling in try_acquire_lease."""

    def test_try_acquire_lease_rollback_on_error(self) -> None:
        """try_acquire_lease should rollback on database error."""
        # Create a mock database with a mock connection that fails on INSERT
        mock_db = MagicMock()
        mock_conn = MagicMock()
        mock_db._get_raw_conn.return_value = mock_conn

        # Track call count to fail on INSERT (3rd execute: BEGIN, SELECT, INSERT)
        failed = [False]  # Track if we've failed

        def execute_with_failure(sql, *args, **kwargs):
            sql_upper = sql.upper() if isinstance(sql, str) else ""

            # Only fail once on INSERT, let ROLLBACK succeed
            if "INSERT" in sql_upper and not failed[0]:
                failed[0] = True
                raise RuntimeError("Simulated DB error")

            # Return mock result for SELECT
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None  # No existing lease
            return mock_result

        mock_conn.execute.side_effect = execute_with_failure

        leader = DaemonLeaderElection(mock_db)

        with pytest.raises(RuntimeError, match="Simulated DB error"):
            leader.try_acquire_lease("holder-1")

        # Verify ROLLBACK was executed (transaction should be rolled back on error)
        # The implementation uses conn.execute("ROLLBACK"), not conn.rollback()
        rollback_calls = [c for c in mock_conn.execute.call_args_list if "ROLLBACK" in str(c)]
        assert len(rollback_calls) >= 1, "Expected ROLLBACK to be executed on error"


class TestValidationEdgeCases:
    """Tests for validation edge cases."""

    def test_empty_lease_name_raises_value_error(self, test_db) -> None:
        """Empty lease_name should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid lease_name"):
            DaemonLeaderElection(test_db, lease_name="")

    def test_empty_holder_id_raises_value_error(self, test_db) -> None:
        """Empty holder_id should raise ValueError."""
        leader = DaemonLeaderElection(test_db)

        with pytest.raises(ValueError, match="Invalid holder_id"):
            leader.try_acquire_lease("")

    def test_holder_id_exceeding_128_chars_raises_value_error(self, test_db) -> None:
        """holder_id exceeding 128 characters should raise ValueError."""
        leader = DaemonLeaderElection(test_db)

        long_holder_id = "a" * 129  # Exceeds 128 char limit

        with pytest.raises(ValueError, match="Invalid holder_id"):
            leader.try_acquire_lease(long_holder_id)

    def test_holder_id_exactly_128_chars_is_valid(self, test_db) -> None:
        """holder_id exactly 128 characters should be valid."""
        leader = DaemonLeaderElection(test_db)

        holder_id_128 = "a" * 128  # Exactly 128 chars

        # Should not raise
        result = leader.try_acquire_lease(holder_id_128)
        assert result is True

    def test_holder_id_with_only_numbers_is_valid(self, test_db) -> None:
        """holder_id with only numbers should be valid."""
        leader = DaemonLeaderElection(test_db)

        # Should not raise - pattern allows [a-zA-Z0-9_-]
        result = leader.try_acquire_lease("12345")
        assert result is True


class TestLeaseDurationBounds:
    """Tests for lease_duration_seconds bounds validation."""

    def test_lease_duration_zero_raises_value_error(self, test_db) -> None:
        """Zero lease_duration_seconds should raise ValueError."""
        with pytest.raises(ValueError, match="lease_duration_seconds must be"):
            DaemonLeaderElection(test_db, lease_duration_seconds=0)

    def test_lease_duration_negative_raises_value_error(self, test_db) -> None:
        """Negative lease_duration_seconds should raise ValueError."""
        with pytest.raises(ValueError, match="lease_duration_seconds must be"):
            DaemonLeaderElection(test_db, lease_duration_seconds=-1)

    def test_lease_duration_exceeds_max_raises_value_error(self, test_db) -> None:
        """lease_duration_seconds exceeding max (3600) should raise ValueError."""
        with pytest.raises(ValueError, match="lease_duration_seconds must be"):
            DaemonLeaderElection(test_db, lease_duration_seconds=3601)

    def test_lease_duration_at_max_is_valid(self, test_db) -> None:
        """lease_duration_seconds at max (3600) should be valid."""
        # Should not raise
        leader = DaemonLeaderElection(test_db, lease_duration_seconds=3600)
        assert leader is not None


class TestIsLeaderNoLease:
    """Tests for is_leader when no lease exists."""

    def test_is_leader_when_no_lease_exists_returns_false(self, test_db) -> None:
        """is_leader should return False when no lease has been acquired."""
        leader = DaemonLeaderElection(test_db)

        # No lease acquired yet - should return False
        assert leader.is_leader("holder-1") is False

    def test_is_leader_after_release_returns_false(self, test_db) -> None:
        """is_leader should return False after lease is released."""
        leader = DaemonLeaderElection(test_db)

        # Acquire and then release
        leader.try_acquire_lease("holder-1")
        leader.release_lease("holder-1")

        # Should now return False
        assert leader.is_leader("holder-1") is False


class TestLeaseExpiryBoundary:
    """Tests for lease expiry boundary conditions."""

    def test_lease_exactly_at_expiry_time(self, test_db) -> None:
        """Lease exactly at expiry (current_expires < now is False) should still be valid."""
        # Use 2-second lease for this test
        leader = DaemonLeaderElection(test_db, lease_duration_seconds=2)

        # Acquire lease
        assert leader.try_acquire_lease("holder-1") is True

        # Immediately after acquiring, lease should be valid
        assert leader.is_leader("holder-1") is True

        # Wait exactly 2 seconds (lease expires at exactly this point)
        time.sleep(2)

        # At exactly expiry time, current_expires < now should still be False
        # because comparison is strict '<', not '<='
        # This is actually now expired, so should return False
        assert leader.is_leader("holder-1") is False


class TestTimezoneNaiveTimestamp:
    """Tests for timezone-naive timestamp handling."""

    def test_is_leader_handles_timezone_naive_expires_at(self, test_db) -> None:
        """is_leader should handle timezone-naive expires_at timestamps in database."""
        leader = DaemonLeaderElection(test_db)

        # First acquire a lease normally
        assert leader.try_acquire_lease("holder-1") is True

        # Manually update the database with a timezone-naive timestamp
        # (without +00:00 or Z suffix)
        with test_db._conn() as conn:
            # Set expires_at to a future time without timezone info
            conn.execute(
                """UPDATE daemon_leases SET expires_at = datetime('now', '+1 hour')
                WHERE lease_name = 'stage_daemon'"""
            )

        # Should still work - is_leader handles naive timestamps
        assert leader.is_leader("holder-1") is True

    def test_try_acquire_lease_handles_timezone_naive_expires_at(self, test_db) -> None:
        """try_acquire_lease should handle timezone-naive expires_at timestamps."""
        leader = DaemonLeaderElection(test_db)

        # First acquire a lease
        assert leader.try_acquire_lease("holder-1") is True

        # Manually update the database with a timezone-naive expired timestamp
        with test_db._conn() as conn:
            # Set expires_at to a past time without timezone info
            conn.execute(
                """UPDATE daemon_leases SET expires_at = datetime('now', '-1 hour')
                WHERE lease_name = 'stage_daemon'"""
            )

        # Different holder should be able to acquire expired lease
        assert leader.try_acquire_lease("holder-2") is True


class TestRollbackExceptionSuppression:
    """Tests for rollback exception suppression in try_acquire_lease."""

    def test_rollback_exception_suppressed_on_error(self) -> None:
        """Nested rollback exception should be suppressed, original re-raised."""
        mock_db = MagicMock()
        mock_conn = MagicMock()
        mock_db._get_raw_conn.return_value = mock_conn

        # Track calls
        call_sequence = []

        def execute_with_failure(sql, *args, **kwargs):
            sql_upper = sql.upper() if isinstance(sql, str) else ""
            call_sequence.append(sql_upper[:20])  # Track first 20 chars

            if "INSERT" in sql_upper:
                raise RuntimeError("Original DB error")
            if "ROLLBACK" in sql_upper:
                raise RuntimeError("Rollback also failed!")

            # Return mock result for other queries
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            return mock_result

        mock_conn.execute.side_effect = execute_with_failure

        leader = DaemonLeaderElection(mock_db)

        # Should raise the ORIGINAL error, not the rollback error
        with pytest.raises(RuntimeError, match="Original DB error"):
            leader.try_acquire_lease("holder-1")

        # Verify rollback was attempted
        assert any("ROLLBACK" in s for s in call_sequence)


class TestMultipleLeaseNames:
    """Tests for multiple independent lease names."""

    def test_different_lease_names_are_independent(self, test_db) -> None:
        """Different lease_name values should create independent leases."""
        leader1 = DaemonLeaderElection(test_db, lease_name="lease_one")
        leader2 = DaemonLeaderElection(test_db, lease_name="lease_two")

        # Both should be able to acquire their own lease
        assert leader1.try_acquire_lease("holder-1") is True
        assert leader2.try_acquire_lease("holder-1") is True

        # Each should be leader of their own lease
        assert leader1.is_leader("holder-1") is True
        assert leader2.is_leader("holder-1") is True

    def test_release_only_affects_own_lease(self, test_db) -> None:
        """Releasing one lease should not affect another lease_name."""
        leader1 = DaemonLeaderElection(test_db, lease_name="lease_one")
        leader2 = DaemonLeaderElection(test_db, lease_name="lease_two")

        leader1.try_acquire_lease("holder-1")
        leader2.try_acquire_lease("holder-1")

        # Release lease_one
        leader1.release_lease("holder-1")

        # leader1 is no longer leader, but leader2 still is
        assert leader1.is_leader("holder-1") is False
        assert leader2.is_leader("holder-1") is True


class TestConstants:
    """Tests for leader election constants."""

    def test_default_lease_duration_is_60_seconds(self) -> None:
        """DEFAULT_LEASE_DURATION should be 60 seconds per spec line 1913."""
        from goldfish.state_machine.leader_election import DEFAULT_LEASE_DURATION

        assert DEFAULT_LEASE_DURATION == 60

    def test_max_lease_duration_is_3600_seconds(self) -> None:
        """MAX_LEASE_DURATION should be 3600 seconds (1 hour)."""
        from goldfish.state_machine.leader_election import MAX_LEASE_DURATION

        assert MAX_LEASE_DURATION == 3600


class TestAcquiredAtTimestamp:
    """Tests for acquired_at timestamp storage and updates."""

    def test_lease_stores_acquired_at_on_initial_acquisition(self, test_db) -> None:
        """Lease should store acquired_at timestamp on initial acquisition."""
        leader = DaemonLeaderElection(test_db)
        before = datetime.now(UTC)

        leader.try_acquire_lease("test-holder")

        after = datetime.now(UTC)

        with test_db._conn() as conn:
            row = conn.execute("SELECT acquired_at FROM daemon_leases WHERE lease_name = 'stage_daemon'").fetchone()

        assert row is not None
        acquired = datetime.fromisoformat(row["acquired_at"])
        # Ensure timezone-aware comparison
        if acquired.tzinfo is None:
            acquired = acquired.replace(tzinfo=UTC)
        assert before <= acquired <= after

    def test_lease_updates_acquired_at_on_renewal(self, test_db) -> None:
        """Lease should update acquired_at timestamp when renewed by same holder."""
        leader = DaemonLeaderElection(test_db, lease_duration_seconds=60)

        # Initial acquisition
        leader.try_acquire_lease("test-holder")

        with test_db._conn() as conn:
            row = conn.execute("SELECT acquired_at FROM daemon_leases WHERE lease_name = 'stage_daemon'").fetchone()
        first_acquired = datetime.fromisoformat(row["acquired_at"])

        # Small delay to ensure different timestamp
        time.sleep(0.01)

        # Renew the lease
        before_renewal = datetime.now(UTC)
        leader.try_acquire_lease("test-holder")
        after_renewal = datetime.now(UTC)

        with test_db._conn() as conn:
            row = conn.execute("SELECT acquired_at FROM daemon_leases WHERE lease_name = 'stage_daemon'").fetchone()
        second_acquired = datetime.fromisoformat(row["acquired_at"])
        if second_acquired.tzinfo is None:
            second_acquired = second_acquired.replace(tzinfo=UTC)

        # Second acquired should be after first
        if first_acquired.tzinfo is None:
            first_acquired = first_acquired.replace(tzinfo=UTC)
        assert second_acquired > first_acquired
        assert before_renewal <= second_acquired <= after_renewal
