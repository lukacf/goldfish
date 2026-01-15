"""Tests for FinalizationTracker - tracking finalization progress.

FinalizationTracker persists finalization progress to the database
to support TIMEOUT handling in FINALIZING state.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from goldfish.db.database import Database
from goldfish.state_machine.core import transition
from goldfish.state_machine.finalization import FinalizationTracker, get_critical_phases_done
from goldfish.state_machine.types import EventContext, StageEvent, StageState
from goldfish.validation import InvalidStageRunIdError


def _create_run_in_state(db: Database, state: StageState) -> str:
    """Create a stage run in a specific state for testing."""
    run_id = f"stage-{uuid.uuid4().hex[:8]}"
    workspace_name = "test-workspace"
    version = "v1"
    now = datetime.now(UTC).isoformat()

    with db._conn() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO workspace_lineage
            (workspace_name, created_at) VALUES (?, ?)""",
            (workspace_name, now),
        )
        conn.execute(
            """INSERT OR IGNORE INTO workspace_versions
            (workspace_name, version, git_tag, git_sha, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (workspace_name, version, f"{workspace_name}-{version}", "abc123", now, "test"),
        )
        conn.execute(
            """INSERT INTO stage_runs
            (id, workspace_name, version, stage_name, status, started_at, state, state_entered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, workspace_name, version, "test-stage", "running", now, state.value, now),
        )

    return run_id


def _create_run_with_flags(
    db: Database,
    output_sync_done: int = 0,
    output_recording_done: int = 0,
    state: StageState = StageState.FINALIZING,
) -> str:
    """Create a stage run with specific flag values."""
    run_id = f"stage-{uuid.uuid4().hex[:8]}"
    workspace_name = "test-workspace"
    version = "v1"
    now = datetime.now(UTC).isoformat()

    with db._conn() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO workspace_lineage
            (workspace_name, created_at) VALUES (?, ?)""",
            (workspace_name, now),
        )
        conn.execute(
            """INSERT OR IGNORE INTO workspace_versions
            (workspace_name, version, git_tag, git_sha, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (workspace_name, version, f"{workspace_name}-{version}", "abc123", now, "test"),
        )
        conn.execute(
            """INSERT INTO stage_runs
            (id, workspace_name, version, stage_name, status, started_at, state, state_entered_at,
             output_sync_done, output_recording_done)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                run_id,
                workspace_name,
                version,
                "test-stage",
                "running",
                now,
                state.value,
                now,
                output_sync_done,
                output_recording_done,
            ),
        )

    return run_id


class TestFinalizationTracker:
    """Tests for FinalizationTracker class."""

    def test_validates_run_id_format(self, test_db: Database) -> None:
        """Test that invalid run_id format raises InvalidStageRunIdError."""
        with pytest.raises(InvalidStageRunIdError):
            FinalizationTracker(test_db, "invalid-run-id")

    def test_accepts_valid_run_id(self, test_db: Database) -> None:
        """Test that valid run_id format is accepted."""
        # Should not raise - even if run doesn't exist in DB
        tracker = FinalizationTracker(test_db, "stage-abc123456")
        assert tracker._run_id == "stage-abc123456"

    def test_mark_output_sync_done_persists_to_database(self, test_db: Database) -> None:
        """mark_output_sync_done() sets the flag in database."""
        run_id = _create_run_in_state(test_db, StageState.FINALIZING)
        tracker = FinalizationTracker(test_db, run_id)

        tracker.mark_output_sync_done()

        with test_db._conn() as conn:
            row = conn.execute("SELECT output_sync_done FROM stage_runs WHERE id = ?", (run_id,)).fetchone()
            assert row["output_sync_done"] == 1

    def test_mark_output_recording_done_persists_to_database(self, test_db: Database) -> None:
        """mark_output_recording_done() sets the flag in database."""
        run_id = _create_run_in_state(test_db, StageState.FINALIZING)
        tracker = FinalizationTracker(test_db, run_id)

        tracker.mark_output_recording_done()

        with test_db._conn() as conn:
            row = conn.execute("SELECT output_recording_done FROM stage_runs WHERE id = ?", (run_id,)).fetchone()
            assert row["output_recording_done"] == 1

    def test_both_flags_can_be_set(self, test_db: Database) -> None:
        """Both flags can be set independently."""
        run_id = _create_run_in_state(test_db, StageState.FINALIZING)
        tracker = FinalizationTracker(test_db, run_id)

        tracker.mark_output_sync_done()
        tracker.mark_output_recording_done()

        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT output_sync_done, output_recording_done FROM stage_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            assert row["output_sync_done"] == 1
            assert row["output_recording_done"] == 1


class TestCriticalPhasesDone:
    """Tests for critical_phases_done derivation."""

    def test_critical_phases_done_when_both_set(self, test_db: Database) -> None:
        """critical_phases_done is True when both flags are set."""
        run_id = _create_run_in_state(test_db, StageState.FINALIZING)
        tracker = FinalizationTracker(test_db, run_id)

        tracker.mark_output_sync_done()
        tracker.mark_output_recording_done()

        assert tracker.critical_phases_done is True

    def test_critical_phases_not_done_when_only_sync_set(self, test_db: Database) -> None:
        """critical_phases_done is False when only sync is done."""
        run_id = _create_run_in_state(test_db, StageState.FINALIZING)
        tracker = FinalizationTracker(test_db, run_id)

        tracker.mark_output_sync_done()

        assert tracker.critical_phases_done is False

    def test_critical_phases_not_done_when_only_recording_set(self, test_db: Database) -> None:
        """critical_phases_done is False when only recording is done."""
        run_id = _create_run_in_state(test_db, StageState.FINALIZING)
        tracker = FinalizationTracker(test_db, run_id)

        tracker.mark_output_recording_done()

        assert tracker.critical_phases_done is False

    def test_critical_phases_not_done_when_neither_set(self, test_db: Database) -> None:
        """critical_phases_done is False when neither flag is set."""
        run_id = _create_run_in_state(test_db, StageState.FINALIZING)
        tracker = FinalizationTracker(test_db, run_id)

        assert tracker.critical_phases_done is False

    def test_critical_phases_done_returns_none_for_nonexistent_run(self, test_db: Database) -> None:
        """critical_phases_done returns None when run does not exist."""
        # Use valid format but non-existent ID
        tracker = FinalizationTracker(test_db, "stage-0000000000")

        assert tracker.critical_phases_done is None


class TestCriticalPhasesDoneFunction:
    """Tests for get_critical_phases_done() function."""

    def test_get_critical_phases_done_returns_true_when_both_set(self, test_db: Database) -> None:
        """get_critical_phases_done() returns True when both flags set."""
        run_id = _create_run_with_flags(test_db, output_sync_done=1, output_recording_done=1)

        assert get_critical_phases_done(test_db, run_id) is True

    def test_get_critical_phases_done_returns_false_when_partial(self, test_db: Database) -> None:
        """get_critical_phases_done() returns False when only one flag set."""
        run_id = _create_run_with_flags(test_db, output_sync_done=1, output_recording_done=0)

        assert get_critical_phases_done(test_db, run_id) is False

    def test_get_critical_phases_done_returns_none_for_nonexistent_run(self, test_db: Database) -> None:
        """get_critical_phases_done() returns None for non-existent run."""
        # Use valid format but non-existent ID
        result = get_critical_phases_done(test_db, "stage-0000000000")

        assert result is None


class TestTimeoutInFinalizingUsesCriticalPhases:
    """Tests for TIMEOUT in FINALIZING using critical_phases_done for outcome."""

    def test_timeout_in_finalizing_with_critical_done_goes_to_completed(self, test_db: Database) -> None:
        """TIMEOUT in FINALIZING with critical_phases_done=True goes to COMPLETED."""
        run_id = _create_run_with_flags(
            test_db, output_sync_done=1, output_recording_done=1, state=StageState.FINALIZING
        )

        context = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            critical_phases_done=True,
        )

        result = transition(test_db, run_id, StageEvent.TIMEOUT, context)

        assert result.success is True
        assert result.new_state == StageState.COMPLETED

    def test_timeout_in_finalizing_without_critical_done_goes_to_failed(self, test_db: Database) -> None:
        """TIMEOUT in FINALIZING with critical_phases_done=False goes to FAILED."""
        run_id = _create_run_with_flags(
            test_db, output_sync_done=0, output_recording_done=0, state=StageState.FINALIZING
        )

        context = EventContext(
            timestamp=datetime.now(UTC),
            source="daemon",
            critical_phases_done=False,
        )

        result = transition(test_db, run_id, StageEvent.TIMEOUT, context)

        assert result.success is True
        assert result.new_state == StageState.FAILED


class TestFinalizationTrackerNonexistentRun:
    """Tests for mark_* behavior on non-existent runs."""

    def test_mark_output_sync_done_on_nonexistent_run_is_noop(self, test_db: Database) -> None:
        """mark_output_sync_done() on non-existent run is silent no-op."""
        # Use valid format but non-existent ID
        tracker = FinalizationTracker(test_db, "stage-0000000000")

        # Should not raise - silently no-op (UPDATE affects 0 rows)
        tracker.mark_output_sync_done()

    def test_mark_output_recording_done_on_nonexistent_run_is_noop(self, test_db: Database) -> None:
        """mark_output_recording_done() on non-existent run is silent no-op."""
        # Use valid format but non-existent ID
        tracker = FinalizationTracker(test_db, "stage-0000000000")

        # Should not raise - silently no-op (UPDATE affects 0 rows)
        tracker.mark_output_recording_done()


class TestFinalizationTrackerIdempotency:
    """Tests for idempotent marking of phases."""

    def test_mark_output_sync_done_is_idempotent(self, test_db: Database) -> None:
        """Calling mark_output_sync_done() multiple times is safe."""
        run_id = _create_run_in_state(test_db, StageState.FINALIZING)
        tracker = FinalizationTracker(test_db, run_id)

        tracker.mark_output_sync_done()
        tracker.mark_output_sync_done()  # Should not raise

        with test_db._conn() as conn:
            row = conn.execute("SELECT output_sync_done FROM stage_runs WHERE id = ?", (run_id,)).fetchone()
            assert row["output_sync_done"] == 1

    def test_mark_output_recording_done_is_idempotent(self, test_db: Database) -> None:
        """Calling mark_output_recording_done() multiple times is safe."""
        run_id = _create_run_in_state(test_db, StageState.FINALIZING)
        tracker = FinalizationTracker(test_db, run_id)

        tracker.mark_output_recording_done()
        tracker.mark_output_recording_done()  # Should not raise

        with test_db._conn() as conn:
            row = conn.execute("SELECT output_recording_done FROM stage_runs WHERE id = ?", (run_id,)).fetchone()
            assert row["output_recording_done"] == 1


class TestGetCriticalPhasesDone:
    """Tests for get_critical_phases_done function."""

    def test_validates_run_id_format(self, test_db: Database) -> None:
        """Test that invalid run_id format raises InvalidStageRunIdError."""
        with pytest.raises(InvalidStageRunIdError):
            get_critical_phases_done(test_db, "invalid-run-id")

    def test_returns_none_for_nonexistent_run(self, test_db: Database) -> None:
        """Test that non-existent run returns None."""
        # Use valid hex format but run doesn't exist in DB
        result = get_critical_phases_done(test_db, "stage-abc123456789")
        assert result is None

    def test_returns_false_when_not_done(self, test_db: Database) -> None:
        """Test that returns False when phases not done."""
        run_id = _create_run_with_flags(
            test_db,
            output_sync_done=0,
            output_recording_done=0,
        )
        result = get_critical_phases_done(test_db, run_id)
        assert result is False

    def test_returns_true_when_both_done(self, test_db: Database) -> None:
        """Test that returns True when both phases done."""
        run_id = _create_run_with_flags(
            test_db,
            output_sync_done=1,
            output_recording_done=1,
        )
        result = get_critical_phases_done(test_db, run_id)
        assert result is True
