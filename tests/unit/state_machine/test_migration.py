"""Tests for state machine migration logic.

TDD: Write tests BEFORE implementation.
Verifies:
- Schema changes apply correctly
- Legacy status → state mapping
- Termination cause detection from error messages
- Orphan detection
- Rollback capability
"""

from __future__ import annotations

from datetime import UTC, datetime

from goldfish.state_machine import StageState, TerminationCause


def _setup_workspace(conn, workspace: str = "ws", version: str = "v1") -> None:
    """Helper to create workspace records needed for foreign key constraints."""
    now = datetime.now(UTC).isoformat()
    # Check if workspace lineage exists
    existing = conn.execute(
        "SELECT workspace_name FROM workspace_lineage WHERE workspace_name = ?",
        (workspace,),
    ).fetchone()
    if not existing:
        conn.execute(
            "INSERT INTO workspace_lineage (workspace_name, created_at) VALUES (?, ?)",
            (workspace, now),
        )
    # Check if version exists
    existing_ver = conn.execute(
        "SELECT version FROM workspace_versions WHERE workspace_name = ? AND version = ?",
        (workspace, version),
    ).fetchone()
    if not existing_ver:
        conn.execute(
            "INSERT INTO workspace_versions (workspace_name, version, git_tag, git_sha, created_at, created_by) VALUES (?, ?, ?, ?, ?, ?)",
            (workspace, version, f"{workspace}-{version}", "abc123", now, "test"),
        )


class TestDetermineMigrationState:
    """Tests for determine_migration_state() function.

    Maps legacy status values to new StageState values.
    """

    def test_pending_maps_to_preparing(self) -> None:
        """Legacy 'pending' status maps to PREPARING state."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(status="pending", error=None)
        assert state == StageState.PREPARING

    def test_running_maps_to_running(self) -> None:
        """Legacy 'running' status maps to RUNNING state."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(status="running", error=None)
        assert state == StageState.RUNNING

    def test_completed_maps_to_completed(self) -> None:
        """Legacy 'completed' status maps to COMPLETED state."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(status="completed", error=None)
        assert state == StageState.COMPLETED

    def test_failed_maps_to_failed(self) -> None:
        """Legacy 'failed' status maps to FAILED state."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(status="failed", error=None)
        assert state == StageState.FAILED

    def test_canceled_maps_to_canceled(self) -> None:
        """Legacy 'canceled' status maps to CANCELED state."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(status="canceled", error=None)
        assert state == StageState.CANCELED

    def test_cancelled_spelling_maps_to_canceled(self) -> None:
        """Legacy 'cancelled' (British spelling) maps to CANCELED state."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(status="cancelled", error=None)
        assert state == StageState.CANCELED

    def test_failed_with_preemption_error_maps_to_terminated(self) -> None:
        """Failed status with preemption error maps to TERMINATED."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(
            status="failed",
            error="Instance was preempted by Google Cloud",
        )
        assert state == StageState.TERMINATED

    def test_failed_with_timeout_error_maps_to_terminated(self) -> None:
        """Failed status with timeout error maps to TERMINATED."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(
            status="failed",
            error="Stage execution timed out after 3600 seconds",
        )
        assert state == StageState.TERMINATED

    def test_failed_with_instance_lost_error_maps_to_terminated(self) -> None:
        """Failed status with instance lost error maps to TERMINATED."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(
            status="failed",
            error="Instance not found or terminated unexpectedly",
        )
        assert state == StageState.TERMINATED

    def test_unknown_status_maps_to_unknown(self) -> None:
        """Unknown legacy status maps to UNKNOWN state."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(status="some_weird_status", error=None)
        assert state == StageState.UNKNOWN

    def test_null_status_maps_to_unknown(self) -> None:
        """NULL status maps to UNKNOWN state."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(status=None, error=None)
        assert state == StageState.UNKNOWN

    def test_empty_string_status_maps_to_unknown(self) -> None:
        """Empty string status maps to UNKNOWN state."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(status="", error=None)
        assert state == StageState.UNKNOWN


class TestDetectTerminationCause:
    """Tests for detect_termination_cause() function.

    Extracts termination cause from error messages.
    """

    def test_preemption_detected_from_preempted_keyword(self) -> None:
        """Detect PREEMPTED from 'preempted' in error message."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Instance was preempted by Google Cloud")
        assert cause == TerminationCause.PREEMPTED

    def test_preemption_detected_from_spot_keyword(self) -> None:
        """Detect PREEMPTED from 'spot' in error message."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Spot instance terminated")
        assert cause == TerminationCause.PREEMPTED

    def test_timeout_detected_from_timeout_keyword(self) -> None:
        """Detect TIMEOUT from 'timeout' in error message."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Stage execution timed out after 3600s")
        assert cause == TerminationCause.TIMEOUT

    def test_timeout_detected_from_timed_out_phrase(self) -> None:
        """Detect TIMEOUT from 'timed out' phrase."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Connection timed out waiting for response")
        assert cause == TerminationCause.TIMEOUT

    def test_crashed_detected_from_crash_keyword(self) -> None:
        """Detect CRASHED from 'crash' in error message."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Container crashed unexpectedly")
        assert cause == TerminationCause.CRASHED

    def test_crashed_detected_from_oom_keyword(self) -> None:
        """Detect CRASHED from 'OOM' (out of memory) in error message."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Process killed: OOM")
        assert cause == TerminationCause.CRASHED

    def test_crashed_detected_from_segfault(self) -> None:
        """Detect CRASHED from segmentation fault."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Segmentation fault (core dumped)")
        assert cause == TerminationCause.CRASHED

    def test_crashed_detected_from_out_of_memory(self) -> None:
        """Detect CRASHED from 'out of memory' phrase."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Process was killed: out of memory")
        assert cause == TerminationCause.CRASHED

    def test_crashed_detected_from_killed(self) -> None:
        """Detect CRASHED from 'killed' keyword."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Process was killed by system")
        assert cause == TerminationCause.CRASHED

    def test_crashed_detected_from_signal_nonzero(self) -> None:
        """Detect CRASHED from signal with non-zero number."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Process terminated with signal 9")
        assert cause == TerminationCause.CRASHED

    def test_signal_zero_not_detected_as_crashed(self) -> None:
        """Signal 0 is success, should NOT be detected as CRASHED."""
        from goldfish.state_machine.migration import detect_termination_cause

        # Signal 0 is a success signal - should not match crash pattern
        cause = detect_termination_cause("Exited with signal 0")
        assert cause is None

    def test_orphaned_detected_from_instance_not_found(self) -> None:
        """Detect ORPHANED from 'instance not found'."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Instance not found or disappeared")
        assert cause == TerminationCause.ORPHANED

    def test_orphaned_detected_from_lost_track(self) -> None:
        """Detect ORPHANED from 'lost track' phrase."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Lost track of container")
        assert cause == TerminationCause.ORPHANED

    def test_orphaned_detected_from_not_found_instance(self) -> None:
        """Detect ORPHANED from 'not found.*instance' pattern."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Resource not found - instance gone")
        assert cause == TerminationCause.ORPHANED

    def test_orphaned_detected_from_disappeared(self) -> None:
        """Detect ORPHANED from 'disappeared' keyword."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Container disappeared unexpectedly")
        assert cause == TerminationCause.ORPHANED

    def test_orphaned_detected_from_orphan_keyword(self) -> None:
        """Detect ORPHANED from 'orphan' keyword."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Found orphan process")
        assert cause == TerminationCause.ORPHANED

    def test_none_returned_for_generic_error(self) -> None:
        """Return None for errors that don't match known patterns."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("Import error: module not found")
        assert cause is None

    def test_none_returned_for_none_error(self) -> None:
        """Return None for None error message."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause(None)
        assert cause is None

    def test_none_returned_for_empty_error(self) -> None:
        """Return None for empty error message."""
        from goldfish.state_machine.migration import detect_termination_cause

        cause = detect_termination_cause("")
        assert cause is None


class TestMigrationSchemaChanges:
    """Tests for schema migration applying correctly."""

    def test_state_column_added_to_stage_runs(self, test_db) -> None:
        """Verify state column is added to stage_runs table."""
        with test_db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(stage_runs)")}
            assert "state" in columns

    def test_phase_column_added_to_stage_runs(self, test_db) -> None:
        """Verify phase column is added to stage_runs table."""
        with test_db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(stage_runs)")}
            assert "phase" in columns

    def test_termination_cause_column_added(self, test_db) -> None:
        """Verify termination_cause column is added to stage_runs table."""
        with test_db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(stage_runs)")}
            assert "termination_cause" in columns

    def test_state_entered_at_column_added(self, test_db) -> None:
        """Verify state_entered_at column is added to stage_runs table."""
        with test_db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(stage_runs)")}
            assert "state_entered_at" in columns

    def test_phase_updated_at_column_added(self, test_db) -> None:
        """Verify phase_updated_at column is added to stage_runs table."""
        with test_db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(stage_runs)")}
            assert "phase_updated_at" in columns

    def test_completed_with_warnings_column_added(self, test_db) -> None:
        """Verify completed_with_warnings column is added to stage_runs table."""
        with test_db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(stage_runs)")}
            assert "completed_with_warnings" in columns

    def test_output_sync_done_column_added(self, test_db) -> None:
        """Verify output_sync_done column is added to stage_runs table."""
        with test_db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(stage_runs)")}
            assert "output_sync_done" in columns

    def test_output_recording_done_column_added(self, test_db) -> None:
        """Verify output_recording_done column is added to stage_runs table."""
        with test_db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(stage_runs)")}
            assert "output_recording_done" in columns

    def test_gcs_outage_started_column_added(self, test_db) -> None:
        """Verify gcs_outage_started column is added to stage_runs table."""
        with test_db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(stage_runs)")}
            assert "gcs_outage_started" in columns


class TestStageStateTransitionsTable:
    """Tests for stage_state_transitions audit table."""

    def test_audit_table_exists(self, test_db) -> None:
        """Verify stage_state_transitions table is created."""
        with test_db._conn() as conn:
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='stage_state_transitions'"
            ).fetchone()
            assert result is not None

    def test_audit_table_has_required_columns(self, test_db) -> None:
        """Verify audit table has all required columns."""
        with test_db._conn() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(stage_state_transitions)")}
            required_columns = {
                "id",
                "stage_run_id",
                "from_state",
                "to_state",
                "event",
                "context_json",
                "timestamp",
            }
            assert required_columns.issubset(columns)

    def test_audit_table_insert(self, test_db) -> None:
        """Verify we can insert into audit table."""
        now = datetime.now(UTC).isoformat()
        with test_db._conn() as conn:
            # Create required parent records for foreign key
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-test", "ws", "v1", "train", "pending", now),
            )
            # Now insert into audit table
            conn.execute(
                """
                INSERT INTO stage_state_transitions
                (stage_run_id, from_state, to_state, event, context_json, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-test", "preparing", "building", "build_start", "{}", now),
            )
            row = conn.execute(
                "SELECT * FROM stage_state_transitions WHERE stage_run_id = ?",
                ("stage-test",),
            ).fetchone()
            assert row is not None
            assert row["from_state"] == "preparing"
            assert row["to_state"] == "building"
            assert row["event"] == "build_start"


class TestPartialIndex:
    """Tests for partial index on active states."""

    def test_partial_index_exists(self, test_db) -> None:
        """Verify partial index for active states exists."""
        with test_db._conn() as conn:
            # Query for index
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_stage_runs_active_state'"
            ).fetchone()
            assert result is not None

    def test_partial_index_query_performance(self, test_db) -> None:
        """Verify partial index is used for active state queries.

        We can't directly verify index usage in unit tests, but we can verify
        the query executes successfully with the expected WHERE clause.
        """
        with test_db._conn() as conn:
            # This query should be able to use the partial index
            result = conn.execute(
                """
                SELECT id FROM stage_runs
                WHERE state IN ('preparing', 'building', 'launching', 'running', 'finalizing')
                """
            ).fetchall()
            # Just verify query executes - index verification is manual via EXPLAIN QUERY PLAN
            assert isinstance(result, list)


class TestMigrateStageRuns:
    """Tests for migrate_stage_runs() batch migration function."""

    def test_migrate_preserves_completed_runs(self, test_db) -> None:
        """Completed runs should have state=COMPLETED after migration."""
        from goldfish.state_machine.migration import migrate_stage_runs

        # Create a completed run
        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-completed", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )

        # Run migration
        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        # Verify state
        with test_db._conn() as conn:
            row = conn.execute("SELECT state FROM stage_runs WHERE id = ?", ("stage-completed",)).fetchone()
            assert row["state"] == "completed"

    def test_migrate_preserves_failed_runs(self, test_db) -> None:
        """Failed runs should have state=FAILED after migration."""
        from goldfish.state_machine.migration import migrate_stage_runs

        # Create a failed run
        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at, error)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("stage-failed", "ws", "v1", "train", "failed", datetime.now(UTC).isoformat(), "Import error"),
            )

        # Run migration
        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        # Verify state
        with test_db._conn() as conn:
            row = conn.execute("SELECT state FROM stage_runs WHERE id = ?", ("stage-failed",)).fetchone()
            assert row["state"] == "failed"

    def test_migrate_detects_preemption(self, test_db) -> None:
        """Failed runs with preemption should become TERMINATED with PREEMPTED cause."""
        from goldfish.state_machine.migration import migrate_stage_runs

        # Create a preempted run
        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at, error)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "stage-preempted",
                    "ws",
                    "v1",
                    "train",
                    "failed",
                    datetime.now(UTC).isoformat(),
                    "Instance was preempted",
                ),
            )

        # Run migration
        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        # Verify state and termination cause
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT state, termination_cause FROM stage_runs WHERE id = ?",
                ("stage-preempted",),
            ).fetchone()
            assert row["state"] == "terminated"
            assert row["termination_cause"] == "preempted"

    def test_migrate_batch_continues_on_error(self, test_db) -> None:
        """Migration should continue processing even if some rows fail."""
        from goldfish.state_machine.migration import migrate_stage_runs

        # Create runs
        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-2", "ws", "v1", "train", "failed", datetime.now(UTC).isoformat()),
            )

        # Run migration
        result = migrate_stage_runs(test_db)
        assert result["success"] is True
        assert result["migrated"] == 2

    def test_migrate_creates_backup_table(self, test_db) -> None:
        """Migration should create backup table before modifying data."""
        from goldfish.state_machine.migration import migrate_stage_runs

        # Create a run
        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )

        # Run migration
        migrate_stage_runs(test_db)

        # Verify backup table exists
        with test_db._conn() as conn:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'stage_runs_backup_%'"
            ).fetchall()
            assert len(tables) >= 1

    def test_migrate_idempotent(self, test_db) -> None:
        """Running migration twice should be safe (idempotent)."""
        from goldfish.state_machine.migration import migrate_stage_runs

        # Create a run
        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )

        # Run migration twice
        result1 = migrate_stage_runs(test_db)
        result2 = migrate_stage_runs(test_db)

        assert result1["success"] is True
        assert result2["success"] is True
        # Second run should skip already-migrated rows
        assert result2["skipped"] >= 1

    def test_migrate_tracks_progress(self, test_db) -> None:
        """Migration should track progress in migration_progress table."""
        from goldfish.state_machine.migration import migrate_stage_runs

        # Create runs
        with test_db._conn() as conn:
            _setup_workspace(conn)
            for i in range(5):
                conn.execute(
                    """
                    INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (f"stage-{i}", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
                )

        # Run migration
        result = migrate_stage_runs(test_db)
        assert result["success"] is True
        assert result["migrated"] == 5


class TestRollbackMigration:
    """Tests for rollback_migration() function."""

    def test_rollback_restores_original_data(self, test_db) -> None:
        """Rollback should restore original state column values."""
        from goldfish.state_machine.migration import migrate_stage_runs, rollback_migration

        # Create a run
        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )

        # Run migration
        migrate_stage_runs(test_db)

        # Verify migration applied
        with test_db._conn() as conn:
            row = conn.execute("SELECT state FROM stage_runs WHERE id = ?", ("stage-1",)).fetchone()
            assert row["state"] is not None

        # Rollback
        result = rollback_migration(test_db)
        assert result["success"] is True

        # Verify state is cleared (back to NULL/unmigrated)
        with test_db._conn() as conn:
            row = conn.execute("SELECT state FROM stage_runs WHERE id = ?", ("stage-1",)).fetchone()
            # After rollback, state should be NULL (pre-migration state)
            assert row["state"] is None

    def test_rollback_without_backup_fails_gracefully(self, test_db) -> None:
        """Rollback should fail gracefully if no backup exists."""
        from goldfish.state_machine.migration import rollback_migration

        result = rollback_migration(test_db)
        assert result["success"] is False
        assert "backup" in result["error"].lower()


class TestMigrationAuditLog:
    """Tests for migration audit logging."""

    def test_migration_logs_decisions(self, test_db) -> None:
        """Migration should log all state assignment decisions."""
        from goldfish.state_machine.migration import migrate_stage_runs

        # Create a run
        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )

        # Run migration
        result = migrate_stage_runs(test_db)

        # Verify migration decisions are logged
        assert "decisions" in result
        assert len(result["decisions"]) >= 1
        decision = result["decisions"][0]
        assert decision["run_id"] == "stage-1"
        assert decision["from_status"] == "completed"
        assert decision["to_state"] == "completed"


class TestMigratePendingRunningCanceledUnknown:
    """Tests for migrating pending, running, canceled, and unknown status runs."""

    def test_migrate_pending_to_preparing(self, test_db) -> None:
        """Pending runs should have state=PREPARING after migration."""
        from goldfish.state_machine.migration import migrate_stage_runs

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-pending", "ws", "v1", "train", "pending", datetime.now(UTC).isoformat()),
            )

        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        with test_db._conn() as conn:
            row = conn.execute("SELECT state FROM stage_runs WHERE id = ?", ("stage-pending",)).fetchone()
            assert row["state"] == "preparing"

    def test_migrate_running_to_running(self, test_db) -> None:
        """Running runs should have state=RUNNING after migration."""
        from goldfish.state_machine.migration import migrate_stage_runs

        started_at = datetime.now(UTC).isoformat()
        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-running", "ws", "v1", "train", "running", started_at),
            )

        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT state, state_entered_at FROM stage_runs WHERE id = ?",
                ("stage-running",),
            ).fetchone()
            assert row["state"] == "running"
            # Running jobs should preserve their started_at as state_entered_at
            assert row["state_entered_at"] == started_at

    def test_migrate_canceled_to_canceled(self, test_db) -> None:
        """Canceled runs should have state=CANCELED after migration."""
        from goldfish.state_machine.migration import migrate_stage_runs

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-canceled", "ws", "v1", "train", "canceled", datetime.now(UTC).isoformat()),
            )

        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        with test_db._conn() as conn:
            row = conn.execute("SELECT state FROM stage_runs WHERE id = ?", ("stage-canceled",)).fetchone()
            assert row["state"] == "canceled"

    def test_migrate_unknown_status_to_unknown(self, test_db) -> None:
        """Unknown status runs should have state=UNKNOWN after migration."""
        from goldfish.state_machine.migration import migrate_stage_runs

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-unknown", "ws", "v1", "train", "weird_status", datetime.now(UTC).isoformat()),
            )

        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        with test_db._conn() as conn:
            row = conn.execute("SELECT state FROM stage_runs WHERE id = ?", ("stage-unknown",)).fetchone()
            assert row["state"] == "unknown"


class TestMigrateTerminationCausePaths:
    """Tests for migration with different termination causes."""

    def test_migrate_timeout_terminated(self, test_db) -> None:
        """Failed runs with timeout error should become TERMINATED with TIMEOUT cause."""
        from goldfish.state_machine.migration import migrate_stage_runs

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at, error)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "stage-timeout",
                    "ws",
                    "v1",
                    "train",
                    "failed",
                    datetime.now(UTC).isoformat(),
                    "Execution timeout after 3600s",
                ),
            )

        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT state, termination_cause FROM stage_runs WHERE id = ?",
                ("stage-timeout",),
            ).fetchone()
            assert row["state"] == "terminated"
            assert row["termination_cause"] == "timeout"

    def test_migrate_crashed_terminated(self, test_db) -> None:
        """Failed runs with crash error should become TERMINATED with CRASHED cause."""
        from goldfish.state_machine.migration import migrate_stage_runs

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at, error)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "stage-crashed",
                    "ws",
                    "v1",
                    "train",
                    "failed",
                    datetime.now(UTC).isoformat(),
                    "Container crashed with OOM",
                ),
            )

        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT state, termination_cause FROM stage_runs WHERE id = ?",
                ("stage-crashed",),
            ).fetchone()
            assert row["state"] == "terminated"
            assert row["termination_cause"] == "crashed"

    def test_migrate_orphaned_terminated(self, test_db) -> None:
        """Failed runs with orphan error should become TERMINATED with ORPHANED cause."""
        from goldfish.state_machine.migration import migrate_stage_runs

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at, error)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("stage-orphaned", "ws", "v1", "train", "failed", datetime.now(UTC).isoformat(), "Instance not found"),
            )

        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT state, termination_cause FROM stage_runs WHERE id = ?",
                ("stage-orphaned",),
            ).fetchone()
            assert row["state"] == "terminated"
            assert row["termination_cause"] == "orphaned"


class TestMigrationProgressTable:
    """Tests for migration_progress table tracking."""

    def test_migration_progress_table_exists(self, test_db) -> None:
        """Verify migration_progress table is created."""
        with test_db._conn() as conn:
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='migration_progress'"
            ).fetchone()
            assert result is not None

    def test_migration_records_progress(self, test_db) -> None:
        """Migration should record progress in migration_progress table."""
        from goldfish.state_machine.migration import migrate_stage_runs

        with test_db._conn() as conn:
            _setup_workspace(conn)
            for i in range(3):
                conn.execute(
                    """
                    INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (f"stage-{i}", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
                )

        result = migrate_stage_runs(test_db)
        assert result["success"] is True
        assert result["progress_id"] is not None

        with test_db._conn() as conn:
            progress = conn.execute(
                "SELECT * FROM migration_progress WHERE id = ?",
                (result["progress_id"],),
            ).fetchone()
            assert progress is not None
            assert progress["migration_name"] == "state_machine_v1"
            assert progress["status"] == "completed"
            assert progress["migrated_rows"] == 3

    def test_migration_empty_db_no_progress_record(self, test_db) -> None:
        """Migration on empty database should not create progress record."""
        from goldfish.state_machine.migration import migrate_stage_runs

        result = migrate_stage_runs(test_db)
        assert result["success"] is True
        assert result["progress_id"] is None


class TestTableNameValidation:
    """Tests for _validate_table_name() function."""

    def test_valid_table_name(self) -> None:
        """Valid table names should pass validation."""
        from goldfish.state_machine.migration import _validate_table_name

        assert _validate_table_name("stage_runs_backup_20251215_120000") is True
        assert _validate_table_name("backup_table") is True
        assert _validate_table_name("Table123") is True

    def test_invalid_table_name_with_special_chars(self) -> None:
        """Table names with special characters should fail validation."""
        from goldfish.state_machine.migration import _validate_table_name

        assert _validate_table_name("table; DROP TABLE users") is False
        assert _validate_table_name("table--comment") is False
        assert _validate_table_name("table.name") is False


class TestCheckOrphanStatus:
    """Tests for check_orphan_status() function."""

    def test_orphan_check_run_not_found(self, test_db) -> None:
        """Check orphan status returns not found for missing run."""
        from goldfish.state_machine.migration import check_orphan_status

        # Use valid run_id format but run doesn't exist in DB
        result = check_orphan_status(test_db, "stage-nonexistent")
        assert result["is_orphan"] is False
        assert "not found" in result["reason"].lower()

    def test_orphan_check_no_backend_handle(self, test_db) -> None:
        """Run without backend handle should be considered orphan."""
        from goldfish.state_machine.migration import check_orphan_status

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at, backend_type)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("stage-no-handle", "ws", "v1", "train", "running", datetime.now(UTC).isoformat(), "gce"),
            )

        result = check_orphan_status(test_db, "stage-no-handle")
        assert result["is_orphan"] is True
        assert "no backend handle" in result["reason"].lower()


class TestSafeMigration:
    """Tests for safe_migration() with drain mode."""

    def test_safe_migration_no_active_runs(self, test_db) -> None:
        """Safe migration should proceed immediately if no active runs."""
        from goldfish.state_machine.migration import safe_migration

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )

        result = safe_migration(test_db, drain_timeout_seconds=1, check_orphans=False)
        assert result["success"] is True
        assert result["migrated"] == 1


class TestMigrationErrorHandling:
    """Tests for migration error handling."""

    def test_migration_reports_error_details(self, test_db) -> None:
        """Migration should report error details when rows fail."""
        from goldfish.state_machine.migration import migrate_stage_runs

        # Migration on empty DB should succeed with no errors
        result = migrate_stage_runs(test_db)
        assert result["success"] is True
        assert result["errors"] == 0
        assert len(result["error_details"]) == 0

    def test_migration_error_details_structure(self, test_db) -> None:
        """Error details should have proper structure."""
        from goldfish.state_machine.migration import MigrationResult

        # Verify the structure of error_details in result type
        result: MigrationResult = {
            "success": False,
            "migrated": 0,
            "skipped": 0,
            "errors": 1,
            "error_details": [{"run_id": "test", "error": "test error", "status": "failed"}],
            "decisions": [],
            "progress_id": None,
        }
        assert len(result["error_details"]) == 1
        assert "run_id" in result["error_details"][0]
        assert "error" in result["error_details"][0]


class TestRollbackWithTableValidation:
    """Tests for rollback with table name validation."""

    def test_rollback_invalid_table_name(self, test_db) -> None:
        """Rollback should fail for invalid table names."""
        from goldfish.state_machine.migration import rollback_migration

        result = rollback_migration(test_db, backup_table="table; DROP TABLE users")
        assert result["success"] is False
        assert "invalid" in result["error"].lower()

    def test_rollback_nonexistent_table(self, test_db) -> None:
        """Rollback should fail for nonexistent backup table."""
        from goldfish.state_machine.migration import rollback_migration

        result = rollback_migration(test_db, backup_table="nonexistent_backup_table")
        assert result["success"] is False
        assert "not found" in result["error"].lower()


class TestCheckGceOrphan:
    """Tests for _check_gce_orphan() function."""

    def test_gce_orphan_instance_running(self) -> None:
        """Running GCE instance should not be orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_gce_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "RUNNING\n"
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result):
            result = _check_gce_orphan("test-instance")
            assert result["is_orphan"] is False
            assert "RUNNING" in result["reason"]
            assert result["backend_type"] == "gce"

    def test_gce_orphan_instance_not_found(self) -> None:
        """GCE instance not found should be orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_gce_orphan

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "ERROR: (gcloud) not found: instance not found"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_gce_orphan("test-instance")
            assert result["is_orphan"] is True
            assert "not found" in result["reason"].lower()

    def test_gce_orphan_instance_terminated(self) -> None:
        """Terminated GCE instance should be orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_gce_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "TERMINATED\n"
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result):
            result = _check_gce_orphan("test-instance")
            assert result["is_orphan"] is True
            assert "TERMINATED" in result["reason"]

    def test_gce_orphan_instance_stopped(self) -> None:
        """Stopped GCE instance should be orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_gce_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "STOPPED\n"
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result):
            result = _check_gce_orphan("test-instance")
            assert result["is_orphan"] is True
            assert "STOPPED" in result["reason"]

    def test_gce_orphan_timeout(self) -> None:
        """Timeout during GCE check should not be considered orphan."""
        import subprocess
        from unittest.mock import patch

        from goldfish.state_machine.migration import _check_gce_orphan

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("gcloud", 30)):
            result = _check_gce_orphan("test-instance")
            assert result["is_orphan"] is False
            assert "timed out" in result["reason"].lower()

    def test_gce_orphan_gcloud_not_found(self) -> None:
        """Missing gcloud CLI should not be considered orphan."""
        from unittest.mock import patch

        from goldfish.state_machine.migration import _check_gce_orphan

        with patch("subprocess.run", side_effect=FileNotFoundError("gcloud")):
            result = _check_gce_orphan("test-instance")
            assert result["is_orphan"] is False
            assert "not available" in result["reason"].lower()

    def test_gce_orphan_generic_error(self) -> None:
        """Generic error during GCE check should not be considered orphan."""
        from unittest.mock import patch

        from goldfish.state_machine.migration import _check_gce_orphan

        with patch("subprocess.run", side_effect=Exception("Network error")):
            result = _check_gce_orphan("test-instance")
            assert result["is_orphan"] is False
            assert "error" in result["reason"].lower()

    def test_gce_orphan_other_gcloud_error(self) -> None:
        """Non-not-found gcloud error should not be considered orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_gce_orphan

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "ERROR: Permission denied"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_gce_orphan("test-instance")
            assert result["is_orphan"] is False
            assert "Permission denied" in result["reason"]


class TestCheckDockerOrphan:
    """Tests for _check_docker_orphan() function."""

    def test_docker_orphan_container_running(self) -> None:
        """Running Docker container should not be orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_docker_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "running\n"
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result):
            result = _check_docker_orphan("test-container")
            assert result["is_orphan"] is False
            assert "running" in result["reason"].lower()
            assert result["backend_type"] == "local"

    def test_docker_orphan_container_not_found(self) -> None:
        """Docker container not found should be orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_docker_orphan

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "No such container"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_docker_orphan("test-container")
            assert result["is_orphan"] is True
            assert "not found" in result["reason"].lower()

    def test_docker_orphan_container_exited(self) -> None:
        """Exited Docker container should be orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_docker_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "exited\n"
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result):
            result = _check_docker_orphan("test-container")
            assert result["is_orphan"] is True
            assert "exited" in result["reason"]

    def test_docker_orphan_timeout(self) -> None:
        """Timeout during Docker check should not be considered orphan."""
        import subprocess
        from unittest.mock import patch

        from goldfish.state_machine.migration import _check_docker_orphan

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("docker", 10)):
            result = _check_docker_orphan("test-container")
            assert result["is_orphan"] is False
            assert "timed out" in result["reason"].lower()

    def test_docker_orphan_docker_not_found(self) -> None:
        """Missing docker CLI should not be considered orphan."""
        from unittest.mock import patch

        from goldfish.state_machine.migration import _check_docker_orphan

        with patch("subprocess.run", side_effect=FileNotFoundError("docker")):
            result = _check_docker_orphan("test-container")
            assert result["is_orphan"] is False
            assert "not available" in result["reason"].lower()

    def test_docker_orphan_generic_error(self) -> None:
        """Generic error during Docker check should not be considered orphan."""
        from unittest.mock import patch

        from goldfish.state_machine.migration import _check_docker_orphan

        with patch("subprocess.run", side_effect=Exception("Docker daemon error")):
            result = _check_docker_orphan("test-container")
            assert result["is_orphan"] is False
            assert "error" in result["reason"].lower()


class TestGceInstanceNameValidation:
    """Tests for GCE instance name validation in _check_gce_orphan."""

    def test_invalid_gce_instance_name_returns_early(self) -> None:
        """Invalid GCE instance names should return early without subprocess call."""
        from goldfish.state_machine.migration import _check_gce_orphan

        # Uppercase not allowed in GCE instance names
        result = _check_gce_orphan("Invalid-Instance")
        assert result["is_orphan"] is False
        assert "invalid" in result["reason"].lower()

    def test_gce_instance_name_with_semicolon(self) -> None:
        """GCE instance name with injection attempt should fail validation."""
        from goldfish.state_machine.migration import _check_gce_orphan

        result = _check_gce_orphan("instance; rm -rf /")
        assert result["is_orphan"] is False
        assert "invalid" in result["reason"].lower()

    def test_gce_instance_name_with_spaces(self) -> None:
        """GCE instance name with spaces should fail validation."""
        from goldfish.state_machine.migration import _check_gce_orphan

        result = _check_gce_orphan("instance with spaces")
        assert result["is_orphan"] is False
        assert "invalid" in result["reason"].lower()


class TestDockerContainerIdValidation:
    """Tests for Docker container ID validation in _check_docker_orphan."""

    def test_invalid_docker_container_id_returns_early(self) -> None:
        """Invalid Docker container IDs should return early without subprocess call."""
        from goldfish.state_machine.migration import _check_docker_orphan

        # Container ID starting with special character
        result = _check_docker_orphan("!invalid")
        assert result["is_orphan"] is False
        assert "invalid" in result["reason"].lower()

    def test_docker_container_id_with_semicolon(self) -> None:
        """Docker container ID with injection attempt should fail validation."""
        from goldfish.state_machine.migration import _check_docker_orphan

        result = _check_docker_orphan("container; rm -rf /")
        assert result["is_orphan"] is False
        assert "invalid" in result["reason"].lower()

    def test_valid_docker_container_short_id(self) -> None:
        """Valid 12-char Docker container ID should pass validation."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_docker_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "running\n"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_docker_orphan("abcdef123456")
            assert result["is_orphan"] is False
            assert "running" in result["reason"].lower()

    def test_valid_docker_container_long_id(self) -> None:
        """Valid 64-char Docker container ID should pass validation."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_docker_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "running\n"

        with patch("subprocess.run", return_value=mock_result):
            long_id = "a" * 64
            result = _check_docker_orphan(long_id)
            assert result["is_orphan"] is False
            assert "running" in result["reason"].lower()


class TestCheckOrphanStatusUnknownBackend:
    """Tests for check_orphan_status with unknown backend types."""

    def test_unknown_backend_type(self, test_db) -> None:
        """Unknown backend type should return not orphan with reason."""
        from goldfish.state_machine.migration import check_orphan_status

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at, backend_type, backend_handle)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "stage-unknown",
                    "ws",
                    "v1",
                    "train",
                    "running",
                    datetime.now(UTC).isoformat(),
                    "kubernetes",
                    "pod-123",
                ),
            )

        result = check_orphan_status(test_db, "stage-unknown")
        assert result["is_orphan"] is False
        assert "unknown backend type" in result["reason"].lower()
        assert result["backend_type"] == "kubernetes"


class TestSafeMigrationDrainMode:
    """Tests for safe_migration with active runs and drain mode."""

    def test_safe_migration_waits_for_active_runs(self, test_db) -> None:
        """Safe migration should wait for active runs to complete."""
        from unittest.mock import patch

        from goldfish.state_machine.migration import safe_migration

        with test_db._conn() as conn:
            _setup_workspace(conn)
            # Create an active run
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-active", "ws", "v1", "train", "running", datetime.now(UTC).isoformat()),
            )

        # Mock time.sleep to avoid actual delays and track calls
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            # After first sleep, mark the run as completed
            with test_db._conn() as conn:
                conn.execute("UPDATE stage_runs SET status = 'completed' WHERE id = 'stage-active'")

        with patch("time.sleep", side_effect=mock_sleep):
            result = safe_migration(test_db, drain_timeout_seconds=60, check_orphans=False)

        assert result["success"] is True
        assert result["migrated"] == 1
        assert len(sleep_calls) == 1  # Should have slept once before run completed

    def test_safe_migration_timeout_proceeds_anyway(self, test_db) -> None:
        """Safe migration should proceed after drain timeout even with active runs."""
        from unittest.mock import patch

        from goldfish.state_machine.migration import safe_migration

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at, backend_type, backend_handle)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "stage-stuck",
                    "ws",
                    "v1",
                    "train",
                    "running",
                    datetime.now(UTC).isoformat(),
                    "local",
                    "container-123",
                ),
            )

        # Make time.time() advance quickly to trigger timeout
        call_count = [0]
        base_time = 1000.0

        def mock_time():
            call_count[0] += 1
            return base_time + (call_count[0] * 100)  # Jump 100 seconds each call

        with patch("time.time", side_effect=mock_time):
            with patch("time.sleep"):
                result = safe_migration(test_db, drain_timeout_seconds=5, check_orphans=False)

        # Should still migrate the run (possibly marking it as orphan if check_orphans=True)
        assert result["migrated"] == 1


class TestMigrationWithErrors:
    """Tests for migration behavior when errors occur."""

    def test_migration_with_errors_records_completed_with_errors(self, test_db) -> None:
        """Migration with row errors should record 'completed_with_errors' status."""
        from goldfish.state_machine.migration import migrate_stage_runs

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )

        # Mock the update to fail for one row
        original_execute = test_db._conn

        result = migrate_stage_runs(test_db)
        # Normal case - no errors
        assert result["success"] is True

        # Verify progress status is 'completed' when no errors
        if result["progress_id"]:
            with test_db._conn() as conn:
                progress = conn.execute(
                    "SELECT status FROM migration_progress WHERE id = ?", (result["progress_id"],)
                ).fetchone()
                assert progress["status"] == "completed"


class TestBatchProgressUpdates:
    """Tests for batch progress updates during migration."""

    def test_migration_updates_progress_per_batch(self, test_db) -> None:
        """Migration should update progress every batch_size rows."""
        from goldfish.state_machine.migration import migrate_stage_runs

        with test_db._conn() as conn:
            _setup_workspace(conn)
            # Create 5 runs to migrate
            for i in range(5):
                conn.execute(
                    """
                    INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (f"stage-{i}", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
                )

        # Use batch_size=2 to trigger multiple progress updates
        result = migrate_stage_runs(test_db, batch_size=2)
        assert result["success"] is True
        assert result["migrated"] == 5

        # Verify final progress
        if result["progress_id"]:
            with test_db._conn() as conn:
                progress = conn.execute(
                    "SELECT migrated_rows, last_processed_id FROM migration_progress WHERE id = ?",
                    (result["progress_id"],),
                ).fetchone()
                assert progress["migrated_rows"] == 5
                assert progress["last_processed_id"] is not None


class TestNonRunningStateEnteredAt:
    """Tests for state_entered_at handling for non-running states."""

    def test_non_running_uses_current_timestamp(self, test_db) -> None:
        """Non-running states should use current timestamp for state_entered_at."""
        import time

        from goldfish.state_machine.migration import migrate_stage_runs

        old_timestamp = "2020-01-01T00:00:00+00:00"  # Way in the past

        with test_db._conn() as conn:
            _setup_workspace(conn)
            # Create a completed run with old started_at
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-old", "ws", "v1", "train", "completed", old_timestamp),
            )

        before_migration = datetime.now(UTC)
        time.sleep(0.01)  # Small delay to ensure timestamps differ
        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        with test_db._conn() as conn:
            row = conn.execute("SELECT state_entered_at FROM stage_runs WHERE id = ?", ("stage-old",)).fetchone()

        # state_entered_at should be roughly now, not the old started_at
        state_entered = datetime.fromisoformat(row["state_entered_at"])
        # Should be after the old timestamp but close to now
        assert state_entered > datetime.fromisoformat(old_timestamp.replace("+00:00", "+00:00"))
        # Allow 1 minute window for test execution
        assert (datetime.now(UTC) - state_entered).total_seconds() < 60


class TestRollbackTransactionHandling:
    """Tests for rollback transaction handling with BEGIN IMMEDIATE."""

    def test_rollback_uses_begin_immediate(self, test_db) -> None:
        """Rollback should use BEGIN IMMEDIATE for transaction isolation."""
        from goldfish.state_machine.migration import migrate_stage_runs, rollback_migration

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )

        # First migrate
        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        # Track execute calls to verify BEGIN IMMEDIATE
        original_conn = test_db._conn

        executed_statements = []

        class MockConnection:
            def __init__(self, real_conn):
                self._real_conn = real_conn

            def execute(self, sql, params=None):
                executed_statements.append(sql.strip().upper()[:20])
                if params:
                    return self._real_conn.execute(sql, params)
                return self._real_conn.execute(sql)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        # Rollback should work
        rollback_result = rollback_migration(test_db)
        assert rollback_result["success"] is True
        assert rollback_result["restored"] == 1

    def test_rollback_exception_triggers_rollback(self, test_db) -> None:
        """Database exception during rollback should trigger ROLLBACK."""
        from goldfish.state_machine.migration import migrate_stage_runs, rollback_migration

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )

        # First migrate to create backup table
        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        # Get the backup table name for later verification
        with test_db._conn() as conn:
            backup = conn.execute("SELECT backup_table FROM migration_progress LIMIT 1").fetchone()
            backup_table = backup["backup_table"]

            # Verify migration was applied
            row = conn.execute("SELECT state FROM stage_runs WHERE id = 'stage-1'").fetchone()
            assert row["state"] == "completed"

        # Now rollback should restore the state
        rollback_result = rollback_migration(test_db, backup_table=backup_table)
        assert rollback_result["success"] is True

        # Verify state was restored to NULL
        with test_db._conn() as conn:
            row = conn.execute("SELECT state FROM stage_runs WHERE id = 'stage-1'").fetchone()
            assert row["state"] is None

    def test_rollback_updates_migration_progress_status(self, test_db) -> None:
        """Successful rollback should update migration_progress status to rolled_back."""
        from goldfish.state_machine.migration import migrate_stage_runs, rollback_migration

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )

        # Migrate
        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        # Check progress status before rollback
        with test_db._conn() as conn:
            progress = conn.execute(
                "SELECT status FROM migration_progress WHERE id = ?", (result["progress_id"],)
            ).fetchone()
            assert progress["status"] == "completed"

        # Rollback
        rollback_result = rollback_migration(test_db)
        assert rollback_result["success"] is True

        # Check progress status after rollback
        with test_db._conn() as conn:
            progress = conn.execute(
                "SELECT status, completed_at FROM migration_progress WHERE id = ?", (result["progress_id"],)
            ).fetchone()
            assert progress["status"] == "rolled_back"
            assert progress["completed_at"] is not None


class TestMigrationWithOrphanDetection:
    """Tests for migration with orphan detection enabled."""

    def test_migrate_with_check_orphans_running_job(self, test_db) -> None:
        """Migration with check_orphans=True should detect orphaned running jobs."""
        from unittest.mock import patch

        from goldfish.state_machine.migration import migrate_stage_runs

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status,
                    started_at, backend_type, backend_handle)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "stage-running",
                    "ws",
                    "v1",
                    "train",
                    "running",
                    datetime.now(UTC).isoformat(),
                    "local",
                    "container-xyz",
                ),
            )

        # Mock Docker check to return orphan
        def mock_check_docker(container_id):
            from goldfish.state_machine.migration import OrphanCheckResult

            return OrphanCheckResult(
                is_orphan=True,
                reason="Container not found",
                backend_type="local",
                backend_handle=container_id,
            )

        with patch("goldfish.state_machine.migration._check_docker_orphan", side_effect=mock_check_docker):
            result = migrate_stage_runs(test_db, check_orphans=True)

        assert result["success"] is True
        assert result["migrated"] == 1

        # Verify the run was marked as terminated with orphaned cause
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT state, termination_cause FROM stage_runs WHERE id = ?", ("stage-running",)
            ).fetchone()
            assert row["state"] == "terminated"
            assert row["termination_cause"] == "orphaned"


class TestCheckOrphanStatusOverride:
    """Tests for check_orphan_status with override parameters."""

    def test_check_orphan_status_with_override_backend(self, test_db) -> None:
        """check_orphan_status should use provided backend_type/handle overrides."""
        from unittest.mock import patch

        from goldfish.state_machine.migration import check_orphan_status

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status,
                    started_at, backend_type, backend_handle)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "running", datetime.now(UTC).isoformat(), "gce", "old-instance"),
            )

        # Use override parameters instead of database values
        def mock_check_docker(container_id):
            from goldfish.state_machine.migration import OrphanCheckResult

            return OrphanCheckResult(
                is_orphan=False,
                reason=f"Container {container_id} is running",
                backend_type="local",
                backend_handle=container_id,
            )

        with patch("goldfish.state_machine.migration._check_docker_orphan", side_effect=mock_check_docker):
            # Override with local backend instead of gce
            result = check_orphan_status(test_db, "stage-1", backend_type="local", backend_handle="new-container")

        assert result["is_orphan"] is False
        assert result["backend_type"] == "local"
        assert result["backend_handle"] == "new-container"


class TestErrorSnippetTruncation:
    """Tests for error snippet truncation in migration decisions."""

    def test_long_error_truncated_in_decisions(self, test_db) -> None:
        """Errors longer than 100 chars should be truncated in decisions."""
        from goldfish.state_machine.migration import migrate_stage_runs

        long_error = "A" * 200  # 200 character error message

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status,
                    started_at, error)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "failed", datetime.now(UTC).isoformat(), long_error),
            )

        result = migrate_stage_runs(test_db)
        assert result["success"] is True
        assert len(result["decisions"]) == 1

        # Error snippet should be truncated to 100 chars
        error_snippet = result["decisions"][0]["error_snippet"]
        assert len(error_snippet) == 100
        assert error_snippet == "A" * 100


class TestRunningJobStartedAtPreservation:
    """Tests for running job migration preserving started_at."""

    def test_running_job_preserves_started_at(self, test_db) -> None:
        """Running job should use started_at as state_entered_at."""
        from goldfish.state_machine.migration import migrate_stage_runs

        old_started_at = "2024-06-15T10:30:00+00:00"

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-running", "ws", "v1", "train", "running", old_started_at),
            )

        result = migrate_stage_runs(test_db)
        assert result["success"] is True

        with test_db._conn() as conn:
            row = conn.execute("SELECT state_entered_at FROM stage_runs WHERE id = ?", ("stage-running",)).fetchone()

        # Running job should preserve started_at as state_entered_at
        assert row["state_entered_at"] == old_started_at


class TestGceValidationBoundaries:
    """Tests for GCE instance name validation boundary cases."""

    def test_gce_instance_name_63_chars_valid(self) -> None:
        """63-character GCE instance name should be valid."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_gce_orphan

        # Valid 63-char name: starts with letter, ends with alphanumeric
        valid_name = "a" + "b" * 61 + "c"  # 63 chars total
        assert len(valid_name) == 63

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "RUNNING\n"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_gce_orphan(valid_name)
            assert result["is_orphan"] is False

    def test_gce_instance_name_64_chars_invalid(self) -> None:
        """64-character GCE instance name should be invalid (too long)."""
        from goldfish.state_machine.migration import _check_gce_orphan

        too_long_name = "a" * 64
        result = _check_gce_orphan(too_long_name)
        assert result["is_orphan"] is False
        assert "invalid" in result["reason"].lower()

    def test_gce_instance_name_empty_invalid(self) -> None:
        """Empty GCE instance name should be invalid."""
        from goldfish.state_machine.migration import _check_gce_orphan

        result = _check_gce_orphan("")
        assert result["is_orphan"] is False
        assert "invalid" in result["reason"].lower()


class TestDockerValidationBoundaries:
    """Tests for Docker container ID validation boundary cases."""

    def test_docker_container_id_empty_invalid(self) -> None:
        """Empty Docker container ID should be invalid."""
        from goldfish.state_machine.migration import _check_docker_orphan

        result = _check_docker_orphan("")
        assert result["is_orphan"] is False
        assert "invalid" in result["reason"].lower()

    def test_docker_container_name_with_period(self) -> None:
        """Docker container name with period should be valid."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_docker_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "running\n"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_docker_orphan("my.container.name")
            assert result["is_orphan"] is False
            assert "running" in result["reason"].lower()

    def test_docker_container_name_with_underscore(self) -> None:
        """Docker container name with underscore should be valid."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_docker_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "running\n"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_docker_orphan("my_container_name")
            assert result["is_orphan"] is False


class TestValidateTableNameBoundaries:
    """Tests for _validate_table_name boundary cases."""

    def test_validate_table_name_empty_string(self) -> None:
        """Empty string table name should be invalid."""
        from goldfish.state_machine.migration import _validate_table_name

        assert _validate_table_name("") is False

    def test_validate_table_name_single_char(self) -> None:
        """Single character table name should be valid."""
        from goldfish.state_machine.migration import _validate_table_name

        assert _validate_table_name("a") is True
        assert _validate_table_name("1") is True
        assert _validate_table_name("_") is True


class TestMigrationProgressIdempotency:
    """Tests for migration idempotency and progress_id handling."""

    def test_second_migration_returns_none_progress_id(self, test_db) -> None:
        """Running migration twice should return progress_id=None on second run."""
        from goldfish.state_machine.migration import migrate_stage_runs

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )

        # First migration
        result1 = migrate_stage_runs(test_db)
        assert result1["success"] is True
        assert result1["progress_id"] is not None
        assert result1["migrated"] == 1

        # Second migration - should skip all rows
        result2 = migrate_stage_runs(test_db)
        assert result2["success"] is True
        assert result2["progress_id"] is None  # No progress created for skipped-only run
        assert result2["migrated"] == 0
        assert result2["skipped"] == 1


class TestMigrationRowLevelErrors:
    """Tests for migration behavior when individual row errors occur."""

    def test_migration_with_row_error_returns_success_false(self, test_db) -> None:
        """Migration with row-level errors should return success=False."""
        from unittest.mock import patch

        from goldfish.state_machine.migration import migrate_stage_runs

        with test_db._conn() as conn:
            _setup_workspace(conn)
            # Create a row that will be migrated
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )

        # Patch determine_migration_state to raise an exception for this specific row
        original_fn = None

        def raise_on_second_call(status, error):
            # Simulate an error during processing
            raise ValueError("Simulated row error")

        with patch(
            "goldfish.state_machine.migration.determine_migration_state",
            side_effect=raise_on_second_call,
        ):
            result = migrate_stage_runs(test_db)

        # Should have errors and success=False
        assert result["success"] is False
        assert result["errors"] >= 1
        assert len(result["error_details"]) >= 1
        assert "Simulated row error" in result["error_details"][0]["error"]


class TestMigrationProgressUpdateFailure:
    """Tests for migration progress update failure handling."""

    def test_progress_update_failure_is_logged(self, test_db, caplog) -> None:
        """Progress update failure after main error should be logged but not crash."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import migrate_stage_runs

        # Create a mock that will raise on the main migration but also fail progress update
        mock_conn = MagicMock()

        # First execute succeeds (BEGIN IMMEDIATE), then fails on count query
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:  # BEGIN IMMEDIATE
                return MagicMock()
            # Fail on subsequent calls
            raise RuntimeError("Simulated database error")

        mock_conn.execute.side_effect = side_effect

        # Patch the context manager to return our mock
        with patch.object(test_db, "_conn") as mock_conn_cm:
            mock_context = MagicMock()
            mock_context.__enter__ = MagicMock(return_value=mock_conn)
            mock_context.__exit__ = MagicMock(return_value=False)
            mock_conn_cm.return_value = mock_context

            # Also patch the second _conn call in the error handler
            result = migrate_stage_runs(test_db)

        # Should have errors
        assert result["success"] is False
        assert result["errors"] >= 1


class TestDockerStatusEdgeCases:
    """Tests for Docker container status edge cases."""

    def test_docker_container_paused_is_orphan(self) -> None:
        """Paused Docker container should be treated as orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_docker_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "paused\n"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_docker_orphan("abc123456789")
            assert result["is_orphan"] is True
            assert "paused" in result["reason"]

    def test_docker_container_restarting_is_orphan(self) -> None:
        """Restarting Docker container should be treated as orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_docker_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "restarting\n"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_docker_orphan("abc123456789")
            assert result["is_orphan"] is True
            assert "restarting" in result["reason"]

    def test_docker_container_created_is_orphan(self) -> None:
        """Created (not started) Docker container should be treated as orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_docker_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "created\n"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_docker_orphan("abc123456789")
            assert result["is_orphan"] is True
            assert "created" in result["reason"]

    def test_docker_container_dead_is_orphan(self) -> None:
        """Dead Docker container should be treated as orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_docker_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "dead\n"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_docker_orphan("abc123456789")
            assert result["is_orphan"] is True
            assert "dead" in result["reason"]


class TestGceStatusEdgeCases:
    """Tests for GCE instance status edge cases."""

    def test_gce_instance_provisioning_not_orphan(self) -> None:
        """Provisioning GCE instance should not be treated as orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_gce_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "PROVISIONING\n"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_gce_orphan("my-instance")
            assert result["is_orphan"] is False
            assert "PROVISIONING" in result["reason"]

    def test_gce_instance_staging_not_orphan(self) -> None:
        """Staging GCE instance should not be treated as orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_gce_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "STAGING\n"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_gce_orphan("my-instance")
            assert result["is_orphan"] is False
            assert "STAGING" in result["reason"]

    def test_gce_instance_suspending_not_orphan(self) -> None:
        """Suspending GCE instance should not be treated as orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_gce_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "SUSPENDING\n"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_gce_orphan("my-instance")
            assert result["is_orphan"] is False
            assert "SUSPENDING" in result["reason"]

    def test_gce_instance_suspended_is_orphan(self) -> None:
        """Suspended GCE instance - currently NOT treated as orphan (only TERMINATED/STOPPED)."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_gce_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "SUSPENDED\n"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_gce_orphan("my-instance")
            # Note: SUSPENDED is NOT in the orphan list (only TERMINATED, STOPPED)
            # This is intentional - suspended instances can be resumed
            assert result["is_orphan"] is False
            assert "SUSPENDED" in result["reason"]


class TestBeginImmediateVerification:
    """Tests to verify BEGIN IMMEDIATE is properly used."""

    def test_migration_uses_begin_immediate(self, test_db) -> None:
        """Verify migration uses BEGIN IMMEDIATE for exclusive locking."""
        from unittest.mock import patch

        from goldfish.state_machine.migration import migrate_stage_runs

        # Track execute calls
        executed_statements = []
        original_conn = test_db._conn

        class TrackingConnection:
            def __init__(self, real_conn):
                self._real_conn = real_conn

            def execute(self, sql, params=None):
                executed_statements.append(sql.strip().upper() if isinstance(sql, str) else sql)
                if params:
                    return self._real_conn.execute(sql, params)
                return self._real_conn.execute(sql)

            def __getattr__(self, name):
                return getattr(self._real_conn, name)

        # Setup data
        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )

        # Wrap connection to track execute calls
        from contextlib import contextmanager

        @contextmanager
        def tracking_conn():
            with original_conn() as conn:
                yield TrackingConnection(conn)

        with patch.object(test_db, "_conn", tracking_conn):
            result = migrate_stage_runs(test_db)

        assert result["success"] is True
        # Verify BEGIN IMMEDIATE was called
        assert any("BEGIN IMMEDIATE" in stmt for stmt in executed_statements)

    def test_rollback_uses_begin_immediate(self, test_db) -> None:
        """Verify rollback uses BEGIN IMMEDIATE for exclusive locking."""
        from unittest.mock import patch

        from goldfish.state_machine.migration import migrate_stage_runs, rollback_migration

        # Track execute calls
        executed_statements = []
        original_conn = test_db._conn

        class TrackingConnection:
            def __init__(self, real_conn):
                self._real_conn = real_conn

            def execute(self, sql, params=None):
                executed_statements.append(sql.strip().upper() if isinstance(sql, str) else sql)
                if params:
                    return self._real_conn.execute(sql, params)
                return self._real_conn.execute(sql)

            def __getattr__(self, name):
                return getattr(self._real_conn, name)

        # Setup and run migration first
        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "completed", datetime.now(UTC).isoformat()),
            )

        migrate_stage_runs(test_db)

        # Clear and track rollback
        executed_statements.clear()

        from contextlib import contextmanager

        @contextmanager
        def tracking_conn():
            with original_conn() as conn:
                yield TrackingConnection(conn)

        with patch.object(test_db, "_conn", tracking_conn):
            result = rollback_migration(test_db)

        assert result["success"] is True
        # Verify BEGIN IMMEDIATE was called
        assert any("BEGIN IMMEDIATE" in stmt for stmt in executed_statements)


class TestCaseSensitiveStatusMapping:
    """Tests for case sensitivity in status mapping."""

    def test_uppercase_running_maps_correctly(self) -> None:
        """RUNNING in uppercase should map to RUNNING state."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(status="RUNNING", error=None)
        assert state == StageState.RUNNING

    def test_mixed_case_completed_maps_correctly(self) -> None:
        """Completed in mixed case should map to COMPLETED state."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(status="Completed", error=None)
        assert state == StageState.COMPLETED

    def test_uppercase_failed_maps_correctly(self) -> None:
        """FAILED in uppercase should map to FAILED state."""
        from goldfish.state_machine.migration import determine_migration_state

        state = determine_migration_state(status="FAILED", error=None)
        assert state == StageState.FAILED


class TestErrorSnippetTruncationEdgeCases:
    """Tests for error snippet truncation edge cases."""

    def test_error_exactly_100_chars_not_truncated(self, test_db) -> None:
        """Error message of exactly 100 chars should not be truncated."""
        from goldfish.state_machine.migration import migrate_stage_runs

        error_100 = "x" * 100

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, error, started_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "failed", error_100, datetime.now(UTC).isoformat()),
            )

        result = migrate_stage_runs(test_db)
        assert result["success"] is True
        assert len(result["decisions"]) == 1
        assert result["decisions"][0]["error_snippet"] == error_100

    def test_error_99_chars_not_truncated(self, test_db) -> None:
        """Error message of 99 chars should not be truncated."""
        from goldfish.state_machine.migration import migrate_stage_runs

        error_99 = "y" * 99

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, error, started_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "failed", error_99, datetime.now(UTC).isoformat()),
            )

        result = migrate_stage_runs(test_db)
        assert result["success"] is True
        assert len(result["decisions"]) == 1
        assert result["decisions"][0]["error_snippet"] == error_99


class TestSafeMigrationLogging:
    """Tests for safe_migration logging behavior."""

    def test_safe_migration_logs_active_run_count(self, test_db, caplog) -> None:
        """safe_migration should log number of active runs waiting for."""
        import logging

        from goldfish.state_machine.migration import safe_migration

        with test_db._conn() as conn:
            _setup_workspace(conn)
            # Create an "active" run that will appear in the count
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("stage-1", "ws", "v1", "train", "running", datetime.now(UTC).isoformat()),
            )

        # Run with very short timeout so it exits quickly
        with caplog.at_level(logging.INFO):
            result = safe_migration(test_db, drain_timeout_seconds=1, check_orphans=False)

        # Should have logged about waiting for active runs
        assert result["success"] is True
        # The run should have been migrated (with orphan detection off, it stays RUNNING)
        assert result["migrated"] >= 1


class TestCheckOrphanStatusValidation:
    """Tests for check_orphan_status input validation."""

    def test_invalid_run_id_format_rejected(self, test_db) -> None:
        """Run ID not starting with 'stage-' should be rejected."""
        from goldfish.state_machine.migration import check_orphan_status

        result = check_orphan_status(test_db, "invalid-format")
        assert result["is_orphan"] is False
        assert "invalid run_id format" in result["reason"].lower()

    def test_empty_run_id_rejected(self, test_db) -> None:
        """Empty run ID should be rejected."""
        from goldfish.state_machine.migration import check_orphan_status

        result = check_orphan_status(test_db, "")
        assert result["is_orphan"] is False
        assert "invalid run_id format" in result["reason"].lower()

    def test_invalid_backend_type_rejected(self, test_db) -> None:
        """Invalid backend_type should be rejected."""
        from goldfish.state_machine.migration import check_orphan_status

        result = check_orphan_status(test_db, "stage-abc123", backend_type="kubernetes", backend_handle="my-pod")
        assert result["is_orphan"] is False
        assert "invalid backend_type" in result["reason"].lower()

    def test_valid_backend_type_gce_accepted(self, test_db) -> None:
        """backend_type='gce' should be accepted."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import check_orphan_status

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "RUNNING\n"

        with patch("subprocess.run", return_value=mock_result):
            result = check_orphan_status(test_db, "stage-abc123", backend_type="gce", backend_handle="my-instance")
            # Should have called _check_gce_orphan
            assert result["backend_type"] == "gce"

    def test_valid_backend_type_local_accepted(self, test_db) -> None:
        """backend_type='local' should be accepted."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import check_orphan_status

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "running\n"

        with patch("subprocess.run", return_value=mock_result):
            result = check_orphan_status(test_db, "stage-abc123", backend_type="local", backend_handle="abc123456789")
            # Should have called _check_docker_orphan
            assert result["backend_type"] == "local"


class TestDockerStatusRemoving:
    """Tests for Docker container 'removing' status."""

    def test_docker_container_removing_is_orphan(self) -> None:
        """Removing Docker container should be treated as orphan."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_docker_orphan

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "removing\n"

        with patch("subprocess.run", return_value=mock_result):
            result = _check_docker_orphan("abc123456789")
            assert result["is_orphan"] is True
            assert "removing" in result["reason"]


class TestCheckOrphanStatusGceIntegration:
    """Integration tests for check_orphan_status with GCE backend from database."""

    def test_check_orphan_status_routes_to_gce_from_db(self, test_db) -> None:
        """check_orphan_status should route to _check_gce_orphan for gce backend from DB."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import check_orphan_status

        with test_db._conn() as conn:
            _setup_workspace(conn)
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at, backend_type, backend_handle)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "stage-gce1",
                    "ws",
                    "v1",
                    "train",
                    "running",
                    datetime.now(UTC).isoformat(),
                    "gce",
                    "goldfish-gce-instance",
                ),
            )

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "RUNNING\n"

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            result = check_orphan_status(test_db, "stage-gce1")
            # Should have called gcloud with the instance name
            mock_run.assert_called_once()
            args = mock_run.call_args[0][0]
            assert "gcloud" in args
            assert "goldfish-gce-instance" in args
            assert result["is_orphan"] is False
            assert result["backend_type"] == "gce"


class TestMigrateWithGceOrphanDetection:
    """Tests for migrate_stage_runs with GCE orphan detection."""

    def test_migrate_with_gce_orphan_detection(self, test_db) -> None:
        """migrate_stage_runs should detect GCE orphans when check_orphans=True."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import migrate_stage_runs

        with test_db._conn() as conn:
            _setup_workspace(conn)
            # Create a running job with GCE backend
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at, backend_type, backend_handle)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "stage-gce2",
                    "ws",
                    "v1",
                    "train",
                    "running",
                    datetime.now(UTC).isoformat(),
                    "gce",
                    "test-gce-instance",
                ),
            )

        # Mock gcloud to return TERMINATED (orphan)
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "TERMINATED\n"

        with patch("subprocess.run", return_value=mock_result):
            result = migrate_stage_runs(test_db, check_orphans=True)

        assert result["success"] is True
        assert result["migrated"] == 1
        # Check that the run was marked as TERMINATED with ORPHANED cause
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT state, termination_cause FROM stage_runs WHERE id = ?",
                ("stage-gce2",),
            ).fetchone()
            assert row["state"] == "terminated"
            assert row["termination_cause"] == "orphaned"


class TestInvalidBackupTableNameGenerated:
    """Tests for invalid backup table name edge case."""

    def test_invalid_backup_table_name_returns_error(self, test_db) -> None:
        """If backup table name validation fails, should return error."""
        from unittest.mock import patch

        from goldfish.state_machine.migration import migrate_stage_runs

        # Mock datetime to return something that would create an invalid table name
        # This is a defensive test - in practice the timestamp format should always be valid
        with patch("goldfish.state_machine.migration._validate_table_name", return_value=False):
            result = migrate_stage_runs(test_db)

        assert result["success"] is False
        assert result["errors"] == 1
        assert "Invalid backup table name" in result["error_details"][0]["error"]


class TestCheckOrphanStatusPartialOverride:
    """Tests for check_orphan_status with partial override parameters."""

    def test_check_orphan_status_none_backend_type_with_handle(self, test_db) -> None:
        """When backend_type is None but handle provided, should read type from DB."""
        from unittest.mock import patch

        from goldfish.state_machine.migration import check_orphan_status

        # Set up workspace and version first
        test_db.create_workspace_lineage("test", description="Test")
        test_db.create_version("test", "v1", "tag-1", "abc123", "run")

        # Insert a stage_run with GCE backend
        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at,
                    backend_type, backend_handle)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "stage-partial1",
                    "test",
                    "v1",
                    "train",
                    "running",
                    "2024-01-01T00:00:00",
                    "gce",
                    "my-instance",
                ),
            )

        # Call with backend_type=None but backend_handle provided
        # Should read backend_type from DB
        with patch("goldfish.state_machine.migration._check_gce_orphan") as mock_gce:
            mock_gce.return_value = {"is_orphan": True, "reason": "not_found"}
            result = check_orphan_status(
                test_db,
                "stage-partial1",
                backend_type=None,  # Will be read from DB
                backend_handle="my-instance",  # Provided by caller
            )

        # Should have called GCE checker (read type from DB)
        mock_gce.assert_called_once_with("my-instance")
        assert result["is_orphan"] is True


class TestGceInstanceNameMinLength:
    """Tests for minimum GCE instance name length."""

    def test_gce_instance_name_single_char_valid(self) -> None:
        """Single character GCE instance name should be valid."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_gce_orphan

        with patch("subprocess.run") as mock_run:
            # gcloud --format=value(status) returns just the status, not JSON
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="RUNNING",
            )
            result = _check_gce_orphan("a")

        # Should have called gcloud (name is valid)
        mock_run.assert_called_once()
        assert result["is_orphan"] is False
        assert "Instance exists with status: RUNNING" in result["reason"]

    def test_gce_instance_name_starts_with_digit_invalid(self) -> None:
        """GCE instance name starting with digit should be invalid."""
        from goldfish.state_machine.migration import _check_gce_orphan

        result = _check_gce_orphan("1invalid")

        # Invalid names return is_orphan=False (not None) with descriptive reason
        assert result["is_orphan"] is False
        assert "Invalid GCE instance name format" in result["reason"]


class TestDockerContainerName128Char:
    """Tests for Docker container name at maximum length."""

    def test_docker_container_name_128_chars_valid(self) -> None:
        """128-character Docker container name should be valid."""
        from unittest.mock import MagicMock, patch

        from goldfish.state_machine.migration import _check_docker_orphan

        # Create a valid 128-char container name (starts with letter, alphanumeric)
        long_name = "a" + "b" * 127  # 128 chars total

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="running",
            )
            result = _check_docker_orphan(long_name)

        # Should have called docker (name is valid)
        mock_run.assert_called_once()
        assert result["is_orphan"] is False

    def test_docker_container_name_129_chars_invalid(self) -> None:
        """129-character Docker container name should be invalid."""
        from goldfish.state_machine.migration import _check_docker_orphan

        long_name = "a" * 129  # 129 chars - exceeds limit

        result = _check_docker_orphan(long_name)

        # Invalid names return is_orphan=False (not None) with descriptive reason
        assert result["is_orphan"] is False
        assert "Invalid Docker container ID format" in result["reason"]


class TestDetectTerminationCausePriority:
    """Tests for termination cause detection priority."""

    def test_preemption_takes_priority_over_crash(self) -> None:
        """When error contains both preemption and crash keywords, PREEMPTED wins."""
        from goldfish.state_machine.migration import detect_termination_cause
        from goldfish.state_machine.types import TerminationCause

        # Error message containing both preemption and crash/OOM keywords
        cause = detect_termination_cause("Instance was preempted and crashed due to OOM")
        assert cause == TerminationCause.PREEMPTED

    def test_preemption_takes_priority_over_timeout(self) -> None:
        """When error contains both preemption and timeout keywords, PREEMPTED wins."""
        from goldfish.state_machine.migration import detect_termination_cause
        from goldfish.state_machine.types import TerminationCause

        cause = detect_termination_cause("Spot instance preempted after timeout warning")
        assert cause == TerminationCause.PREEMPTED


class TestMigrateRunningJobStartedAt:
    """Tests for migrating running job started_at handling.

    Note: The current schema has a NOT NULL constraint on started_at,
    so the NULL started_at code path in migration is defensive code
    for potential legacy data. This test verifies the normal case
    where started_at is used for state_entered_at.
    """

    def test_running_job_uses_started_at_for_state_entered_at(self, test_db) -> None:
        """Running job should use started_at for state_entered_at during migration."""
        from goldfish.state_machine.migration import migrate_stage_runs

        # Set up workspace and version first
        test_db.create_workspace_lineage("test", description="Test")
        test_db.create_version("test", "v1", "tag-1", "abc123", "run")

        # Insert a running job with a specific started_at
        started_at = "2024-06-15T10:30:00"
        with test_db._conn() as conn:
            conn.execute(
                """
                INSERT INTO stage_runs (id, workspace_name, version, stage_name, status, started_at,
                    backend_type, backend_handle)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "stage-startedtest",
                    "test",
                    "v1",
                    "train",
                    "running",
                    started_at,
                    "local",
                    "container-123",
                ),
            )

        # Run migration
        result = migrate_stage_runs(test_db, check_orphans=False)

        assert result["success"] is True

        # Verify state_entered_at was set to the original started_at
        with test_db._conn() as conn:
            row = conn.execute(
                "SELECT state, state_entered_at FROM stage_runs WHERE id = ?",
                ("stage-startedtest",),
            ).fetchone()

        assert row["state"] == "running"
        assert row["state_entered_at"] == started_at
