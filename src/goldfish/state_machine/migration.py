"""State machine migration logic.

This module handles:
- Mapping legacy status values to new StageState values
- Detecting termination causes from error messages
- Batch migration with backup and rollback support
- Audit logging for all migration decisions
- Safe migration with drain mode for active runs
- Orphan detection via backend APIs
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, TypedDict, cast

from goldfish.state_machine.types import StageState, TerminationCause

if TYPE_CHECKING:
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)

# Valid characters for backup table names (alphanumeric and underscore only)
_TABLE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_]+$")

# Valid GCE instance names: lowercase letters, numbers, hyphens (1-63 chars)
# https://cloud.google.com/compute/docs/naming-resources#resource-name-format
_GCE_INSTANCE_PATTERN = re.compile(r"^[a-z][a-z0-9-]{0,62}$")

# Valid Docker container IDs: 12 or 64 hex characters, or container names
# Container names: alphanumeric, underscore, period, hyphen (1-128 chars)
_DOCKER_CONTAINER_PATTERN = re.compile(r"^([a-f0-9]{12}|[a-f0-9]{64}|[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127})$")


class MigrationResult(TypedDict):
    """Result of a migration operation."""

    success: bool
    migrated: int
    skipped: int
    errors: int
    error_details: list[dict]
    decisions: list[dict]
    progress_id: int | None


class RollbackResult(TypedDict):
    """Result of a rollback operation."""

    success: bool
    restored: int
    error: str | None


class OrphanCheckResult(TypedDict):
    """Result of orphan status check."""

    is_orphan: bool
    reason: str | None
    backend_type: str | None
    backend_handle: str | None


def _validate_table_name(name: str) -> bool:
    """Validate table name to prevent SQL injection.

    Args:
        name: Table name to validate

    Returns:
        True if name is safe, False otherwise
    """
    return bool(_TABLE_NAME_PATTERN.match(name))


def determine_migration_state(status: str | None, error: str | None) -> StageState:
    """Determine the new state from legacy status and error message.

    Maps legacy status values to new StageState values:
    - pending → PREPARING
    - running → RUNNING
    - completed → COMPLETED
    - failed → FAILED (or TERMINATED if error indicates infrastructure failure)
    - canceled/cancelled → CANCELED
    - unknown/other/empty → UNKNOWN

    Args:
        status: Legacy status value from database (None or empty string → UNKNOWN)
        error: Error message (may contain hints about termination cause)

    Returns:
        StageState: The appropriate state for this run
    """
    if status is None or status == "":
        return StageState.UNKNOWN

    status_lower = status.lower()

    # Check for infrastructure failure indicators in error message
    if status_lower == "failed" and error:
        cause = detect_termination_cause(error)
        if cause is not None:
            return StageState.TERMINATED

    # Direct status mappings
    status_map = {
        "pending": StageState.PREPARING,
        "running": StageState.RUNNING,
        "completed": StageState.COMPLETED,
        "failed": StageState.FAILED,
        "canceled": StageState.CANCELED,
        "cancelled": StageState.CANCELED,  # British spelling
    }

    return status_map.get(status_lower, StageState.UNKNOWN)


def detect_termination_cause(error: str | None) -> TerminationCause | None:
    """Detect termination cause from error message.

    Parses error messages to identify infrastructure-level failures:
    - PREEMPTED: Spot instance preemption
    - TIMEOUT: Execution timeout
    - CRASHED: Container crash, OOM, segfault, killed, signal
    - ORPHANED: Instance not found, lost track, disappeared

    Pattern matching order matters: preemption is checked first as it's
    typically the root cause even if other symptoms are present.

    Args:
        error: Error message to analyze

    Returns:
        TerminationCause if a pattern matches, None otherwise
    """
    if not error:
        return None

    error_lower = error.lower()

    # Preemption patterns (check first - root cause priority)
    preemption_patterns = [
        r"preempt",
        r"spot.*terminat",
        r"spot.*instance",
    ]
    for pattern in preemption_patterns:
        if re.search(pattern, error_lower):
            return TerminationCause.PREEMPTED

    # Timeout patterns
    timeout_patterns = [
        r"timeout",
        r"timed\s*out",
    ]
    for pattern in timeout_patterns:
        if re.search(pattern, error_lower):
            return TerminationCause.TIMEOUT

    # Crash patterns
    crash_patterns = [
        r"crash",
        r"\boom\b",  # OOM - word boundary to avoid matching "room" etc
        r"out\s*of\s*memory",
        r"segmentation\s*fault",
        r"segfault",
        r"killed",
        r"signal\s*[1-9]\d*",  # signal followed by non-zero number (signal 0 is success)
    ]
    for pattern in crash_patterns:
        if re.search(pattern, error_lower):
            return TerminationCause.CRASHED

    # Orphaned patterns
    orphaned_patterns = [
        r"instance\s*not\s*found",
        r"not\s*found.*instance",
        r"lost\s*track",
        r"disappeared",
        r"orphan",
    ]
    for pattern in orphaned_patterns:
        if re.search(pattern, error_lower):
            return TerminationCause.ORPHANED

    return None


def check_orphan_status(
    db: Database,
    run_id: str,
    backend_type: str | None = None,
    backend_handle: str | None = None,
) -> OrphanCheckResult:
    """Check if a stage run is orphaned by querying backend APIs.

    For GCE instances, checks if the instance still exists.
    For Docker containers, checks if the container is running.

    Args:
        db: Database instance
        run_id: Stage run ID to check
        backend_type: Override backend type (default: read from database)
        backend_handle: Override backend handle (default: read from database)

    Returns:
        OrphanCheckResult with is_orphan status and reason
    """
    # Get run info if not provided
    if backend_type is None or backend_handle is None:
        with db._conn() as conn:
            row = conn.execute(
                "SELECT backend_type, backend_handle FROM stage_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                return OrphanCheckResult(
                    is_orphan=False,
                    reason="Run not found",
                    backend_type=None,
                    backend_handle=None,
                )
            backend_type = row["backend_type"]
            backend_handle = row["backend_handle"]

    if not backend_handle:
        return OrphanCheckResult(
            is_orphan=True,
            reason="No backend handle recorded",
            backend_type=backend_type,
            backend_handle=backend_handle,
        )

    # GCE orphan detection
    if backend_type == "gce":
        return _check_gce_orphan(backend_handle)

    # Docker orphan detection
    if backend_type == "local":
        return _check_docker_orphan(backend_handle)

    return OrphanCheckResult(
        is_orphan=False,
        reason=f"Unknown backend type: {backend_type}",
        backend_type=backend_type,
        backend_handle=backend_handle,
    )


def _check_gce_orphan(instance_name: str) -> OrphanCheckResult:
    """Check if a GCE instance exists.

    Uses gcloud CLI to check instance existence.
    Returns orphan=True if instance not found.
    """
    import subprocess

    # Validate instance name to prevent command injection
    if not _GCE_INSTANCE_PATTERN.match(instance_name):
        return OrphanCheckResult(
            is_orphan=False,
            reason=f"Invalid GCE instance name format: {instance_name[:50]}",
            backend_type="gce",
            backend_handle=instance_name,
        )

    try:
        result = subprocess.run(
            [
                "gcloud",
                "compute",
                "instances",
                "describe",
                instance_name,
                "--format=value(status)",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode != 0:
            # Instance not found
            if "not found" in result.stderr.lower():
                return OrphanCheckResult(
                    is_orphan=True,
                    reason=f"Instance '{instance_name}' not found in GCE",
                    backend_type="gce",
                    backend_handle=instance_name,
                )
            # Other error - can't determine orphan status
            return OrphanCheckResult(
                is_orphan=False,
                reason=f"GCE check failed: {result.stderr}",
                backend_type="gce",
                backend_handle=instance_name,
            )

        # Instance exists
        status = result.stdout.strip()
        if status in ("TERMINATED", "STOPPED"):
            return OrphanCheckResult(
                is_orphan=True,
                reason=f"Instance exists but is {status}",
                backend_type="gce",
                backend_handle=instance_name,
            )

        return OrphanCheckResult(
            is_orphan=False,
            reason=f"Instance exists with status: {status}",
            backend_type="gce",
            backend_handle=instance_name,
        )

    except subprocess.TimeoutExpired:
        return OrphanCheckResult(
            is_orphan=False,
            reason="GCE check timed out",
            backend_type="gce",
            backend_handle=instance_name,
        )
    except FileNotFoundError:
        return OrphanCheckResult(
            is_orphan=False,
            reason="gcloud CLI not available",
            backend_type="gce",
            backend_handle=instance_name,
        )
    except Exception as e:
        return OrphanCheckResult(
            is_orphan=False,
            reason=f"GCE check error: {e}",
            backend_type="gce",
            backend_handle=instance_name,
        )


def _check_docker_orphan(container_id: str) -> OrphanCheckResult:
    """Check if a Docker container exists and is running.

    Uses docker CLI to check container status.
    Returns orphan=True if container not found or not running.
    """
    import subprocess

    # Validate container ID to prevent command injection
    if not _DOCKER_CONTAINER_PATTERN.match(container_id):
        return OrphanCheckResult(
            is_orphan=False,
            reason=f"Invalid Docker container ID format: {container_id[:50]}",
            backend_type="local",
            backend_handle=container_id,
        )

    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Status}}", container_id],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode != 0:
            # Container not found
            return OrphanCheckResult(
                is_orphan=True,
                reason=f"Container '{container_id}' not found",
                backend_type="local",
                backend_handle=container_id,
            )

        status = result.stdout.strip()
        if status != "running":
            return OrphanCheckResult(
                is_orphan=True,
                reason=f"Container exists but is {status}",
                backend_type="local",
                backend_handle=container_id,
            )

        return OrphanCheckResult(
            is_orphan=False,
            reason="Container is running",
            backend_type="local",
            backend_handle=container_id,
        )

    except subprocess.TimeoutExpired:
        return OrphanCheckResult(
            is_orphan=False,
            reason="Docker check timed out",
            backend_type="local",
            backend_handle=container_id,
        )
    except FileNotFoundError:
        return OrphanCheckResult(
            is_orphan=False,
            reason="docker CLI not available",
            backend_type="local",
            backend_handle=container_id,
        )
    except Exception as e:
        return OrphanCheckResult(
            is_orphan=False,
            reason=f"Docker check error: {e}",
            backend_type="local",
            backend_handle=container_id,
        )


def migrate_stage_runs(
    db: Database,
    *,
    check_orphans: bool = False,
    batch_size: int = 100,
) -> MigrationResult:
    """Migrate existing stage runs to use new state column.

    This function:
    1. Records progress in migration_progress table
    2. Creates a backup table before any modifications
    3. Migrates in batches using BEGIN IMMEDIATE for safety
    4. Logs all migration decisions for audit
    5. Skips already-migrated rows (idempotent)
    6. Continues on individual row errors with detailed logging
    7. Optionally checks backend APIs for orphan detection

    Args:
        db: Database instance
        check_orphans: If True, query GCE/Docker APIs to detect orphaned runs
        batch_size: Number of rows to process per batch (for progress updates)

    Returns:
        MigrationResult with migration statistics and decisions
    """
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    backup_table = f"stage_runs_backup_{timestamp}"

    # Validate generated table name (should always pass, but defense in depth)
    if not _validate_table_name(backup_table):
        return MigrationResult(
            success=False,
            migrated=0,
            skipped=0,
            errors=1,
            error_details=[{"error": "Invalid backup table name generated"}],
            decisions=[],
            progress_id=None,
        )

    decisions: list[dict] = []
    error_details: list[dict] = []
    migrated = 0
    skipped = 0
    errors = 0
    progress_id: int | None = None

    try:
        with db._conn() as conn:
            # Use BEGIN IMMEDIATE for exclusive write lock
            conn.execute("BEGIN IMMEDIATE")

            try:
                # 1. Count total rows needing migration
                total_count = conn.execute("SELECT COUNT(*) as cnt FROM stage_runs WHERE state IS NULL").fetchone()
                total_rows = total_count["cnt"] if total_count else 0

                if total_rows == 0:
                    # Check if there are any rows at all
                    all_count = conn.execute("SELECT COUNT(*) as cnt FROM stage_runs").fetchone()
                    conn.execute("COMMIT")
                    if all_count and all_count["cnt"] > 0:
                        return MigrationResult(
                            success=True,
                            migrated=0,
                            skipped=all_count["cnt"],
                            errors=0,
                            error_details=[],
                            decisions=[],
                            progress_id=None,
                        )
                    return MigrationResult(
                        success=True,
                        migrated=0,
                        skipped=0,
                        errors=0,
                        error_details=[],
                        decisions=[],
                        progress_id=None,
                    )

                # 2. Record migration start in progress table
                now = datetime.now(UTC).isoformat()
                cursor = conn.execute(
                    """
                    INSERT INTO migration_progress
                    (migration_name, started_at, status, total_rows, backup_table)
                    VALUES (?, ?, 'running', ?, ?)
                    """,
                    ("state_machine_v1", now, total_rows, backup_table),
                )
                progress_id = cursor.lastrowid

                # 3. Create backup table with all relevant columns
                # Note: Using string format for table name is necessary as SQLite
                # doesn't support parameterized table names. We validated the name above.
                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {backup_table} AS
                    SELECT id, status, state, termination_cause, state_entered_at,
                           error, started_at, backend_type, backend_handle
                    FROM stage_runs
                    """
                )

                # 4. Find rows needing migration
                rows = conn.execute(
                    """
                    SELECT id, status, error, state, started_at,
                           backend_type, backend_handle
                    FROM stage_runs
                    WHERE state IS NULL
                    ORDER BY id
                    """
                ).fetchall()

                # 5. Migrate each row
                for i, row in enumerate(rows):
                    run_id = row["id"]
                    status = row["status"]
                    error = row["error"]
                    started_at = row["started_at"]
                    backend_type = row["backend_type"]
                    backend_handle = row["backend_handle"]

                    try:
                        # Determine new state
                        new_state = determine_migration_state(status, error)

                        # Determine termination cause if applicable
                        term_cause = None
                        if new_state == StageState.TERMINATED:
                            term_cause = detect_termination_cause(error)

                        # Check for orphans if requested and status suggests it might be
                        if check_orphans and new_state == StageState.RUNNING and backend_handle:
                            orphan_result = check_orphan_status(db, run_id, backend_type, backend_handle)
                            if orphan_result["is_orphan"]:
                                new_state = StageState.TERMINATED
                                term_cause = TerminationCause.ORPHANED
                                logger.info(f"Run {run_id} detected as orphan: {orphan_result['reason']}")

                        # Use started_at as state_entered_at for running jobs
                        # (preserves timing accuracy for timeout calculations)
                        # For other states, use now
                        if new_state == StageState.RUNNING and started_at:
                            state_entered_at = started_at
                        else:
                            state_entered_at = now

                        # Update the row
                        conn.execute(
                            """
                            UPDATE stage_runs
                            SET state = ?,
                                termination_cause = ?,
                                state_entered_at = ?
                            WHERE id = ?
                            """,
                            (
                                new_state.value,
                                term_cause.value if term_cause else None,
                                state_entered_at,
                                run_id,
                            ),
                        )

                        # Record decision for audit
                        decisions.append(
                            {
                                "run_id": run_id,
                                "from_status": status,
                                "to_state": new_state.value,
                                "termination_cause": term_cause.value if term_cause else None,
                                "state_entered_at": state_entered_at,
                                "error_snippet": error[:100] if error else None,
                            }
                        )

                        migrated += 1

                        # Update progress periodically
                        if (i + 1) % batch_size == 0:
                            conn.execute(
                                """
                                UPDATE migration_progress
                                SET migrated_rows = ?, last_processed_id = ?
                                WHERE id = ?
                                """,
                                (migrated, run_id, progress_id),
                            )

                    except Exception as e:
                        errors += 1
                        error_msg = str(e)
                        logger.error(
                            f"Migration error for run {run_id}: {error_msg}",
                            exc_info=True,
                        )
                        error_details.append(
                            {
                                "run_id": run_id,
                                "error": error_msg,
                                "status": status,
                            }
                        )

                # 6. Update final progress
                final_status = "completed" if errors == 0 else "completed_with_errors"
                conn.execute(
                    """
                    UPDATE migration_progress
                    SET status = ?,
                        completed_at = ?,
                        migrated_rows = ?,
                        failed_rows = ?,
                        last_processed_id = ?
                    WHERE id = ?
                    """,
                    (
                        final_status,
                        datetime.now(UTC).isoformat(),
                        migrated,
                        errors,
                        rows[-1]["id"] if rows else None,
                        progress_id,
                    ),
                )

                conn.execute("COMMIT")

            except Exception:
                conn.execute("ROLLBACK")
                raise

    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        # Try to update progress on failure
        if progress_id is not None:
            try:
                with db._conn() as conn:
                    conn.execute(
                        """
                        UPDATE migration_progress
                        SET status = 'failed',
                            completed_at = ?,
                            error = ?
                        WHERE id = ?
                        """,
                        (datetime.now(UTC).isoformat(), str(e), progress_id),
                    )
            except Exception as progress_err:
                logger.debug(f"Failed to update progress on failure: {progress_err}")

        return cast(
            MigrationResult,
            {
                "success": False,
                "migrated": migrated,
                "skipped": skipped,
                "errors": errors + 1,
                "error_details": error_details + [{"error": str(e)}],
                "decisions": decisions,
                "progress_id": progress_id,
            },
        )

    return MigrationResult(
        success=errors == 0,
        migrated=migrated,
        skipped=skipped,
        errors=errors,
        error_details=error_details,
        decisions=decisions,
        progress_id=progress_id,
    )


def rollback_migration(db: Database, backup_table: str | None = None) -> RollbackResult:
    """Rollback migration by restoring state column to NULL.

    Finds the most recent backup table (or uses provided one) and
    restores original values.

    Args:
        db: Database instance
        backup_table: Specific backup table to use (default: most recent)

    Returns:
        RollbackResult with success status and row count
    """
    with db._conn() as conn:
        # Use BEGIN IMMEDIATE for transaction isolation
        conn.execute("BEGIN IMMEDIATE")

        try:
            # Find backup table if not provided
            if backup_table is None:
                tables = conn.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name LIKE 'stage_runs_backup_%'
                    ORDER BY name DESC
                    LIMIT 1
                    """
                ).fetchone()

                if not tables:
                    conn.execute("ROLLBACK")
                    return RollbackResult(
                        success=False,
                        restored=0,
                        error="No backup table found. Cannot rollback.",
                    )

                backup_table = tables["name"]

            # Validate table name to prevent SQL injection
            if not _validate_table_name(backup_table):
                conn.execute("ROLLBACK")
                return RollbackResult(
                    success=False,
                    restored=0,
                    error=f"Invalid backup table name: {backup_table}",
                )

            # Verify table exists
            exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
                (backup_table,),
            ).fetchone()

            if not exists:
                conn.execute("ROLLBACK")
                return RollbackResult(
                    success=False,
                    restored=0,
                    error=f"Backup table '{backup_table}' not found.",
                )

            # Reset state columns for rows in backup
            # Note: Using string format for table name is necessary as SQLite
            # doesn't support parameterized table names. We validated the name above.
            result = conn.execute(
                f"""
                UPDATE stage_runs
                SET state = NULL,
                    termination_cause = NULL,
                    state_entered_at = NULL
                WHERE id IN (SELECT id FROM {backup_table})
                """
            )

            # Update migration progress if exists
            conn.execute(
                """
                UPDATE migration_progress
                SET status = 'rolled_back',
                    completed_at = ?
                WHERE backup_table = ?
                """,
                (datetime.now(UTC).isoformat(), backup_table),
            )

            conn.execute("COMMIT")

            return RollbackResult(
                success=True,
                restored=result.rowcount,
                error=None,
            )

        except Exception:
            conn.execute("ROLLBACK")
            raise


def safe_migration(
    db: Database,
    *,
    drain_timeout_seconds: int = 300,
    check_orphans: bool = True,
) -> MigrationResult:
    """Perform migration with drain mode for active runs.

    This function:
    1. Checks for active runs (running state in old status column)
    2. Waits up to drain_timeout for active runs to complete
    3. If any active runs remain, marks them as orphans for review
    4. Proceeds with migration

    Args:
        db: Database instance
        drain_timeout_seconds: Max time to wait for active runs to drain
        check_orphans: If True, query backend APIs for orphan detection

    Returns:
        MigrationResult with migration statistics
    """
    import time

    logger.info("Starting safe migration with drain mode")

    start_time = time.time()
    poll_interval = 5  # seconds

    while time.time() - start_time < drain_timeout_seconds:
        with db._conn() as conn:
            # Count active runs (running status, no state yet)
            active = conn.execute(
                """
                SELECT COUNT(*) as cnt FROM stage_runs
                WHERE status = 'running' AND state IS NULL
                """
            ).fetchone()

            active_count = active["cnt"] if active else 0

            if active_count == 0:
                logger.info("No active runs - proceeding with migration")
                break

            logger.info(
                f"Waiting for {active_count} active runs to complete "
                f"({int(drain_timeout_seconds - (time.time() - start_time))}s remaining)"
            )

        time.sleep(poll_interval)

    # Proceed with migration (orphan detection will handle any remaining active runs)
    return migrate_stage_runs(db, check_orphans=check_orphans)
