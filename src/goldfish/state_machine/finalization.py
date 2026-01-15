"""Finalization tracking for stage runs.

This module provides FinalizationTracker to track progress through
the FINALIZING state, specifically the critical phases (output_sync
and output_recording) that determine whether a TIMEOUT in FINALIZING
should result in COMPLETED or FAILED.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from goldfish.validation import validate_stage_run_id

if TYPE_CHECKING:
    from goldfish.db.database import Database

logger = logging.getLogger(__name__)


class FinalizationTracker:
    """Track progress through finalization phases.

    Used during stage finalization to persist progress to the database.
    The critical_phases_done property is used by TIMEOUT handling to
    determine whether to complete or fail the run.

    Critical phases:
    - output_sync: Copying outputs from container to persistent storage
    - output_recording: Recording output metadata in signal_lineage table
    """

    def __init__(self, db: Database, run_id: str) -> None:
        """Initialize tracker.

        Args:
            db: Database instance.
            run_id: Stage run ID to track.

        Raises:
            InvalidStageRunIdError: If run_id format is invalid.
        """
        validate_stage_run_id(run_id)
        self._db = db
        self._run_id = run_id

    def mark_output_sync_done(self) -> None:
        """Mark output sync phase as complete.

        Output sync copies outputs from container to GCS/local storage.
        This is a silent no-op if the run does not exist (UPDATE affects 0 rows).
        """
        with self._db._conn() as conn:
            cursor = conn.execute(
                "UPDATE stage_runs SET output_sync_done = 1 WHERE id = ?",
                (self._run_id,),
            )
            if cursor.rowcount > 0:
                logger.debug("Marked output_sync_done for run %s", self._run_id)

    def mark_output_recording_done(self) -> None:
        """Mark output recording phase as complete.

        Output recording inserts metadata into signal_lineage table.
        This is a silent no-op if the run does not exist (UPDATE affects 0 rows).
        """
        with self._db._conn() as conn:
            cursor = conn.execute(
                "UPDATE stage_runs SET output_recording_done = 1 WHERE id = ?",
                (self._run_id,),
            )
            if cursor.rowcount > 0:
                logger.debug("Marked output_recording_done for run %s", self._run_id)

    @property
    def critical_phases_done(self) -> bool | None:
        """Check if all critical phases are complete.

        Returns:
            True if both output_sync and output_recording are done,
            False if run exists but phases not done,
            None if run not found.
        """
        with self._db._conn() as conn:
            row = conn.execute(
                "SELECT output_sync_done, output_recording_done FROM stage_runs WHERE id = ?",
                (self._run_id,),
            ).fetchone()

            if row is None:
                return None

            return bool(row["output_sync_done"]) and bool(row["output_recording_done"])


def get_critical_phases_done(db: Database, run_id: str) -> bool | None:
    """Get critical_phases_done status for a run.

    Standalone function for use outside FinalizationTracker context.
    This delegates to FinalizationTracker to avoid code duplication.

    Args:
        db: Database instance.
        run_id: Stage run ID.

    Returns:
        True if both critical phases done, False if not, None if run not found.

    Raises:
        InvalidStageRunIdError: If run_id format is invalid.
    """
    # Validation happens in FinalizationTracker.__init__
    return FinalizationTracker(db, run_id).critical_phases_done
