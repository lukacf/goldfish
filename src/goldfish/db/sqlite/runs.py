"""SQLite implementation of StageRunStore."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from goldfish.config.settings import GoldfishSettings
from goldfish.db.database import Database
from goldfish.db.types import StageRunRow


class SQLiteStageRunStore:
    """SQLite-backed StageRunStore implementation."""

    def __init__(self, db: Database, settings: GoldfishSettings) -> None:
        self._db = db
        self.settings = settings

    def get_stage_run(self, run_id: str) -> StageRunRow | None:
        """Return stage run row or None if missing."""
        with self._db._conn() as conn:
            row = conn.execute(
                """
                SELECT id, workspace_name, version, stage_name, status, started_at, completed_at
                FROM stage_runs
                WHERE id = ?
                """,
                (run_id,),
            ).fetchone()
            return cast(StageRunRow, dict(row)) if row else None

    def create_stage_run(self, **kwargs: object) -> StageRunRow:
        """Create and return a stage run row."""
        run_id = cast(str, kwargs["id"])
        workspace_name = cast(str, kwargs["workspace_name"])
        version = cast(str, kwargs["version"])
        stage_name = cast(str, kwargs["stage_name"])
        status = cast(str, kwargs.get("status", "pending"))
        started_at = cast(str, kwargs.get("started_at", datetime.now(UTC).isoformat()))
        completed_at = cast(str | None, kwargs.get("completed_at"))

        with self._db._conn() as conn:
            conn.execute(
                """
                INSERT INTO stage_runs
                    (id, workspace_name, version, stage_name, status, started_at, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, workspace_name, version, stage_name, status, started_at, completed_at),
            )

        row = self.get_stage_run(run_id)
        if row is None:
            raise RuntimeError(f"Stage run '{run_id}' not found after creation")
        return row

    def update_status(self, run_id: str, status: str) -> None:
        """Update stage run status."""
        with self._db._conn() as conn:
            conn.execute("UPDATE stage_runs SET status = ? WHERE id = ?", (status, run_id))

    def get_stage_runs_by_workspace(self, workspace: str) -> list[StageRunRow]:
        """List stage runs for a given workspace."""
        with self._db._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, workspace_name, version, stage_name, status, started_at, completed_at
                FROM stage_runs
                WHERE workspace_name = ?
                ORDER BY started_at DESC, id DESC
                """,
                (workspace,),
            ).fetchall()
            return [cast(StageRunRow, dict(row)) for row in rows]
