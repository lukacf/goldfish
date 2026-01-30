"""SQLite implementation of WorkspaceStore."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from goldfish.config.settings import GoldfishSettings
from goldfish.db.database import Database
from goldfish.db.types import WorkspaceRow


class SQLiteWorkspaceStore:
    """SQLite-backed WorkspaceStore implementation."""

    def __init__(self, db: Database, settings: GoldfishSettings) -> None:
        self._db = db
        self.settings = settings

    def get_workspace(self, name: str) -> WorkspaceRow | None:
        """Return workspace row or None if missing."""
        with self._db._conn() as conn:
            row = conn.execute(
                """
                SELECT
                    wl.workspace_name AS workspace_name,
                    COALESCE(wg.goal, '') AS goal,
                    wl.created_at AS created_at,
                    COALESCE(wg.updated_at, wl.created_at) AS updated_at
                FROM workspace_lineage AS wl
                LEFT JOIN workspace_goals AS wg
                    ON wg.workspace = wl.workspace_name
                WHERE wl.workspace_name = ?
                """,
                (name,),
            ).fetchone()
            return cast(WorkspaceRow, dict(row)) if row else None

    def create_workspace(self, name: str, goal: str) -> WorkspaceRow:
        """Create and return the new workspace row."""
        timestamp = datetime.now(UTC).isoformat()
        with self._db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO workspace_lineage
                    (workspace_name, parent_workspace, parent_version, created_at, description)
                VALUES (?, ?, ?, ?, ?)
                """,
                (name, None, None, timestamp, None),
            )
            conn.execute(
                """
                INSERT INTO workspace_goals (workspace, goal, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (name, goal, timestamp, timestamp),
            )

        row = self.get_workspace(name)
        if row is None:
            raise RuntimeError(f"Workspace '{name}' not found after creation")
        return row

    def list_workspaces(self) -> list[WorkspaceRow]:
        """List all workspaces."""
        with self._db._conn() as conn:
            rows = conn.execute(
                """
                SELECT
                    wl.workspace_name AS workspace_name,
                    COALESCE(wg.goal, '') AS goal,
                    wl.created_at AS created_at,
                    COALESCE(wg.updated_at, wl.created_at) AS updated_at
                FROM workspace_lineage AS wl
                LEFT JOIN workspace_goals AS wg
                    ON wg.workspace = wl.workspace_name
                ORDER BY wl.workspace_name ASC
                """
            ).fetchall()
            return [cast(WorkspaceRow, dict(row)) for row in rows]

    def delete_workspace(self, name: str) -> None:
        """Delete a workspace (no-op if missing)."""
        with self._db.transaction() as conn:
            conn.execute("DELETE FROM workspace_goals WHERE workspace = ?", (name,))
            conn.execute("DELETE FROM workspace_lineage WHERE workspace_name = ?", (name,))
