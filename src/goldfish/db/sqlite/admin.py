"""SQLite implementation of AuditStore."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import cast

from goldfish.config.settings import GoldfishSettings
from goldfish.db.database import Database
from goldfish.db.types import AuditRow


class SQLiteAuditStore:
    """SQLite-backed AuditStore implementation."""

    def __init__(self, db: Database, settings: GoldfishSettings) -> None:
        self._db = db
        self.settings = settings

    def record_audit(self, operation: str, workspace: str | None, details: dict[str, object]) -> None:
        """Record an audit event."""
        timestamp = datetime.now(UTC).isoformat()
        reason = f"Recorded by AuditStore: {operation}"
        details_json = json.dumps(details)
        with self._db._conn() as conn:
            conn.execute(
                """
                INSERT INTO audit (timestamp, operation, slot, workspace, reason, details)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (timestamp, operation, None, workspace, reason, details_json),
            )

    def get_audit_log(self, limit: int) -> list[AuditRow]:
        """Return latest audit rows, most recent first."""
        with self._db._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM audit ORDER BY timestamp DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [cast(AuditRow, dict(row)) for row in rows]
