"""SQLite implementation of SourceStore."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import cast

from goldfish.config.settings import GoldfishSettings
from goldfish.db.database import Database
from goldfish.db.types import SourceRow


class SQLiteSourceStore:
    """SQLite-backed SourceStore implementation."""

    def __init__(self, db: Database, settings: GoldfishSettings) -> None:
        self._db = db
        self.settings = settings

    def register_source(self, **kwargs: object) -> SourceRow:
        """Register and return a source row."""
        source_id = cast(str, kwargs["id"])
        name = cast(str, kwargs["name"])
        description = cast(str | None, kwargs.get("description"))
        created_at = cast(str, kwargs.get("created_at", datetime.now(UTC).isoformat()))
        created_by = cast(str, kwargs["created_by"])
        gcs_location = cast(str, kwargs["gcs_location"])
        size_bytes = cast(int | None, kwargs.get("size_bytes"))
        status = cast(str, kwargs.get("status", "available"))

        metadata: str | None
        metadata_value = kwargs.get("metadata")
        if isinstance(metadata_value, dict):
            metadata = json.dumps(metadata_value)
        else:
            metadata = cast(str | None, metadata_value)

        with self._db._conn() as conn:
            conn.execute(
                """
                INSERT INTO sources
                    (id, name, description, created_at, created_by, gcs_location, size_bytes, status, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (source_id, name, description, created_at, created_by, gcs_location, size_bytes, status, metadata),
            )

        row = self.get_source(source_id)
        if row is None:
            raise RuntimeError(f"Source '{source_id}' not found after registration")
        return row

    def get_source(self, source_id: str) -> SourceRow | None:
        """Return a source row or None if missing."""
        with self._db._conn() as conn:
            row = conn.execute("SELECT * FROM sources WHERE id = ?", (source_id,)).fetchone()
            return cast(SourceRow, dict(row)) if row else None

    def list_sources(self) -> list[SourceRow]:
        """List all sources."""
        with self._db._conn() as conn:
            rows = conn.execute("SELECT * FROM sources ORDER BY created_at DESC, id ASC").fetchall()
            return [cast(SourceRow, dict(row)) for row in rows]
