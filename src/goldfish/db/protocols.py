"""Protocol interfaces for database access.

These protocols define the stable boundary between Goldfish Core logic and
database implementations (SQLite today, Postgres tomorrow).

Gate 0 focuses on representation only: the protocols describe shape, not
behavior. Concrete implementations are introduced in later phases.
"""

from __future__ import annotations

from typing import Protocol

from goldfish.db.types import AuditRow, MetricRow, SourceRow, StageRunRow, WorkspaceRow


class WorkspaceStore(Protocol):
    """Workspace persistence API."""

    def get_workspace(self, name: str) -> WorkspaceRow | None:
        """Return workspace row or None if missing."""

    def create_workspace(self, name: str, goal: str) -> WorkspaceRow:
        """Create and return the new workspace row."""

    def list_workspaces(self) -> list[WorkspaceRow]:
        """List all workspaces."""

    def delete_workspace(self, name: str) -> None:
        """Delete a workspace (no-op if missing)."""


class StageRunStore(Protocol):
    """Stage run persistence API."""

    def get_stage_run(self, run_id: str) -> StageRunRow | None:
        """Return stage run row or None if missing."""

    def create_stage_run(self, **kwargs: object) -> StageRunRow:
        """Create and return a stage run row.

        The exact creation parameters are finalized during implementation phases.
        """

    def update_status(self, run_id: str, status: str) -> None:
        """Update stage run status."""

    def get_stage_runs_by_workspace(self, workspace: str) -> list[StageRunRow]:
        """List stage runs for a given workspace."""


class MetricsStore(Protocol):
    """Run metrics persistence API."""

    def record_metric(self, run_id: str, name: str, value: float, step: int | None) -> None:
        """Record a single metric observation."""

    def get_metrics(self, run_id: str) -> list[MetricRow]:
        """Return all metrics for a stage run."""


class SourceStore(Protocol):
    """Source registry persistence API."""

    def register_source(self, **kwargs: object) -> SourceRow:
        """Register and return a source row.

        The exact creation parameters are finalized during implementation phases.
        """

    def get_source(self, source_id: str) -> SourceRow | None:
        """Return a source row or None if missing."""

    def list_sources(self) -> list[SourceRow]:
        """List all sources."""


class AuditStore(Protocol):
    """Audit trail persistence API."""

    def record_audit(self, operation: str, workspace: str | None, details: dict[str, object]) -> None:
        """Record an audit event."""

    def get_audit_log(self, limit: int) -> list[AuditRow]:
        """Return latest audit rows, most recent first."""
