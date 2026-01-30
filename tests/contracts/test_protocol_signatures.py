"""Protocol signature contract tests.

These tests define the required shape of the new db store abstraction layer.
They intentionally avoid behavior checks: Gate 0 is representation-only.
"""

from __future__ import annotations

import inspect
from typing import get_type_hints

from goldfish.db.types import AuditRow, MetricRow, SourceRow, StageRunRow, WorkspaceRow


def _assert_method_signature(
    method: object,
    *,
    param_names: list[str],
    return_type: object,
) -> None:
    sig = inspect.signature(method)  # type: ignore[arg-type]
    assert list(sig.parameters.keys()) == param_names
    hints = get_type_hints(method)  # type: ignore[arg-type]
    assert hints["return"] == return_type


def test_workspace_store_protocol_when_imported_has_expected_signatures() -> None:
    """WorkspaceStore protocol exposes stable CRUD surface."""

    from goldfish.db.protocols import WorkspaceStore

    _assert_method_signature(
        WorkspaceStore.get_workspace,
        param_names=["self", "name"],
        return_type=WorkspaceRow | None,
    )
    _assert_method_signature(
        WorkspaceStore.create_workspace,
        param_names=["self", "name", "goal"],
        return_type=WorkspaceRow,
    )
    _assert_method_signature(
        WorkspaceStore.list_workspaces,
        param_names=["self"],
        return_type=list[WorkspaceRow],
    )
    _assert_method_signature(
        WorkspaceStore.delete_workspace,
        param_names=["self", "name"],
        return_type=type(None),
    )


def test_stage_run_store_protocol_when_imported_has_expected_signatures() -> None:
    """StageRunStore protocol exposes minimal stage run lifecycle API."""

    from goldfish.db.protocols import StageRunStore

    _assert_method_signature(
        StageRunStore.get_stage_run,
        param_names=["self", "run_id"],
        return_type=StageRunRow | None,
    )
    _assert_method_signature(
        StageRunStore.update_status,
        param_names=["self", "run_id", "status"],
        return_type=type(None),
    )
    _assert_method_signature(
        StageRunStore.get_stage_runs_by_workspace,
        param_names=["self", "workspace"],
        return_type=list[StageRunRow],
    )

    # create_stage_run is intentionally flexible in this phase (spec uses "...").
    hints = get_type_hints(StageRunStore.create_stage_run)
    assert hints["return"] == StageRunRow


def test_metrics_store_protocol_when_imported_has_expected_signatures() -> None:
    """MetricsStore protocol supports recording and retrieval."""

    from goldfish.db.protocols import MetricsStore

    _assert_method_signature(
        MetricsStore.record_metric,
        param_names=["self", "run_id", "name", "value", "step"],
        return_type=type(None),
    )
    _assert_method_signature(
        MetricsStore.get_metrics,
        param_names=["self", "run_id"],
        return_type=list[MetricRow],
    )


def test_source_store_protocol_when_imported_has_expected_signatures() -> None:
    """SourceStore protocol supports source registry reads/writes."""

    from goldfish.db.protocols import SourceStore

    _assert_method_signature(
        SourceStore.get_source,
        param_names=["self", "source_id"],
        return_type=SourceRow | None,
    )
    _assert_method_signature(
        SourceStore.list_sources,
        param_names=["self"],
        return_type=list[SourceRow],
    )

    # register_source is intentionally flexible in this phase (spec uses "...").
    hints = get_type_hints(SourceStore.register_source)
    assert hints["return"] == SourceRow


def test_audit_store_protocol_when_imported_has_expected_signatures() -> None:
    """AuditStore protocol supports structured audit logging."""

    from goldfish.db.protocols import AuditStore

    _assert_method_signature(
        AuditStore.record_audit,
        param_names=["self", "operation", "workspace", "details"],
        return_type=type(None),
    )
    _assert_method_signature(
        AuditStore.get_audit_log,
        param_names=["self", "limit"],
        return_type=list[AuditRow],
    )
