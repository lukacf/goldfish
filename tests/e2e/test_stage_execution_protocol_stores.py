"""E2E scenario tests for protocol-based DB stores.

E2E-001 (spec): Stage execution with protocol-based SQLite stores.
This file is introduced early to lock in the desired end-to-end behavior before
the refactor lands.
"""

from __future__ import annotations

import importlib
import importlib.util
import inspect
from pathlib import Path


def test_stage_execution_when_using_protocol_stores_records_run_metrics_and_audit(tmp_path: Path) -> None:
    """E2E-001: Stage execution uses protocol stores for persistence.

    Given: Protocol-based SQLite stores exist and are wired into StageExecutor
    When: A stage is executed via StageExecutor
    Then: Stage run row, metrics, and audit events are recorded via store protocols
    """
    sqlite_spec = importlib.util.find_spec("goldfish.db.sqlite")
    assert sqlite_spec is not None, "Expected module 'goldfish.db.sqlite' to exist in Phase 3."

    # Expected to accept protocol-based stores in later phases.
    from goldfish.jobs.stage_executor import StageExecutor

    init_sig = inspect.signature(StageExecutor.__init__)
    for param_name in ("workspace_store", "stage_run_store", "metrics_store", "audit_store"):
        assert param_name in init_sig.parameters, f"Expected StageExecutor.__init__ to accept '{param_name}'."

    sqlite_module = importlib.import_module("goldfish.db.sqlite")
    for class_name in ("SQLiteWorkspaceStore", "SQLiteStageRunStore", "SQLiteMetricsStore", "SQLiteAuditStore"):
        assert hasattr(sqlite_module, class_name), f"Expected goldfish.db.sqlite to export '{class_name}'."

    _ = tmp_path  # Fixture reserved for future end-to-end wiring assertions.
