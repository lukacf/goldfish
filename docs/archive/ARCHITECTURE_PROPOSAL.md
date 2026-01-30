# Goldfish Architecture Redesign - Final Plan

> **Date:** 2026-01-28
> **Authors:** Claude (Opus 4.5) + GPT-5.2 (via adversarial debate)
> **Principle:** Do the right thing, not the easy thing.

---

## Context

**Goldfish is not a hobby project.** It's being tested by OpenAI, Anthropic, and Microsoft. It needs to grow from experimental tool to professional-grade infrastructure used by serious ML engineers.

**Key constraints:**
- SQLite is temporary → team/multiplayer requires a real database (Postgres)
- Must be invisible to ML engineers - "they just use Claude and it works"
- Reliability and ease of configuration are paramount
- Not full enterprise, but not toy assumptions either

---

## Executive Summary

A pragmatic but professional architecture that:
- Establishes clear three-layer boundaries (API / Core / Infra)
- Prepares for SQLite → Postgres migration via protocols
- Adds observability for production debugging
- Enforces boundaries with tooling, not just documentation
- Enables contract testing across implementations

---

## Three-Layer Model

```
┌─────────────────────────────────────────────────────────────────────┐
│                         API SURFACE                                  │
│  server.py, server_tools/*, cli.py, web_server.py                   │
│  - MCP tools, CLI commands, HTTP endpoints                          │
│  - Input validation, response formatting                            │
│  - NO business logic - thin wrappers that call Core                 │
└─────────────────────────────────────────────────────────────────────┘
                                  │ calls
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                            CORE                                      │
│  jobs/          - Stage execution, pipeline orchestration           │
│  workspace/     - Workspace management (manager.py)                 │
│  pipeline/      - Pipeline parsing, validation                      │
│  state_machine/ - Stage state transitions                           │
│  svs/           - Semantic validation                               │
│  cloud/protocols.py, cloud/contracts.py - Interfaces                │
│  config/        - Settings, constants                               │
│  validation.py, errors.py, models.py                                │
└─────────────────────────────────────────────────────────────────────┘
                                  │ uses
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         INFRASTRUCTURE                               │
│  db/            - Database access (SQLite now, Postgres later)      │
│  cloud/adapters/- Cloud implementations (local, gcp)                │
│  workspace/git_layer.py - Git operations                            │
│  infra/         - Docker builder, base images                       │
└─────────────────────────────────────────────────────────────────────┘
```

**Rules:**
- API Surface calls Core, never Infra directly
- Core defines interfaces (protocols), Infra implements them
- Infra never imports from API Surface or Core business logic
- Enforced via import-linter in CI

**Package placement:**

| Package | Layer | Notes |
|---------|-------|-------|
| `server_tools/` | API Surface | Thin wrappers only |
| `jobs/` | Core | Stage execution logic |
| `workspace/manager.py` | Core | Business logic |
| `workspace/git_layer.py` | Infra | Git operations |
| `cloud/protocols.py` | Core | Interfaces |
| `cloud/adapters/*` | Infra | Implementations |
| `db/` | Infra | All database access |

---

## Phase 1: Database Abstraction (Prepare for Postgres)

**Goal:** Enable SQLite → Postgres migration without rewriting Core.

**Approach:** Protocols now, raw SQL now, SQLAlchemy Core later.

**Target structure:**
```
db/
├── __init__.py           # Re-exports
├── connection.py         # Connection management, migrations
├── protocols.py          # Store interfaces (NEW)
├── sqlite/               # Current implementation (NEW organization)
│   ├── __init__.py
│   ├── workspaces.py
│   ├── runs.py
│   ├── metrics.py
│   ├── sources.py
│   ├── records.py
│   └── admin.py
├── types.py              # TypedDict definitions
└── migrations/
    ├── runner.py
    └── versions/
```

**Protocol pattern:**
```python
# db/protocols.py
from typing import Protocol

class WorkspaceStore(Protocol):
    """Interface for workspace persistence."""

    def get_workspace(self, name: str) -> WorkspaceRow | None: ...
    def create_workspace(self, name: str, goal: str) -> WorkspaceRow: ...
    def list_workspaces(self) -> list[WorkspaceRow]: ...
    def delete_workspace(self, name: str) -> None: ...

class StageRunStore(Protocol):
    """Interface for stage run persistence."""

    def get_stage_run(self, run_id: str) -> StageRunRow | None: ...
    def create_stage_run(self, ...) -> StageRunRow: ...
    def update_status(self, run_id: str, status: str) -> None: ...
```

**SQLite implementation:**
```python
# db/sqlite/workspaces.py
class SQLiteWorkspaceStore:
    def __init__(self, conn_factory: Callable[[], Connection]):
        self._conn = conn_factory

    def get_workspace(self, name: str) -> WorkspaceRow | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM workspaces WHERE name = ?", (name,)
            ).fetchone()
            return cast(WorkspaceRow, dict(row)) if row else None
```

**Future Postgres:**
```python
# db/postgres/workspaces.py (when needed)
class PostgresWorkspaceStore:
    def __init__(self, pool: ConnectionPool):
        self._pool = pool

    def get_workspace(self, name: str) -> WorkspaceRow | None:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT * FROM workspaces WHERE name = %s", (name,)
            ).fetchone()
            return cast(WorkspaceRow, dict(row)) if row else None
```

**SQL portability rules:**
- No SQLite-only features (AUTOINCREMENT exceptions, WITHOUT ROWID)
- Use standard SQL types
- `RETURNING` is fine (both support it)
- Parameterized queries always (? for SQLite, %s for Postgres - abstracted in store)

---

## Phase 2: Split stage_executor.py

Same as before - phase functions with explicit context:

```
jobs/
├── stage_executor.py    # Thin orchestrator (~300 lines)
├── phases/
│   ├── context.py       # StageRunContext dataclass
│   ├── validate.py      # validate_stage_request()
│   ├── resolve.py       # resolve_inputs()
│   ├── sync.py          # sync_workspace()
│   ├── build.py         # build_image()
│   ├── launch.py        # launch_container()
│   ├── monitor.py       # monitor_status()
│   ├── finalize.py      # finalize_outputs()
│   └── review.py        # pre_run_review()
```

---

## Phase 3: Move GCP Code

Same as before - all GCP code to `cloud/adapters/gcp/`:

| From | To |
|------|-----|
| `infra/gce_launcher.py` | `cloud/adapters/gcp/gce_launcher.py` |
| `infra/startup_builder.py` | `cloud/adapters/gcp/startup_builder.py` |
| `infra/metadata/gcp.py` | `cloud/adapters/gcp/metadata_bus.py` |

---

## Phase 4: Import Boundary Enforcement

**import-linter contracts:**

```toml
[tool.importlinter]
root_packages = ["goldfish"]

# Core cannot import API Surface
[[tool.importlinter.contracts]]
name = "core-no-api-surface"
type = "forbidden"
source_modules = ["goldfish.jobs", "goldfish.workspace.manager", "goldfish.pipeline"]
forbidden_modules = ["goldfish.server", "goldfish.server_tools"]

# Core protocols cannot import adapters
[[tool.importlinter.contracts]]
name = "protocols-no-adapters"
type = "forbidden"
source_modules = ["goldfish.cloud.protocols", "goldfish.cloud.contracts"]
forbidden_modules = ["goldfish.cloud.adapters"]

# Infra cannot import API Surface
[[tool.importlinter.contracts]]
name = "infra-no-api"
type = "forbidden"
source_modules = ["goldfish.db", "goldfish.cloud.adapters", "goldfish.infra"]
forbidden_modules = ["goldfish.server", "goldfish.server_tools"]

# Only factory imports adapters
[[tool.importlinter.contracts]]
name = "only-factory-imports-adapters"
type = "forbidden"
source_modules = ["goldfish.cloud"]
forbidden_modules = ["goldfish.cloud.adapters"]
ignore_imports = ["goldfish.cloud.factory"]
```

---

## Phase 5: Observability

**Goal:** When something goes wrong, debugging is easy. "Invisible" requires excellent visibility when needed.

### Structured Logging

```python
# config/logging.py
import logging
import json
from contextvars import ContextVar

# Context for correlation IDs
current_stage_run_id: ContextVar[str | None] = ContextVar('stage_run_id', default=None)
current_request_id: ContextVar[str | None] = ContextVar('request_id', default=None)

class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "stage_run_id": current_stage_run_id.get(),
            "request_id": current_request_id.get(),
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)

# Usage in stage execution
def run_stage(self, request: StageRequest) -> StageResult:
    token = current_stage_run_id.set(request.run_id)
    try:
        # All logs in this context include stage_run_id
        logger.info("Starting stage execution")
        ...
    finally:
        current_stage_run_id.reset(token)
```

**Configuration:**
```python
# In GoldfishSettings
log_format: Literal["json", "console"] = "console"  # json for production
log_level: str = "INFO"
```

### Trace IDs

Every log line during stage execution includes:
- `stage_run_id` - correlates all logs for one stage run
- `request_id` - correlates logs for one MCP request (may span multiple stages)

### Metrics Hooks (Future)

Prepare hooks for metrics without implementing full telemetry:
```python
# metrics/hooks.py
class MetricsHook(Protocol):
    def stage_started(self, run_id: str, stage_name: str) -> None: ...
    def stage_completed(self, run_id: str, duration_seconds: float) -> None: ...
    def stage_failed(self, run_id: str, error: str) -> None: ...

# Default no-op implementation, can be replaced with Prometheus/DataDog/etc
```

**Defer OpenTelemetry** until we need cross-service tracing.

---

## Phase 6: Contract Tests

**Goal:** Verify implementations satisfy protocols. Enable safe swapping of SQLite → Postgres.

### Database Contract Tests

```python
# tests/contracts/test_workspace_store.py
import pytest
from goldfish.db.protocols import WorkspaceStore

class WorkspaceStoreContract:
    """Contract tests that any WorkspaceStore implementation must pass."""

    @pytest.fixture
    def store(self) -> WorkspaceStore:
        raise NotImplementedError("Subclass must provide store fixture")

    def test_create_and_get_workspace(self, store: WorkspaceStore):
        result = store.create_workspace("test-ws", "Test goal")
        assert result["name"] == "test-ws"

        fetched = store.get_workspace("test-ws")
        assert fetched is not None
        assert fetched["name"] == "test-ws"

    def test_get_nonexistent_returns_none(self, store: WorkspaceStore):
        assert store.get_workspace("nonexistent") is None

    def test_list_workspaces(self, store: WorkspaceStore):
        store.create_workspace("ws1", "Goal 1")
        store.create_workspace("ws2", "Goal 2")

        workspaces = store.list_workspaces()
        names = {w["name"] for w in workspaces}
        assert "ws1" in names
        assert "ws2" in names

# tests/contracts/test_sqlite_workspace_store.py
class TestSQLiteWorkspaceStore(WorkspaceStoreContract):
    @pytest.fixture
    def store(self, test_db):
        return SQLiteWorkspaceStore(test_db._conn)

# tests/contracts/test_postgres_workspace_store.py (future)
class TestPostgresWorkspaceStore(WorkspaceStoreContract):
    @pytest.fixture
    def store(self, postgres_test_db):
        return PostgresWorkspaceStore(postgres_test_db.pool)
```

### Cloud Adapter Contract Tests

```python
# tests/contracts/test_run_backend.py
class RunBackendContract:
    """Contract tests for RunBackend implementations."""

    @pytest.fixture
    def backend(self) -> RunBackend:
        raise NotImplementedError

    def test_launch_returns_handle(self, backend: RunBackend):
        spec = make_test_run_spec()
        handle = backend.launch(spec)
        assert handle.backend_type in ("local", "gce")
        assert handle.backend_handle is not None

    def test_get_status_for_running(self, backend: RunBackend):
        handle = backend.launch(make_test_run_spec())
        status = backend.get_status(handle)
        assert status.state in ("preparing", "running", "completed", "failed")

# Run against LocalRunBackend always
class TestLocalRunBackend(RunBackendContract):
    @pytest.fixture
    def backend(self):
        return LocalRunBackend(...)

# Run against GCERunBackend in deluxe tests
@pytest.mark.deluxe_gce
class TestGCERunBackend(RunBackendContract):
    @pytest.fixture
    def backend(self):
        return GCERunBackend(...)
```

---

## Phase 7: Centralize Configuration

Same as before:

```python
# config/settings.py
@dataclass(frozen=True)
class GoldfishSettings:
    """Single source of truth. Immutable after load."""

    # Core settings
    project_name: str
    dev_repo_path: Path
    workspaces_path: Path
    backend: Literal["local", "gce"]

    # Database (future: connection string for Postgres)
    db_path: Path
    db_backend: Literal["sqlite", "postgres"] = "sqlite"

    # Observability
    log_format: Literal["json", "console"] = "console"
    log_level: str = "INFO"

    # Timeouts (all in one place)
    stage_timeout: int = 3600
    gce_launch_timeout: int = 1200
    # ...
```

---

## Phase 8: Strict Typing

Same as before - enable after structural changes settle.

---

## Execution Order

| # | Phase | Effort | Rationale |
|---|-------|--------|-----------|
| 1 | Database protocols + split | 3 days | Foundation for Postgres migration |
| 2 | Split stage_executor.py | 2 days | Second biggest pain point |
| 3 | Move GCP code | 1 day | Enables import enforcement |
| 4 | Import boundary enforcement | 0.5 day | Gates new violations |
| 5 | Observability (structured logs) | 1 day | Critical for production debugging |
| 6 | Contract tests | 1 day | Enables safe migrations |
| 7 | Centralize config | 1-2 days | Threading through code |
| 8 | Strict typing | 2 days | After dust settles |

**Total: ~2 weeks**

---

## What We Keep Simple

Based on debate, we explicitly chose NOT to:

| Pattern | Why Not |
|---------|---------|
| Full 4-layer architecture | 3 layers sufficient, less ceremony |
| Repository classes | Functions with protocols are simpler |
| Handler pipeline | Explicit phase calls are clearer |
| DI container | Factory functions + explicit passing enough |
| Full ORM | SQLAlchemy Core only when Postgres arrives |
| OpenTelemetry now | Structured logs + trace IDs sufficient for now |
| Event sourcing | Way overkill |

---

## What This Enables

**Short term:**
- Maintainable codebase (no more 5500-line files)
- Safe refactoring (import-linter catches violations)
- Debuggable production issues (structured logs + trace IDs)

**Medium term:**
- SQLite → Postgres migration (protocols + contract tests)
- Additional cloud providers (protocol-based adapters)
- Team/multiplayer features (proper database)

**Long term:**
- Professional-grade tool used by ML teams at top AI labs
- "Invisible" infrastructure that just works
- Reliable enough that ML engineers don't think about it

---

## Agreement

This plan was developed through adversarial debate between Claude (Opus 4.5) and GPT-5.2, then refined based on user input about Goldfish's actual trajectory (professional tool, not hobby project; Postgres planned).

The architecture is professional-grade without being enterprise-ceremony. It prepares for growth while staying pragmatic.
