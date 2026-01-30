# Goldfish Development Guide

> **For AI assistants working on this codebase.** Everything you need to develop Goldfish effectively—compact, scannable, and action-oriented.

## Quick Reference

```bash
# Development cycle
make lint              # Ruff + mypy via pre-commit - run before commits
make test              # Fast unit tests (<1s) - run frequently
make test-integration  # Integration tests (~2min) - before pushing
make ci                # Full CI suite (lint + all tests)

# First-time setup
uv pip install -e ".[dev]"
make install-hooks     # REQUIRED: installs pre-commit hooks
```

**Golden rule**: Never suppress lint errors—always fix the source.

---

## TDD: Test-Driven Development

**This codebase uses TDD. Write tests BEFORE implementation.**

```
RED    → Write failing test that defines expected behavior
GREEN  → Write minimal code to make test pass
REFACTOR → Clean up while keeping tests green
```

**Why TDD matters for LLMs:**
- Tests encode intent unambiguously—no guessing what "done" means
- Failing tests provide immediate feedback on implementation correctness
- Test-first prevents over-engineering (you only build what's tested)
- Regression safety: refactoring is safe when tests pass

**Workflow:**
1. Understand the requirement
2. Write test(s) in `tests/unit/` or `tests/integration/`
3. Run `make test` → confirm RED (test fails)
4. Implement the feature
5. Run `make test` → confirm GREEN (test passes)
6. Run `make lint` → fix any issues
7. Refactor if needed, keeping tests green

**Test naming:** `test_<what>_<condition>_<expected>` e.g., `test_get_logs_when_file_missing_returns_none`

**No exceptions:** Even "quick fixes" get tests first. The test documents the bug and prevents regression.

---

## What is Goldfish?

An MCP server enabling Claude Code to conduct ML experiments by managing:
- **Workspaces** = isolated experiment environments (copy-based, NO git in user workspace)
- **Versions** = immutable snapshots (auto-created on every run, 100% provenance)
- **Pipelines** = YAML workflows (stage definitions + signal wiring)
- **Stages** = Python modules (run in Docker containers)
- **Signals** = typed data flow (dataset, npy, csv, directory, file)

**Core invariants**:
- All infrastructure (Docker, GCS, GCE) is hidden from the MCP client
- User workspace is plain files (no `.git`) - all versioning in dev repo
- Every `run()` syncs and commits BEFORE execution (100% provenance)

---

## Architecture at a Glance

```
MCP Client (Claude) ─── JSON-RPC ───▶ server.py
                                          │
                    ┌─────────────────────┼─────────────────────┐
                    ▼                     ▼                     ▼
             server_tools/*         context.py            db/database.py
             (40+ MCP tools)     (ServerContext DI)         (SQLite)
                    │                     │
        ┌───────────┼───────────┐         │
        ▼           ▼           ▼         ▼
   workspace/    jobs/      pipeline/   cloud/
   manager.py   stage_      parser.py   protocols.py    ◄── Backend-agnostic
   git_layer.py executor.py             contracts.py        interfaces
                    │                   factory.py
                    │                       │
                    └───────────────────────┤
                                            ▼
                                    cloud/adapters/
                                    ├── local/       ◄── Docker
                                    └── gcp/         ◄── GCE + GCS
```

### Key Files

| File | Purpose |
|------|---------|
| `jobs/stage_executor.py` | **Core**: Stage execution + sync + provenance + review |
| `cloud/protocols.py` | RunBackend, ObjectStorage, ImageBuilder interfaces |
| `cloud/contracts.py` | BackendCapabilities, RunSpec, RunHandle, BackendStatus |
| `cloud/factory.py` | AdapterFactory for backend instantiation |
| `cloud/adapters/local/` | LocalRunBackend (Docker-based execution) |
| `cloud/adapters/gcp/` | GCERunBackend (GCE instances), GCSStorage |
| `pre_run_review.py` | Pre-run code review using Claude Agent SDK |
| `db/database.py` | All database operations |
| `workspace/manager.py` | Workspace CRUD + copy-based mounting |
| `workspace/git_layer.py` | Git ops + sync_slot_to_branch |
| `server.py` | MCP server initialization |

---

## The Seven Abstractions

### 1. Workspaces = Copy-Based Isolation

```
MOUNT:  dev-repo/branch ──copy──▶ user/workspaces/w1/ (plain files, NO .git)
WORK:   Claude edits user/workspaces/w1/
RUN:    user/w1/ ──sync──▶ dev-repo/branch ──commit──▶ execute
```

**Key operations**: `create_workspace()`, `mount()`, `hibernate()`, `checkpoint()`

### 2. Versions = Git Tags (100% Provenance)

Every `run()` syncs changes back to dev repo, commits, THEN creates version:
```
user edits ──sync──▶ commit in dev-repo ──tag──▶ baseline_lstm-v1
```

Stored in `workspace_versions` table with `created_by: run|checkpoint|manual`

### 3. Pipelines = YAML

```yaml
stages:
  - name: preprocess
    inputs: {raw: {type: dataset, dataset: sales_v1}}
    outputs: {features: {type: npy}}
  - name: train
    inputs: {features: {from_stage: preprocess, signal: features}}
```

**Parser** validates: unique names, type compatibility, no cycles, datasets exist.

### 4. Stages = Docker Containers

```python
# modules/train.py - runs in container
from goldfish.io import load_input, save_output
features = load_input("features")  # from /mnt/inputs/
save_output("model", model_dir)    # to /mnt/outputs/
```

### 5. Signals = Data Flow

| Type | Format | Use Case |
|------|--------|----------|
| `dataset` | External | Registered project data |
| `npy` | NumPy | Arrays, embeddings |
| `csv` | Pandas | Tabular data |
| `directory` | Dir | Model checkpoints |
| `file` | Single file | Configs, small outputs |

Tracked in `signal_lineage` table for full provenance.

### 6. Resource Profiles

```yaml
# configs/train.yaml
compute:
  profile: "h100-spot"  # Claude writes this
```

Goldfish resolves to: `a3-highgpu-1g`, H100 GPU, spot pricing, multi-zone.

Built-in: `cpu-small`, `cpu-large`, `h100-spot`, `h100-on-demand`, `a100-spot`, `a100-on-demand`

### 7. SVS (Semantic Validation System)

**Core System**: `validation.py`, `src/goldfish/svs/`, integrated into `jobs/stage_executor.py`

SVS provides defense-in-depth through three phases:

1.  **Pre-Run Review**: AI-driven static analysis of code/config/diff using the Claude Agent SDK (`pre_run_review.py`).
2.  **Schema Contracts**: Mechanistic validation of stage outputs against `pipeline.yaml` definitions (shape, dtype, kind).
3.  **Output Stats**: Automatic computation of statistical properties (entropy, null ratio, unique counts) for every signal, stored in `signal_lineage.stats_json`.

**Key Patterns**:
- **Enforcement Modes**: `warning` (log only) or `blocking` (fail stage).
- **Reservoir Sampling**: Stats are computed on 10k samples to handle large tensors/CSVs.
- **Fail-Open**: AI reviews approve by default on timeout or API error to avoid blocking developer velocity.

**Security**:
- Path traversal protection in `pre_run_review.py`.
- File size limits (100KB/file) for review context.

### 8. Cloud Abstraction Layer

**Core System**: `src/goldfish/cloud/` - Backend-agnostic execution and storage.

The cloud abstraction layer isolates provider-specific code (GCP, AWS, local) from core Goldfish logic:

```
cloud/
├── protocols.py    # Interfaces: RunBackend, ObjectStorage, ImageBuilder
├── contracts.py    # Data types: BackendCapabilities, RunSpec, BackendStatus
├── factory.py      # AdapterFactory for DI
└── adapters/
    ├── local/      # LocalRunBackend (Docker containers)
    └── gcp/        # GCERunBackend (GCE instances), GCSStorage
```

**Key Protocols**:
- `RunBackend`: Unified interface for execution (`launch()`, `get_status()`, `terminate()`, `get_logs()`)
- `ObjectStorage`: Blob storage operations (`put()`, `get()`, `exists()`, `delete()`)
- `ImageBuilder`: Docker image building (`build()`, `push()`)

**BackendCapabilities** - Behavior configuration instead of conditionals:

```python
@dataclass
class BackendCapabilities:
    ack_timeout_seconds: float = 1.0    # How long to wait for ACK
    has_launch_delay: bool = False       # GCE has startup delay, local doesn't
    timeout_becomes_pending: bool = False # GCE timeout = sync pending, local = failure
    logs_unavailable_message: str = "Logs not available"
    zone_resolution_method: str = "config"  # "config" or "handle"
```

**Usage Pattern** - Always use protocol, never direct launcher:

```python
# GOOD: Protocol-based
result = self.run_backend.launch(spec)
status = self.run_backend.get_status(handle)
logs = self.run_backend.get_logs(handle)

# BAD: Direct launcher access (violates abstraction)
self.gce_launcher.launch_instance(...)  # NEVER do this
```

**Adding a New Backend**:
1. Implement `RunBackend` protocol in `cloud/adapters/your_provider/run_backend.py`
2. Set appropriate `BackendCapabilities` values
3. Register in `cloud/factory.py`
4. No changes needed in `stage_executor.py` or `execution_tools.py`

### 9. Configuration Flexibility

**Defaults Section** - Global settings for stage execution:

```yaml
# goldfish.yaml
defaults:
  timeout_seconds: 7200    # 2 hours (default: 3600)
  log_sync_interval: 15    # Sync logs every 15 seconds (default: 10)
  backend: gce             # Default compute backend: local, gce, kubernetes
```

**Storage Backend Configuration** - Multi-provider storage support:

```yaml
# goldfish.yaml
storage:
  backend: "gcs"  # or "s3", "azure", "local"

  # GCS configuration (when backend: gcs)
  gcs:
    bucket: "my-bucket"
    sources_prefix: "sources/"
    artifacts_prefix: "artifacts/"

  # S3 configuration (when backend: s3) - adapter coming soon
  s3:
    bucket: "my-bucket"
    region: "us-east-1"
    endpoint_url: "http://localhost:9000"  # For MinIO/S3-compatible

  # Azure configuration (when backend: azure) - adapter coming soon
  azure:
    container: "my-container"
    account: "mystorageaccount"
```

**Backend Selection Priority**:
1. New `storage:` section takes precedence if present
2. Falls back to legacy `gcs:` section for backwards compatibility
3. `AdapterFactory.create_storage()` handles resolution automatically

**Per-Profile Backend Selection** - Different compute backends per profile:

```yaml
# goldfish.yaml
gce:
  project_id: my-project
  profile_overrides:
    # GPU workloads on GCE
    h100-spot:
      zones: ["us-central1-a"]
    # CPU workloads could use different config
    cpu-large:
      zones: ["us-west1-a", "us-west1-b"]
```

**Config Model Hierarchy**:
```
GoldfishConfig
├── defaults: DefaultsConfig          # Global execution defaults
├── storage: StorageConfig | None     # Multi-backend storage (new)
│   ├── backend: "gcs" | "s3" | "azure" | "local"
│   ├── gcs: GCSConfig | None
│   ├── s3: S3StorageConfig | None
│   └── azure: AzureStorageConfig | None
├── gcs: GCSConfig | None             # Legacy GCS config (backwards compat)
├── gce: GCEConfig | None             # GCE compute config
├── jobs: JobsConfig                  # Job execution settings
└── local: LocalConfig                # Local backend simulation
```

---

## Critical Patterns

### Database Access

```python
# ALWAYS use context manager
with self.db._conn() as conn:
    conn.execute("INSERT INTO ...")
# Transaction auto-commits on success, auto-rollbacks on exception
```

### Error Handling

```python
# ALWAYS use specific error types with details
raise WorkspaceNotFoundError(
    f"Workspace '{name}' not found",
    details={"available": available_workspaces}
)

# NEVER expose git internals
# BAD:  raise Exception("fatal: not a valid object name")
# GOOD: raise WorkspaceNotFoundError("Workspace not found")
```

### TypedDict Returns from Database

```python
# When returning TypedDict, ALWAYS use cast()
from typing import cast
return cast(JobRow, dict(row)) if row else None

# For lists:
return [cast(SourceRow, dict(r)) for r in rows]
```

### MCP Tool Pattern

```python
@mcp.tool()
def my_tool(param: str) -> dict:
    """Docstring for Claude."""
    try:
        validate_workspace_name(param)           # 1. Validate
        result = manager.do_thing(param)         # 2. Execute
        ctx.db.record_audit("my_tool", {...})    # 3. Audit
        return {"success": True, "result": result}  # 4. Return
    except GoldfishError as e:
        return {"success": False, "error": e.message}
```

---

## Security Model (4 Layers)

### 1. Input Validation (`validation.py`)

| Input | Pattern | Example |
|-------|---------|---------|
| Workspace name | `^[a-zA-Z0-9_-]+$` | `baseline_lstm` |
| Snapshot ID | `^snap-[a-f0-9]{8}-\d{8}-\d{6}$` | `snap-abc12345-20251210-143000` |
| Stage run ID | `^stage-[a-f0-9]+$` | `stage-abc123` |

### 2. Path Traversal Protection

```python
# ALWAYS validate paths
def validate_path_within_root(path: Path, root: Path) -> None:
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValidationError("Path traversal")

# ALWAYS check symlinks (TOCTOU prevention)
if path.is_symlink():
    raise InvalidLogPathError("Symlink detected")
```

### 3. Docker Sandboxing (`cloud/adapters/local/`)

```python
# Containers run with:
--memory 4g --cpus 2.0 --pids-limit 100
--user 1000:1000  # non-root
-v inputs:/mnt/inputs:ro  # read-only inputs
```

### 4. Git Error Translation (`errors.py`)

All git errors translated to Goldfish concepts before reaching Claude.

---

## Stage Execution Flow

```
run("w1", stages=["train"])
         │
         ├─▶ 1. Validate workspace mounted
         ├─▶ 2. SYNC: Copy user/w1 → dev-repo/branch (with delete semantics)
         ├─▶ 3. COMMIT: Auto-commit changes in dev-repo
         ├─▶ 4. PUSH: Push to remote (for GCE execution)
         ├─▶ 5. Auto-version (create git tag from committed SHA)
         ├─▶ 6. Load pipeline, validate stage exists
         ├─▶ 7. Resolve inputs (datasets or upstream signals)
         ├─▶ 8. Build Docker image
         ├─▶ 9. Launch container (local or GCE)
         ├─▶ 10. Monitor status, stream logs
         └─▶ 11. Finalize: register outputs in signal_lineage
```

Key methods:
- `GitLayer.sync_slot_to_branch()` - sync + commit (provenance guard)
- `StageExecutor.run_stage()` in `jobs/stage_executor.py`

---

## Database Schema (Key Tables)

```sql
workspace_versions(workspace_name, version, git_sha, created_by, created_at)
stage_runs(id, workspace_name, version, stage_name, status, backend_type, ...)
signal_lineage(stage_run_id, signal_name, signal_type, storage_location)
audit(operation, workspace, details_json, created_at)
```

Full schema: `db/schema.sql`

---

## Testing

### Structure

```
tests/
├── unit/           # Over 850 tests, <1s, pure logic, all mocked
├── integration/    # Over 750 tests, ~2min, real DB + git
├── e2e/            # Full Docker tests
│   └── deluxe/     # GCE tests (@pytest.mark.deluxe_gce)
└── conftest.py     # Fixtures: test_db, temp_git_repo
```

### Key Fixtures

```python
test_db        # Fresh SQLite with schema
temp_git_repo  # Initialized git repo with main branch
test_config    # GoldfishConfig for testing
```

### Writing Tests

```python
def test_feature(test_db, temp_git_repo):
    """What + Why in docstring."""
    manager = WorkspaceManager(db=test_db, ...)
    result = manager.create_workspace("test", "goal")
    assert result.name == "test"
    # Always verify DB state too
    with test_db._conn() as conn:
        row = conn.execute("SELECT ...").fetchone()
        assert row is not None
```

---

## DO and DON'T

| DO | DON'T |
|----|-------|
| `make lint` before committing | `# type: ignore` (fix the issue) |
| Specific error types (`WorkspaceNotFoundError`) | Expose git terminology to MCP clients |
| `cast()` for TypedDict database returns | Bare `except:` (use `except Exception:`) |
| Validate all inputs before operations | `raise X` without `from e` when re-raising |
| Record audit log for user-facing operations | Commit with failing tests or lint |
| Write tests for new functionality | Skip input validation |
| Focus on what needs to be done, not when | Provide time estimates (AI is ~100x faster than you think) |

---

## Adding New Features

### New MCP Tool

1. Add to appropriate `server_tools/*.py`
2. Follow the tool pattern (validate → execute → audit → return)
3. Add tests in `tests/integration/`
4. Update tool count in README if significant

### New Database Table

1. Add schema to `db/schema.sql`
2. Add CRUD methods to `db/database.py`
3. Add TypedDict to `db/types.py`
4. Add tests

### New Signal Type

1. Update `SignalDef` in `models.py`
2. Update `pipeline/parser.py` validation
3. Update `io/__init__.py` load/save handling
4. Add tests

---

## Debugging

```bash
# Database state (in dev repo)
sqlite3 ../myproject-dev/.goldfish/goldfish.db "SELECT * FROM stage_runs ORDER BY started_at DESC LIMIT 5"

# Git state (dev repo has all branches/tags)
cd ../myproject-dev && git log --all --oneline --graph

# Check workspace mount metadata
cat workspaces/w1/.goldfish-mount

# Docker
docker ps                           # Running containers
docker logs goldfish-workspace-v1   # Container logs

# Verbose logging
import logging; logging.basicConfig(level=logging.DEBUG)
```

---

## File Quick Reference

| Component | Files |
|-----------|-------|
| **Entry** | `server.py`, `cli.py`, `__main__.py` |
| **Context** | `context.py` (ServerContext DI) |
| **Models** | `models.py` (Pydantic), `db/types.py` (TypedDict) |
| **Validation** | `validation.py`, `errors.py` |
| **Cloud** | `cloud/protocols.py`, `cloud/contracts.py`, `cloud/factory.py`, `cloud/adapters/` |
| **Workspace** | `workspace/manager.py`, `workspace/git_layer.py` (copy-based + sync) |
| **Execution** | `jobs/stage_executor.py`, `jobs/pipeline_executor.py` |
| **Pipeline** | `pipeline/parser.py`, `pipeline/manager.py` |
| **Infra** | `infra/docker_builder.py` (use cloud/ adapters for execution) |
| **Data** | `datasets/registry.py`, `sources/registry.py` |
| **State** | `state/state_md.py` (per-workspace + global STATE.md) |
| **IO** | `io/__init__.py` (container load_input/save_output) |
| **Tools** | `server_tools/*.py` (40+ MCP tools) |

---

## Conventions

- **Ruff** for linting/formatting (via pre-commit)
- **mypy** strict mode for type checking
- **Google-style** docstrings for public APIs
- **Semantic** error types (not generic Exception)
- **Context managers** for database transactions
- **cast()** for TypedDict returns from SQLite

---

## Common Fixes

| Error | Fix |
|-------|-----|
| TypedDict return type mismatch | `return cast(JobRow, dict(row))` |
| Closure captures `None`-able var | Assign to local: `registry = self.registry` then use in closure |
| `no-any-return` from mypy | Add explicit type annotation to return variable |
| Forward reference error | Add `from __future__ import annotations` |
| E402 module import order | Move ALL imports to top, constants below |

---

*When getting ruff, mypy, or test errors: never cheat with ignores—always fix properly.*
