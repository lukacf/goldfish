# Goldfish Architecture Review

> **Date:** 2026-01-28
> **Reviewers:** 8-agent swarm (cloud, db, error, workspace, jobs, infra, smell, test)
> **Scope:** 53,756 lines across 106 Python files

This document captures architectural issues, code smells, and technical debt identified during a comprehensive codebase review. Issues are categorized by type and prioritized for remediation.

---

## Strictness First

Before addressing individual issues, we should raise the floor on code quality by making Python's optional strictness mandatory.

### Recommended mypy Configuration

```toml
# pyproject.toml
[tool.mypy]
strict = true
warn_return_any = true
warn_unused_ignores = true
disallow_any_explicit = true
disallow_any_generics = true
disallow_untyped_defs = true
disallow_incomplete_defs = true
check_untyped_defs = true
no_implicit_optional = true
warn_redundant_casts = true
warn_unreachable = true
```

### Pre-commit Enforcement

```yaml
# .pre-commit-config.yaml addition
- repo: local
  hooks:
    - id: no-type-ignore
      name: Check for unexplained type ignores
      entry: bash -c 'grep -rn "type: ignore" --include="*.py" | grep -v "# type: ignore\[" && exit 1 || exit 0'
      language: system
      pass_filenames: false
```

Every `# type: ignore` must specify the error code and ideally link to an issue explaining why it's necessary.

### Bare Exception Audit

Add ruff rule to flag bare `except Exception:` without logging:

```toml
# pyproject.toml
[tool.ruff.lint]
select = ["BLE"]  # blind-except
```

---

## 1. God Classes

Large files that have grown beyond maintainability. These are the highest-impact refactoring targets.

### database.py (5,503 lines)

**Location:** `src/goldfish/db/database.py`

**Problem:** Single class handling all database operations across unrelated domains: workspaces, jobs, metrics, SVS, sources, backups, audit, daemon leases, docker builds, and schema migrations.

**Recommendation:** Split into domain-focused modules:
```
db/
├── __init__.py          # Re-exports for backward compat
├── base.py              # Database base class, connection management
├── workspaces.py        # Workspace CRUD, mounts, versions
├── jobs.py              # Stage runs, signal lineage
├── metrics.py           # Metrics recording and queries
├── sources.py           # Source registry operations
├── migrations.py        # Schema versioning (extract 576-line function)
└── types.py             # TypedDict definitions (existing)
```

### stage_executor.py (3,275 lines)

**Location:** `src/goldfish/jobs/stage_executor.py`

**Problem:** Orchestrates everything: container launching, Docker builds, input resolution, pre-run reviews, status monitoring, live sync, post-run processing, and finalization.

**Recommendation:** Extract focused modules:
```
jobs/
├── stage_executor.py    # Core orchestration only (~500 lines)
├── input_resolver.py    # Input resolution logic (lines 665-941)
├── finalization.py      # Post-run processing (lines 2435-2671)
├── monitoring.py        # Status polling, live sync (lines 1658-1930)
└── constants.py         # All timeout/limit constants
```

---

## 2. Abstraction Leaks

GCP-specific code living in provider-agnostic layers, violating the cloud abstraction architecture.

### Files That Should Move to cloud/adapters/gcp/

| Current Location | Issue | Target Location |
|------------------|-------|-----------------|
| `infra/gce_launcher.py` | 1,100+ lines of GCE-specific code | `cloud/adapters/gcp/gce_launcher.py` |
| `infra/startup_builder.py` | 1,000+ lines of GCE startup scripts | `cloud/adapters/gcp/startup_builder.py` |
| `infra/metadata/gcp.py` | GCP metadata bus | `cloud/adapters/gcp/metadata_bus.py` |

### Hardcoded Backend Checks

| Location | Code | Fix |
|----------|------|-----|
| `infra/docker_builder.py:278` | `if jobs_backend == "gce":` | Use `BackendCapabilities.platform` |
| `cloud/contracts.py:338-344` | `if backend_type == "local"` / `elif backend_type == "gce"` | Move validation to adapters |
| `infra/base_images/manager.py:789` | `if backend == "cloud":` | Use `ImageBuilder` protocol consistently |

### Registry Coupling in DockerBuilder

`src/goldfish/infra/docker_builder.py` is tightly coupled to Google Artifact Registry via direct `gcloud` calls.

| Location | Issue | Fix |
|----------|-------|-----|
| `docker_builder.py:318` | `push_image` calls `gcloud auth` and `docker push` directly | Extract `RegistryAdapter` interface |
| `docker_builder.py:444` | `_image_exists_in_registry` calls `gcloud artifacts` | Move to `GCPRegistryAdapter` |

---

## 3. Duplicate Definitions

Same concepts defined in multiple places with different implementations.

### Duplicate Error Classes

| Class | Location 1 | Location 2 | Fix |
|-------|------------|------------|-----|
| `CapacityError` | `errors.py:142` (extends `GoldfishError`) | `cloud/adapters/gcp/resource_launcher.py:30` (standalone) | Remove from resource_launcher, import from errors |
| `InvalidSnapshotIdError` | `errors.py:78` (extends `GoldfishError`) | `validation.py:83` (extends `ValidationError`) | Keep validation.py version, remove from errors.py |
| `InvalidSourceNameError` | `errors.py:86` (extends `GoldfishError`) | `validation.py:50` (extends `ValidationError`) | Keep validation.py version, remove from errors.py |

### Duplicate Utility Functions

| Function | Locations | Fix |
|----------|-----------|-----|
| `_check_docker_available()` | `cloud/adapters/local/image.py:64`, `local/image.py:302`, `gcp/image.py:346` | Extract to `cloud/utils.py` |
| `_check_gcloud_available()` | `cloud/adapters/gcp/image.py:68`, `gcp/image.py:337` | Extract to `cloud/adapters/gcp/utils.py` |
| `_poll_interval()` | `jobs/stage_executor.py:2106`, `jobs/pipeline_executor.py:675` | Extract to `jobs/utils.py` |

---

## 4. DRY Violations

Repeated code patterns that should be consolidated.

### Workspace Layer

| Pattern | Locations | Fix |
|---------|-----------|-----|
| `exclude_patterns` list | `workspace/manager.py:1176`, `git_layer.py:612`, `git_layer.py:726`, `git_layer.py:1078` | Define `EXCLUDE_PATTERNS` constant in `git_layer.py` |
| Temp worktree creation/cleanup | `git_layer.py:598` (diff), `git_layer.py:713` (dirty check), `git_layer.py:1040` (sync) | Extract `@contextmanager _temp_worktree()` |
| Metadata file read/write | 10+ locations across manager.py and git_layer.py | Create `_read_mount_metadata()` / `_write_mount_metadata()` |
| Hardcoded `.goldfish/*` paths | `manager.py:101`, `git_layer.py:598,713,1040` | Define `GOLDFISH_*_DIR` constants |

### Duplicate Imports Inside Methods

| Location | Import | Fix |
|----------|--------|-----|
| `stage_executor.py:1475,1578,1760,1825,2490` | `get_capabilities_for_backend` | Import once at module level |
| `manager.py:534,1390` | `from datetime import datetime` | Remove, use module-level import |
| `database.py:3583,3639,4147,4168,5416,5469` | `from datetime import UTC, datetime` | Remove, use module-level import |

---

## 5. Type Safety Gaps

Missing or incorrect type annotations that defeat static analysis.

### Database Methods Returning Untyped Dicts

These methods return `dict(row)` or `dict | None` instead of proper TypedDict:

| Method | Location | Fix |
|--------|----------|-----|
| `get_mount()` | `database.py:1307` | Return `MountRow \| None` with `cast()` |
| `get_mount_by_workspace()` | `database.py:1323` | Return `MountRow \| None` |
| `get_workspace_lineage()` | `database.py:1459` | Create `WorkspaceLineageRow` TypedDict |
| `get_version()` | `database.py:1556` | Create `WorkspaceVersionRow` TypedDict |
| `list_versions()` | `database.py:1580` | Return `list[WorkspaceVersionRow]` |
| `get_stage_run()` | `database.py:1647` | Create `StageRunRow` TypedDict |
| `list_stage_runs()` | `database.py:1672` | Return `list[StageRunRow]` |
| +6 more | Various | Add TypedDict definitions |

### Missing TypedDict Field

| TypedDict | Missing Field | Location |
|-----------|---------------|----------|
| `BackupRow` | `id: int` | `db/types.py:231` |

---

## 6. Error Handling Issues

### Bare Exception Handlers (65 total)

High-concentration files:

| File | Count | Concern |
|------|-------|---------|
| `jobs/stage_executor.py` | 8 | Stage execution errors silently swallowed |
| `workspace/manager.py` | 5 | Workspace operations may mask errors |
| `db/database.py` | 4 | Database failures may go unnoticed |
| `io/__init__.py` | 5 | Import/IO errors silently caught |
| `mcp_proxy.py` | 3 | Daemon connection errors ignored |

**Fix:** Add `logger.debug("Failed to X: %s", e, exc_info=True)` to all bare handlers.

### Raw Exception Messages Exposed to MCP Clients

| Location | Code |
|----------|------|
| `server_tools/utility_tools.py:94,133` | `return {"error": str(e)}` |
| `server_tools/infra_tools.py:217,256` | `return {"error": str(e)}` |
| `server_tools/backup_tools.py:130,185,225,264` | `return {"error": str(e)}` |
| `server_tools/logging_tools.py:40` | `return {"error": str(e)}` |

**Fix:** For non-`GoldfishError` exceptions, return sanitized message while logging full exception.

---

## 7. Magic Numbers

Hardcoded constants scattered throughout the codebase without explanation.

### Timeouts (stage_executor.py)

| Line | Value | Meaning |
|------|-------|---------|
| 64 | `1000` | Log tail lines for finalize |
| 2673 | `3600` | Default stage timeout (1 hour) |
| 2694 | `300` | GCE not-found timeout (5 min) |
| 2744 | `1200` | GCE launch timeout (20 min) |
| 1663 | `15` | Metrics live sync interval |
| 1815 | `10` | SVS live sync interval |
| 2591 | `500` | Error log truncation |
| 2628 | `200` | Different error log truncation |

**Fix:** Consolidate into `jobs/constants.py` with docstrings explaining rationale.

### Infrastructure Constants

| Location | Value | Meaning |
|----------|-------|---------|
| `gce_launcher.py:29` | `80000` | Hyperdisk IOPS |
| `gce_launcher.py:30` | `2400` | Hyperdisk throughput |
| `gce_launcher.py:36` | `21600` | Default max runtime (6 hours) |
| `startup_builder.py:17` | `160` | GPU driver install max attempts |
| `startup_builder.py:21` | `5` | gcsfuse mount max attempts |

---

## 8. Concurrency Concerns

### Potential Race Conditions

| Location | Issue | Fix |
|----------|-------|-----|
| `stage_executor.py:1670-1676` | `_metrics_sync_lock` and `state.sync_lock` acquired separately | Use single lock or document ordering |
| `base_images/manager.py:139` | `_builds` dict accessed in threads, some methods don't acquire lock | Review all `_builds` access for proper locking |

### Daemon Threads Without Tracking

| Location | Issue |
|----------|-------|
| `stage_executor.py:2611` | Pattern extraction thread spawned with no completion tracking |

---

## 9. Schema Issues

### Missing Indexes

| Table | Query Pattern | Recommended Index |
|-------|---------------|-------------------|
| `workspace_versions` | `WHERE workspace_name = ? ORDER BY created_at` | `(workspace_name, created_at)` |
| `daemon_leases` | `WHERE lease_name = ?` | `(lease_name)` - minor impact |

### Missing Constraints

| Table.Column | Issue | Fix |
|--------------|-------|-----|
| `svs_reviews.decision` | No CHECK constraint | `CHECK(decision IN ('approved', 'blocked', 'warned'))` |
| `audit.details` | Nullable but always provided | Consider `NOT NULL` |
| `schema_version` | Not documented in schema.sql | Add table definition for documentation |

---

## 10. Test Quality Issues

### Flaky Test Patterns

| Location | Issue | Fix |
|----------|-------|-----|
| `tests/unit/state_machine/test_leader_election.py:62,133,217,433,602` | Real `time.sleep(1.5)` | Mock time or use threading events |
| `tests/unit/experiment_model/test_experiment_records.py:39` | `time.sleep(0.015)` for timestamps | Use deterministic timestamps |
| `tests/integration/test_concurrent.py:107` | Fixed delays in concurrent tests | Use condition variables |

### Skipped Tests Indicating Missing Features

| Location | Reason |
|----------|--------|
| `tests/integration/svs/test_svs_migration.py:178` | "Migration tests too fragile" |
| `tests/integration/test_config_validation.py:231` | "Stage config validation not implemented" |

### Coverage Gaps

- `jobs/stage_executor.py` error paths
- `cloud/factory.py` instantiation failures
- `workspace/git_layer.py` sync conflict handling
- `infra/base_images/manager.py` build failures

---

## 11. Dead Code Candidates

| Location | Issue |
|----------|-------|
| `jobs/tracker.py` | Legacy Job system; new code uses `stage_executor.py` with `StageState` |
| `errors.py:78,86` | `InvalidSnapshotIdError`, `InvalidSourceNameError` duplicates of validation.py versions |

---

## 12. Import Cycles & Layering Inversions

Several dependency cycles exist today and are “avoided” with local imports or `TYPE_CHECKING`. This is fragile and makes refactors risky.

### Notable Cycles / Inversions

| Cycle / Inversion | Evidence | Risk | Recommendation |
|------------------|----------|------|----------------|
| `db.database` → `experiment_model.records` → `state_machine` → `db.database` | `database.py:487` imports `generate_record_id`; `experiment_model/records.py:26` imports `goldfish.state_machine`; multiple state_machine modules import `Database` | Layering inversion between persistence, experiment model, and state machine; easy to trigger runtime cycles when refactoring | Move `generate_record_id` to a neutral module (e.g., `utils/ids.py`) or keep it inside db; keep experiment_model independent of state_machine or inject transition function |
| `jobs.stage_executor` → `svs.patterns.extractor` → `server_core` → server/runtime context | `stage_executor.py:2588-2589` imports `extract_failure_pattern`; `svs/patterns/extractor.py:254` imports `server_core._get_config` | SVS module depends on server runtime; hard to reuse in tests/CLI; hidden import cycles when tools import server_core | Pass config/fallback model into extractor from caller; move config access to SVS layer instead of server_core |
| `server` ↔ `server_tools.utility_tools` | `server_tools/utility_tools.py:43` imports `_init_server` from `server.py` | Tool modules depend on server entrypoint; refactors can break imports; harder to isolate tools | Move `_init_server` to `server_core` or `init.py` and import from there |
| `server` ↔ `server_tools` (runtime) | `server.py` imports all tools; `utility_tools.py` imports `_init_server` from `server` inside functions | Runtime circular dependency. If `server` is refactored, tools break. `_init_server` is a core initialization logic trapped in the entrypoint. | Extract `_init_server` to `server_core.py` to break the cycle. |

---

## 13. Configuration Sprawl & Env-Driven Defaults

Many runtime behaviors are controlled via environment variables read at import time or deep inside modules, separate from `GoldfishConfig` / `goldfish.yaml`.

### Examples

| Location | Pattern | Impact | Recommendation |
|----------|---------|--------|----------------|
| `jobs/pipeline_executor.py:28-31` | Thread pool size and error limits set via env at import | Changes require process restart; tests can be order-dependent | Move to config object or load env in `__init__` |
| `jobs/stage_executor.py:64` | `STAGE_LOG_TAIL_FOR_FINALIZE` set at import from env | Hidden config surface; reload_config doesn’t affect | Centralize in config/settings and read once at startup |
| `io/__init__.py:667-704`, `metrics/*`, `logging/settings.py` | Multiple subsystems read `GOLDFISH_*` env vars directly | Config split between YAML + env with no single source of truth | Introduce a unified `RuntimeSettings` (env + yaml) and pass into subsystems |

**Recommendation:** Make environment overrides explicit in `GoldfishConfig` (or a separate Settings object) and avoid module-level env reads. This makes `reload_config()` meaningful and keeps tests deterministic.

---

## 14. Registry / Manager API Drift

High-level registries exist but are bypassed by tools, leading to duplicated validation logic and risk of drift.

### Example: Source Registry vs Tool Layer

| Concern | Evidence | Risk | Recommendation |
|---------|----------|------|----------------|
| `SourceRegistry` lacks update/delete APIs, while tools perform updates directly against DB | `sources/registry.py` has TODO for update; `server_tools/data_tools.py` implements update/delete using DB directly | Two code paths for validation + metadata rules; inconsistent behavior over time | Add update/delete APIs to `SourceRegistry` and route `manage_sources` through it, or remove registry abstraction entirely |

---

## 15. Public API Documentation / Behavior Mismatch

User-facing APIs in `goldfish.io` have mismatches between docs and actual behavior.

### Checkpoint API

| Issue | Evidence | Impact | Recommendation |
|-------|----------|--------|----------------|
| `load_checkpoint()` example implies data return, but function returns `Path | None` | `io/__init__.py:1069-1078` example uses `load_checkpoint("training_state")["step"]` | Misleads users; suggests API supports unpickling but it doesn’t | Fix docstring or add a `load_checkpoint_data()` helper that mirrors `save_checkpoint()` behavior |
| `load_checkpoint()` return type mismatch | Implementation returns `Path` object (not subscriptable), docstring example attempts to subscript it | Runtime `TypeError: 'PosixPath' object is not subscriptable` for users following docs | Update example to show loading data from the returned path (e.g. `torch.load(path)`) |

---

## 16. Security Considerations

### GCE Input Validation

`src/goldfish/cloud/adapters/gcp/resource_launcher.py` passes `instance_name` and `zone` directly to `gcloud` commands without explicit validation/sanitization in that module. While `DockerBuilder` sanitizes image tags, `GCELauncher` relies on callers.

**Risk:** Potential command injection if `instance_name` or `zone` comes from untrusted input (e.g. user-provided workspace names containing shell metacharacters).

**Recommendation:** Add strict validation (regex) for `instance_name` and `zone` in `GCELauncher.__init__` or `launch`.

---

## 17. Logging Inconsistencies

Logging patterns are inconsistent across the codebase, making log filtering and configuration difficult.

| Pattern | Locations | Issue | Recommendation |
|---------|-----------|-------|----------------|
| `logging.getLogger("goldfish.server")` | All `server_tools/*`, `server.py`, `infra_tools.py` | All tools share one logger name; hard to debug specific tools | Use `logging.getLogger(__name__)` or `goldfish.server.tools.<tool_name>` |
| `logging.getLogger(__name__)` | Most core modules (`db`, `jobs`, `workspace`) | Standard pattern, but inconsistent with tools | Standardize on `__name__` everywhere |
| Hardcoded names (`"goldfish.proxy"`, `"goldfish.daemon"`) | `mcp_proxy.py`, `daemon.py` | Inconsistent naming scheme | Use `__name__` or define constants |

---

## Priority Matrix

### P0 - Fix Before Next Major Feature

1. Enable stricter mypy configuration
2. Add TypedDict to database return types (type safety)
3. Consolidate duplicate error classes (correctness)
4. Add logging to bare exception handlers (debuggability)

### P1 - Next Refactoring Sprint

5. Split `database.py` into domain modules
6. Split `stage_executor.py` into focused modules
7. Move GCP code from `infra/` to `cloud/adapters/gcp/`
8. Extract DRY violations (exclude_patterns, poll_interval, etc.)

### P2 - Ongoing Cleanup

9. Consolidate magic numbers into constants modules
10. Add missing schema constraints and indexes
11. Fix flaky tests
12. Review and remove dead code
13. Untangle import cycles and server/tool dependencies
14. Centralize env-driven configuration into config/settings
15. Align SourceRegistry and data tools APIs
16. Document/fix checkpoint API mismatch

---

## Tracking

Create GitHub issues for P0 and P1 items. P2 items can be addressed opportunistically during related work.

```bash
# Example issue creation
gh issue create --title "Split database.py into domain modules" \
  --body "See docs/ARCHITECTURE_REVIEW.md section 1" \
  --label "refactoring,tech-debt"
```
