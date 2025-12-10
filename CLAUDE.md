# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Goldfish is an MCP server for pipeline-based ML experimentation. It enables Claude Code to conduct ML research by managing workspaces (git branches), executing pipeline stages (Docker containers), and tracking full experiment provenance.

**Core Value**: Maintains state and provenance across long conversations through auto-generated STATE.md and comprehensive audit logging.

## Development Commands

### Testing

```bash
# Run all tests (559 tests)
pytest

# Run specific test file
pytest tests/test_workspace_manager.py

# Run single test
pytest tests/test_workspace_manager.py::TestWorkspaceCreation::test_create_workspace

# Run with coverage
pytest --cov=goldfish --cov-report=html

# Run security tests only
pytest tests/test_security*.py tests/test_tracker_security.py tests/test_exporter_security.py

# Run E2E tests
pytest tests/test_e2e_pipeline_execution.py tests/test_e2e_workflows.py
```

### Running the MCP Server

```bash
# Development mode (with specific project)
python -m goldfish serve --project /path/to/project

# Initialize new project
python -m goldfish init myproject
cd myproject
```

### Code Quality

```bash
# Format code (if configured)
black src/ tests/

# Type checking (if configured)
mypy src/
```

## Architecture: The Five Core Abstractions

Understanding these five concepts is critical to working with Goldfish:

### 1. Workspaces = Git Branches
- **What**: Development environments for ML experiments
- **Git mapping**: Each workspace is a branch `experiment/{name}`
- **Filesystem**: Mounted to slots (w1, w2, w3) via git worktrees
- **Key insight**: All workspace operations are git operations in disguise
- **Files**: `workspace/manager.py` (high-level), `workspace/git_layer.py` (low-level git ops)

### 2. Versions = Git Tags
- **What**: Immutable snapshots of workspace state
- **Git mapping**: Git tags like `workspace-v1`, `workspace-v2`
- **Creation**: Automatic on every `run_stage()` call OR manual via `checkpoint()`
- **Purpose**: Full reproducibility - can recreate exact workspace state from any version
- **Database**: `workspace_versions` table tracks version → git tag → git SHA mapping

### 3. Pipelines = YAML Workflows
- **What**: ML workflow definitions (preprocess → tokenize → train)
- **Location**: `workspaces/w1/pipeline.yaml` (Claude edits directly)
- **Signal chaining**: Stage outputs wire to next stage inputs (`from_stage: preprocess`)
- **Validation**: Checks file existence, type compatibility, no cycles
- **Files**: `pipeline/parser.py` (YAML parsing), `pipeline/manager.py` (validation)

### 4. Stages = Execution Units
- **What**: Individual steps in a pipeline (e.g., "preprocess", "tokenize")
- **Execution**: Runs in Docker container with mounted inputs/outputs
- **Code**: Python modules in `workspaces/w1/modules/{stage}.py`
- **Config**: YAML files in `workspaces/w1/configs/{stage}.yaml` (compute resources, env vars)
- **Key insight**: One stage can run independently (partial pipeline execution)

### 5. Signals = Data Flow
- **What**: Data passed between stages (inputs/outputs)
- **Types**: dataset (external), npy, csv, directory, file
- **Lineage**: Tracked in `signal_lineage` table (stage_run → signal → consumed_by)
- **Storage**: Can be local, GCS, or hyperdisk (abstracted from Claude)

### 6. Resource Profiles = Compute Abstraction
- **What**: Pre-defined GCE machine configurations (cpu-small, h100-spot, a100-on-demand)
- **Built-in profiles**: Goldfish ships with optimized profiles for ML workloads
- **Stage usage**: Claude specifies `profile: "h100-spot"` in stage config, Goldfish handles machine types/zones/disks
- **Customization**: Users can override profiles in goldfish.yaml if needed
- **Key insight**: Claude doesn't see GCE internals (machine types, accelerator strings, disk types)

## Critical Implementation Details

### Git Layer is Internal
**CRITICAL**: Git operations must NEVER be exposed to Claude. All git errors are translated to Goldfish-speak in `workspace/git_layer.py`.

```python
# BAD - exposes git internals
raise Exception("fatal: not a valid object name: 'main'")

# GOOD - translated to Goldfish concepts
raise WorkspaceNotFoundError("Workspace 'baseline_lstm' not found")
```

The `translate_git_error()` function in `errors.py` handles this translation.

### Error Handling Philosophy
All errors inherit from `GoldfishError` with structured details:

```python
raise GoldfishError(
    "Operation failed: workspace not found",
    details={"workspace": name, "available": available_workspaces}
)
```

This allows MCP tools to return structured error information to Claude.

### Database: SQLite with Transactions
- **Pattern**: Always use `with db._conn() as conn:` for transactions
- **Atomic operations**: Commit or rollback, never partial state
- **Schema**: See `db/schema.sql` for full table definitions
- **Key tables**: `workspaces`, `workspace_versions`, `stage_runs`, `signal_lineage`, `audit_log`

### Security: Four Layers

1. **Path Validation** (`validation.py`):
   - Reject `../` traversal attempts
   - Validate against workspace root
   - Check for symlinks (TOCTOU prevention)

2. **Input Validation**:
   - Workspace names: `^[a-zA-Z0-9_-]+$` (no shell metacharacters)
   - Snapshot IDs: `^snap-[a-f0-9]{8}-\d{8}-\d{6}$`
   - All inputs validated before database/git operations

3. **Resource Limits**:
   - Docker containers: 4GB memory, 2.0 CPU, 100 PIDs
   - File size limits in exporter
   - Job timeouts (configurable)

4. **Concurrency Safety**:
   - Slot-level locking prevents workspace corruption
   - Database transactions for atomicity
   - Git worktree operations are atomic (either succeed or fail completely)

### The Execution Pipeline (run_stage)

When `run_stage("w1", "preprocess")` is called, this happens:

1. **Auto-version** (`stage_executor.py:_auto_version()`):
   - Get current workspace HEAD SHA
   - Create git tag `{workspace}-v{n}`
   - Record in `workspace_versions` table

2. **Input resolution** (`stage_executor.py:_resolve_inputs()`):
   - Find where inputs come from (datasets or previous stage runs)
   - Query `signal_lineage` table for upstream outputs
   - Validate types match

3. **Docker build** (`infra/docker_builder.py`):
   - Generate Dockerfile from workspace code
   - Include Goldfish IO library
   - Build image: `goldfish-{workspace}-{version}`

4. **Container launch** (`infra/local_executor.py` or `infra/gce_launcher.py`):
   - Mount `/mnt/inputs` and `/mnt/outputs`
   - Set environment variables from config
   - Execute `python -m modules.{stage_name}`

5. **Job tracking** (`jobs/tracker.py`):
   - Poll container status
   - Update `stage_runs` table
   - Record output signals in `signal_lineage`

### STATE.md Generation

**Purpose**: Context recovery after conversation summarization

Generated by `state/state_md.py`, includes:
- Current slot status (what's mounted where)
- Recent operations (from audit log)
- Workspace lineage and versions
- Active jobs and their status

**Critical**: Always keep STATE.md up-to-date so Claude can resume work after context loss.

## MCP Server Architecture

The server is split into tool modules in `server_tools/`:

- `workspace_tools.py` - 15 tools for workspace management
- `execution_tools.py` - 8 tools for running stages/pipelines
- `data_tools.py` - 9 tools for datasets and sources
- `pipeline_tools.py` - 3 tools for pipeline operations
- `lineage_tools.py` - 3 tools for provenance tracking
- `utility_tools.py` - 3 tools for status and audit

Each tool:
1. Validates inputs using `validation.py`
2. Performs operation via managers (`workspace/`, `jobs/`, etc.)
3. Records audit log entry
4. Returns structured result (success + data OR error)

## Resource Profiles (GCE Compute)

### Built-in Profiles

Goldfish ships with optimized GCE profiles for ML workloads:

**CPU Profiles:**
- `cpu-small` - n2-standard-4 for light compute
- `cpu-large` - c4-highcpu-192 for heavy CPU workloads

**GPU Profiles:**
- `h100-spot` - H100 GPU, preemptible (cost-optimized)
- `h100-on-demand` - H100 GPU, on-demand (reliability)
- `a100-spot` - A100 GPU, preemptible
- `a100-on-demand` - A100 GPU, on-demand

### Using Profiles in Stage Configs

Claude specifies profiles by name in stage configs:

```yaml
# workspaces/w1/configs/train.yaml
compute:
  profile: "h100-spot"  # Simple! No GCE internals

env:
  EPOCHS: "100"
  LEARNING_RATE: "0.001"
```

Goldfish automatically resolves to:
- Machine type: `a3-highgpu-1g`
- GPU: `nvidia-h100-80gb` (1x)
- Zones: `[us-central1-a, us-central1-b, us-central1-c, us-west4-a]`
- Disks: `hyperdisk-balanced` 600GB boot + data
- Preemptibility: spot-first with fallback

### Customizing Profiles (Optional)

Power users can override profiles in goldfish.yaml:

```yaml
gce:
  project_id: "my-gcp-project"

  # Override zones for a built-in profile
  profile_overrides:
    h100-spot:
      zones: ["us-west1-a"]  # Restrict to one zone

    # Or define a completely custom profile
    my-custom-machine:
      machine_type: "n2-standard-16"
      zones: ["us-east1-b"]
      gpu:
        type: "none"
        count: 0
      boot_disk:
        type: "pd-ssd"
        size_gb: 200
      data_disk:
        type: "pd-ssd"
        size_gb: 500
```

### Implementation Files

- `infra/profiles.py` - Built-in profiles and ProfileResolver
- `config.py` - GCEConfig with profile_overrides
- Stage configs reference profiles by name

## Common Patterns

### Creating New Tools

```python
# In server_tools/your_category_tools.py
@mcp.tool()
def your_tool(param: str) -> dict:
    """Tool description for Claude.

    Args:
        param: Parameter description

    Returns:
        dict with success, result, or error
    """
    try:
        # 1. Validate inputs
        validate_workspace_name(param)

        # 2. Perform operation
        result = manager.do_operation(param)

        # 3. Record audit
        context.db.record_audit(
            operation="your_tool",
            details={"param": param}
        )

        # 4. Return result
        return {"success": True, "result": result}

    except GoldfishError as e:
        return {"success": False, "error": e.message}
```

### Adding Database Tables

1. Add schema to `db/schema.sql`
2. Add methods to `db/database.py`
3. Add migration (if needed)
4. Update tests

### Extending Pipeline Stages

To add a new signal type:
1. Update `models.py` SignalDef types
2. Update `pipeline/parser.py` validation
3. Update Goldfish IO library (`io/__init__.py`)
4. Add tests

## Test Architecture

**Philosophy**: Test real components, minimize mocks

### Test Categories

1. **Unit tests** (`test_*.py`):
   - Test individual components
   - Use fixtures for database and config
   - Mock external services (GCS, Docker)

2. **Integration tests** (`test_e2e_*.py`):
   - Test complete workflows
   - Use real Database, WorkspaceManager, PipelineManager
   - Create temp directories for isolation

3. **Security tests** (`test_security*.py`):
   - Path traversal attempts
   - Command injection attempts
   - Resource exhaustion scenarios
   - Concurrent access patterns

4. **Concurrency tests** (`test_concurrent.py`):
   - Multi-threaded operations
   - Slot locking behavior
   - Database transaction isolation

### Writing Tests

```python
def test_your_feature(test_db, test_config):
    """Test description.

    Use descriptive docstrings explaining WHAT is being tested
    and WHY it matters.
    """
    # Setup
    manager = YourManager(db=test_db, config=test_config)

    # Execute
    result = manager.operation()

    # Verify
    assert result.success is True
    assert result.data["key"] == "expected_value"

    # Verify database state
    with test_db._conn() as conn:
        record = conn.execute("SELECT * FROM table WHERE id = ?", (id,)).fetchone()
        assert record["status"] == "expected"
```

### Fixtures (`conftest.py`)

- `test_db` - Temporary database with schema
- `test_config` - Test configuration
- `temp_dir` - Temporary directory (auto-cleanup)
- `pipeline_project` - Full project setup (for E2E tests)

## Infrastructure Layer

### Local vs GCE Execution

**Local** (`infra/local_executor.py`):
- Uses Docker API directly
- Suitable for development
- Fast iteration
- No cloud costs

**GCE** (`infra/gce_launcher.py`):
- Launches VM instances on Google Compute Engine
- GPU support via NVIDIA drivers
- Capacity-aware multi-zone search (`infra/resource_launcher.py`)
- Automatic cleanup after job completion

### Docker Security

All Docker containers run with:
- Non-root user (UID 1000)
- Memory limit (4GB)
- CPU limit (2.0 cores)
- PID limit (100 processes)
- Read-only input volumes
- No network access (unless explicitly needed)

Version strings are sanitized to prevent injection:
```python
# BAD - allows injection
image_tag = f"goldfish-{workspace}-{version}"

# GOOD - validates version format
validate_version(version)  # Must match ^v\d+$
image_tag = f"goldfish-{workspace}-{version}"
```

## Debugging Tips

### Enable Verbose Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Inspect Database State

```bash
sqlite3 .goldfish/goldfish.db
sqlite> SELECT * FROM workspace_versions;
sqlite> SELECT * FROM stage_runs ORDER BY started_at DESC LIMIT 10;
```

### Git State

```bash
cd .goldfish/dev
git log --all --graph --oneline
git worktree list
git tag -l
```

### Docker Inspection

```bash
docker ps  # Running containers
docker logs goldfish-{workspace}-v1  # Container logs
docker inspect goldfish-{workspace}-v1  # Full container details
```

## Key Files Reference

### Entry Points
- `server.py` - MCP server (loads all tools)
- `__main__.py` - CLI entry point
- `cli.py` - Command-line interface

### Core Managers
- `workspace/manager.py` - Workspace operations (create, mount, checkpoint)
- `jobs/stage_executor.py` - Stage execution engine
- `pipeline/manager.py` - Pipeline validation and management
- `db/database.py` - All database operations

### Infrastructure
- `infra/docker_builder.py` - Docker image building
- `infra/local_executor.py` - Local Docker execution
- `infra/gce_launcher.py` - GCE instance management (594 lines)
- `infra/resource_launcher.py` - Capacity-aware launcher (540 lines)
- `infra/startup_builder.py` - GCE startup scripts (289 lines)

### Validation & Errors
- `validation.py` - Input validation (paths, names, IDs)
- `errors.py` - Error types and git error translation

## Design Constraints

### What Claude Should NOT See

1. **Git internals**: All git terminology hidden behind workspace/snapshot abstractions
2. **Docker details**: Containerization completely abstracted
3. **File system paths**: All operations relative to workspace root
4. **Database schema**: Access only through manager APIs

### Performance Considerations

1. **Database queries**: Use indexes for workspace_name, status, created_at
2. **Git operations**: Worktrees allow parallel workspace access
3. **Docker builds**: Layer caching critical for fast iteration
4. **Signal storage**: Local for intermediates, GCS for artifacts

### Future Extension Points

1. **New execution backends**: Implement LocalExecutor/GCELauncher interface
2. **New signal types**: Update SignalDef + parser + IO library
3. **New audit operations**: Add to AuditOperation enum
4. **New MCP tools**: Add to appropriate tool module in `server_tools/`
- When getting ruff, mypy or test errors, never cheat, always solve properly.