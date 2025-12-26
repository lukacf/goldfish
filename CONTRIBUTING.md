# Contributing to Goldfish

Thank you for your interest in contributing to Goldfish! This guide covers everything you need to know to contribute effectively, from setting up your development environment to understanding the architecture.

## Table of Contents

- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Architecture Deep-Dive](#architecture-deep-dive)
- [Core Abstractions](#core-abstractions)
- [Data Flow](#data-flow)
- [Database Schema](#database-schema)
- [Security Model](#security-model)
- [Testing](#testing)
- [Code Style](#code-style)
- [Pull Request Process](#pull-request-process)
- [Configuration Reference](#configuration-reference)

---

## Development Setup

### Prerequisites

- Python 3.11+
- Git
- Docker Desktop (for running stages)
- Optional: Google Cloud SDK (for GCE features)

### Quick Setup

```bash
# Clone the repository
git clone https://github.com/lukacf/goldfish.git
cd goldfish

# Create virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows

# Install with development dependencies
pip install -e ".[dev]"

# Install pre-commit hooks (REQUIRED)
make install-hooks

# Verify setup
make ci  # Should pass all checks
```

### Development Commands

```bash
make test              # Fast unit tests (<1s)
make test-unit         # Unit tests with coverage
make test-integration  # Integration tests (~2 min)
make test-e2e          # E2E tests (requires Docker)
make lint              # Run ruff + mypy via pre-commit
make ci                # Full CI suite (lint + unit + integration)
make clean             # Remove caches and build artifacts
```

### IDE Setup

**VS Code** (recommended):
```json
// .vscode/settings.json
{
  "python.defaultInterpreterPath": ".venv/bin/python",
  "python.analysis.typeCheckingMode": "basic",
  "editor.formatOnSave": true,
  "[python]": {
    "editor.defaultFormatter": "charliermarsh.ruff"
  }
}
```

---

## Project Structure

```
goldfish/
├── src/goldfish/                 # Main package
│   ├── __init__.py
│   ├── __main__.py               # CLI entry: python -m goldfish
│   ├── cli.py                    # Click CLI commands
│   ├── server.py                 # MCP server initialization
│   ├── context.py                # ServerContext (dependency injection)
│   ├── config.py                 # Configuration loading
│   ├── models.py                 # Pydantic models
│   ├── errors.py                 # Error types + git error translation
│   ├── validation.py             # Input validation (security)
│   ├── utils.py                  # Utility functions
│   │
│   ├── db/                       # Database layer
│   │   ├── database.py           # SQLite operations (1200+ lines)
│   │   ├── schema.sql            # Table definitions
│   │   └── types.py              # TypedDict definitions
│   │
│   ├── workspace/                # Workspace management
│   │   ├── manager.py            # High-level workspace ops
│   │   └── git_layer.py          # Low-level git operations
│   │
│   ├── pipeline/                 # Pipeline handling
│   │   ├── parser.py             # YAML parsing + validation
│   │   └── manager.py            # Pipeline CRUD
│   │
│   ├── jobs/                     # Execution engine
│   │   ├── stage_executor.py     # Core stage execution (1400 lines)
│   │   ├── pipeline_executor.py  # Pipeline orchestration
│   │   ├── tracker.py            # Job status tracking
│   │   ├── launcher.py           # Legacy job launcher
│   │   ├── conversion.py         # Dict ↔ model conversion
│   │   └── exporter.py           # Experiment export
│   │
│   ├── pre_run_review.py         # Pre-run code review (500 lines)
│   │
│   ├── infra/                    # Infrastructure layer
│   │   ├── docker_builder.py     # Dockerfile generation + build
│   │   ├── local_executor.py     # Local Docker execution
│   │   ├── gce_launcher.py       # GCE instance management
│   │   ├── resource_launcher.py  # Capacity-aware multi-zone
│   │   ├── startup_builder.py    # GCE startup scripts
│   │   └── profiles.py           # Resource profiles (H100, A100, etc.)
│   │
│   ├── datasets/                 # Dataset management
│   │   └── registry.py           # Dataset registration
│   │
│   ├── sources/                  # Data sources (legacy)
│   │   ├── registry.py           # Source registration
│   │   └── lineage.py            # Source lineage
│   │
│   ├── lineage/                  # Provenance tracking
│   │   └── manager.py            # Workspace/run lineage
│   │
│   ├── state/                    # Context recovery
│   │   └── state_md.py           # STATE.md generation
│   │
│   ├── io/                       # Container IO library
│   │   ├── __init__.py           # load_input/save_output
│   │   └── stats.py              # SVS stats collection (container-side)
│   │
│   ├── svs/                      # Semantic Validation System
│   │   ├── contract.py           # Schema-as-contract validation
│   │   ├── agent.py              # Agent abstraction (Claude/Codex/Gemini)
│   │   ├── post_run.py           # Post-run AI review
│   │   └── patterns/             # Failure pattern extraction/management
│   │
│   └── server_tools/             # MCP tool definitions
│       ├── __init__.py
│       ├── workspace_tools.py    # 18 workspace tools
│       ├── execution_tools.py    # 14 execution tools
│       ├── data_tools.py         # 9 data tools
│       ├── pipeline_tools.py     # 4 pipeline tools
│       ├── lineage_tools.py      # 3 lineage tools
│       ├── svs_tools.py          # SVS tooling (patterns, reviews)
│       └── utility_tools.py      # 4 utility tools
│
├── tests/                        # Test suite (700+ tests)
│   ├── conftest.py               # Shared fixtures
│   ├── unit/                     # Fast unit tests (<100ms each)
│   ├── integration/              # Component tests (real DB/git)
│   └── e2e/                      # Full system tests
│       └── deluxe/               # GCE cloud tests (opt-in)
│
├── pyproject.toml                # Package configuration
├── Makefile                      # Development commands
├── .pre-commit-config.yaml       # Git hooks
├── goldfish.yaml.example         # Example configuration
│
├── README.md                     # User-facing documentation
├── CONTRIBUTING.md               # This file
├── CLAUDE.md                     # AI assistant instructions
├── AGENTS.md                     # Symlink to CLAUDE.md
└── llms.txt                      # Machine-readable doc index
```

---

## Architecture Deep-Dive

### Component Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                         MCP Client (Claude Code)                      │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │ JSON-RPC over stdio
┌────────────────────────────────▼─────────────────────────────────────┐
│                           server.py                                   │
│  - FastMCP server initialization                                      │
│  - Tool registration from server_tools/*                              │
│  - ServerContext initialization                                       │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │
┌────────────────────────────────▼─────────────────────────────────────┐
│                          context.py                                   │
│  ServerContext (dataclass):                                          │
│  - project_root: Path                                                │
│  - config: GoldfishConfig                                            │
│  - db: Database                                                      │
│  - workspace_manager: WorkspaceManager                               │
│  - pipeline_manager: PipelineManager                                 │
│  - stage_executor: StageExecutor                                     │
│  - pipeline_executor: PipelineExecutor                               │
│  - job_tracker: JobTracker                                           │
│  - dataset_registry: DatasetRegistry                                 │
│  - state_manager: StateManager                                       │
└──────────────┬──────────────┬──────────────┬────────────────────────┘
               │              │              │
    ┌──────────▼──────┐ ┌─────▼─────┐ ┌──────▼──────┐
    │ WorkspaceManager│ │ Database  │ │StageExecutor│
    │   + GitLayer    │ │  (SQLite) │ │             │
    └─────────────────┘ └───────────┘ └──────┬──────┘
                                             │
                        ┌────────────────────┼────────────────────┐
                        │                    │                    │
                 ┌──────▼──────┐     ┌───────▼──────┐     ┌───────▼──────┐
                 │DockerBuilder│     │LocalExecutor │     │ GCELauncher  │
                 │             │     │              │     │              │
                 └─────────────┘     └──────────────┘     └──────────────┘
```

### Key Design Decisions

1. **Copy-Based Isolation**: User works with plain file copies (no `.git` in workspace). All versioning happens in the separate goldfish dev repo. This keeps the user's project pristine.

2. **100% Provenance**: Every `run()` syncs changes back to git and commits BEFORE execution. The version SHA always matches what actually ran.

3. **Hidden Infrastructure**: Claude never sees Docker commands, GCS paths, or GCE APIs. Everything is abstracted behind MCP tools.

4. **Per-Workspace STATE.md**: Each workspace has its own experiment narrative, enabling context recovery after conversation summarization.

5. **Typed Boundaries**: All MCP tool inputs/outputs use Pydantic models. Database rows use TypedDict for type safety.

6. **Defense in Depth**: Four security layers protect against injection and traversal attacks.

---

## Core Abstractions

### 1. Workspaces = Copy-Based Isolation

A workspace is an isolated experiment environment. Internally, each workspace is a git branch in the goldfish dev repo, but the user works with **plain file copies**.

```
ARCHITECTURE:
goldfish-dev/                    ← Goldfish dev repo (all versioning here)
├── .git/
│   └── experiment/baseline_lstm ← Workspace branch
└── .goldfish/goldfish.db        ← Database

user-project/                    ← User's project (stays pristine)
└── workspaces/w1/               ← Copied files (NO .git)
    ├── pipeline.yaml
    ├── modules/
    └── .goldfish-mount          ← Only Goldfish metadata
```

**Workflow**:
```
MOUNT:  gf-dev/branch ──copy──▶ user/w1 (plain files, NO .git)
WORK:   Claude edits user/w1
RUN:    user/w1 ──sync──▶ gf-dev/branch ──commit──▶ execute with SHA
```

```python
# workspace/manager.py
class WorkspaceManager:
    def create_workspace(self, name: str, goal: str) -> WorkspaceInfo:
        """Create experiment/{name} branch in dev repo"""

    def mount(self, workspace: str, slot: str) -> SlotInfo:
        """Copy branch content to slot directory (plain files)"""

    def hibernate(self, slot: str) -> None:
        """Sync changes back to branch, commit, remove slot"""

    def checkpoint(self, slot: str, message: str) -> VersionInfo:
        """Sync + create tagged version"""
```

**Git mapping** (internal, hidden from user):
- Workspace "baseline_lstm" → branch `experiment/baseline_lstm` in dev repo
- Mounted to slot w1 → plain directory at `user-project/workspaces/w1/`
- Version v3 → git tag `baseline_lstm-v3` in dev repo

### 2. Versions = Git Tags (with 100% Provenance)

Every `run()` call syncs changes back to the dev repo and creates an immutable version.

```python
# jobs/stage_executor.py
def run_stage(self, workspace: str, stage: str, ...):
    # 1. Sync slot changes back to dev repo branch
    sha = self.git.sync_slot_to_branch(slot_path, workspace, commit_msg)

    # 2. Push for remote execution (GCE)
    if self.config.execution.push_before_run:
        self.git.push_branch(workspace)

    # 3. Create version tag
    version = self._auto_version(workspace)  # Points to committed SHA

    # 4. Execute with guaranteed provenance
    self._launch_container(sha, ...)
```

**Provenance guarantee**: Code is always committed before execution. The version SHA matches exactly what ran.

### 3. Pipelines = YAML Workflows

```yaml
# pipeline.yaml
name: baseline_training
stages:
  - name: preprocess
    inputs:
      raw_data: { type: dataset, dataset: sales_2024 }
    outputs:
      features: { type: npy }

  - name: train
    inputs:
      features: { from_stage: preprocess, signal: features }
    outputs:
      model: { type: directory }
```

**Parser validation** (`pipeline/parser.py`):
- Stage names are unique
- Signal types match between producer/consumer
- No cycles in DAG
- Referenced datasets exist

### 4. Stages = Execution Units

Each stage is a Python module in `modules/{stage}.py`:

```python
# modules/preprocess.py
from goldfish.io import load_input, save_output

def main():
    raw_data = load_input("raw_data")  # Load from /mnt/inputs/
    features = process(raw_data)
    save_output("features", features)   # Save to /mnt/outputs/

if __name__ == "__main__":
    main()
```

**Execution flow** (`jobs/stage_executor.py`):
1. Auto-version workspace
2. Load pipeline, validate stage
3. Resolve inputs (datasets or upstream outputs)
4. Build Docker image
5. Launch container with mounted volumes
6. Track status, capture logs
7. Register output signals

### 5. Signals = Data Flow

Signals are typed data passed between stages:

| Type | Description | Storage |
|------|-------------|---------|
| `dataset` | External data source | GCS or local |
| `npy` | NumPy array | `.npy` file |
| `csv` | Tabular data | `.csv` file |
| `directory` | Directory of files | Directory |
| `file` | Single file | File |

**Lineage tracking** (`signal_lineage` table):
```sql
stage_run_id    | signal_name | signal_type | storage_location    | is_artifact
stage-abc123    | features    | npy         | /outputs/features.npy | false
stage-abc123    | model       | directory   | /outputs/model/       | true
```

### 6. Resource Profiles = Compute Abstraction

Profiles abstract GCE machine configuration:

```python
# infra/profiles.py
BUILTIN_PROFILES = {
    "cpu-small": {
        "machine_type": "n2-standard-4",
        "gpu": {"type": "none", "count": 0},
        "zones": ["us-central1-a", "us-central1-b"],
    },
    "h100-spot": {
        "machine_type": "a3-highgpu-1g",
        "gpu": {"type": "nvidia-h100-80gb", "count": 1},
        "preemptible": True,
        "zones": ["us-central1-a", "us-west4-a"],
    },
}
```

Claude just writes `profile: "h100-spot"` in configs; Goldfish handles the rest.

### 7. Pre-Run Review = Automatic Code Review

**Location**: `pre_run_review.py` (497 lines)

Before executing any stage, Goldfish reviews the code using Claude Agent SDK:

```python
# Integration in jobs/stage_executor.py:1240-1275
def run_stage(self, workspace, stage_name, ...):
    # After sync+version, before Docker build
    review = self._perform_pre_run_review(...)
    if not review.approved:
        self._create_blocked_stage_run(...)  # Create FAILED stage run
        return blocked_info

    # Proceed with execution...
```

**Review context includes:**
- `pipeline.yaml` - Workflow structure
- `modules/{stage}.py` - Stage implementation
- `configs/{stage}.yaml` - Configuration
- Git diff from last successful run
- RunReason (hypothesis, approach, goals)

**Security measures** (`pre_run_review.py:270-310`):
- Path traversal protection with symlink detection
- File size limits (100KB/file, 500KB total context)
- Safe filename validation (rejects `../`, `.hidden`)
- API timeout with `asyncio.wait_for()`
- Fails open on error (approves to avoid blocking)

**Async/sync bridging** (`stage_executor.py:1276-1310`):
```python
def _run_async_review(self, coro: Coroutine[Any, Any, RunReview]):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)  # No loop - use asyncio.run()

    # Already in async context - use ThreadPoolExecutor
    # Safe because PreRunReviewer.review() is self-contained
```

**RunReason model** (`models.py:574-619`):
- Structured experiment hypothesis tracking
- All fields have max_length constraints (DoS protection)
- Stored as JSON in `stage_runs.reason_json` column

**Testing**:
- 37 unit tests (security, parsing, timeout handling)
- 14 integration tests (real files, executor integration)
- Test coverage: 95%+

---

## Data Flow

### Stage Execution Sequence

```
run("w1", stages=["train"])
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. VALIDATION                                                    │
│    - Validate workspace mounted to slot                         │
│    - Load and validate pipeline.yaml                            │
│    - Check stage exists in pipeline                             │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. SYNC + AUTO-VERSION (Provenance Guard)                       │
│    - Sync slot files back to dev repo branch                    │
│    - Commit changes in dev repo                                 │
│    - Push to remote (for GCE execution)                         │
│    - Create git tag: baseline_lstm-v4                           │
│    - Record in workspace_versions table                         │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. INPUT RESOLUTION                                              │
│    - For each input in stage definition:                        │
│      - dataset: lookup in dataset_registry                      │
│      - from_stage: query signal_lineage for latest output       │
│    - Prepare input manifest for container                       │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. DOCKER BUILD                                                  │
│    - Generate Dockerfile from workspace                         │
│    - COPY modules/, configs/, requirements.txt                  │
│    - Install goldfish.io library                                │
│    - Build image: goldfish-baseline_lstm-v4                     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. CONTAINER LAUNCH                                              │
│    backend=local:                                               │
│      docker run -v inputs:/mnt/inputs:ro                       │
│                 -v outputs:/mnt/outputs                         │
│                 --memory 4g --cpus 2.0                          │
│                 goldfish-baseline_lstm-v4                       │
│                 python -m modules.train                         │
│                                                                  │
│    backend=gce:                                                 │
│      GCELauncher.launch_instance()                              │
│      - Resolve profile to machine type                          │
│      - Multi-zone capacity search                               │
│      - Create hyperdisk, attach GPU                             │
│      - Upload startup script                                    │
│      - Sync inputs from GCS                                     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. MONITORING                                                    │
│    - Poll container/instance status                             │
│    - Stream logs to stage_runs.log_uri                          │
│    - Update progress: build → launch → running → finalizing     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 7. FINALIZATION                                                  │
│    - Parse output manifest from container                       │
│    - Register signals in signal_lineage                         │
│    - Update stage_runs: status=completed, outputs_json          │
│    - Regenerate STATE.md                                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Database Schema

### Tables Overview

```sql
-- Core tracking
audit              -- All operations with timestamps and reasons
workspace_lineage  -- Workspace creation and branching history
workspace_versions -- Version records (git tags)
stage_runs         -- Stage execution records
pipeline_runs      -- Pipeline invocation grouping
signal_lineage     -- Data flow between stages

-- Data management
sources            -- Legacy data sources
source_lineage     -- Source derivation tracking
datasets           -- Project-level datasets

-- Execution
jobs               -- Legacy job records
job_inputs         -- Job-to-source mapping
pipeline_stage_queue -- Async pipeline queue

-- Metadata
workspace_goals    -- Workspace objective tracking
```

### Key Table: `stage_runs`

```sql
CREATE TABLE stage_runs (
    id TEXT PRIMARY KEY,              -- "stage-abc123"
    job_id TEXT,                      -- Legacy grouping
    pipeline_run_id TEXT,             -- Pipeline grouping
    workspace_name TEXT NOT NULL,
    pipeline_name TEXT,               -- Named pipeline file
    version TEXT NOT NULL,            -- "v4"
    stage_name TEXT NOT NULL,         -- "train"
    status TEXT NOT NULL,             -- pending|running|completed|failed|cancelled
    started_at TEXT NOT NULL,
    completed_at TEXT,
    log_uri TEXT,                     -- Path to log file
    artifact_uri TEXT,
    progress TEXT,                    -- build|launch|running|finalizing
    profile TEXT,                     -- Resolved profile name
    hints_json TEXT,                  -- JSON execution hints
    outputs_json TEXT,                -- JSON output manifest
    config_json TEXT,                 -- Effective configuration
    inputs_json TEXT,                 -- Resolved inputs
    backend_type TEXT,                -- local|gce
    backend_handle TEXT,              -- Container/instance ID
    error TEXT                        -- Error message if failed
);
```

### Key Table: `signal_lineage`

```sql
CREATE TABLE signal_lineage (
    id INTEGER PRIMARY KEY,
    stage_run_id TEXT NOT NULL,       -- Producing stage run
    signal_name TEXT NOT NULL,        -- "features"
    signal_type TEXT NOT NULL,        -- "npy"
    storage_location TEXT,            -- "/outputs/features.npy"
    is_artifact INTEGER DEFAULT 0,    -- Permanent storage flag
    consumed_by_run_id TEXT,          -- Consuming stage run
    created_at TEXT NOT NULL,
    UNIQUE(stage_run_id, signal_name)
);
```

---

## Security Model

### Layer 1: Input Validation

All user inputs are validated before any operation:

```python
# validation.py
WORKSPACE_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")
SNAPSHOT_PATTERN = re.compile(r"^snap-[a-f0-9]{8}-\d{8}-\d{6}$")

def validate_workspace_name(name: str) -> None:
    if not WORKSPACE_PATTERN.match(name):
        raise ValidationError(f"Invalid workspace name: {name}")
```

### Layer 2: Path Traversal Protection

```python
# validation.py
def validate_path_within_root(path: Path, root: Path) -> None:
    """Prevent ../../../etc/passwd attacks"""
    resolved = path.resolve()
    root_resolved = root.resolve()
    if not resolved.is_relative_to(root_resolved):
        raise ValidationError("Path traversal detected")
```

```python
# jobs/tracker.py - TOCTOU prevention
def _read_log_safely(path: Path) -> str:
    # Check BEFORE opening
    if path.is_symlink():
        raise InvalidLogPathError("Symlink detected")
    # Use O_NOFOLLOW to prevent race conditions
    with open(path, opener=_safe_opener) as f:
        return f.read()
```

### Layer 3: Docker Sandboxing

```python
# infra/local_executor.py
docker_cmd.extend([
    "--memory", "4g",           # Memory limit
    "--cpus", "2.0",            # CPU limit
    "--pids-limit", "100",      # Process limit
    "--user", "1000:1000",      # Non-root execution
    "--read-only",              # Read-only root filesystem
    "--network", "none",        # No network (unless required)
])
```

### Layer 4: Git Error Translation

Git internals are hidden from Claude:

```python
# errors.py
def translate_git_error(git_error: str) -> GoldfishError:
    if "not a valid object name" in git_error:
        return WorkspaceNotFoundError("Workspace not found")
    if "already exists" in git_error:
        return WorkspaceAlreadyExistsError("Workspace already exists")
    # ... more translations
```

---

## Testing

### Test Categories

| Category | Location | Duration | What's Tested |
|----------|----------|----------|---------------|
| **Unit** | `tests/unit/` | <1s | Pure logic, mocked deps |
| **Integration** | `tests/integration/` | ~2 min | Real SQLite, real git |
| **E2E** | `tests/e2e/` | ~5 min | Full system with Docker |
| **Deluxe** | `tests/e2e/deluxe/` | ~30 min | Real GCE (opt-in) |

### Running Tests

```bash
make test              # Unit tests only (fast)
make test-unit         # Unit with coverage
make test-integration  # Integration tests
make ci                # Full CI suite
pytest tests/ -v       # All tests verbose
pytest tests/unit/test_validation.py -k "traversal"  # Specific test
```

### Writing Tests

```python
# tests/integration/test_workspace.py
def test_create_workspace(test_db, test_config, temp_git_repo):
    """Workspace creation should create branch and record in DB."""
    manager = WorkspaceManager(
        db=test_db,
        config=test_config,
        project_root=temp_git_repo,
    )

    result = manager.create_workspace("my_exp", "Test goal")

    assert result.name == "my_exp"
    assert result.branch == "experiment/my_exp"

    # Verify database state
    with test_db._conn() as conn:
        row = conn.execute(
            "SELECT * FROM workspace_lineage WHERE workspace_name = ?",
            ("my_exp",)
        ).fetchone()
        assert row is not None
```

### Test Fixtures

```python
# tests/conftest.py
@pytest.fixture
def test_db(tmp_path):
    """Fresh database with schema"""
    db = Database(tmp_path / "test.db")
    db.initialize()
    return db

@pytest.fixture
def temp_git_repo(tmp_path):
    """Initialized git repository"""
    subprocess.run(["git", "init"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "--allow-empty", "-m", "init"],
                   cwd=tmp_path, check=True)
    subprocess.run(["git", "branch", "-M", "main"], cwd=tmp_path, check=True)
    return tmp_path
```

---

## Code Style

### Enforced by Pre-commit

- **Ruff** — Linting and formatting (replaces black, isort, flake8)
- **mypy** — Type checking with strict mode
- **gitleaks** — Secret detection

### Conventions

1. **Type Hints Required**: All function signatures must have type hints
   ```python
   def create_workspace(name: str, goal: str) -> WorkspaceInfo:
   ```

2. **Docstrings for Public APIs**: Use Google style
   ```python
   def mount(self, workspace: str, slot: str) -> SlotInfo:
       """Mount a workspace to a slot.

       Args:
           workspace: Name of the workspace to mount
           slot: Slot identifier (w1, w2, w3)

       Returns:
           SlotInfo with mount details

       Raises:
           WorkspaceNotFoundError: If workspace doesn't exist
       """
   ```

3. **Error Handling**: Use specific exception types
   ```python
   # Good
   raise WorkspaceNotFoundError(f"Workspace '{name}' not found")

   # Bad
   raise Exception("Not found")
   ```

4. **Database Access**: Always use context manager
   ```python
   with self.db._conn() as conn:
       conn.execute("INSERT INTO ...")
   ```

5. **No `# type: ignore`**: Fix the type issue instead

---

## Pull Request Process

### Before You Start

1. Check existing issues and PRs
2. For large changes, open an issue first to discuss

### Development Workflow

```bash
# Create feature branch
git checkout -b feature/my-feature

# Make changes, test locally
make ci  # Must pass

# Commit (hooks run automatically)
git add .
git commit -m "Add feature X"

# Push and create PR
git push -u origin feature/my-feature
gh pr create
```

### PR Requirements

- [ ] All CI checks pass
- [ ] Tests added for new functionality
- [ ] Documentation updated if needed
- [ ] No `# type: ignore` without justification
- [ ] Audit logging for new operations
- [ ] Security considerations documented

### Commit Message Format

```
<type>: <description>

<optional body>

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: <name> <email>
```

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`

---

## Configuration Reference

### goldfish.yaml

```yaml
# Project identification
project_name: string              # Required: Project identifier

# Repository layout (copy-based architecture)
dev_repo_path: ../myproject-dev   # Goldfish dev repo (sibling directory)
workspaces_dir: workspaces        # Slot directory in USER project
slots: [w1, w2, w3]               # Available slot names

# Execution policy
execution:
  push_before_run: true           # Push commits for GCE execution

# State management
state_md:
  global_path: STATE.md           # Global overview in dev repo
  per_workspace_enabled: true     # Enable per-workspace STATE.md
  per_workspace_path: STATE.md    # Filename in each workspace
  max_recent_actions: 50          # Actions to keep in narrative

# Audit configuration
audit:
  min_reason_length: 15           # Minimum characters for reason field

# Job execution
jobs:
  backend: local                  # "local" or "gce"
  experiments_dir: experiments    # Export directory
  infra_path: infra               # Infrastructure scripts
  timeout: 86400                  # Max job duration (seconds)

# Google Cloud Storage (optional)
gcs:
  bucket: gs://bucket-name        # GCS bucket for artifacts
  artifacts_prefix: artifacts     # Prefix for artifact storage
  datasets_prefix: datasets       # Prefix for dataset storage

# Google Compute Engine (optional)
gce:
  project_id: string              # GCP project ID
  zones:                          # Preferred zones (ordered)
    - us-central1-a
    - us-central1-b
  artifact_registry: string       # Docker registry URL
  service_account: string         # GCE service account

  # Profile overrides
  profile_overrides:
    h100-spot:
      zones: [us-west1-a]         # Override zones for this profile

# Configuration invariants (shown in STATE.md)
invariants:
  - "Never modify production data"
  - "Always run tests before merging"
```

### Stage Config (configs/{stage}.yaml)

```yaml
# Compute profile
compute:
  profile: "h100-spot"            # Built-in or custom profile
  # OR explicit override:
  machine_type: "n2-standard-8"
  gpu_type: "nvidia-tesla-t4"
  gpu_count: 1

# Environment variables
env:
  EPOCHS: "100"
  LEARNING_RATE: "0.001"
  BATCH_SIZE: "32"

# Execution hints
hints:
  spot_ok: true                   # Allow preemptible instances
  priority: "high"                # Scheduling priority
  timeout: 3600                   # Stage timeout (seconds)
```

---

## Getting Help

- **Issues**: [GitHub Issues](https://github.com/lukacf/goldfish/issues)
- **Discussions**: [GitHub Discussions](https://github.com/lukacf/goldfish/discussions)

---

Thank you for contributing to Goldfish!
