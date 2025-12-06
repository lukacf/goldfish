# Goldfish

**Goldfish** is an MCP (Model Context Protocol) server for pipeline-based ML experimentation. It enables Claude Code to autonomously conduct ML research by managing code workspaces, executing pipeline stages, tracking data lineage, and maintaining full experiment provenance—with built-in context recovery and comprehensive security.

## What is Goldfish?

Goldfish solves the key challenge of ML development with AI assistants: **maintaining state and provenance across long experiments**. When working with Claude, you need a system that remembers workspace state, tracks data lineage, and can recover context after conversation summarization.

Goldfish provides:

- **Pipeline-Based Workflows**: Define ML workflows as YAML pipelines with automatic stage chaining
- **Workspace Management**: Isolated git branches for parallel experimentation with version control
- **Automated Versioning**: Every pipeline run creates an immutable snapshot with full provenance
- **Data Lineage**: Track complete provenance from raw data → processing → models → artifacts
- **Infrastructure Abstraction**: Runs locally or on GCE with Docker containerization
- **Context Recovery**: Auto-generated STATE.md for resuming work after summarization

## Architecture Overview

### Conceptual Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    Claude Code (MCP Client)                  │
│  "Add attention mechanism to transformer"                    │
│  "Run tokenize stage with vocab_size=20000"                 │
└────────────────────────────┬────────────────────────────────┘
                             │ MCP Protocol (41 tools)
┌────────────────────────────▼────────────────────────────────┐
│                   Goldfish MCP Server                        │
│  Tools: run_stage(), update_pipeline(), get_lineage()      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │  Workspaces  │  │   Pipelines  │  │   Modules    │     │
│  │ (git branch) │  │ (YAML flow)  │  │ (ML code)    │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Versions   │  │    Signals   │  │   Datasets   │     │
│  │  (git tags)  │  │ (data flow)  │  │ (sources)    │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           Job Execution Engine                      │   │
│  │  - Stage running (partial/full pipeline)           │   │
│  │  - Docker containerization (hidden from Claude)    │   │
│  │  - Storage abstraction (GCS/hyperdisk/local)       │   │
│  │  - Signal chaining (stage outputs → inputs)        │   │
│  │  - Lineage tracking (full provenance)              │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           Infrastructure Layer                       │   │
│  │  - Docker builder (secure image generation)        │   │
│  │  - Local executor (development mode)               │   │
│  │  - GCE launcher (production with GPU support)      │   │
│  │  - Resource launcher (capacity-aware multi-zone)   │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│  SQLite Database (jobs, sources, lineage, audit)            │
├─────────────────────────────────────────────────────────────┤
│  Git Repository (.goldfish/dev/ - branches + tags)          │
└─────────────────────────────────────────────────────────────┘
```

## How It Works

### Core Concepts

1. **Pipelines** - YAML workflow definitions:
   - Sequence of stages (preprocess → tokenize → train)
   - Signal wiring (stage outputs → next stage inputs)
   - Claude edits directly, Goldfish validates

2. **Modules** - Python scripts implementing stages:
   - One module per stage (e.g., `modules/tokenize.py`)
   - Uses Goldfish IO library for storage abstraction
   - No infrastructure concerns - just ML logic

3. **Workspaces** - Development environments:
   - Git branch containing: pipeline + modules + configs
   - Mounted to slots (w1, w2, w3) for editing
   - Auto-versioned on every run

4. **Datasets** - Project-level data sources:
   - Registered once, shared across workspaces
   - Goldfish manages upload to GCS/hyperdisk
   - Immutable or versioned

5. **Runs/Jobs** - Pipeline stage execution:
   - Run single stage or full pipeline
   - Full provenance tracked via lineage
   - Docker/GCE completely hidden from Claude

### Workspace Structure

```
workspace/
├── pipeline.yaml              # Pipeline definition (Claude edits)
├── configs/
│   ├── CLAUDE.md             # Config documentation
│   ├── preprocess.yaml       # Stage config
│   ├── tokenize.yaml
│   └── train.yaml
├── modules/
│   ├── CLAUDE.md             # Module documentation
│   ├── preprocess.py         # Stage implementation
│   ├── tokenize.py
│   └── train.py
└── STATE.md                   # Workspace state + lineage
```

### Workflow Example

```
1. Register dataset
   └─> register_dataset("eurusd_raw_v3", "local:/data/eurusd.csv")

2. Create workspace
   └─> create_workspace("baseline_lstm", goal="Train baseline LSTM model")

3. Mount to slot
   └─> mount("baseline_lstm", "w1")

4. Define pipeline
   └─> Claude edits w1/pipeline.yaml:
       stages:
         - name: preprocess
           inputs: {raw_data: {type: dataset, dataset: eurusd_raw_v3}}
         - name: tokenize
           inputs: {features: {from: preprocess.features}}
         - name: train
           inputs: {tokens: {from: tokenize.tokens}}

5. Implement modules
   └─> Claude writes w1/modules/preprocess.py, tokenize.py, train.py

6. Run single stage (partial pipeline)
   └─> run_stage("w1", "tokenize", config_override={"VOCAB_SIZE": "20000"})
   └─> Auto-creates version: baseline_lstm-v1
   └─> Launches Docker container with stage code
   └─> Tracks input/output signals

7. Run full pipeline
   └─> run_pipeline("w1", reason="Full baseline training")
   └─> Auto-creates version: baseline_lstm-v2
   └─> Runs all stages in sequence
   └─> Records complete lineage

8. Promote artifact
   └─> promote_artifact(job_id="job-123", output_name="trained_model",
                        source_name="lstm_baseline_v1")
   └─> Registers as reusable dataset

9. Branch workspace
   └─> branch_workspace("baseline_lstm", "v2", "baseline_lstm_attention")
   └─> Experiment from known-good version
```

## Installation

### Prerequisites

- Python 3.11+
- Git
- Docker (for job execution)
- Claude Code (for MCP integration)
- Optional: GCS access (for cloud artifacts)

### Install Goldfish

```bash
# Clone the repository
git clone <goldfish-repo-url>
cd goldfish

# Install in development mode
pip install -e .
```

## Adding to Claude Code

To use Goldfish with Claude Code, add it as an MCP server:

### 1. Find Your Claude Code MCP Config

The config file is located at:
- **macOS/Linux**: `~/.config/claude-code/mcp_config.json`
- **Windows**: `%APPDATA%\claude-code\mcp_config.json`

### 2. Add Goldfish Server

Edit `mcp_config.json` to add Goldfish:

```json
{
  "mcpServers": {
    "goldfish": {
      "command": "python",
      "args": [
        "-m",
        "goldfish",
        "serve",
        "--project",
        "/path/to/your/ml-project"
      ]
    }
  }
}
```

Replace `/path/to/your/ml-project` with the absolute path to your project directory.

### 3. Restart Claude Code

Restart Claude Code to load the MCP server. Goldfish tools will now be available.

## Available Tools (41 Total)

Claude Code has access to these Goldfish tools:

### Workspace Management (15 tools)
- `create_workspace()` - Create new workspace from main
- `list_workspaces()` - List all workspaces
- `get_workspace()` - Get workspace details
- `mount()` - Mount workspace to slot
- `hibernate()` - Save and unmount workspace
- `checkpoint()` - Create snapshot manually
- `rollback()` - Rollback to previous snapshot
- `list_snapshots()` - List workspace snapshots with pagination
- `get_snapshot()` - Get snapshot details
- `diff()` - Show uncommitted changes
- `delete_workspace()` - Delete workspace and snapshots
- `delete_snapshot()` - Delete specific snapshot
- `get_workspace_goal()` - Get workspace goal
- `update_workspace_goal()` - Update workspace goal
- `branch_workspace()` - Create workspace from specific version

### Pipeline & Execution (8 tools)
- `get_pipeline()` - Get pipeline definition
- `validate_pipeline()` - Validate pipeline YAML
- `update_pipeline()` - Update pipeline definition
- `run_stage()` - Run single pipeline stage
- `run_pipeline()` - Run full pipeline
- `run_partial_pipeline()` - Run stages from X to Y
- `run_job()` - Launch job (legacy - use run_stage)
- `job_status()` - Get job status
- `get_job_logs()` - Retrieve job logs
- `cancel_job()` - Cancel running job
- `list_jobs()` - List jobs with filters
- `delete_job()` - Delete completed job

### Data Source Management (9 tools)
- `register_dataset()` - Register project-level dataset
- `list_datasets()` - List all datasets
- `get_dataset()` - Get dataset details
- `register_source()` - Register external data source (legacy)
- `list_sources()` - List data sources with pagination
- `get_source()` - Get source details
- `promote_artifact()` - Promote job output to dataset
- `get_source_lineage()` - Get source provenance
- `delete_source()` - Delete data source

### Lineage Tracking (3 tools)
- `get_workspace_lineage()` - Get workspace evolution history
- `get_version_diff()` - Compare two versions
- `get_run_provenance()` - Get exact provenance of a stage run

### Utilities (3 tools)
- `status()` - Get overall project status + STATE.md
- `get_audit_log()` - Get recent operations
- `log_thought()` - Record reasoning for audit trail

## Configuration

### goldfish.yaml Options

```yaml
project_name: string              # Project identifier

dev_repo_path: .goldfish/dev      # Git repository path
workspaces_dir: workspaces        # Workspace files location
slots: [w1, w2, w3]               # Workspace slot names

state_md:
  path: STATE.md                  # Context recovery file
  max_recent_actions: 15          # Actions to show
  show_lineage: true              # Display lineage chains

audit:
  min_reason_length: 15           # Require descriptive reasons

jobs:
  backend: local                  # local or gce
  experiments_dir: experiments    # Export directory
  infra_path: infra               # Infrastructure scripts

gcs:                              # Optional GCS config
  bucket: string                  # GCS bucket name
  artifacts_prefix: string        # Artifact path prefix
  datasets_prefix: string         # Dataset path prefix

gce:                              # Optional GCE config
  project_id: string              # GCP project
  zones: [us-central1-a, ...]     # Preferred zones
  machine_types:                  # Machine type preferences
    cpu_only: [n2-standard-4, ...]
    gpu: [n1-standard-8, ...]
  gpu_types: [nvidia-tesla-t4]    # GPU types
  boot_disk_size_gb: 200          # Boot disk size
  timeout_minutes: 1440           # Max runtime (24 hours)

invariants:                       # Configuration rules
  - string                        # Things that must not change
```

## Security Features

Goldfish has comprehensive security measures:

### Path Validation
- ✅ Rejects path traversal attempts (`../../../etc/passwd`)
- ✅ Validates all file paths before operations
- ✅ Symlink detection and rejection (TOCTOU prevention)
- ✅ Script path validation (no absolute paths)

### Command Injection Prevention
- ✅ Workspace name validation (no shell metacharacters)
- ✅ Snapshot ID validation (prevents git ref injection)
- ✅ Output name validation (prevents directory traversal)
- ✅ GCE startup script escaping with `shlex.quote()`

### Resource Limits
- ✅ File size limits (prevents DoS via memory exhaustion)
- ✅ Docker resource limits (memory, CPU, PIDs)
- ✅ Job timeouts (prevents runaway processes)
- ✅ Database transaction limits

### Concurrency Safety
- ✅ Slot-level locking (prevents workspace corruption)
- ✅ Database transactions (atomic operations)
- ✅ Git index locking (prevents concurrent git operations)
- ✅ Thread-safe database operations


## Development

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_workspace_manager.py

# Run with coverage
pytest --cov=goldfish tests/

# Run security tests only
pytest tests/test_security*.py tests/test_tracker_security.py tests/test_exporter_security.py
```

### Project Structure

```
goldfish/
├── src/goldfish/
│   ├── server.py              # MCP server entry point (271 lines)
│   ├── server_tools/          # Tool modules (organized by function)
│   │   ├── workspace_tools.py  # 15 workspace tools
│   │   ├── execution_tools.py  # 8 execution tools
│   │   ├── data_tools.py       # 9 data tools
│   │   ├── pipeline_tools.py   # 3 pipeline tools
│   │   ├── lineage_tools.py    # 3 lineage tools
│   │   └── utility_tools.py    # 3 utility tools
│   ├── config.py              # Configuration management
│   ├── context.py             # Server context management
│   ├── models.py              # Pydantic models
│   ├── validation.py          # Input validation
│   ├── errors.py              # Error classes
│   ├── db/
│   │   ├── database.py        # SQLite operations
│   │   └── schema.sql         # Database schema
│   ├── workspace/
│   │   ├── manager.py         # High-level workspace ops
│   │   └── git_layer.py       # Git operations
│   ├── jobs/
│   │   ├── launcher.py        # Job launching
│   │   ├── tracker.py         # Status tracking
│   │   ├── exporter.py        # Experiment export
│   │   ├── stage_executor.py  # Stage execution
│   │   └── pipeline_executor.py # Pipeline execution
│   ├── pipeline/
│   │   ├── parser.py          # Pipeline YAML parsing
│   │   └── manager.py         # Pipeline management
│   ├── datasets/
│   │   └── registry.py        # Dataset management
│   ├── lineage/
│   │   └── manager.py         # Lineage tracking
│   ├── infra/
│   │   ├── docker_builder.py  # Docker image building
│   │   ├── local_executor.py  # Local execution
│   │   ├── gce_launcher.py    # GCE instance management
│   │   ├── resource_launcher.py # Capacity-aware launcher
│   │   └── startup_builder.py  # GCE startup scripts
│   ├── io/
│   │   └── __init__.py        # Goldfish IO library
│   ├── state/
│   │   └── state_md.py        # STATE.md generation
│   └── cli.py                 # Command-line interface
├── tests/                     # 556 tests (all passing)
│   ├── test_security*.py      # Security tests
│   ├── test_concurrent.py     # Concurrency tests
│   ├── test_e2e_workflows.py  # Integration tests
│   ├── test_gcs_failures.py   # GCS failure handling
│   └── ...
└── README.md                  # This file
```

## Design Principles

1. **Pipeline-First Architecture** - Workflows are first-class citizens
2. **Conversation Resilience** - STATE.md enables context recovery
3. **Audit Everything** - All operations logged for reproducibility
4. **Atomic Operations** - Transactions prevent partial state
5. **Security First** - Path validation, injection prevention, resource limits
6. **Git as Source of Truth** - Workspaces = branches, versions = tags
7. **Infrastructure Abstraction** - Claude sees ML logic, not Docker/GCS
8. **Full Provenance** - Track data from raw sources to final artifacts

## Troubleshooting

### MCP Server Not Loading

Check Claude Code logs:
- macOS/Linux: `~/.config/claude-code/logs/`
- Windows: `%APPDATA%\claude-code\logs\`

Common issues:
- Wrong Python path in `mcp_config.json`
- Project directory doesn't exist
- Goldfish not installed (`pip install -e .`)

### Git Errors

```
Error: fatal: not a git repository
```

Solution: Initialize project first:
```bash
python -m goldfish init myproject
```

### Docker Errors

```
Error: Docker daemon not running
```

Solution: Start Docker Desktop or Docker daemon

### GCE Permission Errors

```
Error: 403 Forbidden - Insufficient permissions
```

Solution: Authenticate with GCP:
```bash
gcloud auth application-default login
```

## License

MIT License - See LICENSE file for details

## Contributing

Contributions welcome! Please:
1. Write tests for new features
2. Follow existing code style
3. Update documentation
4. Add audit logging for new operations
5. Include security considerations

## Acknowledgments

Built with:
- [FastMCP](https://github.com/jlowin/fastmcp) - MCP server framework
- [GitPython](https://github.com/gitpython-developers/GitPython) - Git operations
- SQLite - Embedded database
- Docker - Containerization
- Google Cloud Platform - Infrastructure

