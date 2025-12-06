# Goldfish

**Goldfish** is an MCP (Model Context Protocol) server for managing ML experimentation workflows. It provides Claude Code with tools to manage code workspaces, launch training jobs, track data lineage, and maintain experiment history—all with built-in context recovery after conversation summarization.

## What is Goldfish?

Goldfish solves a key problem in ML development with AI assistants: **context loss during long experiments**. When Claude's conversation gets summarized, it can forget where experiments are, what's running, and which data sources exist.

Goldfish provides persistent state management through:

- **Workspaces**: Isolated git branches for parallel experimentation
- **Snapshots**: Automatic checkpointing of code state before job launches
- **Jobs**: Background training runs with lineage tracking
- **Sources**: Data source registry with provenance
- **STATE.md**: Auto-generated context recovery document

## How It Works

### Core Concepts

1. **Workspaces** - Each workspace is a git branch in your dev repository:
   - `w1`, `w2`, `w3` - Three slots for mounting workspaces
   - Isolated environments for parallel experiments
   - Mount/hibernate to switch between experiments

2. **Jobs** - Background training runs:
   - Snapshots code state before launch
   - Exports to isolated experiment directory
   - Tracks input sources and output artifacts
   - Records lineage for reproducibility

3. **Sources** - Data source registry:
   - External data (uploaded/provided)
   - Promoted artifacts (from job outputs)
   - Full lineage tracking (which jobs created which sources)

4. **STATE.md** - Context recovery:
   - Auto-generated workspace summary
   - Recent actions log
   - Active jobs status
   - Regenerated after every operation

### Workflow Example

```
1. Initialize project
   └─> Creates .goldfish/ directory and dev repo

2. Create workspace
   └─> Creates git branch for experiment

3. Mount to slot
   └─> Checks out branch to w1/ directory

4. Edit code, checkpoint
   └─> Creates tagged snapshot

5. Run job
   └─> Snapshots → exports → launches
   └─> Records in database

6. Job completes
   └─> Artifacts written to GCS

7. Promote artifact
   └─> Registers as reusable source
   └─> Records lineage

8. Use source in next job
   └─> Full provenance tracked
```

## Installation

### Prerequisites

- Python 3.11+
- Git
- Claude Code (for MCP integration)

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

## Quick Start

### 1. Initialize a Project

```bash
python -m goldfish init myproject
cd myproject
```

This creates:
```
myproject/
├── .goldfish/
│   ├── goldfish.yaml      # Configuration
│   ├── goldfish.db        # SQLite database
│   └── dev/               # Git repository
├── w1/                    # Workspace slot 1
├── w2/                    # Workspace slot 2
├── w3/                    # Workspace slot 3
├── experiments/           # Job export directory
└── STATE.md               # Context recovery file
```

### 2. Configure Your Project

Edit `.goldfish/goldfish.yaml`:

```yaml
project_name: myproject
dev_repo_path: .goldfish/dev
workspaces_dir: workspaces

# Optional: Configure GCS for artifacts
gcs:
  bucket: my-ml-experiments
  artifacts_prefix: goldfish-artifacts

# Optional: Add configuration invariants
invariants:
  - "Always use Python 3.11+"
  - "Training data is in gs://my-data/"
```

### 3. Create Your First Workspace

Ask Claude Code:

```
Create a workspace called "baseline" with the goal "Train baseline LSTM model"
```

Claude will use Goldfish tools to:
1. Create a git branch: `baseline`
2. Initialize workspace structure
3. Update STATE.md

### 4. Mount and Work

```
Mount the baseline workspace to w1
```

Now `w1/` contains your workspace. Edit code as needed:

```
Add a training script to w1/train.py
```

### 5. Checkpoint and Launch

```
Checkpoint the workspace with message "Added LSTM training script"
Run a job in w1 using train.py with reason "Testing baseline LSTM"
```

Goldfish will:
1. Create a snapshot (git tag)
2. Export to `experiments/exp-<id>/`
3. Launch the job (if infra configured)
4. Track in database

### 6. Check Status

```
What jobs are running?
Show me recent audit logs
```

Claude can query job status and history using Goldfish tools.

## Available Tools

Claude Code has access to these Goldfish tools:

### Workspace Management
- `create_workspace()` - Create new workspace
- `list_workspaces()` - List all workspaces
- `mount()` - Mount workspace to slot
- `hibernate()` - Unmount workspace from slot
- `checkpoint()` - Create snapshot
- `rollback()` - Rollback to previous snapshot
- `list_snapshots()` - List workspace snapshots
- `diff()` - Show uncommitted changes
- `delete_workspace()` - Delete workspace and snapshots

### Job Management
- `run_job()` - Launch training job
- `get_job()` - Get job status
- `list_jobs()` - List jobs with filters
- `get_job_logs()` - Retrieve job logs
- `cancel_job()` - Cancel running job
- `delete_job()` - Delete completed job

### Data Source Management
- `register_source()` - Register external data source
- `list_sources()` - List data sources
- `get_source()` - Get source details
- `promote_artifact()` - Promote job output to source
- `get_source_lineage()` - Get source provenance
- `delete_source()` - Delete data source

### Utilities
- `get_status()` - Get overall project status
- `get_recent_audit()` - Get recent operations
- `set_goal()` - Update active goal

## Architecture

### Components

```
┌─────────────────────────────────────────┐
│           Claude Code                   │
│  (MCP Client)                           │
└─────────────────┬───────────────────────┘
                  │ MCP Protocol
┌─────────────────▼───────────────────────┐
│       Goldfish MCP Server               │
│  src/goldfish/server.py                 │
├─────────────────────────────────────────┤
│  Workspace Manager  │  Job Launcher     │
│  (git operations)   │  (experiment mgmt)│
├─────────────────────┼───────────────────┤
│  Job Tracker        │  Source Registry  │
│  (status/logs)      │  (data lineage)   │
├─────────────────────┴───────────────────┤
│         SQLite Database                 │
│  (jobs, sources, lineage, audit)        │
├─────────────────────────────────────────┤
│           Git Layer                     │
│  (branches, tags, worktrees)            │
└─────────────────────────────────────────┘
```

### Database Schema

- **jobs** - Job records (status, URIs, metadata)
- **job_inputs** - Job → source mappings
- **sources** - Data source registry
- **source_lineage** - Source → parent mappings
- **workspace_goals** - Workspace objectives
- **audit** - Operation audit log

### Security Features

- Path validation (no traversal attacks)
- Symlink protection (O_NOFOLLOW)
- File size limits (prevent DoS)
- Atomic transactions (no partial state)
- Lock-based concurrency control

## Configuration

### goldfish.yaml Options

```yaml
project_name: string              # Project identifier

dev_repo_path: string             # Path to git repository
workspaces_dir: string            # Where to store workspace files
slots: [w1, w2, w3]               # Workspace slot names

state_md:
  path: STATE.md                  # Context recovery file
  max_recent_actions: 15          # Actions to show

audit:
  min_reason_length: 15           # Require descriptive reasons

jobs:
  backend: local                  # local or gce
  experiments_dir: experiments    # Export directory
  infra_path: infra               # Infrastructure scripts

gcs:                              # Optional GCS config
  bucket: string                  # GCS bucket name
  artifacts_prefix: string        # Artifact path prefix

invariants:                       # Configuration rules
  - string                        # Things that must not change
```

## Infrastructure Integration

### Job Launch Flow

By default, jobs are recorded but not executed. To actually run jobs:

1. Create `infra/create_run.py`:

```python
#!/usr/bin/env python
"""Launch job to compute infrastructure."""
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--experiment', required=True)
    parser.add_argument('--script', required=True)
    parser.add_argument('--job-id', required=True)
    args = parser.parse_args()

    # Your infrastructure logic:
    # - Build Docker image from experiment dir
    # - Launch GCE instance / Kubernetes job
    # - Stream logs to GCS
    # - Write completion markers

if __name__ == '__main__':
    main()
```

2. Set `infra_path: infra` in `goldfish.yaml`

3. Jobs will now launch via your infrastructure

### Job Completion Markers

Jobs signal completion by writing to experiment directory:

- `COMPLETED` - Job succeeded (exit code 0)
- `FAILED` - Job failed (contains error message)

Goldfish polls for these markers when you check job status.

## Development

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_workspace_manager.py

# Run with coverage
pytest --cov=goldfish tests/
```

### Project Structure

```
goldfish/
├── src/goldfish/
│   ├── server.py           # MCP server and tool definitions
│   ├── config.py           # Configuration management
│   ├── db/
│   │   ├── database.py     # SQLite operations
│   │   └── schema.sql      # Database schema
│   ├── workspace/
│   │   ├── manager.py      # High-level workspace ops
│   │   └── git_layer.py    # Git operations
│   ├── jobs/
│   │   ├── launcher.py     # Job launching
│   │   ├── tracker.py      # Status tracking
│   │   └── exporter.py     # Experiment export
│   ├── sources/
│   │   ├── registry.py     # Data source management
│   │   └── lineage.py      # Lineage tracking
│   ├── state/
│   │   └── state_md.py     # STATE.md generation
│   └── cli.py              # Command-line interface
├── tests/                  # Test suite
└── README.md               # This file
```

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

### Lock Timeout Errors

```
Error: workspace is locked - another operation may be in progress
```

Solution: Wait for concurrent operation to complete, or remove stale lock:
```bash
rm .goldfish-locks/w1.lock
```

## Design Principles

1. **Conversation Resilience** - STATE.md enables context recovery
2. **Audit Everything** - All operations logged for reproducibility
3. **Atomic Operations** - Transactions prevent partial state
4. **Security First** - Path validation, symlink protection, size limits
5. **Git as Source of Truth** - Workspaces = branches, snapshots = tags

## License

MIT License - See LICENSE file for details

## Contributing

Contributions welcome! Please:
1. Write tests for new features
2. Follow existing code style
3. Update documentation
4. Add audit logging for new operations

## Acknowledgments

Built with:
- [FastMCP](https://github.com/jlowin/fastmcp) - MCP server framework
- [GitPython](https://github.com/gitpython-developers/GitPython) - Git operations
- SQLite - Embedded database

---

**Note**: Goldfish is designed for single-user local development. For team collaboration, consider using shared git remotes and cloud storage backends.
