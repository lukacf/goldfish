# Goldfish Architecture

> Comprehensive architecture guide for Goldfish - the MCP server for AI-driven ML experimentation.

## Overview

Goldfish is an **MCP (Model Context Protocol) server** that enables AI agents to conduct reliable, stateful ML experiments. It solves the "goldfish memory" problem by providing:

- **Isolated Workspaces** - Sandboxed environments for experiments
- **Automatic Versioning** - Every run backed by immutable git tags (hidden from agent)
- **Pipeline Workflows** - YAML-defined DAGs with typed data signals
- **Infrastructure Abstraction** - Agents write Python; Goldfish handles Docker/GCE/GCS
- **Pre-Run Validation** - Automated code review to catch bugs before execution

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              MCP Client                                  │
│                         (Claude Code, etc.)                              │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │ JSON-RPC
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           Goldfish Server                                │
│                                                                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │  Workspace  │  │   Pipeline  │  │    Stage    │  │     MCP     │    │
│  │   Manager   │  │   Manager   │  │  Executor   │  │    Tools    │    │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └─────────────┘    │
│         │                │                │                              │
│         └────────────────┴────────────────┘                              │
│                          │                                               │
│                  ┌───────▼───────┐                                       │
│                  │   Protocols   │  ◄── Backend-agnostic interfaces      │
│                  │   Contracts   │                                       │
│                  └───────┬───────┘                                       │
└──────────────────────────┼───────────────────────────────────────────────┘
                           │
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
     ┌──────────┐   ┌──────────┐   ┌────────────┐
     │  Local   │   │   GCE    │   │ Kubernetes │
     │ (Docker) │   │  (GCP)   │   │  (future)  │
     └──────────┘   └──────────┘   └────────────┘
```

> **Note**: The Kubernetes backend is defined in the codebase but currently raises `NotImplementedError`. It is planned for a future release.

---

## Core Abstractions

### 1. Workspaces

Isolated experiment environments using copy-based mounting:

```
MOUNT:  dev-repo/branch ──copy──▶ user/workspaces/w1/ (plain files, NO .git)
WORK:   Claude edits user/workspaces/w1/
RUN:    user/w1/ ──sync──▶ dev-repo/branch ──commit──▶ execute
```

- User workspace has no `.git` directory - all versioning hidden in dev repo
- Changes synced back before every run (100% provenance)
- Three slots available: w1, w2, w3

### 2. Versions

Immutable git tags created automatically:

```
user edits ──sync──▶ commit in dev-repo ──tag──▶ baseline_lstm-v1
```

- Every `run()` creates a version (auto-versioning)
- Manual versions via `save_version()`
- Stored in `workspace_versions` table with `created_by: run|checkpoint|manual`

### 3. Pipelines

YAML-defined stage workflows:

```yaml
stages:
  - name: preprocess
    inputs: {raw: {type: dataset, dataset: sales_v1}}
    outputs: {features: {type: npy}}
  - name: train
    inputs: {features: {from_stage: preprocess, signal: features}}
    outputs: {model: {type: directory}}
```

Parser validates: unique names, type compatibility, no cycles, datasets exist.

### 4. Stages

Python modules executed in Docker containers:

```python
# modules/train.py
from goldfish.io import load_input, save_output

features = load_input("features")  # from /mnt/inputs/
model = train_model(features)
save_output("model", model_dir)    # to /mnt/outputs/
```

### 5. Signals

Typed data flow between stages:

| Type | Format | Use Case |
|------|--------|----------|
| `dataset` | External | Registered project data |
| `npy` | NumPy | Arrays, embeddings |
| `csv` | Pandas | Tabular data |
| `directory` | Dir | Model checkpoints |
| `file` | Single file | Configs, outputs |

Tracked in `signal_lineage` table for full provenance.

### 6. Resource Profiles

Declarative compute specification:

```yaml
compute:
  profile: "h100-spot"  # Claude writes this
```

Goldfish resolves to: `a3-highgpu-1g`, H100 GPU, spot pricing, multi-zone.

Built-in profiles: `cpu-small`, `cpu-large`, `h100-spot`, `h100-on-demand`, `a100-spot`, `a100-on-demand`

### 7. Semantic Validation System (SVS)

Defense-in-depth through three phases:

1. **Pre-Run Review** - AI-driven static analysis of code/config/diff
2. **Schema Contracts** - Mechanistic validation of outputs against pipeline.yaml
3. **Output Stats** - Automatic statistical properties (entropy, null ratio, etc.)

Enforcement modes: `warning` (log only) or `blocking` (fail stage).

---

## Cloud Abstraction Layer

Protocol-based design enabling backend portability without code changes.

### Protocols

| Protocol | Purpose | Methods |
|----------|---------|---------|
| `RunBackend` | Compute execution | `launch()`, `get_status()`, `get_logs()`, `terminate()` |
| `ObjectStorage` | Blob storage | `put()`, `get()`, `exists()`, `delete()` |
| `SignalBus` | Control plane | `set_signal()`, `get_signal()` |
| `ImageBuilder` | Container builds | `build()`, `get_build_status()` |

### BackendCapabilities

Behavior configuration replaces scattered conditionals:

```python
# BAD: if backend == "gce": timeout = 3.0
# GOOD: timeout = backend.capabilities.ack_timeout_seconds

@dataclass
class BackendCapabilities:
    supports_gpu: bool = False
    supports_spot: bool = False
    ack_timeout_seconds: float = 1.0
    has_launch_delay: bool = False
    timeout_becomes_pending: bool = False
    # ... additional fields defined in cloud/contracts.py
```

> **Note**: The fields shown above are a subset. See `src/goldfish/cloud/contracts.py` for the complete `BackendCapabilities` definition.

### Key Files

```
src/goldfish/cloud/
├── protocols.py      # Protocol definitions
├── contracts.py      # BackendCapabilities, RunSpec, RunHandle
├── factory.py        # AdapterFactory for DI
└── adapters/
    ├── local/        # Docker-based (dev/testing)
    └── gcp/          # GCE + GCS (production)
```

### Adding New Backends

1. Implement protocols in `cloud/adapters/yourprovider/`
2. Define `BackendCapabilities` for your backend
3. Register in `factory.py`
4. No changes needed in core code

---

## Configuration Flexibility

Goldfish supports flexible configuration for multi-cloud and multi-environment deployments.

### Global Defaults

Set project-wide execution defaults:

```yaml
# goldfish.yaml
defaults:
  timeout_seconds: 7200    # Default stage timeout (2 hours)
  log_sync_interval: 15    # How often to sync logs
  backend: gce             # Default compute: local, gce, kubernetes
```

### Storage Backend Selection

Choose your storage provider independently of compute:

```yaml
# goldfish.yaml
storage:
  backend: "gcs"  # or "s3", "azure", "local"

  gcs:
    bucket: "my-artifacts"
    sources_prefix: "sources/"

  s3:
    bucket: "my-artifacts"
    region: "us-east-1"
    endpoint_url: "http://localhost:9000"  # MinIO

  azure:
    container: "my-artifacts"
    account: "mystorageaccount"
```

| Backend | Status | Use Case |
|---------|--------|----------|
| `gcs` | Complete | GCP users |
| `s3` | Config ready, adapter coming | AWS users, MinIO |
| `azure` | Config ready, adapter coming | Azure users |
| `local` | Complete | Development, testing |

**Backwards Compatibility**: The legacy `gcs:` section at root level still works. New `storage:` section takes precedence when present.

### Profile-Based Configuration

Override settings per compute profile:

```yaml
# goldfish.yaml
gce:
  project_id: my-project
  zones: ["us-central1-a", "us-central1-b"]

  profile_overrides:
    h100-spot:
      zones: ["us-central1-a"]  # Specific zone for H100 quota

    cpu-large:
      zones: ["us-west1-a"]     # Different region for CPU
```

### Configuration Model

```
GoldfishConfig
├── project_name: str
├── dev_repo_path: str
├── defaults: DefaultsConfig       # Global execution defaults
├── storage: StorageConfig | None  # Multi-backend storage
├── gcs: GCSConfig | None          # Legacy GCS (backwards compat)
├── gce: GCEConfig | None          # GCE compute settings
├── jobs: JobsConfig               # Job execution config
├── local: LocalConfig             # Local backend simulation
├── pre_run_review: PreRunReviewConfig  # AI code review settings
├── docker: DockerConfig           # Image customization
└── svs: SVSConfig                 # Validation settings
```

---

## Execution Model

### Stage Execution Flow

```
run("w1", stages=["train"])
    │
    ├─▶ 1. Validate workspace mounted
    ├─▶ 2. SYNC: Copy user/w1 → dev-repo/branch
    ├─▶ 3. COMMIT: Auto-commit changes
    ├─▶ 4. PUSH: Push to remote (for GCE)
    ├─▶ 5. Auto-version (git tag)
    ├─▶ 6. Pre-run SVS review
    ├─▶ 7. Build Docker image
    ├─▶ 8. Launch container (local/GCE)
    ├─▶ 9. Monitor status, stream logs
    └─▶ 10. Finalize: register outputs
```

### State Machine

Stages progress through states:

```
PENDING → PREPARING → LAUNCHING → RUNNING → POST_RUN → COMPLETED
                                         ↘ FAILED
                                         ↘ CANCELED
```

Key properties:
- **Single source of truth** - State machine owns state
- **Event-driven** - External systems emit events
- **CAS semantics** - Compare-and-swap prevents races
- **Full audit** - Every transition recorded

---

## Key Files Reference

### Core
| File | Purpose |
|------|---------|
| `server.py` | MCP server entry point |
| `context.py` | ServerContext dependency injection |
| `jobs/stage_executor.py` | Stage execution engine |
| `workspace/manager.py` | Workspace CRUD + mounting |
| `workspace/git_layer.py` | Git operations + sync |

### Cloud
| File | Purpose |
|------|---------|
| `cloud/protocols.py` | Protocol definitions |
| `cloud/contracts.py` | Data contracts |
| `cloud/factory.py` | Adapter factory |
| `cloud/adapters/local/` | Docker backend |
| `cloud/adapters/gcp/` | GCE/GCS backend |

### Database
| File | Purpose |
|------|---------|
| `db/database.py` | All database operations |
| `db/schema.sql` | SQLite schema |
| `db/types.py` | TypedDict definitions |

### Tools
| File | Purpose |
|------|---------|
| `server_tools/execution_tools.py` | run, logs, cancel |
| `server_tools/workspace_tools.py` | mount, hibernate, status |
| `server_tools/experiment_tools.py` | finalize_run, tag_record |

---

## Database Schema (Key Tables)

```sql
workspace_versions(workspace_name, version, git_sha, created_by, created_at)
stage_runs(id, workspace_name, version, stage_name, state, backend_type, ...)
signal_lineage(stage_run_id, signal_name, signal_type, storage_location, stats_json)
audit(operation, workspace, details_json, created_at)
```

---

## Security Model

### Input Validation
| Input | Pattern |
|-------|---------|
| Workspace name | `^[a-zA-Z0-9_-]+$` |
| Snapshot ID | `^snap-[a-f0-9]{8}-\d{8}-\d{6}$` |
| Stage run ID | `^stage-[a-f0-9]+$` |

### Path Traversal Protection
```python
def validate_path_within_root(path: Path, root: Path) -> None:
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValidationError("Path traversal")
```

### Docker Sandboxing
```python
# Containers run with:
--memory 4g --cpus 2.0 --pids-limit 100
--user 1000:1000  # non-root
-v inputs:/mnt/inputs:ro  # read-only inputs
```

---

## Related Documentation

- [CLAUDE.md](../CLAUDE.md) - Development guide for AI assistants
- [GETTING_STARTED.md](GETTING_STARTED.md) - Installation and first run
- [GCP_SETUP.md](GCP_SETUP.md) - GCP infrastructure setup
- [archive/](archive/) - Historical specs and detailed implementation docs
