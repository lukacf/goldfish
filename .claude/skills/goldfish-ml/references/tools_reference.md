# Goldfish MCP Tools Reference

Complete API documentation for all 39 Goldfish MCP tools organized by category.

## Workspace Management Tools

### status()

Get current system status. **Call this first when starting or recovering context.**

**Parameters:** None

**Returns:**
```python
StatusResponse:
  project_name: str
  slots: list[SlotInfo]      # w1, w2, etc. with mount status
  active_jobs: list[JobInfo] # Currently running jobs
  source_count: int          # Number of registered sources
  state_md: str              # Full STATE.md content
```

**Example:**
```python
status()
# Returns orientation: what's mounted, what's running, recent history
```

---

### create_workspace(name, goal)

Create a new experiment workspace.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | str | Yes | Workspace identifier (alphanumeric + underscore/hyphen) |
| `goal` | str | Yes | Clear description of experiment objective |
| `fork_from` | str | No | Existing workspace to branch from |

**Returns:**
```python
CreateWorkspaceResponse:
  success: bool
  workspace: str
  forked_from: str  # "main" or parent workspace
  state_md: str
```

**Example:**
```python
create_workspace(
    name="lstm_v1",
    goal="Train baseline LSTM for EUR/USD prediction"
)

# Branch from existing:
create_workspace(
    name="lstm_v2_attention",
    goal="Add attention mechanism to LSTM",
    fork_from="lstm_v1"
)
```

---

### mount(slot, workspace)

Mount a workspace to an editing slot. Required before editing files.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `slot` | str | Yes | Slot name (w1, w2, etc.) |
| `workspace` | str | Yes | Workspace name to mount |

**Returns:**
```python
MountResponse:
  success: bool
  slot: str
  workspace: str
  dirty: DirtyState  # "clean" or "dirty"
  last_checkpoint: str | None
  state_md: str
  warning: str | None  # Soft limit warnings
```

**Example:**
```python
mount(slot="w1", workspace="lstm_v1")
# Now files are at: workspaces/w1/
```

---

### hibernate(slot, reason)

Deactivate a workspace slot. Auto-checkpoints if dirty.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `slot` | str | Yes | Slot to hibernate |
| `reason` | str | Yes | Why hibernating (min 15 chars) |

**Returns:**
```python
HibernateResponse:
  success: bool
  slot: str
  workspace: str
  auto_checkpointed: bool
  checkpoint_id: str | None
  pushed_to_remote: bool
  state_md: str
```

**Example:**
```python
hibernate(slot="w1", reason="Completed baseline training, switching to attention experiment")
```

---

### save_version(slot, message)

Create a version of the current slot state. **This is the primary way to create save points.**

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `slot` | str | Yes | Slot to save |
| `message` | str | Yes | Descriptive message (min 15 chars) |

**Returns:**
```python
SaveVersionResponse:
  success: bool
  slot: str
  version: str      # e.g., "v1", "v2" (primary identifier)
  git_tag: str      # Internal git tag (e.g., "snap-a1b2c3d4-...")
  git_sha: str      # Full git SHA for provenance
  message: str
  state_md: str
```

**Notes:**
- Each call creates a version (v1, v2, v3...) in the workspace's version history
- Versions have `created_by: "save_version"` in lineage data
- Use `get_workspace_lineage()` to see all versions
- Use `rollback()` with the version string (e.g., "v1") to restore

**Example:**
```python
save_version(slot="w1", message="Working preprocessing pipeline before adding feature engineering")
# Returns SaveVersionResponse with version="v1"
```

---

### checkpoint(slot, message) [DEPRECATED]

**DEPRECATED: Use `save_version()` instead.**

Creates a version (internally calls `save_version()`). Returns legacy `CheckpointResponse` with `snapshot_id`.

**Returns:**
```python
CheckpointResponse:
  success: bool
  slot: str
  snapshot_id: str  # Legacy: git tag (use version from save_version instead)
  message: str
  state_md: str
```

---

### get_workspace(workspace)

Get workspace details including pipeline information.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `workspace` | str | Yes | Workspace name |

**Returns:**
```python
WorkspaceInfo:
  name: str
  created_at: datetime
  goal: str
  snapshot_count: int
  last_activity: datetime
  is_mounted: bool
  mounted_slot: str | None
  workflow: WorkflowInfo | None  # Pipeline stages
```

---

### diff(slot)

Show uncommitted changes in a mounted slot.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `slot` | str | Yes | Slot to diff |

**Returns:**
```python
DiffResponse:
  slot: str
  has_changes: bool
  summary: str        # e.g., "2 files changed, 10 insertions(+)"
  files_changed: list[str]
  diff_text: str      # Full diff output
```

---

### rollback(slot, version, reason)

Revert workspace to a previous version.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `slot` | str | Yes | Slot to rollback |
| `version` | str | Yes | Target version (e.g., "v1", "v2") |
| `reason` | str | Yes | Why rolling back (min 15 chars) |

**Returns:**
```python
RollbackResponse:
  success: bool
  slot: str
  version: str       # Version rolled back to
  git_tag: str       # Internal git tag
  files_reverted: int
  state_md: str
```

**Example:**
```python
# Get versions from lineage
lineage = get_workspace_lineage("my-workspace")
# lineage["versions"] shows all versions like {"version": "v1", ...}

# Rollback to a specific version
rollback(slot="w1", version="v1", reason="Reverting to working state before failed experiment")
```

---

### delete_workspace(workspace, reason)

Permanently delete a workspace and all snapshots.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `workspace` | str | Yes | Workspace to delete |
| `reason` | str | Yes | Why deleting (min 15 chars) |

**Returns:**
```python
DeleteWorkspaceResponse:
  success: bool
  workspace: str
  snapshots_deleted: int
```

---

### list_workspaces(limit, offset)

List all workspaces with pagination.

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `limit` | int | No | 50 | Max results per page |
| `offset` | int | No | 0 | Pagination offset |

**Returns:** List of WorkspaceInfo objects with pagination metadata.

---

## Execution Tools

### run(workspace, stages, pipeline, config_override, inputs_override, reason, wait)

Execute pipeline stages. **The primary execution tool.**

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `workspace` | str | Yes | - | Workspace name or slot (e.g., "w1") |
| `stages` | list[str] | No | None | Stages to run (None = all) |
| `pipeline` | str | No | None | Pipeline file (default: pipeline.yaml) |
| `config_override` | dict | No | None | Override config variables |
| `inputs_override` | dict | No | None | Override input sources |
| `reason` | str | No | None | Why running (min 15 chars) |
| `wait` | bool | No | False | Block until completion |

**Returns:**
```python
{
  "runs": [StageRunInfo, ...],  # List of stage run info
  "pipeline_run_id": str | None  # If running multiple stages
}
```

**Examples:**
```python
# Run all stages in pipeline
run("w1")

# Run single stage
run("w1", stages=["train"])

# Run specific stages
run("w1", stages=["preprocess", "train", "evaluate"])

# Override config
run("w1", stages=["train"], config_override={"learning_rate": 0.001})

# Wait for completion
run("w1", stages=["train"], wait=True)
```

---

### get_run(run_id)

Get full details of a specific run.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `run_id` | str | Yes | Run ID (e.g., "stage-abc123") |

**Returns:**
```python
GetRunResponse:
  stage_run: StageRunInfo
  inputs: dict   # Input sources and values
  outputs: list  # Output artifacts
  config: dict   # Full merged config
```

---

### logs(run_id, tail, since)

Get container logs from a run.

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `run_id` | str | Yes | - | Run ID |
| `tail` | int | No | 200 | Lines from end (max 10000) |
| `since` | str | No | None | Only logs after ISO timestamp |

**Returns:**
```python
{
  "run_id": str,
  "status": str,
  "logs": str,      # Log content
  "log_uri": str    # Persistent log location
}
```

**Example:**
```python
logs("stage-abc123", tail=500)
logs("stage-abc123", since="2024-12-10T15:00:00Z")
```

---

### cancel(run_id, reason)

Cancel a running stage.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `run_id` | str | Yes | Run ID to cancel |
| `reason` | str | Yes | Why cancelling (min 15 chars) |

**Returns:**
```python
CancelRunResponse:
  success: bool
  previous_status: str | None
  error: str | None
```

---

### list_runs(workspace, stage, status, pipeline_run_id, limit, offset)

List runs with filters and pagination.

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `workspace` | str | No | None | Filter by workspace |
| `stage` | str | No | None | Filter by stage name |
| `status` | str | No | None | pending/running/completed/failed/canceled |
| `pipeline_run_id` | str | No | None | Filter by pipeline run |
| `limit` | int | No | 50 | Max results |
| `offset` | int | No | 0 | Pagination offset |

**Returns:**
```python
ListRunsResponse:
  runs: list[StageRunInfo]
  total_count: int
  has_more: bool
```

**Examples:**
```python
# All running jobs
list_runs(status="running")

# Recent training runs for workspace
list_runs(workspace="lstm_v1", stage="train", limit=10)

# Failed runs
list_runs(status="failed")
```

---

### get_outputs(run_id)

Get output artifacts from a completed run.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `run_id` | str | Yes | Run ID |

**Returns:**
```python
GetOutputsResponse:
  stage_run_id: str
  outputs: list  # Output artifact details
```

---

## Data Management Tools

### register_dataset(name, source, description, format, metadata)

Register a project-level dataset.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | str | Yes | Dataset identifier (e.g., "eurusd_raw_v3") |
| `source` | str | Yes | "local:/path" or "gs://bucket/path" |
| `description` | str | Yes | Human-readable description |
| `format` | str | Yes | csv, npy, directory, etc. |
| `metadata` | dict | No | Optional metadata (rows, columns, etc.) |

**Returns:**
```python
RegisterDatasetResponse:
  success: bool
  dataset: SourceInfo
```

**Example:**
```python
register_dataset(
    name="eurusd_ticks_v3",
    source="local:/data/eurusd_2024.csv",
    description="EUR/USD tick data for 2024, cleaned and validated",
    format="csv",
    metadata={"rows": 1_000_000, "columns": 5}
)
```

---

### list_sources(status, created_by, limit, offset)

List available data sources with filtering.

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `status` | str | No | None | available/processing/error |
| `created_by` | str | No | None | "external" for datasets, "job:xxx" for artifacts |
| `limit` | int | No | 50 | Max results (1-200) |
| `offset` | int | No | 0 | Pagination offset |

**Returns:**
```python
ListSourcesResponse:
  sources: list[SourceInfo]
  total_count: int
  offset: int
  limit: int
  has_more: bool
  filters_applied: dict
```

**Examples:**
```python
list_sources()                           # All sources
list_sources(created_by="external")      # Only registered datasets
list_sources(status="available")         # Ready to use
```

---

### get_source(name)

Get detailed information about a specific source.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | str | Yes | Source name |

**Returns:**
```python
SourceInfo:
  name: str
  description: str | None
  created_at: datetime
  created_by: str
  gcs_location: str
  size_bytes: int | None
  status: SourceStatus
```

---

### delete_source(source_name, reason)

Delete a data source from the registry. **Irreversible.**

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_name` | str | Yes | Source to delete |
| `reason` | str | Yes | Why deleting (min 15 chars) |

---

### get_source_lineage(source_name)

Get provenance information for a source.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_name` | str | Yes | Source to query |

**Returns:**
```python
SourceLineage:
  source_name: str
  parent_sources: list[str]  # Upstream sources
  job_id: str | None         # Creating job if promoted artifact
```

---

### promote_artifact(job_id, output_name, source_name, reason)

Promote a job output to a reusable data source.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `job_id` | str | Yes | Completed job ID |
| `output_name` | str | Yes | Output in job config (e.g., "preprocessed") |
| `source_name` | str | Yes | Name for new source |
| `reason` | str | Yes | Why promoting (min 15 chars) |

**Returns:**
```python
PromoteArtifactResponse:
  success: bool
  source: SourceInfo
  lineage: SourceLineage  # Records input sources
  state_md: str
```

---

## Lineage & Provenance Tools

### get_workspace_lineage(workspace)

Get full lineage for a workspace including version history and branches.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `workspace` | str | Yes | Workspace name |

**Returns:**
```python
{
  "name": str,
  "created": datetime,
  "parent": str | None,        # Parent workspace if branched
  "parent_version": str | None,
  "description": str,
  "versions": list[dict],      # All versions with metadata
  "branches": list[dict]       # Child workspaces
}
```

---

### get_version_diff(workspace, from_version, to_version)

Compare two versions of a workspace.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `workspace` | str | Yes | Workspace name |
| `from_version` | str | Yes | Starting version (e.g., "v1") |
| `to_version` | str | Yes | Ending version (e.g., "v3") |

**Returns:**
```python
{
  "from_version": str,
  "to_version": str,
  "commits": list[dict],  # Git commits between versions
  "files": list[dict]     # File changes
}
```

---

### get_run_provenance(stage_run_id)

Get exact provenance of a stage run.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `stage_run_id` | str | Yes | Stage run ID |

**Returns:**
```python
{
  "stage_run_id": str,
  "workspace": str,
  "version": str,
  "git_sha": str,           # Exact commit
  "stage": str,
  "config_override": dict,
  "inputs": list[dict],     # Input signals consumed
  "outputs": list[dict]     # Output signals produced
}
```

---

### get_stage_lineage(run_id, max_depth)

Get full upstream lineage tree for a stage run.

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `run_id` | str | Yes | - | Stage run ID |
| `max_depth` | int | No | 10 | Max recursion depth |

**Returns:** Nested lineage tree showing upstream dependencies recursively.

```python
{
  "success": true,
  "lineage": {
    "run_id": "stage-abc123",
    "stage": "training",
    "stage_version_num": 12,
    "git_sha": "a7b2c3d",
    "inputs": {
      "features": {
        "source_type": "stage",
        "source_stage": "preprocessing",
        "source_stage_version_num": 4,
        "upstream": { ... recursive ... }
      }
    }
  }
}
```

---

### list_stage_versions(workspace, stage)

List all versions of stages in a workspace.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `workspace` | str | Yes | Workspace name |
| `stage` | str | No | Filter by stage name |

**Returns:**
```python
{
  "success": true,
  "workspace": str,
  "stage_filter": str | None,
  "versions": [
    {
      "stage": str,
      "version_num": int,
      "git_sha": str,
      "config_hash": str,
      "created_at": str
    }
  ]
}
```

---

### find_runs_using_stage_version(workspace, stage, version)

Find all runs that used a specific stage version as input. **Useful for impact analysis.**

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `workspace` | str | Yes | Workspace name |
| `stage` | str | Yes | Stage name |
| `version` | int | Yes | Version number |

**Returns:**
```python
{
  "success": true,
  "stage_version": {
    "stage": str,
    "version": int,
    "git_sha": str,
    "config_hash": str
  },
  "downstream_runs": [
    {
      "run_id": str,
      "stage": str,
      "status": str,
      "started_at": str
    }
  ]
}
```

---

## Utility Tools

### initialize_project(project_name, project_root, from_existing)

Initialize a new Goldfish project. **First-time setup only.**

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `project_name` | str | Yes | Project name |
| `project_root` | str | Yes | Root directory |
| `from_existing` | str | No | Import existing code from path |

---

### get_audit_log(limit, workspace)

Get recent audit trail entries.

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `limit` | int | No | 20 | Max entries |
| `workspace` | str | No | None | Filter by workspace |

**Returns:**
```python
AuditLogResponse:
  entries: list[AuditEntry]
  count: int

AuditEntry:
  id: int
  timestamp: datetime
  operation: str
  slot: str | None
  workspace: str | None
  reason: str
  details: dict | None
```

---

### log_thought(thought)

Record reasoning for the audit trail. **Use to document decisions.**

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `thought` | str | Yes | Reasoning/decision (min 15 chars) |

**Returns:**
```python
LogThoughtResponse:
  logged: bool
  thought: str
  timestamp: datetime
```

**Example:**
```python
log_thought("Switching to attention mechanism because baseline LSTM plateaued at 0.72 accuracy")
```

---

## Deprecated Tools

These tools still work but are deprecated. Use alternatives:

| Deprecated | Use Instead |
|------------|-------------|
| `checkpoint()` | `save_version()` |
| `get_pipeline()` | `get_workspace()` |
| `validate_pipeline()` | `get_workspace()` |
| `update_pipeline()` | Edit pipeline.yaml directly |
| `list_pipelines()` | `get_workspace()` |
| `list_snapshots()` | `get_workspace_lineage()` |
| `get_snapshot()` | `get_workspace_lineage()` |
| `delete_snapshot()` | Avoid - maintain history |
| `get_workspace_goal()` | `get_workspace()` |
| `update_workspace_goal()` | Direct edit |
| `branch_workspace()` | `create_workspace(fork_from=...)` |
