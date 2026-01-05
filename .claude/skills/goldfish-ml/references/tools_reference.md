# Goldfish MCP Tools Reference

Complete API documentation for the 31 master Goldfish MCP tools organized by category.

## Workspace Management Tools

### status()

Get current system status orientation. **Call this first when starting or recovering context.**
Returns which workspaces are mounted, active jobs, recent audit trail, and full STATE.md.

**Parameters:** None

**Returns:**
```python
StatusResponse:
  project_name: str
  slots: list[SlotInfo]      # w1, w2, etc. with mount status
  active_jobs: list[JobInfo] # Currently running jobs
  source_count: int          # Number of registered sources
  recent_audit: list[dict]   # Last 5 state-changing operations
  state_md: str              # Full STATE.md content
```

---

### dashboard()

Get a quick overview of system state for situational awareness. **Call this to see what needs immediate attention.**
Focuses on failed runs, active runs, and recent outcomes.

**Returns:**
- `failed_runs`: Recent failures with error messages.
- `active_runs`: Currently running or pending stages.
- `workspaces`: Workspace list with dirty status.
- `source_count`: Total registered sources.
- `recent_outcomes`: Success/bad_results trends.

---

### create_workspace(name, goal, reason)

Create a new experiment workspace from main.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | str | Yes | Workspace identifier (alphanumeric + underscore/hyphen) |
| `goal` | str | Yes | Clear description of experiment objective |
| `reason` | str | Yes | Why this workspace is needed (min 15 chars) |

---

### mount(workspace, slot, reason)

Mount a workspace to an editing slot. Required before editing files.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `workspace` | str | Yes | Workspace name to mount |
| `slot` | str | Yes | Target slot (w1, w2, or w3) |
| `reason` | str | Yes | Why you're mounting this workspace (min 15 chars) |

---

### hibernate(slot, reason)

Deactivate a workspace slot. Auto-checkpoints if dirty.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `slot` | str | Yes | Slot to hibernate |
| `reason` | str | Yes | Why hibernating (min 15 chars) |

---

### save_version(slot, message)

Create a version of the current slot state. **Primary way to create save points.**

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `slot` | str | Yes | Slot to save |
| `message` | str | Yes | Descriptive message (min 15 chars) |

---

### inspect_workspace(name, version_limit, version_offset)

Get a comprehensive view of a workspace including history and pipeline.

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | str | Yes | - | Workspace name or slot (e.g., "baseline" or "w1") |
| `version_limit` | int | No | 10 | Max versions to show in history |
| `version_offset` | int | No | 0 | Pagination for history |

---

### diff(target, against)

Compare changes between targets (slots, workspaces, or versions).

---

### rollback(slot, version, reason)

Revert workspace to a previous version.

---

### delete_workspace(workspace, reason)

Permanently delete a workspace and all snapshots.

---

## Execution Tools

### run(workspace, stages, pipeline, config_override, inputs_override, reason, wait, dry_run, skip_review)

Execute pipeline stages. **The primary execution tool.**

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `workspace` | str | Yes | - | Workspace name or slot (e.g., "w1") |
| `stages` | list[str] | No | None | Stages to run (None = all) |
| `reason` | str \| dict | No | None | Why running (min 15 chars description or dict) |

---

### inspect_run(run_id, include)

Get a comprehensive, synthesized view of a run results and health.

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `run_id` | str | Yes | - | Run ID (e.g., "stage-abc123") |
| `include` | list[str] | No | - | Data to include (dashboard, manifest, provenance, svs, thoughts) |

---

### compare_runs(run_id_a, run_id_b)

Compare two runs side-by-side. **Essential for troubleshooting regressions.**

---

### logs(run_id, tail, since, follow)

Get container logs from a run. Supports follow mode for cursor-based streaming.

---

### mark_outcome(run_id, outcome)

Indicate if a completed run produced good results or garbage.

---

### cancel(run_id, reason)

Cancel a running stage.

---

### list_runs(workspace, stage, status, pipeline_run_id, limit, offset)

List recent runs for a workspace (compact view).

---

### list_all_runs(status, limit, offset)

Get a global experiment timeline across ALL workspaces.

---

## Version Management Tools

### manage_versions(workspace, action, version, tag, reason, from_version, to_version, limit, offset)

Unified tool for tagging milestones and cleaning up history.

**Actions:** `list`, `tag`, `untag`, `prune`, `unprune`, `prune_before_tag`

---

## Data Management Tools

### manage_sources(action, name, status, created_by, metadata, reason, limit, offset)

Unified tool for managing the data registry (datasets and artifacts).

---

### register_source(name, gcs_path, description, reason, metadata)

Register an external data source (GCS location).

---

### promote_artifact(job_id, output_name, source_name, reason, metadata)

Promote a stage run output to a reusable data source.

---

## Utility Tools

### validate_config(workspace)

Validate `goldfish.yaml` and pipeline/stage configs for typos and errors.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `workspace` | str | No | Optional workspace to validate pipeline files |

---

### log_thought(thought, workspace, run_id)

Record reasoning for the audit trail and STATE.md.

---

### get_workspace_thoughts(workspace, limit, offset)

Get all thoughts logged for a specific workspace.

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `workspace` | str | Yes | - | Workspace name or slot |
| `limit` | int | No | 50 | Max thoughts to return |

---

### manage_patterns(action, pattern_id, status, reason, dry_run)

Manage the AI failure pattern registry.

---

### search_goldfish_logs(query, show_guide)

Search centralized Goldfish logs using LogsQL via VictoriaLogs.

---

### initialize_project(project_name, project_root, from_existing)

Initialize a new Goldfish project in the specified directory. **First-time setup only.**

---

### reload_config()

Reload configuration from `goldfish.yaml` without restarting the server.

---

### get_audit_log(limit, workspace)

Get recent audit trail entries for compliance and history.

---

## Deprecated Tools

| Deprecated | Use Instead |
|------------|-------------|
| `checkpoint()` | `save_version()` |
| `tag_version()` | `manage_versions(action="tag")` |
| `prune_version()` | `manage_versions(action="prune")` |
| `register_dataset()` | `register_source()` |
| `get_workspace_lineage()` | `inspect_workspace()` |
| `get_run_provenance()` | `inspect_run(include=["provenance"])` |
| `get_audit_log()` | `status()` |