# Goldfish MCP Tools Reference

Complete API documentation for the Goldfish MCP tools organized by category.

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
Returns a structured summary organized for quick action.

**Returns:**
```python
{
  "alerts": {
    "failed_recent": [...],  # Failed runs with reason, error, age
    "svs_reviews": [...]     # New AI reviews (shown once)
  },
  "active": {
    "running": [...]  # Running stages with reason, elapsed time
  },
  "blocks": {
    "pending_finalization": {
      "by_workspace": {...}  # Grouped by workspace with count + example
    }
  },
  "workspaces": {
    "mounted": [...],  # Slots with workspace name, goal, dirty state
    "unmounted_count": int
  },
  "source_count": int
}
```

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
Returns experiment context for quick orientation (current best, pending finalizations, recent trend).

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

### inspect_workspace(name, version_limit, version_offset, include)

Get a comprehensive view of a workspace including history and lineage.

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | str | Yes | - | Workspace name or slot (e.g., "baseline" or "w1") |
| `version_limit` | int | No | 10 | Max versions to show in history |
| `version_offset` | int | No | 0 | Pagination for history |
| `include` | list | No | None | Additional data: `["pipeline"]` to include pipeline definition |

**Note:** Pipeline definition is excluded by default to reduce output size. Use `include=["pipeline"]` when you need to see it.

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

### run(workspace, stages, pipeline, config_override, inputs_override, reason, results_spec, experiment_group, wait, dry_run, skip_review)

Execute pipeline stages. **Primary execution tool.**

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `workspace` | str | Yes | - | Workspace name or slot (e.g., "w1") |
| `stages` | list[str] | No | None | Stages to run (None = all) |
| `pipeline` | str | No | None | Named pipeline file (pipelines/<name>.yaml) |
| `config_override` | dict | No | None | Override config vars per stage |
| `inputs_override` | dict | No | None | Override input sources |
| `reason` | str \| dict | No | None | Why running (min 15 chars; dict supports hypothesis/approach) |
| `results_spec` | dict | **Yes** | - | Expected results spec (required for non-dry runs) |
| `experiment_group` | str | No | None | Optional grouping label for filtering/summaries |
| `wait` | bool | No | False | If True, block until completion |
| `dry_run` | bool | No | False | Validate without launching |
| `skip_review` | bool | No | False | Skip SVS pre-run review (last resort) |

**results_spec (required):**
```json
{
  "primary_metric": "dir_acc_binary",
  "direction": "maximize",
  "min_value": 0.60,
  "goal_value": 0.63,
  "dataset_split": "val",
  "tolerance": 0.003,
  "context": "Verbose experiment intent and constraints",
  "secondary_metrics": ["val_loss"],
  "baseline_run": "@best-25m",
  "failure_threshold": 0.55,
  "known_caveats": ["High variance early epochs"]
}
```

**Note:** `run()` enforces a **finalization gate**: terminal runs without `finalize_run()` will block new runs.

---

### finalize_run(record_or_run_id, results)

Finalize ML results for a run. Authoritative outcome recording.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `record_or_run_id` | str | Yes | Record ID or stage_run_id |
| `results` | dict | Yes | Final results payload |

**results (required fields):**
```json
{
  "primary_metric": "dir_acc_binary",
  "direction": "maximize",
  "value": 0.631,
  "dataset_split": "val",
  "ml_outcome": "success|partial|miss|unknown",
  "notes": "Verbose interpretation (min 15 chars)"
}
```

Optional fields: `unit`, `step`, `epoch`, `secondary`, `termination`.

---

### list_history(workspace, record_type, stage, tagged, metric, min_value, experiment_group, sort_by, desc, include_pruned, include_internal_ids, limit, offset)

List experiment records (runs + checkpoints). Primary history/query tool.

**Returns records with semantic context:**
```python
{
  "record_id": "01KFE41G...",
  "version": "v295",
  "stage": "train_25m",
  "reason": "Testing post_run SVS fix",  # From run's reason_json
  "primary_metric": {"name": "dir_acc", "value": 0.600},  # From results
  "ml_outcome": "partial",
  "age": "2h ago",  # Human-readable relative time
  "tags": ["best"]
}
```

---

### inspect_record(ref, include, workspace)

Inspect a record by record_id, stage_run_id, or @tag (workspace required for tags).

---

### tag_record(ref, tag)

Tag a record. For runs, creates both run_tag and version_tag.

---

### list_unfinalized_runs(workspace)

List terminal runs that still need finalization.

---

### get_experiment_context(workspace)

Returns baseline, pending finalizations, recent trend, regression alerts.

---

### get_debug_info(ref, workspace)

Resolve a record/tag to infra IDs and GCS/log URIs for debugging.

---

### inspect_run(run_id, include)

Infra-level run view (SVS, logs, provenance). Use `inspect_record()` for experiment results.

**Default includes:** `["dashboard", "metadata", "thoughts"]`

**Metadata fields include:**
- `workspace`, `stage`, `state`, `reason` (from reason_json)
- `started_at`, `completed_at`, `error`

**Additional include options:**
- `"svs"`: SVS validation findings
- `"provenance"`: Signal lineage and data dependencies
- `"manifest"`: Full config and I/O details
- `"attempt"`: Attempt history for retries

---

### get_lineage(run_id, direction)

Track which runs consumed this run's outputs (downstream) or produced its inputs (upstream).
**This is the essential tool for understanding experiment dependencies.**

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `run_id` | str | Yes | - | Stage run ID (e.g., "stage-abc123") |
| `direction` | str | No | "downstream" | "downstream" = who consumed my outputs; "upstream" = where did my inputs come from |

**Returns:**
```python
{
  "run_id": "stage-eac4899d",
  "direction": "downstream",
  "consumers": [  # or "producers" for upstream
    {
      "run_id": "stage-11aa...",
      "stage": "compute_state_features",
      "reason": "Testing binary labels",
      "signal_consumed": "events",  # or "signal_produced" for upstream
      "outcome": "completed",
      "age": "2h ago"
    }
  ]
}
```

**Examples:**
```python
get_lineage("stage-abc123")  # What runs used my outputs?
get_lineage("stage-abc123", direction="upstream")  # Where did my data come from?
```

---

### logs(run_id, tail, since, follow)

Get container logs from a run. Supports follow mode for cursor-based streaming.

---

### cancel(run_id, reason)

Cancel a running stage.

---

### save_results_spec(stage_run_id, record_id, spec)

Advanced: persist a results_spec after the run was created.

---

## Version Management Tools

### manage_versions(workspace, action, version, tag, reason, from_version, to_version, limit, offset)

Unified tool for tagging milestones and cleaning up history.

**Actions:** `list`, `tag`, `untag`, `prune`, `unprune`, `prune_before_tag`

**Note:** `action="list"` excludes pruned versions by default. Include `include_pruned=True` to see all versions.

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

