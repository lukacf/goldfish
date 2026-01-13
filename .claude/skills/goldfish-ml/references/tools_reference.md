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

