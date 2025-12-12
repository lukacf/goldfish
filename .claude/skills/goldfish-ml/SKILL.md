---
name: goldfish-ml
description: This skill should be used when working with Goldfish ML, an MCP server for AI-driven machine learning experimentation. Use this skill when the user asks to create workspaces, run ML pipelines, manage datasets, track experiment lineage, or conduct any ML experimentation workflow. Goldfish provides 39 MCP tools for workspace management, pipeline execution, data management, and provenance tracking.
---

# Goldfish ML

This skill enables effective use of Goldfish ML, an MCP server that transforms Claude into an ML experimentation agent with full provenance tracking, reproducibility, and infrastructure abstraction.

## Core Mental Model

Goldfish manages ML experiments through **six key abstractions**:

```
┌─────────────────────────────────────────────────────────────────┐
│                        GOLDFISH ARCHITECTURE                     │
├─────────────────────────────────────────────────────────────────┤
│  WORKSPACE          VERSION           PIPELINE                   │
│  ┌─────────┐       ┌─────────┐       ┌─────────────────────┐    │
│  │ w1/     │──────▶│ v1, v2  │       │ stages:             │    │
│  │ (slot)  │       │ (tags)  │       │   - preprocess      │    │
│  └─────────┘       └─────────┘       │   - train           │    │
│       │                              │   - evaluate        │    │
│       ▼                              └─────────────────────┘    │
│  STAGE (Docker)    SIGNAL            PROFILE                    │
│  ┌─────────┐       ┌─────────┐       ┌─────────────────────┐    │
│  │ train.py│──────▶│ features│       │ h100-spot           │    │
│  │ (module)│       │ (npy)   │       │ a100-on-demand      │    │
│  └─────────┘       └─────────┘       └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**Key invariants:**
- All infrastructure (Docker, GCS, GCE) is hidden from Claude
- User workspace is plain files (no `.git`) - all versioning handled internally
- Every `run()` creates a version BEFORE execution (100% provenance)
- Signals connect stages with typed data flow

## Workflow Decision Tree

```
START: What task?
  │
  ├─▶ "First time / Need orientation"
  │     └─▶ status() → See slots, active jobs, STATE.md
  │
  ├─▶ "Start new experiment"
  │     └─▶ create_workspace() → mount() → Edit files → run()
  │
  ├─▶ "Continue existing work"
  │     └─▶ status() → mount(slot, workspace) → Edit → run()
  │
  ├─▶ "Run ML training"
  │     └─▶ run(workspace, stages=["train"]) or run(workspace) for all
  │
  ├─▶ "Check run status"
  │     └─▶ list_runs() → get_run(run_id) → logs(run_id)
  │
  ├─▶ "Manage data"
  │     └─▶ register_dataset() or list_sources() → get_source()
  │
  ├─▶ "Track lineage"
  │     └─▶ get_workspace_lineage() → get_stage_lineage() → get_run_provenance()
  │
  └─▶ "Save progress / Switch context"
        └─▶ save_version() → hibernate() (auto-saves)
```

## Essential Workflows

### 1. Starting a New Experiment

```
1. Create workspace with clear goal
   create_workspace(name="lstm_baseline", goal="Train LSTM for price prediction")

2. Mount to edit slot
   mount(slot="w1", workspace="lstm_baseline")

3. Create workspace structure in workspaces/w1/:

   workspaces/w1/
   ├── pipeline.yaml          # Define stages and signals
   ├── requirements.txt       # Optional: only for project-specific packages
   ├── configs/
   │   ├── preprocess.yaml
   │   └── train.yaml
   └── modules/
       ├── preprocess.py
       └── train.py

   **Note:** Stages automatically use pre-built images with common ML libraries
   (numpy, pandas, torch, scikit-learn, etc.). No setup required.

4. Run the pipeline
   run("w1")                   # All stages
   run("w1", stages=["train"]) # Single stage
```

### 2. Pipeline Structure

A pipeline.yaml defines stages and their data flow:

```yaml
stages:
  - name: preprocess
    inputs:
      raw_data:
        type: dataset
        dataset: sales_v1      # Registered dataset
    outputs:
      features:
        type: npy              # NumPy array output

  - name: train
    inputs:
      features:
        type: npy
        from_stage: preprocess  # Consume from upstream
        signal: features
    outputs:
      model:
        type: directory         # Model checkpoint dir

  - name: evaluate
    inputs:
      model:
        from_stage: train
        signal: model
      features:
        from_stage: preprocess
        signal: features
    outputs:
      metrics:
        type: csv
```

### 3. Stage Implementation Pattern

Stage modules follow a consistent pattern:

```python
# modules/train.py
from goldfish.io import load_input, save_output

def main():
    # Load inputs (from /mnt/inputs/)
    features = load_input("features")  # Returns numpy array

    # Training logic
    model = train_model(features)

    # Save outputs (to /mnt/outputs/)
    save_output("model", model_dir)

if __name__ == "__main__":
    main()
```

### 4. Monitoring Runs

```
1. List recent runs
   list_runs(workspace="lstm_baseline", status="running")

2. Get run details
   get_run(run_id="stage-abc123")

3. Stream logs
   logs(run_id="stage-abc123", tail=500)

4. Cancel if needed
   cancel(run_id="stage-abc123", reason="Wrong hyperparameters")
```

### 5. Lineage & Provenance

Track exactly what produced what:

```
# Full workspace history
get_workspace_lineage("lstm_baseline")
→ versions, parent workspace, branches

# Compare versions
get_version_diff("lstm_baseline", from_version="v1", to_version="v3")
→ git commits, file changes

# Trace run inputs recursively
get_stage_lineage(run_id="stage-abc123")
→ Which preprocessing version fed this training run

# Full run provenance
get_run_provenance(stage_run_id="stage-abc123")
→ workspace, version, git SHA, config, inputs, outputs
```

## Tool Reference

### Workspace Management

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `status()` | Orientation - slots, jobs, sources | None |
| `create_workspace()` | New experiment | name, goal |
| `mount()` | Activate workspace in slot | slot, workspace |
| `hibernate()` | Deactivate (auto-saves) | slot, reason |
| `save_version()` | Create version save point | slot, message |
| `get_workspace()` | Workspace details + pipeline | workspace |
| `diff()` | Show uncommitted changes | slot |
| `rollback()` | Revert to version | slot, version, reason |
| `delete_workspace()` | Remove workspace | workspace, reason |
| `list_workspaces()` | All workspaces | limit, offset |

### Execution

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `run()` | Execute stages | workspace, stages, wait |
| `get_run()` | Run details | run_id |
| `logs()` | Container logs | run_id, tail, since |
| `cancel()` | Stop run | run_id, reason |
| `list_runs()` | Query runs | workspace, stage, status |
| `get_outputs()` | Run outputs | run_id |

### Data Management

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `register_dataset()` | Register data source | name, source, format |
| `list_sources()` | Available sources | status, created_by |
| `get_source()` | Source details | name |
| `delete_source()` | Remove source | source_name, reason |
| `get_source_lineage()` | Data provenance | source_name |
| `promote_artifact()` | Stage output → source | job_id, output_name |

### Lineage & Provenance

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `get_workspace_lineage()` | Full history | workspace |
| `get_version_diff()` | Compare versions | workspace, from/to |
| `get_run_provenance()` | Exact run inputs | stage_run_id |
| `get_stage_lineage()` | Upstream tree | run_id, max_depth |
| `list_stage_versions()` | Stage version history | workspace, stage |
| `find_runs_using_stage_version()` | Impact analysis | workspace, stage, version |

### Utility

| Tool | Purpose | Key Parameters |
|------|---------|----------------|
| `initialize_project()` | New Goldfish project | project_name, project_root |
| `reload_config()` | Hot-reload goldfish.yaml | None |
| `get_audit_log()` | Operation history | limit, workspace |
| `log_thought()` | Record reasoning | thought |

**Important:** After editing `goldfish.yaml`, always call `reload_config()` to apply changes without restarting the MCP server.

## Signal Types

| Type | Format | Use Case |
|------|--------|----------|
| `dataset` | External | Registered project data |
| `npy` | NumPy | Arrays, embeddings, tensors |
| `csv` | Pandas | Tabular data |
| `directory` | Dir | Model checkpoints, multi-file outputs |
| `file` | Single | Configs, small outputs |

## Resource Profiles

For `compute.profile` in stage configs:

| Profile | Hardware | Use Case |
|---------|----------|----------|
| `cpu-small` | 2 vCPU, 4GB | Light preprocessing |
| `cpu-large` | 8 vCPU, 32GB | Heavy data processing |
| `h100-spot` | H100 GPU, spot | Training (cost-effective) |
| `h100-on-demand` | H100 GPU | Critical training |
| `a100-spot` | A100 GPU, spot | Training alternative |
| `a100-on-demand` | A100 GPU | Guaranteed availability |

## Common Patterns

### Pattern: Iterative Experimentation

```
1. create_workspace("exp_v1", "Baseline LSTM")
2. mount("w1", "exp_v1")
3. Edit code, run, analyze
4. save_version("w1", "Working baseline")  # Creates v1
5. Edit more, run
6. hibernate("w1", "Completed baseline")

# Later: branch for variation
7. create_workspace("exp_v2", "Add attention", fork_from="exp_v1")
```

### Pattern: Debug Failed Run

```
1. list_runs(status="failed")
2. get_run(run_id)           # See error, config
3. logs(run_id, tail=500)    # Full error trace
4. get_run_provenance(run_id) # What inputs were used
5. Fix code, run again
```

### Pattern: Reproduce Past Result

```
1. get_run_provenance(stage_run_id)
   → Returns: workspace, version, git_sha, config, inputs

2. rollback(slot, snapshot_id)  # Restore exact code state

3. run(workspace, stages, config_override)  # Re-run with same config
```

### Pattern: Impact Analysis

```
"If I change preprocessing, what's affected?"

1. list_stage_versions("my_experiment", stage="preprocess")
   → preprocessing-v1, v2, v3...

2. find_runs_using_stage_version("my_experiment", "preprocess", 3)
   → All training runs that consumed preprocessing-v3
```

## Audit & Context Recovery

Goldfish maintains full audit trails:

```
log_thought("Switching to attention mechanism because...")

get_audit_log(limit=20, workspace="lstm_baseline")
→ All operations with reasons and timestamps
```

After context compaction, always start with:
```
status()  # Recover orientation: slots, jobs, STATE.md
```

## Best Practices

1. **Always provide clear goals** when creating workspaces
2. **Checkpoint frequently** with descriptive messages
3. **Use descriptive stage names** that reflect the operation
4. **Register datasets** before referencing in pipelines
5. **Check status()** after context recovery
6. **Use log_thought()** to document decisions
7. **Monitor long runs** with logs() and list_runs()

## Resources

Detailed documentation is available in the references directory:

- `references/config_reference.md` - **goldfish.yaml schema and GCE/GCS setup**
- `references/tools_reference.md` - Complete tool API documentation
- `references/pipeline_guide.md` - Pipeline YAML specification
- `references/stage_authoring.md` - Stage module development guide
- `references/end_to_end_example.md` - Complete worked example from scratch
