---
name: goldfish-ml
description: This skill should be used when working with Goldfish ML, an MCP server for AI-driven machine learning experimentation. Use this skill for workspace management, pipeline execution, data registry operations, and provenance tracking. Goldfish provides 24 master tools for efficient ML workflows.
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
- All infrastructure (Docker, GCS, GCE) is hidden from Claude.
- User workspace is plain files (no `.git`) - all versioning handled internally.
- Every `run()` creates a version BEFORE execution (100% provenance).
- **Inspect-First Workflow**: Use `inspect_run` for real-time dashboards and `inspect_workspace` for global orientation.

## Workflow Decision Tree

```
START: What task?
  │
  ├─▶ "First time / Need orientation"
  │     └─▶ status() → See slots, active jobs, recent audit trail.
  │
  ├─▶ "Start new experiment"
  │     └─▶ create_workspace() → mount() → Edit files → run()
  │
  ├─▶ "Inspect existing workspace"
  │     └─▶ inspect_workspace(name) → Goal, Pipeline, Lineage, Tags.
  │
  ├─▶ "Check run progress/health"
  │     └─▶ inspect_run(run_id) → Dashboard (Trends ↑↓), GPU Health, Provenance.
  │
  ├─▶ "Deep debug failure"
  │     └─▶ inspect_run(run_id, include=["manifest", "svs"]) + logs(run_id).
  │
  ├─▶ "Manage data sources"
  │     └─▶ manage_sources(action="list|get|lineage") or register_source()
  │
  └─▶ "Save progress / Switch context"
        └─▶ save_version() → hibernate() (auto-saves)
```

## 1. Stage Implementation Pattern (Inside Container)

Stage modules follow a consistent pattern. Use `goldfish.io` for data and `goldfish.metrics` for tracking.

```python
# modules/train.py
from goldfish.io import load_input, save_output, heartbeat
from goldfish.metrics import log_metric, log_metrics, finish

def main():
    # 1. Load inputs (from pipeline signals)
    features = load_input("features") 

    # 2. Training logic
    for epoch in range(10):
        # Signal "I'm alive" for long jobs
        heartbeat(f"Training epoch {epoch}") 
        
        loss = train_epoch(features)
        
        # Log progress for the Real-time Dashboard
        log_metrics({"loss": loss, "train/accuracy": 0.85}, step=epoch)

    # 3. Save outputs (to /mnt/outputs/)
    save_output("model", model_dir)
    
    # 4. Finalize metrics
    finish()

if __name__ == "__main__":
    main()
```

## 2. Pipeline Structure (pipeline.yaml)

Define stages and their data flow contracts. **Schemas are required.**

```yaml
stages:
  - name: preprocess
    inputs:
      raw_data:
        type: dataset
        dataset: sales_v1
    outputs:
      features:
        type: npy
        schema:
          kind: tensor
          shape: [null, 768]
          dtype: float32

  - name: train
    inputs:
      features:
        from_stage: preprocess
        signal: features
        schema:
          kind: tensor
          shape: [null, 768]
    outputs:
      model:
        type: directory
        schema: null
```

## 3. Data Source Metadata (Strict)

All new sources must include mandatory metadata for SVS validation.

### Required Structure
```json
{
  "schema_version": 1,
  "description": "Descriptive text (min 20 chars)",
  "source": {
    "format": "npy|npz|csv|file",
    "size_bytes": 123456,
    "created_at": "2025-12-27T10:00:00Z"
  },
  "schema": {
    "kind": "tensor",
    "arrays": {
      "features": { "role": "features", "shape": [1000, 768], "dtype": "float32" }
    }
  }
}
```

## 4. Master Tool Reference

| Category | Tool | Key Purpose |
| :--- | :--- | :--- |
| **Orientation** | `status()` | Get project context, slots, and active jobs. |
| | `inspect_run()` | **The Dashboard.** Progress, Trends ↑↓, Health, and Lineage. |
| | `inspect_workspace()` | Full view of Goal, Pipeline, and Version history. |
| **Execution** | `run()` | Launch stages/pipelines with pre-run AI review. |
| | `logs()` | Raw text stream for deep debugging. |
| | `cancel()` | Immediate stop of a running job. |
| | `mark_outcome()` | Manually classify run result as `success` or `bad_results`. |
| **Versioning** | `manage_versions()` | Unified interface for `tag`, `prune`, and `list` actions. |
| | `save_version()` | Immutable save point for the current slot state. |
| | `rollback()` | Revert slot to a specific version (destructive). |
| | `diff()` | Compare slots, versions, or workspaces. |
| **Data** | `manage_sources()` | Unified interface for `list`, `get`, `lineage`, and `update`. |
| | `register_source()` | Register external GCS data or local datasets. |
| | `promote_artifact()` | Turn a run output into a registered source. |
| **Debug** | `manage_patterns()` | Manage SVS failure patterns and AI librarian reviews. |
| | `search_goldfish_logs()` | Search centralized logs via VictoriaLogs (incl. guide). |
| | `log_thought()` | Record reasoning in the project audit trail. |

## 5. Common Patterns

### Iterative Development
1. `status()` to find an empty slot.
2. `mount(slot="w1", workspace="my-task")`.
3. Edit code in `workspaces/w1/`.
4. `run("w1")` → Watch progress via `inspect_run(run_id)`.
5. If success: `manage_versions(workspace="my-task", action="tag", tag="working")`.
6. `manage_versions(action="prune")` to clean up the intermediate failures.

### Reproducing a Past Result
1. `inspect_run(run_id, include=["provenance"])` to get the exact `version`.
2. `rollback(slot="w2", version="v12")`.
3. `run("w2")` with the same config overrides.

## Best Practices
1. **Never use raw `logs`** if `inspect_run` dashboard provides the answer.
2. **Always trigger a sync**: `inspect_run` automatically sends a metadata signal to the cloud container to flush data.
3. **Use slots**: Work in `w1`, `w2`, etc., to keep experiments isolated.
4. **Document Decisions**: Use `log_thought()` whenever you make a significant change to architecture or hyperparameters.
