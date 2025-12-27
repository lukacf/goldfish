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
- **Low-Latency Sync**: `inspect_run` triggers a metadata signal to the cloud container for real-time "Overdrive" synchronization.

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
  ├─▶ "Run ML training"
  │     └─▶ run(workspace, stages=["train"]) or run(workspace) for all.
  │
  ├─▶ "Check run progress/health"
  │     └─▶ inspect_run(run_id) → Dashboard (Trends ↑↓), GPU Health, Provenance.
  │
  ├─▶ "Deep debug failure"
  │     └─▶ inspect_run(run_id, include=["manifest", "svs"]) + logs(run_id).
  │
  ├─▶ "Manage versions/tags"
  │     └─▶ manage_versions(workspace, action="tag|prune|list")
  │
  ├─▶ "Manage data sources"
  │     └─▶ manage_sources(action="list|get|lineage|update") or register_source()
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
        # Signal "I'm alive" for long jobs to prevent auto-termination
        heartbeat(f"Training epoch {epoch}") 
        
        loss = train_step(features)
        
        # Log progress for the Real-time Dashboard
        log_metrics({"loss": loss, "train/accuracy": 0.85}, step=epoch)

    # 3. Save outputs (to /mnt/outputs/)
    save_output("model", model_dir)
    
    # 4. Finalize metrics (ensures flush to storage)
    finish()

if __name__ == "__main__":
    main()
```

## 2. Data Source Metadata (Strict)

All new sources/artifacts must include **mandatory metadata**. Goldfish does not infer schema and treats local vs GCS data identically.

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
    "kind": "tensor|tabular|file",
    "arrays": { ... } // for tensor
  }
}
```

## 3. Monitoring & Lineage

### Monitoring Runs
1. **List recent runs**: `list_runs(workspace="my-task", status="running")`
2. **Dashboard Overview**: `inspect_run(run_id)` (Triggers real-time sync).
3. **Deep Dive**: `inspect_run(run_id, include=["manifest", "svs"])` to see configs and semantic findings.
4. **Stream logs**: `logs(run_id, tail=500)` for raw error traces.
5. **Cancel**: `cancel(run_id, reason="Diverging loss")`.

### Lineage & Provenance
Track exactly what produced what to ensure 100% reproducibility:
- **Workspace Lineage**: `inspect_workspace(name)` shows parent workspace and branches.
- **Run Provenance**: `inspect_run(run_id, include=["provenance"])` returns exact `git_sha`, `version`, and `config` used.
- **Data Provenance**: `manage_sources(action="lineage", name="source-id")` shows the creating job and its inputs.

## 4. Version Management (Tags & Pruning)

Experimentation generates noise. Use tags to mark milestones and pruning to clean up.

### Tags (Milestones)
Mark significant versions with memorable names (can be applied retroactively):
- `manage_versions(workspace="w1", action="tag", version="v24", tag="baseline-working")`
- `manage_versions(workspace="w1", action="tag", version="v47", tag="best-model")`

### Pruning (Cleanup)
Pruning hides noise versions while preserving the audit trail. **Tagged versions are protected from pruning.**
- `manage_versions(workspace="w1", action="prune", version="v5", reason="Crashed")`
- `manage_versions(workspace="w1", action="prune", from_version="v1", to_version="v23", reason="Before baseline")`
- **Restore**: `manage_versions(action="unprune", version="v5")`

## 5. Tool & Profile Reference

### Signal Types
| Type | Format | Use Case |
|------|--------|----------|
| `npy` | NumPy | Arrays, embeddings, tensors |
| `csv` | Pandas | Tabular data |
| `directory` | Dir | Model checkpoints, multi-file outputs |
| `file` | Single | Configs, small outputs |

### Resource Profiles (`compute.profile`)
| Profile | Hardware | Use Case |
|---------|----------|----------|
| `cpu-small` | 2 vCPU, 4GB | Light preprocessing |
| `cpu-large` | 8 vCPU, 32GB | Heavy data processing |
| `h100-spot` | H100 GPU, spot | Training (cost-effective) |
| `h100-on-demand` | H100 GPU | Critical training |
| `a100-spot` | A100 GPU, spot | Training alternative |

## 6. Troubleshooting & Common Patterns

### SVS Pre-Run Blocks
If `run()` returns `BLOCKED`:
1. **Examine Findings**: Read error messages in the tool output for logic/config flaws.
2. **Apply Fixes**: Edit the files in your workspace slot.
3. **Re-run**: Simply call `run()` again. Bypass only via `skip_review=True` if certain.

### Long-Running Failures
- **Timeout**: Increase `timeout` in `goldfish.yaml` or `hints.timeout` in stage config.
- **Heartbeat Stale**: Ensure code calls `heartbeat()` at least every 10 mins (default).

### Common Failure Patterns (Agent Knowledge)
Be alert for these ML anti-patterns that Goldfish flags:
- **Silent Feature Degradation**: Low entropy signals (e.g. constant values) instead of features.
- **Label/Token Desynchronization**: Failing to update labels when BPE tokens are merged.
- **Horizon Mismatch**: Predicting at high frequency with low-frequency targets.
- **Tool Assumption Mismatch**: Feeding dense data to tools expecting sequence-level inputs.

## Best Practices
1. **Goal First**: Always provide clear goals when creating workspaces.
2. **Inspect First**: Use `inspect_run` dashboard instead of raw logs to save tokens.
3. **Tag Early**: Tag working versions before attempting risky refactors.
4. **Document Reasoning**: Use `log_thought()` to explain architectural or hyperparameter changes.
5. **Sync Hot-Reload**: After editing `goldfish.yaml`, call `reload_config()` to apply changes.