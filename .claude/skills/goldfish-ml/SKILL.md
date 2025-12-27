---
name: goldfish-ml
description: This skill should be used when working with Goldfish ML, an MCP server for AI-driven machine learning experimentation. Use this skill for workspace management, pipeline execution, data registry operations, and provenance tracking. Goldfish provides a consolidated set of 24 master tools for efficient ML workflows.
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
- Every `run()` creates an immutable version BEFORE execution (100% provenance).
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

## Essential Master Tools

### 1. `inspect_run(run_id)` (The Dashboard)
The single source of truth for any execution. Always triggers a fresh sync.
- **Dashboard**: Step/Epoch progress + Metric Trends (↑↓) + ETA.
- **Health**: GPU utilization, VRAM usage, Heartbeat status.
- **Provenance**: Traces exactly which input versions produced the results.
- **Manifest**: Full config, resolved inputs, and output locations.

### 2. `inspect_workspace(name)` (The Overview)
Unified view of a workspace's purpose and state.
- **Goal & Pipeline**: What this workspace is for and how it works.
- **Lineage Tree**: Parent relationship (if branched) and child branches.
- **Version History**: Timeline of immutable versions and their tags.

### 3. `manage_versions(workspace, action)`
Consolidated interface for version maintenance.
- `action="tag"`: Mark milestones (e.g., "baseline", "best").
- `action="prune"`: Hide failed/intermediate versions (protected if tagged).
- `action="list"`: View full history including pruned versions.

### 4. `manage_sources(action)`
Central registry for all data (external datasets and promoted artifacts).
- `action="list"`: Discover available data.
- `action="lineage"`: Trace data back to the job and inputs that produced it.

## Data Source Metadata (Required, Strict)

All new sources/datasets/artifacts must include **mandatory metadata**.

### Required Top-Level Structure
```json
{
  "schema_version": 1,
  "description": "Human/LLM description (min 20 chars)",
  "source": { "format": "npy|csv|file", "size_bytes": 1234, "created_at": "ISO-TS" },
  "schema": { "kind": "tensor|tabular|file", ... }
}
```

## Best Practices

1. **Always use `inspect_run`** instead of raw `logs` for monitoring progress.
2. **Tag significant milestones** (e.g., "v24" -> "baseline-working") before pruning noise.
3. **Check `status()`** after every context recovery/compaction to re-orient.
4. **Provide structured reasons** in `run()` to improve the automated pre-run review quality.
5. **Use `log_thought()`** to document architectural decisions for the audit trail.

## Troubleshooting

### Pre-Run Blocks
If `run()` returns `BLOCKED`:
1. Analyze the SVS findings in the tool output.
2. Fix the code/config in your workspace slot.
3. Re-run. Bypass only if necessary via `skip_review=True`.

### Stale Run Data
If `inspect_run` data feels old, call it again. It sends a low-latency "Overdrive" sync signal to the container via Instance Metadata on every invocation.