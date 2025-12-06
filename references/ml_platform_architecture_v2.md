# Goldfish - ML Platform Architecture v2

## Overview

**Goldfish** is an ML experimentation platform designed for **agentic AI systems** (Claude Code) as the primary user. Solves context compaction, parallel work, and code confusion problems unique to AI-driven development.

*Named ironically: this platform solves the "goldfish memory" problem—AI forgetting context after compaction.*

---

## Core Problem

Agentic AI makes hundreds of tool calls per session, generating millions of tokens. This causes:

| Problem | Description |
|---------|-------------|
| **Groundhog Day** | After context compaction, AI reverts to deprecated configs |
| **Parallel Work Overload** | Jobs take hours; AI must work on multiple things simultaneously |
| **Code Confusion** | Similar code in multiple directories causes grep pollution |
| **Memory Loss** | Context compaction loses critical details |

---

## Architecture

### Three-Repo Model

```
┌─────────────────────────────────────────────────────────────┐
│  Goldfish (MCP Server)                                      │
│  - General-purpose ML platform (reusable across projects)   │
│  - Manages versioning, jobs, artifacts                      │
│  - Creates one "-dev" repo per project                      │
└──────────────────────┬──────────────────────────────────────┘
                       │ MCP interface
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  mlm (Project Repo) - "Where Claude Lives"                  │
│  - Thin: docs/, data/, workspaces/                          │
│  - No experiment code at top level                          │
│  - workspaces/w1-w3 are mount points                        │
└─────────────────────────────────────────────────────────────┘
                       │
                       │ worktrees mounted from
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  mlm-dev (Platform Database) - "Claude Never Sees This"     │
│  - 100% managed by ML Platform                              │
│  - Git branches = workspaces                                │
│  - Git tags = snapshots                                     │
│  - main branch = production/stable code                     │
│  - Pushed to GitHub for backup/review                       │
└─────────────────────────────────────────────────────────────┘
```

### What Claude Sees

```
mlm/
  workspaces/
    w1/                  ← Git worktree (branch in mlm-dev)
    w2/                  ← Git worktree
    w3/                  ← Git worktree (or empty)
  docs/                  ← Project documentation (read)
  data/                  ← Data source configs (read)
  STATE.md               ← Per-project context (maintained by Claude + Platform)
```

### What Claude Does NOT See

- `mlm-dev/` repo (invisible)
- Git commands (hidden behind MCP)
- Old experiments (branches in mlm-dev, not directories)
- Platform internals

---

## Key Design Decisions

### 1. Hydraulic Git

Git as storage engine, completely hidden from Claude.

| MCP Tool | Git Operation (Hidden) |
|----------|------------------------|
| `create_workspace(name)` | `git branch experiment/{name} main` |
| `mount(name, slot)` | `git worktree add workspaces/w{slot} experiment/{name}` |
| `checkpoint(slot, msg)` | `git add . && git commit && git tag snap-{hash}` |
| `hibernate(slot)` | `git push origin && git worktree remove` |

**Benefits:** Instant mount/hibernate, full history, GitHub backup, diffable, PR workflow for humans.

### 2. Rule of Three (Soft)

Maximum 3 active workspaces. Enforced as **warning, not hard block**.

- Slot 1: Current work
- Slot 2: Reference implementation
- Slot 3: Experimental branch

Platform warns at >3, suggests hibernation, but allows override.

### 3. Jobs Run on Snapshots

```
run_job(slot=1, script="train.py")
  → Creates immutable snapshot (git tag)
  → Launches job against snapshot
  → Claude can keep editing slot or hibernate it
  → Job result links back to snapshot
```

Decouples editing from execution. No "which version ran?" confusion.

### 4. Enforced Intent Logging

Every state-changing operation requires `reason` parameter (min 15 chars):

```python
edit_file(slot=1, path="run_tbpe.py",
          reason="Remove aggregate_sequence_labels to fix MI gating")
```

Creates audit trail that survives context compaction.

### 5. STATE.md (Not Librarian v1)

Single context file per project, maintained by Claude + Platform:

```markdown
# Project: MLM
## Active Goal
Fix TBPE label alignment

## Workspaces
- w1: fix-tbpe (DIRTY) - debugging assertion at line 44
- w2: v5-reference (CLEAN) - known working baseline
- w3: [empty]

## Configuration Invariants (DO NOT CHANGE)
- Tokenizer: V5 (8-bin quantile)
- Vocab: 10 tokens (NOT 512)

## Recent Actions
- [14:30] Edited run_tbpe.py (remove aggregation)
- [14:25] Job run-502 FAILED (assertion)

## Background Jobs
- run-503: training (45%, ETA 2h)
```

**Librarian deferred** - evaluate if STATE.md is sufficient after 1 month.

### 6. Promotion Workflow

"Production" = `main` branch in `mlm-dev`.

```
1. Claude finishes experiment in w1
2. Claude: checkpoint(slot=1, msg="TBPE fix complete, run-505 passed")
3. Human: Reviews PR on GitHub (experiment/fix-tbpe → main)
4. Human: Merges PR
5. Platform: Updates mlm-dev/main
6. Next workspace forks from updated main
```

Claude never directly edits "production" - only workspaces. Human controls promotion.

---

## MCP Interface

### Context & Navigation

| Tool | Description |
|------|-------------|
| `status()` | Current slots, jobs, sync state |
| `mount(workspace, slot)` | Load workspace into slot |
| `hibernate(slot, reason)` | Save and free slot |
| `create_workspace(name, goal, reason)` | New workspace from main |
| `list_workspaces()` | All workspaces (active + hibernated) |

### File Operations (Scoped to Slots)

| Tool | Description |
|------|-------------|
| `read_file(slot, path)` | Read from workspace |
| `edit_file(slot, path, edits, reason)` | Edit with intent |
| `search(slot, pattern)` | Search within ONE slot only |
| `diff(slot)` | Changes since last checkpoint |
| `diff_workspaces(slot_a, slot_b)` | Compare two workspaces |

### Versioning

| Tool | Description |
|------|-------------|
| `checkpoint(slot, message)` | Create snapshot (git commit + tag) |
| `rollback(slot, snapshot_id)` | Revert to previous snapshot |
| `history(slot)` | List snapshots for workspace |

### Execution

| Tool | Description |
|------|-------------|
| `run_job(slot, script, config, reason)` | Launch on snapshot |
| `job_status(job_id)` | Status, logs, errors |
| `job_logs(job_id, lines, filter)` | Retrieve logs |
| `cancel_job(job_id, reason)` | Stop running job |
| `list_artifacts(job_id)` | Outputs from job |

### Context Recovery

| Tool | Description |
|------|-------------|
| `get_state()` | Read STATE.md |
| `log_thought(thought, slot)` | Record reasoning for audit |

---

## Artifact Management (To Be Designed)

Open questions:
- Where do job outputs (models, tokens, metrics) live?
- How does Claude reference artifacts from previous jobs?
- Cross-workspace artifact dependencies?

Likely: Global artifact store with ID-based references, not file paths.

---

## Implementation Phases

### Phase 1: Core (Week 1-2)
- [ ] Workspace mount/hibernate (git worktree)
- [ ] STATE.md auto-update
- [ ] Audit trail (SQLite)
- [ ] Basic MCP tools

### Phase 2: Execution (Week 3-4)
- [ ] Snapshot-on-job-launch
- [ ] Job orchestration integration
- [ ] Log retrieval
- [ ] Artifact tracking (basic)

### Phase 3: Polish (Week 5-6)
- [ ] Cross-workspace diff
- [ ] Rollback
- [ ] Snapshot garbage collection
- [ ] GitHub sync on hibernate

### Deferred (Evaluate After Month 1)
- [ ] Librarian agent
- [ ] Hard slot limits
- [ ] Hierarchical summarization
- [ ] Advanced artifact lineage

---

## Migration Path

Current `mlm/experiments/` structure → new model:

1. Create `mlm-dev` repo
2. Import existing experiments as branches
3. Update `mlm` structure (thin: docs/, data/, workspaces/)
4. Move current code to `mlm-dev/main`
5. Workspaces fork from there

---

## Summary

| Concern | Solution |
|---------|----------|
| Context compaction | STATE.md + audit trail |
| Parallel work | 3 slots (soft limit) |
| Code confusion | Isolated workspaces, no grep across experiments |
| Version control | Hydraulic Git (hidden) |
| Job confusion | Snapshots + artifact linking |
| Human review | Standard GitHub PRs on mlm-dev |
| Promotion | Merge to main, Claude never touches production |
| Backup | hibernate() pushes to GitHub |
| Cross-machine | mount() fetches if needed |

**Core insight:** Goldfish as source of truth. Claude queries, doesn't remember. Git provides the backend, MCP provides the interface.
