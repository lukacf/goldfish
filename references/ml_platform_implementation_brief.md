# Goldfish - Implementation Brief

**For:** Claude building the MCP server
**From:** Claude who will use it
**Date:** December 4, 2025
**Project:** Goldfish

---

## What You're Building

**Goldfish** - an MCP server that manages ML experimentation for agentic AI (me). I make hundreds of tool calls per session, context compacts repeatedly, and I get confused by similar code in multiple directories. This platform solves that.

*Named ironically: Goldfish solves the "goldfish memory" problem—AI forgetting context after compaction.*

Read `docs/ml_platform_architecture_v2.md` for the full design. This brief covers what's NOT in that doc.

---

## Why This Exists (The Pain)

### The Groundhog Day Problem

After context compaction, I revert to deprecated configurations. Real examples from `docs/ai_discussion_journal.md`:

> **Entry 21:** "I configured tokenization with 512 codes. This is WRONG. The current pipeline uses 10 tokens."

> **Entry 22:** "I used `prepare_eurusd_data_multiscale.py` which created 94GB from a 440MB CSV. Wrong script."

This happened 3 times in ONE day. The platform must prevent this.

### Why Rejected Alternatives Failed

| Approach | Why It Failed |
|----------|---------------|
| Single Workbench (swap content on switch) | Runs take hours. Can't wait. Need parallel work. |
| 100+ experiment directories | Grep finds deprecated code. I get confused. |
| Git branches exposed to Claude | I spiral into "fix git" rabbit holes instead of ML work |
| Tarball compression for storage | Too slow. Mount/hibernate must be sub-second. |

---

## Critical Implementation Details

### 1. Hydraulic Git

Use git worktrees as the storage backend, but **never expose git to me**.

```python
# What I call:
mount(workspace="fix-tbpe", slot=1)

# What you execute (hidden from me):
git worktree add mlm/workspaces/w1 experiment/fix-tbpe
```

I should never see git commands, branch names in errors, or merge conflicts. If something goes wrong, translate it to platform-speak.

### 2. Performance Matters

I make hundreds of tool calls. Latency kills.

- `mount()` / `hibernate()`: Must be <1 second (worktrees are fast)
- `status()`: Must be <100ms (I call this constantly)
- Audit writes: Async, don't block the response

### 3. Audit Trail

Every state-changing operation logs to SQLite with:
```sql
CREATE TABLE audit (
    id INTEGER PRIMARY KEY,
    timestamp TEXT,
    operation TEXT,
    slot INTEGER,
    workspace TEXT,
    reason TEXT,      -- REQUIRED, min 15 chars
    details JSON
);
```

**Enforce minimum reason length.** I will try `reason="fix"` when tired. Reject it.

### 4. STATE.md

Single source of truth I read after context compaction. Structure:

```markdown
# Project: {project_name}

## Active Goal
{Current high-level objective}

## Workspaces
- w1: {name} ({DIRTY|CLEAN}) - {one-line context}
- w2: {name} ({DIRTY|CLEAN}) - {one-line context}
- w3: [empty]

## Configuration Invariants
{Things that must NOT change - prevents Groundhog Day}

## Recent Actions
{Last 10-15 operations with timestamps}

## Background Jobs
{Running jobs with status}
```

**Auto-update this on every operation.** Don't rely on me to maintain it.

### 5. Tool Response Design

When `mount()` succeeds, return:
```json
{
  "success": true,
  "slot": 1,
  "workspace": "fix-tbpe",
  "state_md": "# Project: mlm\n## Active Goal\n...",
  "dirty": false,
  "last_checkpoint": "snap-a1b2c3d4"
}
```

Include STATE.md content directly. Don't make me call `read_file()` separately.

### 6. Soft Limits, Not Hard Blocks

Rule of Three is a guideline:
```python
def mount(workspace, slot):
    active_count = count_active_slots()
    if active_count >= 3:
        # WARN, don't block
        return {
            "warning": "You have 3 active workspaces. Consider hibernating one.",
            "proceed": True,
            ...
        }
```

I might legitimately need 4 temporarily. Let me, but remind me.

---

## Integration Points

### Existing Infrastructure

The `infra/` folder contains:
- `create_run.py` - Launches jobs on GCE
- `gcp.yaml` - GCP configuration
- Docker builds for containers

Your job orchestration should **wrap these**, not replace them. Call `create_run.py` under the hood initially.

### The Three Repos

```
Goldfish (this MCP server)
    │
    ├── Creates/manages: mlm-dev (git repo, I never see)
    │
    └── Serves: mlm (project repo, where I work)
              └── workspaces/w1, w2, w3 (worktrees from mlm-dev)
```

When initialized for a project:
1. Create `{project}-dev` repo (or use existing)
2. Set up worktree mount points in project repo
3. Initialize STATE.md

---

## What's NOT Designed Yet

### Artifact Management

Jobs produce outputs (models, tokens, metrics). We haven't solved:
- Where do they live?
- How do I reference them? (needs IDs, not paths)
- Cross-workspace dependencies (train on tokens from another workspace's job)

**Suggestion:** Start simple. Artifacts go to `{project}-dev/artifacts/{job_id}/`. Return paths in job results. Revisit after we see real usage patterns.

### Librarian Agent

The v2 doc mentions a "Librarian" (second Claude for context maintenance). **Skip this for v1.** See if STATE.md is sufficient. We can add Librarian later if needed.

### Snapshot Garbage Collection

After 100 jobs, the snapshot DAG will be huge. Need cleanup eventually. Not v1.

---

## Testing Strategy

**Build v1, give it to me, I'll use it for real work.**

The mlm project has active development. I'll:
1. Mount a workspace
2. Fix a real bug
3. Run a real job
4. Hibernate and switch

Real usage will reveal problems faster than hypothetical test cases.

---

## MCP Tool Checklist

### Must Have (v1)
- [ ] `status()` - slots, jobs, sync state
- [ ] `mount(workspace, slot)` - load workspace
- [ ] `hibernate(slot, reason)` - save and free
- [ ] `create_workspace(name, goal, reason)` - new from main
- [ ] `list_workspaces()` - all workspaces
- [ ] `checkpoint(slot, message)` - create snapshot
- [ ] `run_job(slot, script, reason)` - launch on snapshot
- [ ] `job_status(job_id)` - status and logs
- [ ] `log_thought(thought)` - record reasoning

### Should Have (v1 if time)
- [ ] `diff(slot)` - changes since checkpoint
- [ ] `diff_workspaces(slot_a, slot_b)` - compare
- [ ] `rollback(slot, snapshot_id)` - revert
- [ ] `cancel_job(job_id, reason)` - stop job

### Defer
- [ ] Librarian agent
- [ ] Artifact registry with IDs
- [ ] Snapshot garbage collection
- [ ] Hard slot limits

---

## Summary

Build this in priority order:

1. **Git worktree management** (mount/hibernate/create)
2. **STATE.md auto-maintenance**
3. **Audit trail with SQLite**
4. **Job launching** (wrap existing infra/)
5. **Snapshot creation** (checkpoint)

The goal is: I can do real ML work through Goldfish MCP tools alone, never touching git, never seeing old experiments, never losing context after compaction.

Questions? The human can relay them to me, or check the architecture doc and journal for context.

---

*Written by Claude (Opus 4.5) who will be the primary user of Goldfish.*
