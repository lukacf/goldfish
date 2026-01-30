# Documentation Audit Report

> **Date:** 2026-01-29
> **Auditor:** doc-auditor (docs-overhaul team)
> **Context:** Post de-googlify refactor audit

---

## Executive Summary

The de-googlify refactor introduced a cloud abstraction layer (`cloud/protocols.py`, `cloud/contracts.py`, `cloud/adapters/`). CLAUDE.md was updated on 2026-01-29 to reflect this. However, several other docs contain outdated references or need review.

**Overall Status:**
- **29 markdown files** reviewed (excluding .venv, .rct, generated files)
- **5 files** contain outdated references to removed/moved code
- **4 files** are redundant/duplicative
- **2 critical docs** are current and accurate
- **3 areas** need new documentation

---

## 1. Accurate / Current Docs (No Changes Needed)

| File | Last Modified | Status |
|------|---------------|--------|
| `CLAUDE.md` | 2026-01-29 | Current - Updated with cloud abstraction layer |
| `docs/GETTING_STARTED.md` | 2026-01-29 | Current - Updated in de-googlify commit |
| `docs/de-googlify/COMPLETED.md` | 2026-01-24 | Current - Accurate archive of refactor |
| `docs/ARCHITECTURE_REVIEW.md` | 2026-01-28 | Current - References correct file paths |
| `docs/ARCHITECTURE_PROPOSAL.md` | 2026-01-28 | Current - Good architectural roadmap |
| `docs/state-machine-spec.md` | 2026-01-29 | Current - Updated file references |
| `docs/state-machine-implementation.md` | 2026-01-29 | Current - Updated file references |
| `docs/PUBLIC_RELEASE_CHECKLIST.md` | 2026-01-29 | Current - Updated file references |

---

## 2. Outdated Docs (Need Updates)

### 2.1 GEMINI.md (MEDIUM PRIORITY)

**Last Modified:** 2025-12-27
**Issues:**
- References `src/goldfish/infra/` for execution backends instead of `cloud/adapters/`
- Line 88: `- src/goldfish/infra/: Execution backends (Docker, GCE).`
- Missing cloud abstraction layer in "File Structure Reference"
- Lists only "Six Abstractions" but CLAUDE.md now documents "Cloud Abstraction Layer" as #8

**Recommendation:** Update file structure section to match CLAUDE.md. Add cloud abstraction layer to architecture overview.

### 2.2 CONTRIBUTING.md (MEDIUM PRIORITY)

**Last Modified:** 2025-12-27
**Issues:**
- Line 137: References `src/goldfish/jobs/stage_executor.py` for "Change how Docker images are built or launched" - still accurate but incomplete
- Missing mention of `cloud/adapters/` for adding new backends
- Key Subsystems table doesn't include cloud abstraction

**Recommendation:** Add "Cloud Backends" row to Key Subsystems table pointing to `cloud/adapters/`.

### 2.3 ROADMAP.md (LOW PRIORITY - MOSTLY OBSOLETE)

**Last Modified:** 2025-12-22
**Issues:**
- Line 382-383: "Cloud abstraction branch: Remote branch exists with partial GCP abstraction (needs updating)" - This is now DONE
- Section 2 "Multi-Cloud Compute Backends" - Partially implemented via de-googlify
- `ComputeBackend` protocol example uses different interface than actual `RunBackend` protocol
- Several TODO items are now complete

**Recommendation:** Either update to reflect current state or archive. Current roadmap is misleading about what's implemented vs planned.

### 2.4 docs/svs.md (LOW PRIORITY)

**Last Modified:** 2025-12-27
**Issues:**
- Generally accurate but doesn't mention integration with new cloud abstraction
- References to container-side execution don't mention protocol-based backend

**Recommendation:** Minor update to mention RunBackend protocol in execution flow.

### 2.5 docs/GCP_SETUP.md (MINOR)

**Last Modified:** 2026-01-12
**Issues:**
- Still accurate for GCP-specific setup
- Could mention that Goldfish now has a pluggable backend architecture

**Recommendation:** Add note at top that this is for GCP-specific setup when using GCE backend.

---

## 3. Redundant / Duplicative Docs

### 3.1 docs/de-googlify/ folder

Contains multiple related files that overlap:
- `CHECKLIST.md` - Original working checklist
- `COMPLETED.md` - Archived completion status
- `REVIEW_REPORT.md` - Initial audit report
- `GATE_VALIDATION.md` - Validation criteria
- `LOCAL_PARITY_SPEC.md` - Parity specification

**Recommendation:** Keep only `COMPLETED.md` as the archive. The others are working documents that are now historical artifacts. Consider moving to `.claude/plans/de-googlify-archive/` or deleting.

### 3.2 GEMINI.md vs CLAUDE.md

`GEMINI.md` is a simplified version of `CLAUDE.md` that's now outdated.

**Recommendation:** Either:
1. Delete GEMINI.md and use CLAUDE.md for all AI assistants
2. Update GEMINI.md to import/reference CLAUDE.md sections

### 3.3 docs/ARCHITECTURE_REVIEW.md vs docs/ARCHITECTURE_PROPOSAL.md

These complement each other well (Review = issues, Proposal = fixes). Both are current.

**Recommendation:** Keep both. Consider adding cross-references between them.

---

## 4. Missing Documentation

### 4.1 Cloud Abstraction Layer Guide (NEEDED)

No standalone doc explaining:
- How to add a new backend (Kubernetes, AWS Batch, etc.)
- RunBackend / ObjectStorage / ImageBuilder protocols in detail
- BackendCapabilities configuration
- Testing patterns for new backends

**Location:** Should be `docs/CLOUD_ABSTRACTION.md` or `docs/ADDING_BACKENDS.md`

**Note:** CLAUDE.md has a brief "Adding a New Backend" section but it's minimal.

### 4.2 Protocol Reference (NEEDED)

No API reference for:
- `cloud/protocols.py` interfaces
- `cloud/contracts.py` data classes
- Method signatures and expected behavior

**Location:** Should be auto-generated or `docs/PROTOCOL_REFERENCE.md`

### 4.3 Backend Capabilities Reference (NEEDED)

No documentation of what each BackendCapabilities field means and when to use it.

**Location:** Could be in CLOUD_ABSTRACTION.md or separate `docs/BACKEND_CAPABILITIES.md`

---

## 5. Specific Outdated References Found

| File | Line | Outdated Reference | Current Location |
|------|------|-------------------|------------------|
| GEMINI.md | 88 | `src/goldfish/infra/` | `cloud/adapters/` |
| ROADMAP.md | 77-83 | `ComputeBackend` protocol | `RunBackend` protocol |
| ROADMAP.md | 382 | "Cloud abstraction branch" | Merged (de-googlify) |
| docs/de-googlify/GATE_VALIDATION.md | 388 | Wrap `gce_launcher.py` | Already wrapped |
| docs/de-googlify/REVIEW_REPORT.md | 38 | `infra/local_executor.py` | Deleted |

---

## 6. File Reference Summary

### Still Accurate
- `jobs/stage_executor.py` - Correct location, updated to use RunBackend
- `cloud/adapters/gcp/gce_launcher.py` - Correct location for GCE-specific code
- `cloud/protocols.py` - New location for backend interfaces
- `cloud/contracts.py` - New location for data contracts

### Removed/Moved (Update References)
- `infra/local_executor.py` - DELETED, replaced by `cloud/adapters/local/run_backend.py`
- `infra/gce_launcher.py` - MOVED to `cloud/adapters/gcp/gce_launcher.py`

---

## 7. Recommendations Summary

### Priority 1 (Before public release)
1. Update GEMINI.md file structure section
2. Update CONTRIBUTING.md Key Subsystems table
3. Archive or update ROADMAP.md

### Priority 2 (Nice to have)
4. Create `docs/CLOUD_ABSTRACTION.md` guide
5. Clean up docs/de-googlify/ folder
6. Add note to GCP_SETUP.md about pluggable backends

### Priority 3 (Future)
7. Generate protocol reference documentation
8. Add backend capabilities reference
9. Consider consolidating GEMINI.md into CLAUDE.md

---

## 8. Validation Commands

```bash
# Check for outdated local_executor references
grep -rn "local_executor" docs/ --include="*.md" | grep -v "COMPLETED\|CHECKLIST\|REVIEW_REPORT"

# Check for outdated infra/ references (should be cloud/adapters/ now)
grep -rn "infra/gce_launcher\|infra/local" docs/ --include="*.md" | grep -v de-googlify

# Verify CLAUDE.md cloud abstraction section exists
grep -n "Cloud Abstraction Layer" CLAUDE.md
```

---

*Generated by doc-auditor agent, 2026-01-29*
