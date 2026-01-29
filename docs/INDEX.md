# Goldfish Documentation Index

Quick navigation for developers (human or AI) working on Goldfish.

## Getting Started

| Document | Purpose |
|----------|---------|
| [README.md](../README.md) | Project overview, quick start, value proposition |
| [GETTING_STARTED.md](GETTING_STARTED.md) | Full installation and first run guide |
| [GCP_SETUP.md](GCP_SETUP.md) | Detailed GCP infrastructure setup |

## For AI Assistants

| Document | Purpose |
|----------|---------|
| [CLAUDE.md](../CLAUDE.md) | Complete development guide - architecture, patterns, commands, security |
| [GEMINI.md](../GEMINI.md) | Condensed project context for Gemini-based assistants |
| [llms.txt](../llms.txt) | Machine-readable source file reference |
| [SKILL.md](../.claude/skills/goldfish-ml/SKILL.md) | MCP tool reference and workflows |

## Architecture

| Document | Purpose |
|----------|---------|
| [CLOUD_ABSTRACTION.md](CLOUD_ABSTRACTION.md) | Cloud backend protocols, contracts, and how to add new backends |
| [svs.md](svs.md) | Semantic Validation System (SVS) - contracts, checks, patterns |
| [state-machine-spec.md](state-machine-spec.md) | Stage execution state machine specification |
| [ARCHITECTURE_PROPOSAL.md](ARCHITECTURE_PROPOSAL.md) | Architecture redesign proposal |
| [ARCHITECTURE_REVIEW.md](ARCHITECTURE_REVIEW.md) | Architecture review discussion |

## For Contributors

| Document | Purpose |
|----------|---------|
| [CONTRIBUTING.md](../CONTRIBUTING.md) | Development setup, PR process, coding standards |
| [ROADMAP.md](../ROADMAP.md) | Future features and priorities |
| [CHANGELOG.md](../CHANGELOG.md) | Version history |

## Implementation Details

| Directory | Purpose |
|-----------|---------|
| [de-googlify/](de-googlify/) | De-googlify refactor documentation (completed 2026-01-24) |
| [specs/](specs/) | Detailed specifications |

## Source Code Quick Reference

### Core
- `src/goldfish/server.py` - MCP server entry point
- `src/goldfish/context.py` - ServerContext dependency injection
- `src/goldfish/jobs/stage_executor.py` - Stage execution engine
- `src/goldfish/workspace/manager.py` - Workspace operations

### Cloud Abstraction
- `src/goldfish/cloud/protocols.py` - RunBackend, ObjectStorage interfaces
- `src/goldfish/cloud/contracts.py` - BackendCapabilities, RunSpec, RunHandle
- `src/goldfish/cloud/factory.py` - AdapterFactory
- `src/goldfish/cloud/adapters/local/` - Docker-based local backend
- `src/goldfish/cloud/adapters/gcp/` - GCE/GCS backends

### MCP Tools
- `src/goldfish/server_tools/execution_tools.py` - run, logs, cancel
- `src/goldfish/server_tools/workspace_tools.py` - status, manage_versions
- `src/goldfish/server_tools/data_tools.py` - manage_sources, register_source

### Tests
- `tests/unit/` - Fast unit tests (<1s)
- `tests/integration/` - Integration tests (~2min)
- `tests/e2e/` - End-to-end tests
- `tests/e2e/deluxe/` - Real GCE tests

---

## Navigation Tips

**Starting from scratch?** Read README.md then GETTING_STARTED.md.

**Developing on Goldfish?** Read CLAUDE.md (comprehensive) or GEMINI.md (condensed).

**Adding a new cloud backend?** Read CLOUD_ABSTRACTION.md.

**Understanding execution flow?** Read state-machine-spec.md and stage_executor.py.

**Understanding data validation?** Read svs.md.
