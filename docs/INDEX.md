# Goldfish Documentation Index

Quick navigation for developers (human or AI) working on Goldfish.

## Getting Started

| Document | Purpose |
|----------|---------|
| [README.md](../README.md) | Project overview, quick start |
| [GETTING_STARTED.md](GETTING_STARTED.md) | Full installation and first run guide |
| [GETTING_STARTED_KING.md](GETTING_STARTED_KING.md) | King-specific GCP setup and onboarding |
| [GCP_SETUP.md](GCP_SETUP.md) | Detailed GCP infrastructure setup |

## For AI Assistants

| Document | Purpose |
|----------|---------|
| [CLAUDE.md](../CLAUDE.md) | Complete development guide (canonical) |
| [AGENTS.md](../AGENTS.md) | Symlink → CLAUDE.md |
| [GEMINI.md](../GEMINI.md) | Symlink → CLAUDE.md |
| [llms.txt](../llms.txt) | Machine-readable source file reference |
| [SKILL.md](../.claude/skills/goldfish-ml/SKILL.md) | MCP tool reference and workflows |

## Architecture

| Document | Purpose |
|----------|---------|
| [CLOUD_ABSTRACTION.md](CLOUD_ABSTRACTION.md) | Cloud backend protocols, contracts, adding new backends |
| [svs.md](svs.md) | Semantic Validation System - contracts, checks, patterns |
| [state-machine-spec.md](state-machine-spec.md) | Stage execution state machine |
| [state-machine-spec-formal.md](state-machine-spec-formal.md) | Formal state machine specification |
| [state-machine-implementation.md](state-machine-implementation.md) | Implementation details |

## For Contributors

| Document | Purpose |
|----------|---------|
| [CONTRIBUTING.md](../CONTRIBUTING.md) | Development setup, PR process, coding standards |
| [ROADMAP.md](../ROADMAP.md) | Future features and priorities |
| [CHANGELOG.md](../CHANGELOG.md) | Version history |
| [PUBLIC_RELEASE_CHECKLIST.md](PUBLIC_RELEASE_CHECKLIST.md) | Release preparation checklist |

## Other

| Directory | Purpose |
|-----------|---------|
| [specs/](specs/) | Detailed specifications |
| [archive/](archive/) | Archived/historical documentation |

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
- `tests/e2e/deluxe/` - Real GCE tests
