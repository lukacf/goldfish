# Changelog

All notable changes to Goldfish.

## [Unreleased]

### Added
- **Cloud Abstraction Layer** - Protocol-based backend abstraction (`RunBackend`, `ObjectStorage`, `SignalBus`)
- **BackendCapabilities** - Centralized behavior config replacing scattered conditionals
- **Local backend parity** - Full feature parity with GCE including preemption simulation
- **Database-driven base images** - Version tracking for Docker base images
- `docs/ARCHITECTURE.md` - Comprehensive architecture documentation

### Changed
- **De-googlify refactor** - All GCP-specific code moved to `cloud/adapters/gcp/`
- `infra/local_executor.py` → `cloud/adapters/local/run_backend.py`
- `infra/gce_launcher.py` → `cloud/adapters/gcp/gce_launcher.py`
- Documentation overhaul - consolidated docs, archived obsolete files
- `GEMINI.md` now symlinks to `CLAUDE.md` (single source of truth)

### Fixed
- `configure_server()` now sets `_project_root` for tools that need it
- `skip_review` parameter works in async mode
- `get_current_best()` matches bare "best" tags (not just "best-*")
- `delete_workspace()` cleans up stale tmp-sync worktrees
- Backup tools use correct dev_repo path (sibling, not subdirectory)
- Daemon initialization sets `_project_root`

## [0.2.0] - 2026-01-15

### Added
- Stage execution state machine with full audit trail
- Zone tracking for GCE runs
- Content-hash caching for Docker builds
- ML outcome assessment in post-run reviews
- Semantic context in MCP tools

### Fixed
- Instance verification uses zone-agnostic approach
- Prevented INSTANCE_LOST race during post-run phase
- Exit code retrieval from metadata (primary) with file fallback

## [0.1.0] - 2025-12-10

### Added
- Initial release of Goldfish MCP server
- Workspace management via git branches and worktrees
- Pipeline execution with Docker containers
- Signal-based data flow between stages
- Resource profiles for GCE compute (H100, A100, CPU profiles)
- Full experiment provenance tracking via signal lineage
- STATE.md auto-generation for context recovery
- Comprehensive audit logging
