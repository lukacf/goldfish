# Changelog

All notable changes to Goldfish.

## [Unreleased]

## [0.2.2] - 2026-03-18

### Fixed
- **meerkat-sdk is now a core dependency** — SVS AI reviews (pre-run, during-run, post-run)
  work out of the box. Previously meerkat-sdk was optional, causing silent fallback to
  NullProvider with `pip install goldfish-ml`.
- **Container exit detection** — local backend now saves Docker container ID to DB so
  `wait_for_completion()` can find and monitor the container. Previously only saved for GCE.
- **Entrypoint script execution** — use `bash -c` instead of `sh -c` for stage entrypoint
  scripts. Fixes `Illegal option -o pipefail` on images where `/bin/sh` is dash.
- **Entrypoint override** — clear base image ENTRYPOINT when running stage commands so
  containers (e.g., jupyter) exit cleanly after the stage completes.

### Added
- API key check at daemon startup — logs which AI providers are available for SVS reviews,
  or warns clearly if none are configured.

## [0.2.1] - 2026-03-18

### Fixed
- **Critical:** Fix FastMCP 3.x compatibility — daemon and proxy crashed on startup with
  `AttributeError: 'FastMCP' object has no attribute '_tool_manager'`
- Renamed PyPI package from `goldfish` (taken) to `goldfish-ml`

## [0.2.0] - 2026-03-17

### Added
- **Cloud Abstraction Layer** - Protocol-based backend abstraction (`RunBackend`, `ObjectStorage`, `SignalBus`)
- **BackendCapabilities** - Centralized behavior config replacing scattered conditionals
- **Local backend parity** - Full feature parity with GCE including preemption simulation
- **Meerkat SVS integration** - Meerkat as default SVS agent provider for pre-run reviews
- **CI/CD overhaul** - Security audit (pip-audit), import boundary checks, release preflight,
  PyPI publishing, build provenance attestation, workflow_dispatch for retry/dry-run
- **Pre-commit file hygiene** - Trailing whitespace, YAML/TOML validation, large file detection
- Stage execution state machine with full audit trail
- Zone tracking for GCE runs
- Content-hash caching for Docker builds
- ML outcome assessment in post-run reviews
- Semantic context in MCP tools
- Database-driven base images - Version tracking for Docker base images
- `DefaultsConfig` for global stage execution defaults (timeout_seconds, log_sync_interval, backend)
- `StorageConfig` for multi-backend storage configuration
- `S3StorageConfig` and `AzureStorageConfig` for future cloud providers
- Package metadata: authors, project URLs, keywords, classifiers

### Security
- SSRF protection for S3 endpoint_url (blocks localhost, private IPs, metadata endpoints)

### Changed
- **De-googlify refactor** - All GCP-specific code moved to `cloud/adapters/gcp/`
- `infra/local_executor.py` → `cloud/adapters/local/run_backend.py`
- `infra/gce_launcher.py` → `cloud/adapters/gcp/gce_launcher.py`
- Upgraded meerkat-sdk to >=0.4.12 and rkat-rpc to v0.4.12
- Documentation overhaul - consolidated docs, archived obsolete files
- `GEMINI.md` now symlinks to `CLAUDE.md` (single source of truth)
- Makefile: colored output, ci-smoke target, verify-version, release-preflight

### Fixed
- SVS debug print() statements replaced with proper logger.debug() calls
- Removed dead code: `_inline_project_html_backup_marker()`, incomplete `update_status()` stub
- Instance verification uses zone-agnostic approach
- Prevented INSTANCE_LOST race during post-run phase
- Exit code retrieval from metadata (primary) with file fallback
- `configure_server()` now sets `_project_root` for tools that need it
- `skip_review` parameter works in async mode
- `get_current_best()` matches bare "best" tags (not just "best-*")
- `delete_workspace()` cleans up stale tmp-sync worktrees
- Backup tools use correct dev_repo path (sibling, not subdirectory)
- Daemon initialization sets `_project_root`
- Unit/integration tests inject mock storage to prevent GCP credential leaks in CI

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
