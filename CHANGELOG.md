# Changelog

All notable changes to Goldfish.

## [Unreleased]

## [0.3.6] - 2026-03-21

### Fixed
- **Removed 120s instance readiness wait** — after `--async` instance creation,
  `wait_for_instance_ready` blocked for 120s waiting for RUNNING. This is redundant
  since goldfish's own `wait_for_completion()` handles provisioning.
- **Spot VMs: --reservation-affinity=none** — required flag for spot instances.
- **h100-on-demand profile: a3-highgpu-1g → a3-highgpu-8g** — GCP only allows
  a3-highgpu-1g/2g/4g as spot/flex-start. On-demand requires the full 8xH100 machine.

## [0.3.5] - 2026-03-21

### Fixed
- **Instance creation uses --async** — gcloud no longer blocks waiting for RUNNING
  status. A3 (H100) VMs take 5+ minutes to provision, which caused gcloud to time out.
  Now returns immediately; Goldfish's own polling handles the wait.

## [0.3.4] - 2026-03-21

### Fixed
- **Spot VM flags** — `--restart-on-failure` was set for all GPU VMs including spot,
  causing a terminate-restart loop. Now only set for on-demand. Spot VMs get
  `--provisioning-model=SPOT --instance-termination-action=STOP`.

## [0.3.3] - 2026-03-21

### Fixed
- **Workspace lineage idempotent** — `create_workspace_lineage` uses `INSERT OR IGNORE`
  so retrying a failed `create_workspace` no longer crashes with UNIQUE constraint.

## [0.3.2] - 2026-03-21

### Added
- **Workspace branching** — `create_workspace(from_workspace="baseline")` branches from
  another workspace's current head. `from_version="v3"` branches from a specific saved
  version. Mounted workspaces auto-synced before branching. Fork points recorded as
  immutable versions with real git tags.
- **Configurable slots** — `slots: 5` in goldfish.yaml generates [w1..w5]. Accepts
  integer shorthand or explicit list. Rejects booleans, zero, negatives.
- **Profile overrides in resource catalog** — custom profiles from goldfish.yaml are now
  visible to the GCE capacity search. Non-GCE profiles filtered out.

## [0.3.1] - 2026-03-19

### Fixed
- **Base image Python 3.11→3.12** — fixes torch.compile segfault on 3.11.
- **PyTorch 2.7.1→2.9.1** cu128.
- **FlashAttention-3** — beta GCS wheel replaced with official 3.0.0 from PyTorch cu128 index.

## [0.2.9] - 2026-03-19

### Fixed
- **Capacity search picks wrong machine type** — profiles with same GPU but different
  machine sizes (a3-highgpu-1g vs a3-highgpu-8g) now filtered by machine_type.

## [0.2.8] - 2026-03-19

### Fixed
- **Orphaned runs recovered on daemon restart** — runs stuck in preparing/building/launching
  after a daemon crash are now marked failed with a clear error on next startup.

## [0.2.7] - 2026-03-19

### Fixed
- **Version detection broken in uvx** — `_get_version()` used `importlib.metadata.version("goldfish")`
  which fails in uvx (package is `goldfish-ml`). Daemon was never restarted on upgrade.
- **LAUNCHING skip restored** — daemon no longer kills instances during LAUNCHING state
  (reverts de-googlify regression). GPU VMs need 5+ seconds for GCE API propagation.

## [0.2.6] - 2026-03-19

### Fixed
- **LAUNCHING daemon skip** — restored from before de-googlify refactor. Daemon was killing
  instances 5 seconds after launch due to GCE API propagation delay.

## [0.2.5] - 2026-03-19

### Fixed
- **Early finalization** — `finalize_run` during RUNNING now auto-completes instead of
  requiring a second call after AWAITING_USER_FINALIZATION.
- **GCE boot timeout** — `not_found_timeout` increased from 300s to 600s for CPU VMs
  with data_disk provisioning.
- **gpu:null accepted** — CPU profiles no longer require `{type: none, count: 0}`.
- **data_disk optional** — removed from required profile fields.
- **SKILL.md** — config placement, container layout, output API docs.

## [0.2.4] - 2026-03-19

### Fixed
- **VictoriaLogs optional** — defaults to disabled. No longer crashes goldfish when not running.
- **GCE startup exit code in trap** — EXIT trap writes exit code to GCS before self-delete,
  so goldfish detects startup failures immediately instead of waiting 300s.
- **SVS truncated file listing** — shows all input contents (was capped at 50). Prevents
  false-positive blocking when files are in subdirectories beyond the cutoff.
- **GPU container race** — uses profile-based `--gpus all` flag instead of runtime
  nvidia-smi detection that raced with async driver loading.
- **Flaky wandb caplog test** — logger state pollution from setup_logging fixed.

## [0.2.3] - 2026-03-18

### Fixed
- **signal_lineage UNIQUE constraint** — output recording crashed with
  `UNIQUE constraint failed: signal_lineage.stage_run_id, signal_lineage.signal_name`
  when outputs were recorded by both the container SVS hook and the finalization phase.
  Use `INSERT OR REPLACE` to upsert instead of failing on duplicate.

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
