-- Goldfish database schema
-- All tables live in a single SQLite database: .goldfish/goldfish.db

-- Audit trail for all state-changing operations
CREATE TABLE IF NOT EXISTS audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    operation TEXT NOT NULL,
    slot TEXT,
    workspace TEXT,
    reason TEXT NOT NULL,
    details TEXT,  -- JSON
    CHECK(length(reason) >= 15)
);

CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_workspace ON audit(workspace);
CREATE INDEX IF NOT EXISTS idx_audit_operation ON audit(operation);


-- Data source registry
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,              -- e.g., "synth_v11"
    name TEXT NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,         -- "job:{job_id}" or "external"
    gcs_location TEXT NOT NULL,
    size_bytes INTEGER,
    status TEXT NOT NULL DEFAULT 'available',
    metadata TEXT                     -- JSON for future schema info
);

CREATE INDEX IF NOT EXISTS idx_sources_status ON sources(status);
CREATE INDEX IF NOT EXISTS idx_sources_created_by ON sources(created_by);


-- Source lineage tracking
CREATE TABLE IF NOT EXISTS source_lineage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT NOT NULL,
    parent_source_id TEXT,            -- Input source that was used (NULL for external)
    job_id TEXT,                      -- Job that produced this source (NULL for external)
    created_at TEXT NOT NULL,
    FOREIGN KEY (source_id) REFERENCES sources(id),
    FOREIGN KEY (parent_source_id) REFERENCES sources(id)
);

CREATE INDEX IF NOT EXISTS idx_lineage_source ON source_lineage(source_id);
CREATE INDEX IF NOT EXISTS idx_lineage_parent ON source_lineage(parent_source_id);
CREATE INDEX IF NOT EXISTS idx_lineage_job ON source_lineage(job_id);


-- Job tracking (supplements the existing infra registry)
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,              -- e.g., "job-20251204-153000"
    workspace TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    script TEXT NOT NULL,
    experiment_dir TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    started_at TEXT NOT NULL,
    completed_at TEXT,
    log_uri TEXT,
    artifact_uri TEXT,
    error TEXT,
    metadata TEXT                     -- JSON for config overrides, etc.
);

CREATE INDEX IF NOT EXISTS idx_jobs_workspace ON jobs(workspace);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_started ON jobs(started_at);


-- Job input sources (many-to-many relationship)
CREATE TABLE IF NOT EXISTS job_inputs (
    job_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    input_name TEXT NOT NULL,         -- Name in job config (e.g., "raw")
    PRIMARY KEY (job_id, source_id, input_name),
    FOREIGN KEY (job_id) REFERENCES jobs(id),
    FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE INDEX IF NOT EXISTS idx_job_inputs_job ON job_inputs(job_id);
CREATE INDEX IF NOT EXISTS idx_job_inputs_source ON job_inputs(source_id);


-- Workspace goals (persisted across sessions)
CREATE TABLE IF NOT EXISTS workspace_goals (
    workspace TEXT PRIMARY KEY,
    goal TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workspace_goals_updated ON workspace_goals(updated_at);


-- Workspace lineage (tracks workspace creation and branching)
CREATE TABLE IF NOT EXISTS workspace_lineage (
    workspace_name TEXT PRIMARY KEY,
    parent_workspace TEXT,            -- Parent workspace if branched
    parent_version TEXT,              -- Version branched from
    created_at TEXT NOT NULL,
    description TEXT,
    FOREIGN KEY (parent_workspace) REFERENCES workspace_lineage(workspace_name)
);

CREATE INDEX IF NOT EXISTS idx_workspace_lineage_parent ON workspace_lineage(parent_workspace);


-- Workspace versions (git tags, auto-versioned on runs)
CREATE TABLE IF NOT EXISTS workspace_versions (
    workspace_name TEXT,
    version TEXT,                     -- v1, v2, v3, etc.
    git_tag TEXT NOT NULL,            -- Git tag name (e.g., baseline_lstm-v1)
    git_sha TEXT NOT NULL,            -- Git commit SHA
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,         -- 'run', 'checkpoint', 'manual'
    job_id TEXT,                      -- Job that triggered version (if created_by='run')
    description TEXT,
    pruned_at TEXT,                   -- When version was pruned (NULL if not pruned)
    prune_reason TEXT,                -- Why version was pruned
    PRIMARY KEY (workspace_name, version),
    FOREIGN KEY (workspace_name) REFERENCES workspace_lineage(workspace_name),
    FOREIGN KEY (job_id) REFERENCES jobs(id)
);

CREATE INDEX IF NOT EXISTS idx_workspace_versions_workspace ON workspace_versions(workspace_name);
CREATE INDEX IF NOT EXISTS idx_workspace_versions_created ON workspace_versions(created_at);
CREATE INDEX IF NOT EXISTS idx_workspace_versions_pruned ON workspace_versions(workspace_name, pruned_at);
CREATE INDEX IF NOT EXISTS idx_workspace_versions_pruned_version ON workspace_versions(workspace_name, pruned_at, version);


-- Stage versions (tracks unique code + config combinations per stage)
-- Enables "preprocessing-v5", "tokenization-v11" independent of workspace versions
CREATE TABLE IF NOT EXISTS stage_versions (
    id INTEGER PRIMARY KEY,
    workspace_name TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    version_num INTEGER NOT NULL,
    git_sha TEXT NOT NULL,
    config_hash TEXT NOT NULL,            -- Full SHA256 (64 chars)
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(workspace_name, stage_name, version_num),
    UNIQUE(workspace_name, stage_name, git_sha, config_hash),
    FOREIGN KEY (workspace_name) REFERENCES workspace_lineage(workspace_name)
);

CREATE INDEX IF NOT EXISTS idx_stage_versions_lookup
    ON stage_versions(workspace_name, stage_name, git_sha, config_hash);
CREATE INDEX IF NOT EXISTS idx_stage_versions_workspace_stage
    ON stage_versions(workspace_name, stage_name);


-- Stage runs (individual stage executions within pipelines)
CREATE TABLE IF NOT EXISTS stage_runs (
    id TEXT PRIMARY KEY,              -- e.g., "stage-abc123"
    job_id TEXT,                      -- Legacy job grouping (run_job); keep for compatibility
    pipeline_run_id TEXT,             -- Grouping ID for pipeline invocations
    workspace_name TEXT NOT NULL,
    pipeline_name TEXT,               -- Named pipeline file (e.g., train, inference)
    version TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    stage_version_id INTEGER,         -- Links to stage_versions for lineage
    status TEXT NOT NULL DEFAULT 'pending',
    started_at TEXT NOT NULL,
    completed_at TEXT,
    log_uri TEXT,
    artifact_uri TEXT,
    progress TEXT,                    -- Optional progress string
    profile TEXT,                     -- Resolved profile name
    hints_json TEXT,                  -- JSON hints (spot_ok, priority, etc.)
    outputs_json TEXT,                -- JSON map name -> details
    config_json TEXT,                 -- Effective config used
    inputs_json TEXT,                 -- Resolved inputs (URI + ref)
    reason_json TEXT,                 -- Structured RunReason (description, hypothesis, approach, etc.)
    preflight_errors_json TEXT,       -- JSON list of preflight validation errors
    preflight_warnings_json TEXT,     -- JSON list of preflight validation warnings
    backend_type TEXT,                -- local | gce
    backend_handle TEXT,              -- container_id or instance_name for cancel/log lookup
    build_context_hash TEXT,          -- SHA256 cache key of Docker build context
    image_tag TEXT,                   -- Docker tag used for the run image
    instance_zone TEXT,               -- GCE zone where instance was launched (NULL for local)
    error TEXT,
    outcome TEXT,                     -- NULL (unset), 'success', 'bad_results' - semantic result quality
    attempt_num INTEGER,              -- Groups consecutive runs; increments after outcome='success'
    svs_findings_json TEXT,           -- JSON: SVS post-run findings (stats + AI review)
    -- State machine columns (Phase 3)
    state TEXT CHECK(state IS NULL OR state IN ('preparing', 'building', 'launching', 'running', 'post_run', 'awaiting_user_finalization', 'completed', 'failed', 'terminated', 'canceled', 'unknown')),
    phase TEXT,                       -- Sub-phase within state (gcs_check, docker_build, etc.)
    termination_cause TEXT CHECK(termination_cause IS NULL OR termination_cause IN ('preempted', 'crashed', 'orphaned', 'timeout', 'ai_stopped', 'manual')),
    state_entered_at TEXT,            -- When current state was entered (for timeout calculations)
    phase_updated_at TEXT,            -- When phase was last updated
    completed_with_warnings INTEGER DEFAULT 0,  -- 1 if completed with non-critical failures
    output_sync_done INTEGER DEFAULT 0,         -- 1 if output sync completed
    output_recording_done INTEGER DEFAULT 0,    -- 1 if output recording completed
    gcs_outage_started TEXT,          -- When GCS outage was first detected
    FOREIGN KEY (workspace_name, version) REFERENCES workspace_versions(workspace_name, version),
    FOREIGN KEY (stage_version_id) REFERENCES stage_versions(id)
);

CREATE INDEX IF NOT EXISTS idx_stage_runs_workspace ON stage_runs(workspace_name);
CREATE INDEX IF NOT EXISTS idx_stage_runs_status ON stage_runs(status);
CREATE INDEX IF NOT EXISTS idx_stage_runs_started ON stage_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_stage_runs_job ON stage_runs(job_id);
CREATE INDEX IF NOT EXISTS idx_stage_runs_pipeline_run ON stage_runs(pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_stage_runs_ws_stage_status ON stage_runs(workspace_name, stage_name, status);
CREATE INDEX IF NOT EXISTS idx_stage_runs_ws_stage_attempt ON stage_runs(workspace_name, stage_name, attempt_num);
CREATE INDEX IF NOT EXISTS idx_stage_runs_outcome ON stage_runs(outcome);
CREATE INDEX IF NOT EXISTS idx_stage_runs_build_context_hash ON stage_runs(build_context_hash);
-- Partial index for active states (used by daemon polling)
CREATE INDEX IF NOT EXISTS idx_stage_runs_active_state ON stage_runs(state)
    WHERE state IN ('preparing', 'building', 'launching', 'running', 'finalizing', 'unknown');


-- Stage state transitions (audit trail for state machine)
CREATE TABLE IF NOT EXISTS stage_state_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage_run_id TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    event TEXT NOT NULL,
    phase TEXT,
    termination_cause TEXT CHECK(termination_cause IS NULL OR termination_cause IN ('preempted', 'crashed', 'orphaned', 'timeout', 'ai_stopped', 'manual')),
    exit_code INTEGER,
    exit_code_exists INTEGER,         -- 0 or 1
    error_message TEXT,
    svs_review_id TEXT,               -- FK to svs_reviews.id for SVS_BLOCK and AI_STOP events
    source TEXT NOT NULL CHECK(source IN ('mcp_tool', 'executor', 'daemon', 'container')),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id) ON DELETE CASCADE,
    FOREIGN KEY (svs_review_id) REFERENCES svs_reviews(id)
);

CREATE INDEX IF NOT EXISTS idx_state_transitions_stage_run ON stage_state_transitions(stage_run_id, created_at);
CREATE INDEX IF NOT EXISTS idx_state_transitions_created_at ON stage_state_transitions(created_at);


-- Pipeline runs (group stages for one pipeline invocation)
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id TEXT PRIMARY KEY,              -- e.g., "prun-abc123"
    workspace_name TEXT NOT NULL,
    pipeline_name TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    started_at TEXT NOT NULL,
    completed_at TEXT,
    error TEXT,
    config_override TEXT,             -- JSON: per-stage config overrides
    inputs_override TEXT,             -- JSON: per-stage input overrides
    reason_json TEXT,                 -- JSON: structured RunReason (description, hypothesis, etc.)
    results_spec_json TEXT,           -- JSON: results_spec for async runs
    experiment_group TEXT             -- Optional experiment group for async runs
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_workspace ON pipeline_runs(workspace_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);

-- Pipeline stage queue (restart-safe async pipeline execution)
CREATE TABLE IF NOT EXISTS pipeline_stage_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_run_id TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    deps TEXT,                        -- JSON list of dependent stage names
    status TEXT NOT NULL DEFAULT 'pending', -- pending, running, completed, failed, canceled, skipped
    stage_run_id TEXT,                -- filled when launched
    claimed_at TEXT,                  -- worker locking
    error TEXT,                       -- error message for skipped/failed stages
    FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_stage_queue_run ON pipeline_stage_queue(pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_stage_queue_status ON pipeline_stage_queue(status);


-- Signal lineage (tracks data flow between stages)
CREATE TABLE IF NOT EXISTS signal_lineage (
    stage_run_id TEXT,
    signal_name TEXT,                 -- Output signal name (e.g., "tokens", "features")
    signal_type TEXT,                 -- npy, csv, directory, file, dataset
    storage_location TEXT,            -- GCS path, local path, etc.
    size_bytes INTEGER,
    consumed_by TEXT,                 -- Stage run ID that consumed this (NULL if not consumed yet)
    is_artifact BOOLEAN DEFAULT 0,    -- 1 if marked as permanent artifact
    source_stage_run_id TEXT,         -- Upstream stage run that produced this input
    source_stage_version_id INTEGER,  -- Upstream stage version for lineage tracking
    stats_json TEXT,                  -- JSON: SVS output statistics (entropy, null_ratio, etc.)
    PRIMARY KEY (stage_run_id, signal_name),
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id),
    FOREIGN KEY (consumed_by) REFERENCES stage_runs(id),
    FOREIGN KEY (source_stage_run_id) REFERENCES stage_runs(id),
    FOREIGN KEY (source_stage_version_id) REFERENCES stage_versions(id)
);

CREATE INDEX IF NOT EXISTS idx_signal_lineage_stage ON signal_lineage(stage_run_id);
CREATE INDEX IF NOT EXISTS idx_signal_lineage_consumed ON signal_lineage(consumed_by);
CREATE INDEX IF NOT EXISTS idx_signal_lineage_artifact ON signal_lineage(is_artifact);
CREATE INDEX IF NOT EXISTS idx_signal_lineage_source ON signal_lineage(source_stage_run_id);


-- Workspace mounts (tracks active copy-based workspace mounts)
CREATE TABLE IF NOT EXISTS workspace_mounts (
    slot TEXT PRIMARY KEY,
    workspace_name TEXT NOT NULL,
    branch TEXT NOT NULL,
    mounted_sha TEXT NOT NULL,
    mounted_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',  -- 'mounting' | 'active' | 'unmounting' | 'failed'
    FOREIGN KEY (workspace_name) REFERENCES workspace_lineage(workspace_name)
);

CREATE INDEX IF NOT EXISTS idx_mounts_workspace ON workspace_mounts(workspace_name);
CREATE INDEX IF NOT EXISTS idx_mounts_status ON workspace_mounts(status);
-- Prevent concurrent mounts of the same workspace (only one active mount per workspace)
CREATE UNIQUE INDEX IF NOT EXISTS idx_mounts_active_workspace ON workspace_mounts(workspace_name)
    WHERE status = 'active';


-- Run metrics (individual metric data points logged during stage execution)
CREATE TABLE IF NOT EXISTS run_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage_run_id TEXT NOT NULL,
    name TEXT NOT NULL,
    value REAL NOT NULL,
    step INTEGER,
    timestamp TEXT NOT NULL,
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_run_metrics_stage_run ON run_metrics(stage_run_id);
CREATE INDEX IF NOT EXISTS idx_run_metrics_name ON run_metrics(stage_run_id, name);
-- Timestamp index for ORDER BY timestamp ASC queries (critical for pagination performance)
CREATE INDEX IF NOT EXISTS idx_run_metrics_timestamp ON run_metrics(stage_run_id, timestamp);
-- Composite index for pagination stability (ORDER BY timestamp ASC, id ASC)
CREATE INDEX IF NOT EXISTS idx_run_metrics_pagination ON run_metrics(stage_run_id, timestamp, id);
-- Index for trend calculation (ORDER BY name, timestamp DESC, id DESC)
CREATE INDEX IF NOT EXISTS idx_run_metrics_trends ON run_metrics(stage_run_id, name, timestamp DESC, id DESC);
-- Unique constraint to prevent duplicate metrics (idempotency)
-- COALESCE(step, -1) ensures NULL steps are deduplicated too.
CREATE UNIQUE INDEX IF NOT EXISTS idx_run_metrics_unique
    ON run_metrics(stage_run_id, name, COALESCE(step, -1), timestamp);


-- Run metrics summary (aggregated stats for quick queries)
CREATE TABLE IF NOT EXISTS run_metrics_summary (
    stage_run_id TEXT NOT NULL,
    name TEXT NOT NULL,
    min_value REAL,
    max_value REAL,
    last_value REAL,
    last_timestamp TEXT,
    count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (stage_run_id, name),
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_run_metrics_summary_stage_run ON run_metrics_summary(stage_run_id);
CREATE INDEX IF NOT EXISTS idx_run_metrics_summary_name ON run_metrics_summary(name);
CREATE INDEX IF NOT EXISTS idx_run_metrics_summary_stage_name ON run_metrics_summary(stage_run_id, name);


-- Run artifacts (artifacts logged during stage execution)
CREATE TABLE IF NOT EXISTS run_artifacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage_run_id TEXT NOT NULL,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    backend_url TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_run_artifacts_stage_run ON run_artifacts(stage_run_id);


-- =============================================================================
-- SVS (Semantic Validation System) Tables
-- =============================================================================

-- SVS Reviews (AI review results for pre-run, during-run, post-run)
CREATE TABLE IF NOT EXISTS svs_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage_run_id TEXT NOT NULL,
    signal_name TEXT,                     -- NULL for pre-run, signal name for post-run
    review_type TEXT NOT NULL,            -- 'pre_run' | 'during_run' | 'post_run'
    model_used TEXT NOT NULL,             -- e.g., 'claude-opus-4-5-20251101'
    prompt_hash TEXT NOT NULL,            -- SHA256 of prompt for dedup
    stats_json TEXT,                      -- Input stats for post-run reviews
    response_text TEXT,                   -- Raw AI response
    parsed_findings TEXT,                 -- JSON: structured findings
    decision TEXT NOT NULL,               -- 'approved' | 'blocked' | 'warned'
    policy_overrides TEXT,                -- JSON: any policy overrides applied
    reviewed_at TEXT NOT NULL,
    duration_ms INTEGER,
    notified INTEGER DEFAULT 0,           -- 0 = not yet shown in dashboard, 1 = already shown
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_svs_reviews_stage_run ON svs_reviews(stage_run_id);
CREATE INDEX IF NOT EXISTS idx_svs_reviews_type ON svs_reviews(review_type);
CREATE INDEX IF NOT EXISTS idx_svs_reviews_decision ON svs_reviews(decision);
CREATE INDEX IF NOT EXISTS idx_svs_reviews_reviewed_at ON svs_reviews(reviewed_at);
CREATE INDEX IF NOT EXISTS idx_svs_reviews_notified ON svs_reviews(notified);


-- Failure Patterns (self-learning failure detection heuristics)
CREATE TABLE IF NOT EXISTS failure_patterns (
    id TEXT PRIMARY KEY,                  -- UUID
    symptom TEXT NOT NULL,                -- What went wrong
    root_cause TEXT NOT NULL,             -- Why it happened
    detection_heuristic TEXT NOT NULL,    -- How to detect it
    prevention TEXT NOT NULL,             -- How to prevent it
    severity TEXT CHECK(severity IN ('CRITICAL', 'HIGH', 'MEDIUM', 'LOW')),
    stage_type TEXT,                      -- e.g., 'train', 'preprocess', NULL for all
    source_run_id TEXT,                   -- Stage run that triggered extraction
    source_workspace TEXT,
    created_at TEXT NOT NULL,
    last_seen_at TEXT,
    occurrence_count INTEGER DEFAULT 1,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'approved', 'rejected', 'archived')),
    confidence TEXT CHECK(confidence IN ('HIGH', 'MEDIUM', 'LOW')),
    approved_at TEXT,
    approved_by TEXT,
    rejection_reason TEXT,
    manually_edited BOOLEAN DEFAULT 0,
    enabled BOOLEAN DEFAULT 1,
    FOREIGN KEY (source_run_id) REFERENCES stage_runs(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_failure_patterns_status ON failure_patterns(status);
CREATE INDEX IF NOT EXISTS idx_failure_patterns_severity ON failure_patterns(severity);
CREATE INDEX IF NOT EXISTS idx_failure_patterns_stage_type ON failure_patterns(stage_type);
CREATE INDEX IF NOT EXISTS idx_failure_patterns_enabled ON failure_patterns(enabled);
CREATE INDEX IF NOT EXISTS idx_failure_patterns_created ON failure_patterns(created_at);
CREATE INDEX IF NOT EXISTS idx_failure_patterns_source_run ON failure_patterns(source_run_id);


-- Version tags (user-defined names for significant versions)
-- Tags allow marking milestones like "baseline-working", "best-model"
-- Can be applied retroactively to any existing version
CREATE TABLE IF NOT EXISTS workspace_version_tags (
    workspace_name TEXT NOT NULL,
    version TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (workspace_name, tag_name),
    FOREIGN KEY (workspace_name, version) REFERENCES workspace_versions(workspace_name, version)
);

CREATE INDEX IF NOT EXISTS idx_version_tags_version ON workspace_version_tags(workspace_name, version);


-- =============================================================================
-- Docker Image Builds
-- =============================================================================

-- Docker builds (persistent tracking for local and cloud builds)
-- Allows tracking build status across process restarts, especially for Cloud Build
CREATE TABLE IF NOT EXISTS docker_builds (
    id TEXT PRIMARY KEY,              -- "build-{uuid8}"
    image_type TEXT NOT NULL,         -- "cpu" or "gpu"
    target TEXT NOT NULL,             -- "base" (goldfish-base-*), "project" ({project}-*), or "workspace"
    backend TEXT NOT NULL,            -- "local" or "cloud"
    cloud_build_id TEXT,              -- GCP Cloud Build operation ID (if backend=cloud)
    status TEXT NOT NULL,             -- "pending", "building", "completed", "failed", "cancelled"
    image_tag TEXT,                   -- Local tag (e.g., "goldfish-base-gpu:v4")
    registry_tag TEXT,                -- Full registry tag
    started_at TEXT NOT NULL,         -- ISO timestamp
    completed_at TEXT,                -- ISO timestamp
    error TEXT,                       -- Error message if failed
    logs_uri TEXT,                    -- GCS path to logs (Cloud Build only)
    workspace_name TEXT,              -- For workspace builds (NULL for base/project images)
    version TEXT,                     -- Workspace version (NULL for base/project images)
    content_hash TEXT,                -- SHA256 of build context (for cache hit detection)
    dockerfile_hash TEXT,             -- SHA256 of rendered Dockerfile content
    git_sha TEXT,                     -- Git commit SHA of workspace code
    goldfish_runtime_hash TEXT,       -- Hash of Goldfish runtime files copied into build context
    base_image TEXT,                  -- Base image tag used for build
    base_image_digest TEXT,           -- Resolved digest for base image (sha256:...)
    requirements_hash TEXT,           -- SHA256 of requirements.txt (hash of empty string if missing)
    build_args_json TEXT,             -- JSON build args passed to docker build (no secrets)
    build_context_json TEXT,          -- JSON serialization of full BuildContext
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_docker_builds_status ON docker_builds(status);
CREATE INDEX IF NOT EXISTS idx_docker_builds_backend ON docker_builds(backend);
CREATE INDEX IF NOT EXISTS idx_docker_builds_started ON docker_builds(started_at);
CREATE INDEX IF NOT EXISTS idx_docker_builds_workspace ON docker_builds(workspace_name, version);
CREATE INDEX IF NOT EXISTS idx_docker_builds_content_hash ON docker_builds(content_hash);


-- =============================================================================
-- Database Backups
-- =============================================================================

-- Backup history (tracks database backups with tiered retention)
-- Tiers: event (24h), daily (7d), weekly (30d), monthly (365d)
CREATE TABLE IF NOT EXISTS backup_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    backup_id TEXT NOT NULL UNIQUE,       -- UUID "backup-{uuid8}"
    tier TEXT NOT NULL,                   -- 'event', 'daily', 'weekly', 'monthly'
    trigger TEXT NOT NULL,                -- 'run', 'save_version', 'create_workspace', 'manual', etc.
    trigger_details_json TEXT,            -- JSON: workspace, version, run_id, etc.
    gcs_path TEXT NOT NULL,               -- GCS path to backup file
    size_bytes INTEGER,                   -- Compressed size
    created_at TEXT NOT NULL,             -- When backup was created
    expires_at TEXT NOT NULL,             -- When backup should be cleaned up
    deleted_at TEXT,                      -- When backup was deleted (NULL = still exists)
    CHECK(tier IN ('event', 'daily', 'weekly', 'monthly'))
);

CREATE INDEX IF NOT EXISTS idx_backup_history_tier ON backup_history(tier);
CREATE INDEX IF NOT EXISTS idx_backup_history_created ON backup_history(created_at);
CREATE INDEX IF NOT EXISTS idx_backup_history_expires ON backup_history(expires_at);
CREATE INDEX IF NOT EXISTS idx_backup_history_deleted ON backup_history(deleted_at);


-- =============================================================================
-- Experiment Model Tables (New Experiment Memory System)
-- =============================================================================

-- Experiment Records (user-facing entity representing runs or checkpoints)
-- Makes experiment memory first-class (results, comparisons, summaries)
CREATE TABLE IF NOT EXISTS experiment_records (
    record_id TEXT PRIMARY KEY,           -- ULID for lexicographic ordering
    workspace_name TEXT NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('run', 'checkpoint')),
    stage_run_id TEXT,                    -- FK stage_runs (NULL for checkpoints)
    version TEXT NOT NULL,                -- FK workspace_versions
    experiment_group TEXT,                -- Optional grouping for filtering
    created_at TEXT NOT NULL,
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id),
    FOREIGN KEY (workspace_name, version) REFERENCES workspace_versions(workspace_name, version)
);

CREATE INDEX IF NOT EXISTS idx_experiment_records_workspace
    ON experiment_records(workspace_name);
CREATE INDEX IF NOT EXISTS idx_experiment_records_version
    ON experiment_records(workspace_name, version);
CREATE INDEX IF NOT EXISTS idx_experiment_records_run
    ON experiment_records(stage_run_id);
CREATE INDEX IF NOT EXISTS idx_experiment_records_group
    ON experiment_records(workspace_name, experiment_group);


-- Run Results (auto + final results with ML/infra outcome separation)
-- Splits infra outcomes from ML outcomes (preemption != ML failure)
CREATE TABLE IF NOT EXISTS run_results (
    stage_run_id TEXT PRIMARY KEY,        -- FK stage_runs
    record_id TEXT NOT NULL,              -- FK experiment_records
    results_status TEXT NOT NULL CHECK(results_status IN ('missing', 'auto', 'finalized')),
    infra_outcome TEXT NOT NULL CHECK(infra_outcome IN ('completed', 'preempted', 'crashed', 'canceled', 'unknown')),
    ml_outcome TEXT NOT NULL CHECK(ml_outcome IN ('success', 'partial', 'miss', 'unknown')),
    results_auto TEXT,                    -- JSON (immutable, auto-extracted)
    results_final TEXT,                   -- JSON (authoritative, set by finalize_run)
    comparison TEXT,                      -- JSON (computed at finalize time)
    finalized_by TEXT,                    -- Who finalized (e.g., 'ml_claude')
    finalized_at TEXT,                    -- When finalized
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id),
    FOREIGN KEY (record_id) REFERENCES experiment_records(record_id)
);

CREATE INDEX IF NOT EXISTS idx_run_results_record
    ON run_results(record_id);
CREATE INDEX IF NOT EXISTS idx_run_results_status
    ON run_results(results_status);
CREATE INDEX IF NOT EXISTS idx_run_results_ml_outcome
    ON run_results(ml_outcome);


-- Run Results Spec (required at run time for structured + verbose results spec)
-- LLM-friendly, mechanically validated specifications
CREATE TABLE IF NOT EXISTS run_results_spec (
    stage_run_id TEXT PRIMARY KEY,        -- FK stage_runs
    record_id TEXT NOT NULL,              -- FK experiment_records
    spec_json TEXT NOT NULL,              -- JSON results spec
    created_at TEXT NOT NULL,
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id),
    FOREIGN KEY (record_id) REFERENCES experiment_records(record_id)
);

CREATE INDEX IF NOT EXISTS idx_run_results_spec_record
    ON run_results_spec(record_id);


-- Run Tags (user-defined names for significant runs)
-- Allows marking milestones like "@best-25m-63pct"
-- Tag uniqueness per workspace enforced in code across run_tags and workspace_version_tags
CREATE TABLE IF NOT EXISTS run_tags (
    workspace_name TEXT NOT NULL,
    record_id TEXT NOT NULL,              -- FK experiment_records
    tag_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (workspace_name, tag_name),
    FOREIGN KEY (record_id) REFERENCES experiment_records(record_id)
);

CREATE INDEX IF NOT EXISTS idx_run_tags_record
    ON run_tags(record_id);


-- =============================================================================
-- Daemon Leader Election
-- =============================================================================

-- Daemon leases (prevents duplicate event emission from multiple daemons)
-- Uses single-row lease with optimistic locking for leader election
CREATE TABLE IF NOT EXISTS daemon_leases (
    lease_name TEXT PRIMARY KEY,
    holder_id TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);


-- =============================================================================
-- Base Image Version Tracking (per-project)
-- =============================================================================

-- Base image versions (tracks goldfish-base-{cpu,gpu} versions per project)
-- Each project database has its own version history, allowing independent version management
CREATE TABLE IF NOT EXISTS base_image_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    image_type TEXT NOT NULL,                 -- "cpu" or "gpu"
    version TEXT NOT NULL,                    -- "v1", "v2", etc.
    registry_tag TEXT NOT NULL,               -- Full registry tag (e.g., "us-docker.pkg.dev/.../goldfish-base-gpu:v10")
    is_current INTEGER NOT NULL DEFAULT 0,    -- 1 if this is the current version to use
    build_id TEXT,                            -- FK to docker_builds.id (if built via goldfish)
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(image_type, version),
    FOREIGN KEY (build_id) REFERENCES docker_builds(id)
);

CREATE INDEX IF NOT EXISTS idx_base_image_versions_type ON base_image_versions(image_type);
CREATE INDEX IF NOT EXISTS idx_base_image_versions_current ON base_image_versions(image_type, is_current)
    WHERE is_current = 1;


-- =============================================================================
-- Project Image Version Tracking (per-project)
-- =============================================================================

-- Project image versions (tracks {project}-{cpu,gpu} versions per project)
-- Unlike base_image_versions which track goldfish-base-*, this tracks project-specific images
CREATE TABLE IF NOT EXISTS project_image_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_name TEXT NOT NULL,               -- Project name (e.g., "mlm", "celm")
    image_type TEXT NOT NULL,                 -- "cpu" or "gpu"
    version TEXT NOT NULL,                    -- "v1", "v2", etc.
    registry_tag TEXT NOT NULL,               -- Full registry tag (e.g., "us-docker.pkg.dev/.../mlm-gpu:v1")
    is_current INTEGER NOT NULL DEFAULT 0,    -- 1 if this is the current version to use
    build_id TEXT,                            -- FK to docker_builds.id (if built via goldfish)
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(project_name, image_type, version),
    FOREIGN KEY (build_id) REFERENCES docker_builds(id)
);

CREATE INDEX IF NOT EXISTS idx_project_image_versions_project_type ON project_image_versions(project_name, image_type);
CREATE INDEX IF NOT EXISTS idx_project_image_versions_current ON project_image_versions(project_name, image_type, is_current)
    WHERE is_current = 1;


-- =============================================================================
-- Warm Pool Instances (v2: state-machine-driven)
-- =============================================================================

CREATE TABLE IF NOT EXISTS warm_instances (
    instance_name TEXT PRIMARY KEY,
    zone TEXT NOT NULL,
    project_id TEXT NOT NULL,
    machine_type TEXT NOT NULL,
    gpu_count INTEGER NOT NULL DEFAULT 0,
    image_family TEXT NOT NULL DEFAULT 'debian-12',
    image_project TEXT NOT NULL DEFAULT 'debian-cloud',
    preemptible INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'launching'
        CHECK(state IN ('launching', 'busy', 'draining', 'idle_ready', 'deleting', 'gone')),
    image_tag TEXT,
    state_entered_at TEXT,
    current_lease_run_id TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_warm_instances_state ON warm_instances(state);
CREATE INDEX IF NOT EXISTS idx_warm_instances_match
    ON warm_instances(machine_type, gpu_count, image_family, image_project, preemptible)
    WHERE state = 'idle_ready';
CREATE INDEX IF NOT EXISTS idx_warm_instances_idle ON warm_instances(state_entered_at)
    WHERE state = 'idle_ready';


-- Instance state transitions (audit trail)
CREATE TABLE IF NOT EXISTS instance_state_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_name TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    event TEXT NOT NULL,
    stage_run_id TEXT,
    error_message TEXT,
    reason TEXT,
    source TEXT NOT NULL CHECK(source IN ('controller', 'daemon', 'executor', 'warm_pool')),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_instance_transitions_name
    ON instance_state_transitions(instance_name, created_at);


-- Instance leases (explicit run↔instance ownership)
CREATE TABLE IF NOT EXISTS instance_leases (
    instance_name TEXT NOT NULL,
    stage_run_id TEXT NOT NULL,
    lease_state TEXT NOT NULL CHECK(lease_state IN ('active', 'released')),
    claimed_at TEXT NOT NULL,
    released_at TEXT,
    PRIMARY KEY (instance_name, stage_run_id)
);

-- At most one active lease per instance
CREATE UNIQUE INDEX IF NOT EXISTS idx_instance_leases_active
    ON instance_leases(instance_name) WHERE lease_state = 'active';
CREATE INDEX IF NOT EXISTS idx_instance_leases_run
    ON instance_leases(stage_run_id);
