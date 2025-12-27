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
    error TEXT,
    outcome TEXT,                     -- NULL (unset), 'success', 'bad_results' - semantic result quality
    attempt_num INTEGER,              -- Groups consecutive runs; increments after outcome='success'
    svs_findings_json TEXT,           -- JSON: SVS post-run findings (stats + AI review)
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
    reason_json TEXT                  -- JSON: structured RunReason (description, hypothesis, etc.)
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
    FOREIGN KEY (stage_run_id) REFERENCES stage_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_svs_reviews_stage_run ON svs_reviews(stage_run_id);
CREATE INDEX IF NOT EXISTS idx_svs_reviews_type ON svs_reviews(review_type);
CREATE INDEX IF NOT EXISTS idx_svs_reviews_decision ON svs_reviews(decision);
CREATE INDEX IF NOT EXISTS idx_svs_reviews_reviewed_at ON svs_reviews(reviewed_at);


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
