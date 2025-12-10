#!/bin/bash
# Deluxe E2E driven by Claude CLI prompts (MCP server auto-started by Claude)
set -euo pipefail

# Run everything from repo root so Claude picks the right project
cd /ml-project-test-repo

# Env vars are injected via docker compose env_file; just respect them
export GOLDFISH_DELUXE_DRY_RUN=${GOLDFISH_DELUXE_DRY_RUN:-0}

echo "Setting up Claude MCP..."
# Remove stale Claude config to avoid wrong project bindings
rm -f ~/.claude.json
/usr/local/bin/setup_claude.sh

echo "Config:"
echo "  Project: $GOLDFISH_GCE_PROJECT"
echo "  Bucket : $GOLDFISH_GCS_BUCKET"
echo "  DryRun : $GOLDFISH_DELUXE_DRY_RUN"

# Ensure claude CLI present
if ! command -v claude >/dev/null 2>&1; then
  echo "claude CLI not found"; exit 1
fi

# Helper: compact prompt function
cprompt() {
  claude -p --dangerously-skip-permissions "$1"
}

WORKDIR=/ml-project-test-repo
PROJECT=$WORKDIR/test-project

rm -rf "$PROJECT" "$PROJECT-dev" 2>/dev/null || true

echo "=== Phase 1: init project ==="
cprompt "Use Goldfish MCP tool initialize_project with project_name='test-project' and project_root='$WORKDIR'. After it finishes, open goldfish.yaml to CONFIRM artifact_registry is set to 'us-docker.pkg.dev/$GOLDFISH_GCE_PROJECT/goldfish' (do not restart the server or re-run init). Keep replies terse."

if [[ ! -d "$PROJECT" ]]; then
  echo "project missing; retrying init..."
  cprompt "Call initialize_project now with project_name='test-project' and project_root='$WORKDIR'. Confirm creation at '$PROJECT'. Then verify artifact_registry is 'us-docker.pkg.dev/$GOLDFISH_GCE_PROJECT/goldfish' (no edits needed)."
fi

test -d "$PROJECT" || { echo "project still missing"; exit 1; }
test -f "$PROJECT/goldfish.yaml" || { echo "goldfish.yaml missing"; exit 1; }

echo "=== Phase 2: workspace + pipeline setup ==="
cprompt "Use Goldfish MCP tools (create_workspace, mount, validate_pipeline, run_stage) only. Steps: (1) create workspace 'baseline' goal 'deluxe e2e'; (2) mount to slot 'w1'; (3) write modules for stages generate_data, preprocess, train, evaluate that pass numpy arrays via /mnt/inputs and /mnt/outputs/<name>, train uses sklearn LogisticRegression, evaluate writes accuracy to a file; (4) write configs/<stage>.yaml with profile: cpu-small; (5) write pipeline.yaml with name: baseline, proper from_stage chaining, all signals type: dataset; (6) call validate_pipeline until valid. Keep outputs concise."

test -f "$PROJECT/workspaces/w1/pipeline.yaml" || { echo "pipeline missing"; exit 1; }

echo "=== Phase 3: run stages sequentially (wait=true) ==="
if [[ "$GOLDFISH_DELUXE_DRY_RUN" == "1" ]]; then
  echo "Dry run: skip execution"; exit 0
fi

cprompt "Run stages generate_data, preprocess, train, evaluate in workspace 'baseline' using mcp__goldfish__run_stage with wait=true. After each run, call mcp__goldfish__get_outputs to confirm output path. Keep responses short."

# Verify all stage runs completed
python - <<'PY'
import sqlite3, sys, os, json
db = "/ml-project-test-repo/test-project/.goldfish/goldfish.db"
if not os.path.exists(db):
    sys.exit("DB missing")
conn = sqlite3.connect(db)
rows = conn.execute("select stage_name,status from stage_runs where workspace_name='baseline' order by started_at").fetchall()
print(rows)
if not rows or any(s!="completed" for _,s in rows):
    sys.exit("Stages not all completed")
PY

echo "=== Cleanup: GCE instances and GCS artifacts (scoped to this run) ==="
# Collect stage_run_ids from the test project DB
python - <<'PY'
import sqlite3, json, os, sys
db = "/ml-project-test-repo/test-project/.goldfish/goldfish.db"
if not os.path.exists(db):
    sys.exit(0)
conn = sqlite3.connect(db)
rows = conn.execute("select id from stage_runs where workspace_name='baseline'").fetchall()
for (rid,) in rows:
    print(rid)
PY > /tmp/stage_ids.txt

# Delete only the instances and artifacts for those stage IDs
while read -r sid; do
  [ -z "$sid" ] && continue
  gcloud compute instances delete "$sid" --zone us-central1-a --quiet || true
  gsutil -m rm -r "${GOLDFISH_GCS_BUCKET%/}/runs/${sid}/**" >/dev/null 2>&1 || true
done < /tmp/stage_ids.txt

# Clean local run directories for these stage IDs
while read -r sid; do
  [ -z "$sid" ] && continue
  rm -rf "/ml-project-test-repo/test-project/.goldfish/runs/${sid}" || true
done < /tmp/stage_ids.txt

echo "Deluxe E2E prompts finished."
