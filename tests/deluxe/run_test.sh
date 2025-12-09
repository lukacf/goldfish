#!/bin/bash
# Run deluxe E2E test using actual Claude Code

set -e

echo "======================================================================"
echo "DELUXE E2E TEST: Claude Code + Goldfish MCP + GCE"
echo "======================================================================"
echo ""

# Check required environment variables
if [[ -z "$ANTHROPIC_API_KEY" ]]; then
    echo "ERROR: ANTHROPIC_API_KEY not set"
    exit 1
fi

if [[ -z "$GOLDFISH_GCE_PROJECT" ]]; then
    echo "ERROR: GOLDFISH_GCE_PROJECT not set"
    exit 1
fi

if [[ -z "$GOLDFISH_GCS_BUCKET" ]]; then
    echo "ERROR: GOLDFISH_GCS_BUCKET not set"
    exit 1
fi

echo "Configuration:"
echo "  GCP Project: $GOLDFISH_GCE_PROJECT"
echo "  GCS Bucket: $GOLDFISH_GCS_BUCKET"
echo "  Dry-run: ${GOLDFISH_DELUXE_DRY_RUN:-0}"
echo ""

# Setup Claude Code with MCP
/usr/local/bin/setup_claude.sh

# Change to test repo directory
cd /ml-project-test-repo

# Clean up any existing test project from previous runs
echo "Cleaning up previous test artifacts..."
rm -rf test-project test-project-dev 2>/dev/null || true
echo "✓ Cleanup complete"
echo ""

echo "======================================================================"
echo "PHASE 1: Initialize Goldfish Project"
echo "======================================================================"
echo ""

# Prompt Claude to initialize project
# IMPORTANT: This is a TEST of the Goldfish system - Claude must report any tool failures directly
claude -p --dangerously-skip-permissions "IMPORTANT: This is a TEST of the Goldfish MCP system. You must use the Goldfish MCP tools and report any failures directly. Do NOT create files manually or compensate for missing tools.

Initialize a new Goldfish project using the mcp__goldfish__initialize_project tool with project_name='test-project' and project_root='/ml-project-test-repo'."

echo ""
echo "✓ Project initialized"
echo ""

# Verify the created project directory exists
if [[ -d "test-project" ]]; then
    echo "Found project directory: /ml-project-test-repo/test-project"
else
    echo "ERROR: test-project directory not found"
    ls -la /ml-project-test-repo
    exit 1
fi

# Verify goldfish.yaml was created
if [[ ! -f test-project/goldfish.yaml ]]; then
    echo "ERROR: goldfish.yaml not created"
    ls -la test-project/
    exit 1
fi

echo "✓ Found goldfish.yaml"

# Update goldfish.yaml to include artifact_registry
echo "Updating goldfish.yaml with artifact_registry configuration..."
claude -p --dangerously-skip-permissions "Edit the file /ml-project-test-repo/test-project/goldfish.yaml to add 'artifact_registry' field under the 'gce' section. Set it to 'us-docker.pkg.dev/$GOLDFISH_GCE_PROJECT/goldfish'"

echo "✓ Updated goldfish.yaml with artifact_registry"

# IMPORTANT: Stay in /ml-project-test-repo where MCP config is, use relative paths for test-project

echo "======================================================================"
echo "PHASE 2: Create Workspace and Pipeline"
echo "======================================================================"
echo ""

# Prompt Claude to create workspace and pipeline in the test-project
# IMPORTANT: This is a TEST - Claude must report failures, not compensate
claude -p --dangerously-skip-permissions "IMPORTANT: This is a TEST of the Goldfish MCP system. You must use the Goldfish MCP tools and report any failures directly. Do NOT create files manually or compensate for missing tools.

For the Goldfish project at /ml-project-test-repo/test-project: \
1. Use mcp__goldfish__create_workspace to create workspace 'baseline' with goal 'Baseline ML classification model' and reason 'E2E test baseline workspace' \
2. Use mcp__goldfish__mount to mount workspace 'baseline' to slot 'w1' with reason 'Testing baseline pipeline' \
3. In the mounted workspace at /ml-project-test-repo/test-project/workspaces/w1, create a 4-stage ML pipeline: \
   - Stage 1 'generate_data': Generate 1000 synthetic samples (28x28 features, 10 classes) \
   - Stage 2 'preprocess': Normalize and split data (80/20) \
   - Stage 3 'train': Train sklearn LogisticRegression \
   - Stage 4 'evaluate': Compute test accuracy \
4. Create Python modules in /ml-project-test-repo/test-project/workspaces/w1/modules/ for each stage using Write tool \
5. Create stage configs in /ml-project-test-repo/test-project/workspaces/w1/configs/ for each stage using Write tool with profile 'cpu-small' \
6. Create /ml-project-test-repo/test-project/workspaces/w1/pipeline.yaml with proper signal chaining using Write tool \
7. Use mcp__goldfish__validate_pipeline to validate the pipeline for workspace 'baseline'"

echo ""
echo "✓ Workspace and pipeline created"
echo ""

# Verify pipeline was created
if [[ ! -f test-project/workspaces/w1/pipeline.yaml ]]; then
    echo "ERROR: pipeline.yaml not created"
    exit 1
fi

echo "======================================================================"
echo "PHASE 3: Run Pipeline"
echo "======================================================================"
echo ""

if [[ "${GOLDFISH_DELUXE_DRY_RUN:-0}" == "1" ]]; then
    echo "DRY-RUN mode: Skipping pipeline execution"
else
    # Prompt Claude to run the pipeline (explicitly name the MCP tools)
    # IMPORTANT: This is a TEST - Claude must report failures, not compensate
    claude -p --dangerously-skip-permissions "IMPORTANT: This is a TEST of the Goldfish MCP system. You must use the Goldfish MCP tools and report any failures directly. Do NOT compensate for missing tools.

Use the mcp__goldfish__run_pipeline tool to run the full pipeline for workspace 'baseline'. Then use mcp__goldfish__list_jobs to monitor job status. Wait for completion and report the final test accuracy."
fi

echo ""
echo "✓ Pipeline execution requested"
echo ""

echo "======================================================================"
echo "PHASE 4: Verification"
echo "======================================================================"
echo ""

# Verify results using Goldfish MCP status tool (explicitly name the tools)
# IMPORTANT: This is a TEST - Claude must report failures, not compensate
claude -p --dangerously-skip-permissions "IMPORTANT: This is a TEST of the Goldfish MCP system. You must use the Goldfish MCP tools and report any failures directly. Do NOT compensate for missing tools.

Use the mcp__goldfish__status tool to check the current status. Then use mcp__goldfish__list_workspaces and mcp__goldfish__list_jobs with workspace='baseline' to show: \
1. List of workspaces \
2. List of jobs for workspace 'baseline' \
3. Current slot status"

echo ""
echo "======================================================================"
echo "PHASE 5: Cleanup"
echo "======================================================================"
echo ""

# Cleanup GCE instances if they're still running
if [[ "${GOLDFISH_DELUXE_DRY_RUN:-0}" != "1" ]]; then
    echo "Cleaning up GCE instances..."

    # List and delete any instances created by this test
    gcloud compute instances list \
        --filter="name~^goldfish-deluxe-*" \
        --format="value(name,zone)" | \
    while read -r name zone; do
        if [[ -n "$name" ]]; then
            echo "Deleting instance: $name (zone: $zone)"
            gcloud compute instances delete "$name" --zone="$zone" --quiet || true
        fi
    done
fi

echo ""
echo "======================================================================"
echo "DELUXE E2E TEST COMPLETE"
echo "======================================================================"
echo ""
echo "✅ Test completed successfully!"
echo ""
echo "This test validated:"
echo "  • Claude Code CLI execution"
echo "  • MCP server connection (stdio)"
echo "  • Goldfish MCP tool usage"
echo "  • Real GCE instance launches"
echo "  • Multi-stage pipeline execution"
echo "  • Full workflow orchestration"
echo ""
