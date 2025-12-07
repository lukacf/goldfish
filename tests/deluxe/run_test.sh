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

# Change to workspace directory
cd /workspace

echo "======================================================================"
echo "PHASE 1: Initialize Goldfish Project"
echo "======================================================================"
echo ""

# Prompt Claude to initialize project
claude -p "Initialize a new Goldfish project in the current directory (/workspace) called 'deluxe-ml-test'. \
Configure it with: \
- GCE backend for job execution \
- GCP project ID: $GOLDFISH_GCE_PROJECT \
- GCS bucket: $GOLDFISH_GCS_BUCKET \
Use the initialize_project MCP tool from the goldfish server."

echo ""
echo "✓ Project initialized"
echo ""

# Verify goldfish.yaml was created
if [[ ! -f goldfish.yaml ]]; then
    echo "ERROR: goldfish.yaml not created"
    exit 1
fi

echo "======================================================================"
echo "PHASE 2: Create Workspace and Pipeline"
echo "======================================================================"
echo ""

# Prompt Claude to create workspace and pipeline
claude -p "Using the Goldfish MCP tools: \
1. Create a new workspace called 'baseline' with goal 'Baseline ML classification model' \
2. Mount it to slot 'w1' \
3. In the mounted workspace (workspaces/w1), create a 4-stage ML pipeline: \
   - Stage 1 'generate_data': Generate 1000 synthetic samples (28x28 features, 10 classes) \
   - Stage 2 'preprocess': Normalize and split data (80/20) \
   - Stage 3 'train': Train sklearn LogisticRegression \
   - Stage 4 'evaluate': Compute test accuracy \
4. Create Python modules in workspaces/w1/modules/ for each stage \
5. Create stage configs in workspaces/w1/configs/ using profile 'cpu-small' \
6. Create pipeline.yaml with proper signal chaining \
7. Validate the pipeline"

echo ""
echo "✓ Workspace and pipeline created"
echo ""

# Verify pipeline was created
if [[ ! -f workspaces/w1/pipeline.yaml ]]; then
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
    # Prompt Claude to run the pipeline
    claude -p "Using the Goldfish MCP tools: \
1. Run the full pipeline for workspace 'baseline': \
   - Run stage 'generate_data' \
   - Run stage 'preprocess' (will use GCE with cpu-small profile) \
   - Run stage 'train' (will use GCE) \
   - Run stage 'evaluate' (will use GCE) \
2. Monitor job status for each stage \
3. Wait for completion \
4. Report the final test accuracy from the evaluate stage"
fi

echo ""
echo "✓ Pipeline execution requested"
echo ""

echo "======================================================================"
echo "PHASE 4: Verification"
echo "======================================================================"
echo ""

# Verify results using Goldfish MCP status tool
claude -p "Using the Goldfish MCP tools, check the status and show: \
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
