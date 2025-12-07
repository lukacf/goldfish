#!/bin/bash
# Run deluxe E2E test with Claude Code in Docker container
#
# This tests: Claude Code → Goldfish MCP Server → GCE
#
# Usage:
#   ./run_deluxe_tests.sh             # Run test
#   ./run_deluxe_tests.sh --dry-run   # Dry-run mode (no GCE launch)
#   ./run_deluxe_tests.sh --build     # Rebuild Docker image first

set -e

cd "$(dirname "$0")"

# Parse arguments
DRY_RUN=0
BUILD=0

for arg in "$@"; do
    case $arg in
        --dry-run)
            DRY_RUN=1
            export GOLDFISH_DELUXE_DRY_RUN=1
            echo "Running in DRY-RUN mode (no GCE instances will be launched)"
            ;;
        --build)
            BUILD=1
            echo "Will rebuild Docker image"
            ;;
    esac
done

echo ""
echo "======================================================================"
echo "DELUXE E2E TEST RUNNER"
echo "======================================================================"
echo ""

# Check for .env file
if [[ ! -f .env ]]; then
    echo "ERROR: .env file not found"
    echo ""
    echo "Create a .env file with your configuration:"
    echo "  cp .env.example .env"
    echo "  # Edit .env with your actual values"
    echo ""
    cat .env.example
    echo ""
    exit 1
fi

# Load .env file
export $(grep -v '^#' .env | xargs)

# Validate required variables
MISSING=0

if [[ -z "$ANTHROPIC_API_KEY" ]]; then
    echo "ERROR: ANTHROPIC_API_KEY not set in .env"
    MISSING=1
fi

if [[ -z "$GOLDFISH_GCE_PROJECT" ]] || [[ "$GOLDFISH_GCE_PROJECT" == "your-gcp-project-id" ]]; then
    echo "ERROR: GOLDFISH_GCE_PROJECT not set or still has placeholder value"
    MISSING=1
fi

if [[ -z "$GOLDFISH_GCS_BUCKET" ]] || [[ "$GOLDFISH_GCS_BUCKET" == "gs://your-gcs-bucket" ]]; then
    echo "ERROR: GOLDFISH_GCS_BUCKET not set or still has placeholder value"
    MISSING=1
fi

if [[ $MISSING -eq 1 ]]; then
    echo ""
    echo "Please edit .env with your actual values"
    exit 1
fi

# Check GCP authentication
if [[ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
    # Try to use application default credentials
    ADC_PATH="$HOME/.config/gcloud/application_default_credentials.json"
    if [[ -f "$ADC_PATH" ]]; then
        export GOOGLE_APPLICATION_CREDENTIALS="$ADC_PATH"
        echo "Using application default credentials: $ADC_PATH"
    else
        echo "ERROR: No GCP credentials found"
        echo ""
        echo "Either:"
        echo "  1. Set GOOGLE_APPLICATION_CREDENTIALS in .env"
        echo "  2. Run: gcloud auth application-default login"
        exit 1
    fi
fi

if [[ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
    echo "ERROR: Credentials file not found: $GOOGLE_APPLICATION_CREDENTIALS"
    exit 1
fi

echo "✓ Configuration loaded from .env"
echo ""

# Display configuration
echo "Configuration:"
echo "  GCP Project:   $GOLDFISH_GCE_PROJECT"
echo "  GCS Bucket:    $GOLDFISH_GCS_BUCKET"
echo "  Dry-run:       ${GOLDFISH_DELUXE_DRY_RUN:-0}"
echo "  Credentials:   $GOOGLE_APPLICATION_CREDENTIALS"
echo "  API Key:       ${ANTHROPIC_API_KEY:0:20}..."
echo ""

# Estimate cost
if [[ $DRY_RUN -eq 0 ]]; then
    echo "This test will:"
    echo "  • Launch real GCE instances"
    echo "  • Use Claude Code API calls"
    echo "  • Incur cloud costs (~\$0.05 for GCE + API usage)"
    echo ""
    read -p "Continue? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted"
        exit 0
    fi
fi

# Build Docker image if requested
if [[ $BUILD -eq 1 ]]; then
    echo ""
    echo "Building Docker image..."
    docker-compose build
fi

# Create results directory
mkdir -p results

# Run test in Docker container
echo ""
echo "======================================================================"
echo "STARTING TEST IN DOCKER CONTAINER"
echo "======================================================================"
echo ""
echo "Claude Code will:"
echo "  1. Connect to Goldfish MCP server"
echo "  2. Initialize project"
echo "  3. Create workspace and pipeline"
echo "  4. Run pipeline stages on GCE"
echo "  5. Verify results"
echo ""

docker-compose run --rm deluxe-test

echo ""
echo "======================================================================"
echo "TEST COMPLETE"
echo "======================================================================"
echo ""
echo "✓ Deluxe E2E test finished"
echo ""
echo "Results are in: ./results/"
echo ""
