#!/bin/bash
# Quick-start script for running deluxe E2E tests
#
# Usage:
#   ./run_deluxe_tests.sh             # Run tests
#   ./run_deluxe_tests.sh --dry-run   # Dry-run mode (no GCE launch)

set -e

# Check if running in dry-run mode
DRY_RUN=0
if [[ "$1" == "--dry-run" ]]; then
    DRY_RUN=1
    echo "Running in DRY-RUN mode (no GCE instances will be launched)"
fi

# Check required environment variables
if [[ -z "$GOLDFISH_GCE_PROJECT" ]]; then
    echo "ERROR: GOLDFISH_GCE_PROJECT not set"
    echo ""
    echo "Set required environment variables:"
    echo "  export GOLDFISH_GCE_PROJECT=\"your-gcp-project-id\""
    echo "  export GOLDFISH_GCS_BUCKET=\"gs://your-bucket-name\""
    echo "  export GOLDFISH_DELUXE_TEST_ENABLED=\"1\""
    echo ""
    echo "Optional:"
    echo "  export GOLDFISH_DELUXE_ZONE=\"us-central1-a\"  # Override default zone"
    exit 1
fi

if [[ -z "$GOLDFISH_GCS_BUCKET" ]]; then
    echo "ERROR: GOLDFISH_GCS_BUCKET not set"
    exit 1
fi

# Enable deluxe tests
export GOLDFISH_DELUXE_TEST_ENABLED=1

# Set dry-run mode if requested
if [[ $DRY_RUN -eq 1 ]]; then
    export GOLDFISH_DELUXE_DRY_RUN=1
fi

# Verify GCP authentication
echo "Verifying GCP authentication..."
ACTIVE_ACCOUNT=$(gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>/dev/null || echo "")
if [[ -z "$ACTIVE_ACCOUNT" ]]; then
    echo "ERROR: No active GCP authentication"
    echo "Run: gcloud auth login"
    exit 1
fi
echo "✓ Authenticated as: $ACTIVE_ACCOUNT"

# Display configuration
echo ""
echo "Configuration:"
echo "  GCP Project:  $GOLDFISH_GCE_PROJECT"
echo "  GCS Bucket:   $GOLDFISH_GCS_BUCKET"
echo "  Zone:         ${GOLDFISH_DELUXE_ZONE:-us-central1-a (default)}"
echo "  Dry-run:      ${GOLDFISH_DELUXE_DRY_RUN:-0}"
echo ""

# Estimate cost
if [[ $DRY_RUN -eq 0 ]]; then
    echo "Estimated cost per test run: ~\$0.05 USD"
    echo "  - 3x n2-standard-4 instances (~10 minutes total)"
    echo "  - GCS storage and transfer"
    echo ""
    read -p "Continue? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted"
        exit 0
    fi
fi

# Run tests
echo ""
echo "Running deluxe E2E tests..."
echo ""

cd "$(dirname "$0")/../.."
pytest -m deluxe_gce tests/deluxe/ -v -s

echo ""
echo "✓ Tests complete"
