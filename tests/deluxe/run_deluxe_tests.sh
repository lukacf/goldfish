#!/bin/bash
# Run deluxe E2E test in Docker container with MCP client
#
# This tests the full stack: MCP Client → MCP Server → Goldfish → GCE
#
# Usage:
#   ./run_deluxe_tests.sh             # Run test
#   ./run_deluxe_tests.sh --dry-run   # Dry-run mode (no GCE launch)
#   ./run_deluxe_tests.sh --build     # Rebuild Docker image first

set -e

# Parse arguments
DRY_RUN=0
BUILD=0

for arg in "$@"; do
    case $arg in
        --dry-run)
            DRY_RUN=1
            echo "Running in DRY-RUN mode (no GCE instances will be launched)"
            ;;
        --build)
            BUILD=1
            echo "Will rebuild Docker image"
            ;;
    esac
done

# Check required environment variables
if [[ -z "$GOLDFISH_GCE_PROJECT" ]]; then
    echo "ERROR: GOLDFISH_GCE_PROJECT not set"
    echo ""
    echo "Set required environment variables:"
    echo "  export GOLDFISH_GCE_PROJECT=\"your-gcp-project-id\""
    echo "  export GOLDFISH_GCS_BUCKET=\"gs://your-bucket-name\""
    echo ""
    echo "For GCP authentication:"
    echo "  export GOOGLE_APPLICATION_CREDENTIALS=\"/path/to/service-account-key.json\""
    echo "  OR run: gcloud auth application-default login"
    exit 1
fi

if [[ -z "$GOLDFISH_GCS_BUCKET" ]]; then
    echo "ERROR: GOLDFISH_GCS_BUCKET not set"
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
        echo "Either:"
        echo "  1. export GOOGLE_APPLICATION_CREDENTIALS=\"/path/to/key.json\""
        echo "  2. Run: gcloud auth application-default login"
        exit 1
    fi
fi

if [[ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
    echo "ERROR: Credentials file not found: $GOOGLE_APPLICATION_CREDENTIALS"
    exit 1
fi

echo "✓ Found GCP credentials: $GOOGLE_APPLICATION_CREDENTIALS"

# Display configuration
echo ""
echo "Configuration:"
echo "  GCP Project:  $GOLDFISH_GCE_PROJECT"
echo "  GCS Bucket:   $GOLDFISH_GCS_BUCKET"
echo "  Dry-run:      $DRY_RUN"
echo "  Credentials:  $GOOGLE_APPLICATION_CREDENTIALS"
echo ""

# Set dry-run mode if requested
if [[ $DRY_RUN -eq 1 ]]; then
    export GOLDFISH_DELUXE_DRY_RUN=1
fi

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

# Change to tests/deluxe directory
cd "$(dirname "$0")"

# Build Docker image if requested
if [[ $BUILD -eq 1 ]]; then
    echo ""
    echo "Building Docker image..."
    docker-compose build
fi

# Run test in Docker container
echo ""
echo "=" * 70
echo "Running Deluxe E2E Test in Docker Container"
echo "=" * 70
echo ""

docker-compose run --rm deluxe-test

echo ""
echo "✓ Test complete"
