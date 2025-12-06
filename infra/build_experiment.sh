#!/usr/bin/env bash
# Build a Docker image from an experiment directory.
#
# Usage:
#   infra/build_experiment.sh <experiment>
#   infra/build_experiment.sh v13-mi-fix-20251203-123456
#
# The image will be tagged with the experiment name and git SHA.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

EXPERIMENT_REF=${1:-}
if [[ -z "$EXPERIMENT_REF" ]]; then
    echo "Usage: $0 <experiment-name-or-path>"
    echo ""
    echo "Examples:"
    echo "  $0 v13-mi-fix"
    echo "  $0 experiments/v13-mi-fix-20251203-123456"
    exit 1
fi

# Find experiment directory
if [[ "$EXPERIMENT_REF" == experiments/* ]]; then
    EXPERIMENT_DIR="$REPO_ROOT/$EXPERIMENT_REF"
else
    # Search for matching experiment
    MATCHES=($(ls -d "$REPO_ROOT/experiments/${EXPERIMENT_REF}"* 2>/dev/null || true))
    if [[ ${#MATCHES[@]} -eq 0 ]]; then
        echo "Error: No experiment found matching '$EXPERIMENT_REF'"
        exit 1
    elif [[ ${#MATCHES[@]} -gt 1 ]]; then
        echo "Error: Ambiguous reference '$EXPERIMENT_REF'. Matches:"
        printf '  %s\n' "${MATCHES[@]}"
        exit 1
    fi
    EXPERIMENT_DIR="${MATCHES[0]}"
fi

if [[ ! -d "$EXPERIMENT_DIR" ]]; then
    echo "Error: Experiment directory not found: $EXPERIMENT_DIR"
    exit 1
fi

EXPERIMENT_NAME=$(basename "$EXPERIMENT_DIR")
echo "Building from experiment: $EXPERIMENT_NAME"
echo "  Directory: $EXPERIMENT_DIR"

# Forward to main build script with experiment context
cd "$REPO_ROOT"
DOCKERFILE="infra/experiment.Dockerfile"

# Use build_and_push_docker.sh but override the dockerfile and context
exec "$SCRIPT_DIR/build_and_push_docker.sh" \
    "us-docker.pkg.dev/techgen314/mlm-dvae/mlm-dvae" \
    "$EXPERIMENT_DIR" \
    -f "$DOCKERFILE"
