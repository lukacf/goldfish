#!/usr/bin/env bash
# Create a golden snapshot from preprocessed dataset
#
# Usage:
#   ./infra/create_golden_snapshot.sh <source-disk> <source-zone> <snapshot-name>
#
# Example:
#   ./infra/create_golden_snapshot.sh mlm-dvae-cache-ssd-f us-central1-f dataset-v3-snapshot
#
set -euo pipefail

SOURCE_DISK="${1:?Usage: $0 <source-disk> <source-zone> <snapshot-name>}"
SOURCE_ZONE="${2:?Usage: $0 <source-disk> <source-zone> <snapshot-name>}"
SNAPSHOT_NAME="${3:?Usage: $0 <source-disk> <source-zone> <snapshot-name>}"
PROJECT="${GOOGLE_CLOUD_PROJECT:-king-dnn-training-dev}"

echo "=== Creating Golden Snapshot ==="
echo "Source disk: $SOURCE_DISK"
echo "Source zone: $SOURCE_ZONE"
echo "Snapshot name: $SNAPSHOT_NAME"
echo "Project: $PROJECT"
echo

# Create snapshot
echo "Creating snapshot..."
gcloud compute snapshots create "$SNAPSHOT_NAME" \
  --source-disk="$SOURCE_DISK" \
  --source-disk-zone="$SOURCE_ZONE" \
  --storage-location=us-central1 \
  --project="$PROJECT"

echo
echo "=== Snapshot Created ==="
echo "Snapshot: $SNAPSHOT_NAME"
echo "Storage location: us-central1"
echo
echo "Monthly cost: ~\$$(gcloud compute snapshots describe "$SNAPSHOT_NAME" --format='value(diskSizeGb)' | awk '{printf "%.2f", $1 * 0.026}')"
echo
echo "Next steps:"
echo "  1. Update dvae.yaml to reference: $SNAPSHOT_NAME"
echo "  2. Delete source disk if no longer needed:"
echo "     gcloud compute disks delete $SOURCE_DISK --zone=$SOURCE_ZONE"
