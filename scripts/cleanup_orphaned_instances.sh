#!/bin/bash
# Goldfish Cost Protection - Layer 5: External Watchdog
#
# This script runs independently (e.g., via cron) and cleans up any GCE instances
# that have been running too long. It's the last line of defense against runaway costs.
#
# Usage:
#   ./cleanup_orphaned_instances.sh [--dry-run] [--max-age-hours N]
#
# Example crontab entry (run every 30 minutes):
#   */30 * * * * /path/to/cleanup_orphaned_instances.sh >> /var/log/goldfish-cleanup.log 2>&1

set -euo pipefail

# Configuration
MAX_AGE_HOURS="${MAX_AGE_HOURS:-6}"  # Default: delete instances older than 6 hours
INSTANCE_PATTERNS="stage-|job-run-"   # Patterns to match Goldfish instances
DRY_RUN=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --max-age-hours)
            MAX_AGE_HOURS="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "Goldfish Orphaned Instance Cleanup"
echo "Time: $(date -Iseconds)"
echo "Max age: ${MAX_AGE_HOURS} hours"
echo "Dry run: ${DRY_RUN}"
echo "=========================================="

# Get current timestamp
NOW=$(date +%s)
MAX_AGE_SECONDS=$((MAX_AGE_HOURS * 3600))

# List all running instances matching our patterns
INSTANCES=$(gcloud compute instances list \
    --filter="status=RUNNING AND (name~${INSTANCE_PATTERNS})" \
    --format="csv[no-heading](name,zone,creationTimestamp)" 2>/dev/null || echo "")

if [[ -z "$INSTANCES" ]]; then
    echo "No matching instances found."
    exit 0
fi

TOTAL=0
DELETED=0

echo ""
echo "Checking instances..."

while IFS=, read -r NAME ZONE CREATED; do
    TOTAL=$((TOTAL + 1))

    # Parse creation timestamp and calculate age
    CREATED_TS=$(date -d "$CREATED" +%s 2>/dev/null || echo "0")
    AGE_SECONDS=$((NOW - CREATED_TS))
    AGE_HOURS=$((AGE_SECONDS / 3600))
    AGE_MINS=$(((AGE_SECONDS % 3600) / 60))

    if [[ $AGE_SECONDS -gt $MAX_AGE_SECONDS ]]; then
        echo "  ORPHAN: $NAME (zone=$ZONE, age=${AGE_HOURS}h${AGE_MINS}m) - EXCEEDS ${MAX_AGE_HOURS}h limit"

        if [[ "$DRY_RUN" == "true" ]]; then
            echo "    [DRY RUN] Would delete $NAME"
        else
            echo "    Deleting $NAME..."
            if gcloud compute instances delete "$NAME" --zone="$ZONE" --quiet 2>&1; then
                echo "    DELETED: $NAME"
                DELETED=$((DELETED + 1))
            else
                echo "    FAILED to delete $NAME"
            fi
        fi
    else
        echo "  OK: $NAME (age=${AGE_HOURS}h${AGE_MINS}m)"
    fi
done <<< "$INSTANCES"

echo ""
echo "=========================================="
echo "Summary: Checked $TOTAL instances, deleted $DELETED"
echo "=========================================="

# Exit with error if we found orphans but didn't delete (dry run)
if [[ "$DRY_RUN" == "true" && $DELETED -eq 0 ]]; then
    # Count how many would have been deleted
    WOULD_DELETE=$(echo "$INSTANCES" | while IFS=, read -r NAME ZONE CREATED; do
        CREATED_TS=$(date -d "$CREATED" +%s 2>/dev/null || echo "0")
        AGE_SECONDS=$((NOW - CREATED_TS))
        if [[ $AGE_SECONDS -gt $MAX_AGE_SECONDS ]]; then
            echo "1"
        fi
    done | wc -l)

    if [[ $WOULD_DELETE -gt 0 ]]; then
        echo "WARNING: $WOULD_DELETE orphaned instances found (dry run, not deleted)"
    fi
fi
