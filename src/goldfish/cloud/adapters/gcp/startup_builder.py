"""Startup script builder for GCE instances.

Ported from legacy infra/startup_builder.py.
Composable functions that build shell script fragments for:
- GPU driver installation
- gcsfuse mounting
- Disk mounting
- Docker execution with proper environment
- Self-deletion and watchdog timeout for cost protection
"""

from __future__ import annotations

import shlex
from collections.abc import Mapping, Sequence
from pathlib import Path

# Configuration constants
GPU_DRIVER_MAX_ATTEMPTS = 160  # Maximum attempts to wait for GPU driver
GPU_DRIVER_RETRY_SLEEP_SEC = 15  # Seconds to sleep between GPU driver retries
GCSFUSE_MAX_ATTEMPTS = 5  # Maximum attempts to mount gcsfuse
GCSFUSE_RETRY_SLEEP_SEC = 2  # Seconds to sleep between gcsfuse retries
DEFAULT_SHM_SIZE = "16g"  # Default Docker shared memory size


def reboot_cleanup_section(bucket_mount: str, gcsfuse: bool = True) -> str:
    """Generate cleanup for stale state from previous boot (reboot-safe).

    When a preemptible instance is preempted and reboots, there may be stale state:
    - Docker containers still registered (but not running)
    - gcsfuse mount point still present (possibly stale)
    - Lock files or PID files from previous run

    This cleanup ensures the startup script can run successfully after a reboot.

    Args:
        bucket_mount: gcsfuse mount point to clean up
        gcsfuse: Whether gcsfuse is enabled (only clean up mount if True)

    Returns:
        Shell script fragment with cleanup commands
    """
    # Build mount cleanup section only if gcsfuse is enabled
    mount_detection = ""
    mount_cleanup = ""
    if gcsfuse:
        mount_detection = f"""
if mountpoint -q {bucket_mount} 2>/dev/null; then
    echo "REBOOT DETECTED: Found stale mount at {bucket_mount}"
    REBOOT_DETECTED=1
fi"""
        mount_cleanup = f"""
    # Unmount stale mount
    fusermount -u {bucket_mount} 2>/dev/null || true
    umount -f {bucket_mount} 2>/dev/null || true"""

    return f"""
# === REBOOT CLEANUP (handles stale state from previous boot) ===
# Detect if this is a reboot (vs fresh boot) by checking for stale state
REBOOT_DETECTED=0
if docker ps -aq 2>/dev/null | grep -q .; then
    echo "REBOOT DETECTED: Found stale Docker containers"
    REBOOT_DETECTED=1
fi{mount_detection}

if [[ "$REBOOT_DETECTED" == "1" ]]; then
    echo "=== CLEANING UP STALE STATE FROM PREVIOUS BOOT ==="
    log_stage "reboot_cleanup_begin" || true

    # Kill and remove all Docker containers (from previous run)
    docker kill $(docker ps -q) 2>/dev/null || true
    docker rm -f $(docker ps -aq) 2>/dev/null || true
{mount_cleanup}
    # Clean up any stale lock/pid files
    rm -f /tmp/goldfish_*.lock /tmp/goldfish_*.pid 2>/dev/null || true

    log_stage "reboot_cleanup_done" || true
    echo "=== REBOOT CLEANUP COMPLETE ==="
fi
"""


def self_deletion_section() -> str:
    """Generate self-deletion function and trap.

    This ensures the instance deletes itself on ANY exit - success, failure,
    signal, or timeout. Critical for cost protection.

    Returns:
        Shell script fragment with cleanup trap and self-delete function
    """
    return """
# === SELF-DELETION SETUP (Cost Protection Layer 1) ===
# Get instance metadata for self-deletion
INSTANCE_NAME=$(curl -sf -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name || hostname)
INSTANCE_ZONE=$(curl -sf -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}' || echo "unknown")
PROJECT_ID=$(curl -sf -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id || echo "unknown")

# Final log sync function - called by EXIT trap to ensure errors are captured
# GCS paths are set by log_syncer_section or build_startup_script
# IMPORTANT: Metrics and SVS are uploaded FIRST because they're needed by inspect_run
# for real-time dashboard updates. Logs (stdout/stderr) are larger and can wait.
sync_final_logs() {
    echo "=== SYNCING FINAL LOGS ==="
    # Upload metrics FIRST - needed for dashboard/inspect_run
    if [[ -n "${GCS_METRICS_PATH:-}" && -f "${LOCAL_METRICS:-/dev/null}" ]]; then
        echo "Uploading metrics.jsonl..."
        timeout 30 gcloud storage cp "$LOCAL_METRICS" "$GCS_METRICS_PATH" --quiet 2>/dev/null || echo "metrics upload failed"
    fi
    # Upload during-run SVS findings SECOND - needed for dashboard/inspect_run
    if [[ -n "${GCS_SVS_DURING_PATH:-}" && -f "${LOCAL_SVS_DURING:-/dev/null}" ]]; then
        echo "Uploading svs_findings_during.json..."
        timeout 30 gcloud storage cp "$LOCAL_SVS_DURING" "$GCS_SVS_DURING_PATH" --quiet 2>/dev/null || echo "svs_during upload failed"
    fi
    # Upload stdout/stderr LAST - these can be large and are less time-critical
    if [[ -n "${GCS_STDOUT_PATH:-}" && -s "${LOCAL_STDOUT:-/tmp/stdout.log}" ]]; then
        echo "Uploading stdout.log..."
        timeout 30 gcloud storage cp "${LOCAL_STDOUT:-/tmp/stdout.log}" "$GCS_STDOUT_PATH" --quiet 2>/dev/null || echo "stdout upload failed"
    fi
    if [[ -n "${GCS_STDERR_PATH:-}" && -s "${LOCAL_STDERR:-/tmp/stderr.log}" ]]; then
        echo "Uploading stderr.log..."
        timeout 30 gcloud storage cp "${LOCAL_STDERR:-/tmp/stderr.log}" "$GCS_STDERR_PATH" --quiet 2>/dev/null || echo "stderr upload failed"
    fi
    echo "=== FINAL LOG SYNC COMPLETE ==="
}

self_delete() {
    local trap_exit_code=${GOLDFISH_TRAP_EXIT_CODE:-$?}
    echo "=== SELF-DELETING INSTANCE $INSTANCE_NAME in zone $INSTANCE_ZONE (exit=$trap_exit_code) ==="
    log_stage "self_delete_begin" || true

    # CRITICAL: Write exit code to GCS BEFORE deletion so goldfish can detect failure
    # immediately instead of waiting for the 300s not_found_timeout.
    # This handles startup script failures (apt-get, docker pull, etc.) that occur
    # before the Docker container runs and writes its own exit code.
    if [[ -n "${EXIT_CODE_FILE:-}" && ! -f "${EXIT_CODE_FILE:-/dev/null}" ]]; then
        echo "Writing startup failure exit code ($trap_exit_code) to GCS..."
        echo "$trap_exit_code" > "${EXIT_CODE_FILE}" 2>/dev/null || true
        # Also try direct GCS upload in case the fuse mount isn't available yet
        if [[ -n "${GCS_EXIT_CODE_PATH:-}" ]]; then
            echo "$trap_exit_code" | timeout 30 gsutil cp - "${GCS_EXIT_CODE_PATH}" 2>/dev/null || true
        fi
    fi

    # Set exit code in instance metadata as fallback
    if [[ -n "$INSTANCE_NAME" && -n "$INSTANCE_ZONE" && -n "$PROJECT_ID" ]]; then
        gcloud compute instances add-metadata "$INSTANCE_NAME" \
            --zone="$INSTANCE_ZONE" \
            --project="$PROJECT_ID" \
            --metadata "goldfish_exit_code=$trap_exit_code" \
            --quiet 2>/dev/null || true
    fi

    # CRITICAL: Sync final logs before deletion to capture any errors
    sync_final_logs || true
    sync || true
    sleep 2
    # Delete the instance (this terminates the script)
    gcloud compute instances delete "$INSTANCE_NAME" --zone="$INSTANCE_ZONE" --project="$PROJECT_ID" --quiet 2>/dev/null || {
        echo "gcloud delete failed, falling back to shutdown"
        shutdown -h now || true
    }
}

# Trap ensures cleanup runs on ANY exit: normal, error, or signal
# This is the PRIMARY defense against orphaned instances
trap 'GOLDFISH_TRAP_EXIT_CODE=$?; echo "EXIT TRAP TRIGGERED (exit code: $GOLDFISH_TRAP_EXIT_CODE)"; self_delete' EXIT
trap 'echo "SIGTERM received"; exit 143' SIGTERM
trap 'echo "SIGINT received"; exit 130' SIGINT
"""


def upload_helper_section() -> str:
    """Generate helper function for uploading logs with retry and verification.

    Returns:
        Shell script fragment with upload_logs_with_retry and upload_exit_code functions
    """
    return """
# === LOG UPLOAD HELPER (Ensures logs are uploaded before deletion) ===
upload_logs_with_retry() {
    local file=$1
    local dest=$2
    local max_attempts=3

    # Skip if file doesn't exist or is empty
    if [[ ! -f "$file" || ! -s "$file" ]]; then
        echo "Skipping upload: $file (not found or empty)"
        return 0
    fi

    for i in $(seq 1 $max_attempts); do
        echo "Uploading $(basename $file) to GCS (attempt $i/$max_attempts)..."

        # Upload with 60-second timeout
        if timeout 60 gsutil cp "$file" "$dest" 2>&1; then
            # Verify upload succeeded
            if timeout 10 gsutil ls "$dest" &>/dev/null; then
                echo "✓ Upload verified: $dest"
                return 0
            else
                echo "✗ Upload succeeded but verification failed"
            fi
        else
            echo "✗ Upload command failed"
        fi

        # Wait before retry (unless last attempt)
        if [[ $i -lt $max_attempts ]]; then
            sleep 2
        fi
    done

    echo "✗ Failed to upload $file after $max_attempts attempts"
    return 1
}

# === CRITICAL UPLOAD FOR EXIT CODE (State machine depends on this) ===
# exit_code.txt is MANDATORY - the daemon uses it to determine run success/failure.
# If this upload fails and the instance self-deletes, daemon sees "dead + no exit code"
# and incorrectly concludes the run crashed (TERMINATED state).
#
# Unlike stdout/stderr which are nice-to-have, exit_code.txt is CRITICAL.
# We use more retries and longer timeouts to maximize upload success.
upload_exit_code() {
    local file=$1
    local dest=$2
    local max_attempts=10  # More retries than normal uploads
    local timeout_sec=120  # Longer timeout for transient issues

    # Exit code file should always exist (we just created it)
    if [[ ! -f "$file" ]]; then
        echo "ERROR: exit_code file not found: $file"
        return 1
    fi

    for i in $(seq 1 $max_attempts); do
        echo "Uploading exit_code.txt to GCS (attempt $i/$max_attempts)..."

        # Upload with extended timeout
        if timeout $timeout_sec gsutil cp "$file" "$dest" 2>&1; then
            # Verify upload succeeded
            if timeout 30 gsutil ls "$dest" &>/dev/null; then
                echo "✓ Exit code upload verified: $dest"
                return 0
            else
                echo "✗ Exit code upload succeeded but verification failed"
            fi
        else
            echo "✗ Exit code upload command failed"
        fi

        # Exponential backoff: 2s, 4s, 8s, ...
        local sleep_time=$((2 ** i))
        if [[ $sleep_time -gt 30 ]]; then
            sleep_time=30  # Cap at 30 seconds
        fi
        if [[ $i -lt $max_attempts ]]; then
            echo "Retrying in ${sleep_time}s..."
            sleep $sleep_time
        fi
    done

    # CRITICAL: Do NOT return silently here. The caller must know upload failed.
    echo "CRITICAL: Failed to upload exit_code.txt after $max_attempts attempts"
    echo "WARNING: Run may be marked as TERMINATED instead of COMPLETED"
    return 1
}
"""


def watchdog_section(max_runtime_seconds: int) -> str:
    """Generate watchdog process that force-kills after timeout.

    This is the SECONDARY defense - if the main process hangs indefinitely,
    the watchdog will eventually trigger and delete the instance.

    Args:
        max_runtime_seconds: Maximum runtime before forced deletion

    Returns:
        Shell script fragment that starts background watchdog
    """
    return f"""
# === WATCHDOG TIMEOUT (Cost Protection Layer 2) ===
# Background process that will force-delete after {max_runtime_seconds}s ({max_runtime_seconds // 3600}h {(max_runtime_seconds % 3600) // 60}m)
(
    sleep {max_runtime_seconds}
    echo "=== WATCHDOG TIMEOUT REACHED ({max_runtime_seconds}s) - FORCING DELETION ==="
    log_stage "watchdog_timeout" || true
    # Write termination cause to GCS for daemon to detect
    if [[ -n "${{GCS_TERMINATION_CAUSE_PATH:-}}" ]]; then
        echo "watchdog" | gsutil cp - "$GCS_TERMINATION_CAUSE_PATH" 2>/dev/null || true
    fi
    # CRITICAL: Delete instance FIRST, before killing processes
    # Previous bug: pkill -9 -u root killed the watchdog itself before gcloud could run!
    echo "Deleting instance $INSTANCE_NAME..."
    gcloud compute instances delete "$INSTANCE_NAME" --zone="$INSTANCE_ZONE" --project="$PROJECT_ID" --quiet 2>/dev/null &
    DELETE_PID=$!
    # Give gcloud a head start, then kill docker to stop wasting GPU time
    sleep 2
    docker kill $(docker ps -q) 2>/dev/null || true
    # Wait for delete to complete (or timeout after 60s)
    timeout 60 tail --pid=$DELETE_PID -f /dev/null 2>/dev/null || true
    # Fallback: if gcloud delete failed, force shutdown
    shutdown -h now || true
) &
WATCHDOG_PID=$!
echo "Watchdog started (PID=$WATCHDOG_PID, timeout={max_runtime_seconds}s)"
"""


def supervisor_section(heartbeat_timeout_seconds: int = 600, gcs_log_path: str = "") -> str:
    """Generate supervisor process that monitors job health via heartbeat.

    This is Layer 4 defense - monitors the heartbeat file written by
    goldfish.io.heartbeat() calls from user code. If no heartbeat for
    the configured timeout, uploads logs and terminates.

    Args:
        heartbeat_timeout_seconds: Seconds without heartbeat before termination
        gcs_log_path: GCS path to upload logs before termination

    Returns:
        Shell script fragment that starts background supervisor
    """
    return f"""
# === JOB SUPERVISOR (Cost Protection Layer 4) ===
# Monitors heartbeat file from goldfish.io.heartbeat() - if stale for {heartbeat_timeout_seconds}s, terminates
HEARTBEAT_FILE="/mnt/outputs/.goldfish/heartbeat"
HEARTBEAT_TIMEOUT={heartbeat_timeout_seconds}
GCS_LOG_PATH="{gcs_log_path}"

check_heartbeat_age() {{
    # Returns seconds since last heartbeat, or -1 if no file
    if [[ ! -f "$HEARTBEAT_FILE" ]]; then
        echo "-1"
        return
    fi
    local timestamp=$(grep -o '"timestamp": *[0-9.]*' "$HEARTBEAT_FILE" 2>/dev/null | grep -o '[0-9.]*' || echo "0")
    local now=$(date +%s)
    echo $((now - ${{timestamp%.*}}))
}}

upload_logs_before_death() {{
    echo "SUPERVISOR: Uploading logs before termination..."
    if [[ -n "$GCS_LOG_PATH" ]]; then
        gsutil -m cp -r /mnt/outputs/.goldfish "$GCS_LOG_PATH/supervisor_dump/" 2>/dev/null || true
        gsutil cp /tmp/stage_times.log "$GCS_LOG_PATH/supervisor_dump/" 2>/dev/null || true
        # Try to capture docker logs
        docker logs $(docker ps -q) > /tmp/docker_final.log 2>&1 || true
        gsutil cp /tmp/docker_final.log "$GCS_LOG_PATH/supervisor_dump/" 2>/dev/null || true
    fi
    log_stage "supervisor_logs_uploaded" || true
}}

start_supervisor() {{
    (
        local check_interval=30
        local grace_period=120  # Initial grace period for job to start
        local started_at=$(date +%s)

        echo "SUPERVISOR: Started (heartbeat_timeout={heartbeat_timeout_seconds}s, grace_period=${{grace_period}}s)"

        while true; do
            sleep $check_interval

            # Check if docker container is still running
            if ! docker ps -q | grep -q .; then
                echo "SUPERVISOR: No Docker containers running, exiting"
                break
            fi

            local age=$(check_heartbeat_age)
            local elapsed=$(($(date +%s) - started_at))

            # During grace period, don't enforce heartbeat
            if [[ $elapsed -lt $grace_period ]]; then
                echo "SUPERVISOR: In grace period ($elapsed/$grace_period s)"
                continue
            fi

            # If no heartbeat file yet after grace period, that's a problem
            if [[ "$age" == "-1" ]]; then
                echo "SUPERVISOR: No heartbeat file after grace period!"
                if [[ $elapsed -gt $((grace_period + HEARTBEAT_TIMEOUT)) ]]; then
                    echo "=== SUPERVISOR: No heartbeat ever received - TERMINATING ==="
                    log_stage "supervisor_no_heartbeat" || true
                    # Write termination cause to GCS for daemon to detect
                    if [[ -n "${{GCS_TERMINATION_CAUSE_PATH:-}}" ]]; then
                        echo "supervisor" | gsutil cp - "$GCS_TERMINATION_CAUSE_PATH" 2>/dev/null || true
                    fi
                    upload_logs_before_death
                    docker kill $(docker ps -q) 2>/dev/null || true
                    sleep 5
                    exit 1
                fi
                continue
            fi

            # Check heartbeat age
            if [[ $age -gt $HEARTBEAT_TIMEOUT ]]; then
                echo "=== SUPERVISOR: Heartbeat stale (${{age}}s > {heartbeat_timeout_seconds}s) - TERMINATING ==="
                log_stage "supervisor_heartbeat_stale" || true
                # Write termination cause to GCS for daemon to detect
                if [[ -n "${{GCS_TERMINATION_CAUSE_PATH:-}}" ]]; then
                    echo "supervisor" | gsutil cp - "$GCS_TERMINATION_CAUSE_PATH" 2>/dev/null || true
                fi
                upload_logs_before_death
                docker kill $(docker ps -q) 2>/dev/null || true
                sleep 5
                exit 1
            else
                echo "SUPERVISOR: Heartbeat OK (age=${{age}}s)"
            fi
        done
    ) &
    SUPERVISOR_PID=$!
    echo "Supervisor started (PID=$SUPERVISOR_PID, heartbeat_timeout={heartbeat_timeout_seconds}s)"
}}
"""


def gpu_driver_section() -> str:
    """Install NVIDIA drivers when a GPU is present (safe on CPU nodes).

    Returns:
        Shell script fragment
    """
    return f"""
GPU_PRESENT=0
if lspci | grep -i nvidia >/dev/null 2>&1; then
  GPU_PRESENT=1
fi

if [[ "$GPU_PRESENT" == "1" ]]; then
  if ! command -v nvidia-smi >/dev/null 2>&1; then
    log_stage "driver_install"
    apt-get update -y
    apt-get install -y linux-headers-$(uname -r) linux-headers-amd64 curl gnupg
    DISTRO=$(lsb_release -cs 2>/dev/null || echo "bookworm")
    case "$DISTRO" in
      bookworm) CUDA_REPO="https://developer.download.nvidia.com/compute/cuda/repos/debian12/x86_64/" ;;
      bullseye) CUDA_REPO="https://developer.download.nvidia.com/compute/cuda/repos/debian11/x86_64/" ;;
      jammy|noble|focal) CUDA_REPO="https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/" ;;
      *) CUDA_REPO="https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/" ;;
    esac
    curl -fsSL "${{CUDA_REPO}}/3bf863cc.pub" | gpg --dearmor -o /usr/share/keyrings/cuda-archive-keyring.gpg
    echo "deb [signed-by=/usr/share/keyrings/cuda-archive-keyring.gpg] ${{CUDA_REPO}} /" > /etc/apt/sources.list.d/cuda.list
    apt-get update -y
    apt-get install -y cuda-drivers nvidia-container-toolkit
    nvidia-ctk runtime configure --runtime=docker --set-as-default || true
    systemctl restart docker || true
    log_stage "driver_wait"
    DRIVER_READY=0
    for attempt in $(seq 1 {GPU_DRIVER_MAX_ATTEMPTS}); do
      if command -v nvidia-smi >/dev/null 2>&1; then
        if nvidia-smi >/tmp/nvidia-smi.log 2>&1; then
          DRIVER_READY=1
          break
        fi
      fi
      echo "nvidia-smi not ready (attempt ${{attempt}}); sleeping {GPU_DRIVER_RETRY_SLEEP_SEC}s"
      sleep {GPU_DRIVER_RETRY_SLEEP_SEC}
    done
    if [[ "$DRIVER_READY" -ne 1 ]]; then
      echo "NVIDIA driver failed to initialize" >&2
      exit 1
    fi
    log_stage "driver_ready"
  else
    log_stage "driver_ready"
  fi
fi
"""


def gcsfuse_section(bucket: str, mount_point: str) -> str:
    """Install and mount gcsfuse for GCS access.

    Args:
        bucket: GCS bucket name (without gs:// prefix)
        mount_point: Local mount point (e.g., "/mnt/gcs")

    Returns:
        Shell script fragment
    """
    mount_point = str(Path(mount_point))
    return f"""
curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt gcsfuse-$(lsb_release -c -s) main" | tee /etc/apt/sources.list.d/gcsfuse.list
apt-get update -y
timeout 120 apt-get install -y gcsfuse || true
mkdir -p {mount_point}
# Enable user_allow_other in fuse.conf for allow_other to work
grep -q "^user_allow_other" /etc/fuse.conf 2>/dev/null || echo "user_allow_other" >> /etc/fuse.conf
echo "DEBUG: Mounting gcsfuse with uid=1000 gid=100 file-mode=0644 dir-mode=0755 allow_other"
for attempt in $(seq 1 {GCSFUSE_MAX_ATTEMPTS}); do
  if gcsfuse --implicit-dirs --uid=1000 --gid=100 --file-mode=0644 --dir-mode=0755 -o allow_other {bucket} {mount_point}; then
    if mountpoint -q {mount_point}; then
      break
    fi
  fi
  echo "gcsfuse mount retry $attempt failed; sleeping {GCSFUSE_RETRY_SLEEP_SEC}s" >&2
  fusermount -u {mount_point} 2>/dev/null || true
  sleep {GCSFUSE_RETRY_SLEEP_SEC}
done
if ! mountpoint -q {mount_point}; then
  echo "Failed to mount {bucket} at {mount_point}" >&2
  exit 1
fi
"""


def disk_mount_section(disk_id: str, mount_point: str, mode: str = "ro") -> str:
    """Mount an attached disk with retry logic.

    Args:
        disk_id: Disk device name (without /dev/disk/by-id/google- prefix)
        mount_point: Local mount point (e.g., "/mnt/data")
        mode: Mount mode ("ro" or "rw")

    Returns:
        Shell script fragment
    """
    mount_point = str(Path(mount_point))
    device_base = f"/dev/disk/by-id/google-{disk_id}"
    return f"""
DEVICE=""
CANDIDATES=("{device_base}-part1" "{device_base}p1" "{device_base}")
for attempt in $(seq 1 10); do
  for cand in "${{CANDIDATES[@]}}"; do
    if [[ -e "$cand" ]]; then DEVICE="$cand"; break; fi
  done
  [[ -n "$DEVICE" ]] && break
  echo "Waiting for disk {disk_id} (attempt $attempt)"; sleep 2
done
if [[ -z "$DEVICE" ]]; then
  echo "Data disk {disk_id} not found" >&2
  ls -l /dev/disk/by-id || true
  exit 1
fi
mkdir -p {mount_point}
if ! mount -t ext4 -o {mode} "$DEVICE" {mount_point}; then
  echo "Failed to mount {disk_id} at {mount_point}" >&2
  exit 1
fi
"""


def docker_run_section(
    *,
    image: str,
    env_keys: Sequence[str],
    mounts: Sequence[tuple[str, str]],
    entrypoint: str,
    cmd: str = "",
    shm_size: str = DEFAULT_SHM_SIZE,
) -> str:
    """Generate Docker run command with GPU detection.

    Args:
        image: Docker image to run
        env_keys: Environment variable names to pass through
        mounts: List of (host_path, container_path) tuples
        entrypoint: Container entrypoint command
        cmd: Command/script to pass to entrypoint (e.g., "/entrypoint.sh")
        shm_size: Shared memory size (default from DEFAULT_SHM_SIZE)

    Returns:
        Shell script fragment defining DOCKER_CMD array
    """
    env_flags = " ".join(f"-e {key}" for key in env_keys)
    mount_flags = " ".join(f"-v {host}:{target}" for host, target in mounts)
    cmd_part = f" {cmd}" if cmd else ""

    return f"""
DOCKER_GPU_ARGS=""
if command -v nvidia-smi >/dev/null 2>&1; then
  DOCKER_GPU_ARGS="--gpus all"
fi

DOCKER_CMD=(
  docker run --rm $DOCKER_GPU_ARGS \\
  --ipc=host \\
  --ulimit memlock=-1 --ulimit stack=67108864 \\
  --shm-size={shm_size} \\
  {env_flags} \\
  {mount_flags} \\
  --entrypoint {entrypoint} \\
  {image}{cmd_part}
)
"""


def stage_log_section(gcs_uri: str) -> str:
    """Set up stage logging function that streams to GCS.

    Args:
        gcs_uri: GCS URI for stage log (e.g., "gs://bucket/path/stage_times.log")

    Returns:
        Shell script fragment defining log_stage() function
    """
    return f"""
LOCAL_STAGE_LOG=/tmp/stage_times.log
: > "$LOCAL_STAGE_LOG"
log_stage() {{
  echo "$(date --iso-8601=seconds) :: $1" | tee -a "$LOCAL_STAGE_LOG"
  gsutil cp "$LOCAL_STAGE_LOG" {gcs_uri} >/dev/null 2>&1 || true
}}
"""


def log_syncer_section(bucket: str, bucket_path: str, sync_interval: int = 30) -> str:
    """Generate background log syncer that periodically uploads logs to GCS.

    GCSFuse streaming writes don't finalize objects until the file is closed,
    making logs invisible during execution. This syncer copies logs from local
    /tmp/ to GCS periodically, enabling real-time log viewing.

    Files synced:
    - stdout.log: Container stdout
    - stderr.log: Container stderr
    - metrics.jsonl: Goldfish metrics + artifacts (from /mnt/outputs/.goldfish/)
    - svs_findings_during.json: During-run AI review findings (real-time)

    Args:
        bucket: GCS bucket name (without gs:// prefix)
        bucket_path: Path within bucket for this run
        sync_interval: Seconds between syncs (default 30)

    Returns:
        Shell script fragment with start_log_syncer function
    """
    gcs_stdout = f"gs://{bucket}/{bucket_path}/logs/stdout.log"
    gcs_stderr = f"gs://{bucket}/{bucket_path}/logs/stderr.log"
    gcs_metrics = f"gs://{bucket}/{bucket_path}/logs/metrics.jsonl"
    gcs_svs_during = f"gs://{bucket}/{bucket_path}/outputs/.goldfish/svs_findings_during.json"

    return f"""
# === LOG SYNCER (Real-time log visibility) ===
# Background process that periodically uploads logs, metrics, and SVS findings to GCS
# This works around gcsfuse streaming writes not finalizing until close()
LOCAL_STDOUT=/tmp/stdout.log
LOCAL_STDERR=/tmp/stderr.log
LOCAL_METRICS=/mnt/outputs/.goldfish/metrics.jsonl
LOCAL_SVS_DURING=/mnt/outputs/.goldfish/svs_findings_during.json
LOG_SYNC_INTERVAL={sync_interval}

# Export GCS paths for sync_final_logs() to use in EXIT trap
GCS_STDOUT_PATH="{gcs_stdout}"
GCS_STDERR_PATH="{gcs_stderr}"
GCS_METRICS_PATH="{gcs_metrics}"
GCS_SVS_DURING_PATH="{gcs_svs_during}"

start_log_syncer() {{
    (
        # Wait for Docker to start and create initial logs
        sleep 5

        # Sync logs periodically while Docker is running
        # IMPORTANT: Metrics and SVS are synced FIRST for faster dashboard updates
        while kill -0 $DOCKER_PID 2>/dev/null; do
            sleep $LOG_SYNC_INTERVAL

            # Sync metrics.jsonl FIRST if it exists (needed for dashboard)
            if [[ -f "$LOCAL_METRICS" ]]; then
                gcloud storage cp "$LOCAL_METRICS" {gcs_metrics} --quiet 2>/dev/null || true
            fi

            # Sync during-run SVS findings SECOND (needed for dashboard)
            if [[ -f "$LOCAL_SVS_DURING" ]]; then
                gcloud storage cp "$LOCAL_SVS_DURING" {gcs_svs_during} --quiet 2>/dev/null || true
            fi

            # Sync stdout/stderr LAST (can be large, less time-critical)
            gcloud storage cp "$LOCAL_STDOUT" {gcs_stdout} --quiet 2>/dev/null || true
            gcloud storage cp "$LOCAL_STDERR" {gcs_stderr} --quiet 2>/dev/null || true
        done

        # Final sync after Docker exits to capture last logs
        sleep 2
        # Final sync of metrics and SVS findings FIRST
        if [[ -f "$LOCAL_METRICS" ]]; then
            gcloud storage cp "$LOCAL_METRICS" {gcs_metrics} --quiet 2>/dev/null || true
        fi
        if [[ -f "$LOCAL_SVS_DURING" ]]; then
            gcloud storage cp "$LOCAL_SVS_DURING" {gcs_svs_during} --quiet 2>/dev/null || true
        fi
        # Then logs
        gcloud storage cp "$LOCAL_STDOUT" {gcs_stdout} --quiet 2>/dev/null || true
        gcloud storage cp "$LOCAL_STDERR" {gcs_stderr} --quiet 2>/dev/null || true
    ) &
    LOG_SYNCER_PID=$!
    echo "Log syncer started (PID=$LOG_SYNCER_PID, interval={sync_interval}s)"
}}
"""


def metadata_syncer_section(sync_interval: int = 1) -> str:
    """Generate metadata syncer for Overdrive-style on-demand log refresh.

    Args:
        sync_interval: Seconds between metadata polls (default 1).

    Returns:
        Shell script fragment with start_metadata_syncer function.
    """
    return f"""
# === METADATA SYNCER (Overdrive on-demand sync) ===
METADATA_SYNC_INTERVAL={sync_interval}
METADATA_SIGNAL_URL="http://metadata.google.internal/computeMetadata/v1/instance/attributes/goldfish"
METADATA_SYNCER_STARTED=0

start_metadata_syncer() {{
    # Guard against multiple syncers (e.g., if startup script runs twice)
    if [[ "$METADATA_SYNCER_STARTED" == "1" ]]; then
        echo "Metadata syncer already started, skipping"
        return
    fi
    METADATA_SYNCER_STARTED=1

    (
        set +e
        LAST_ACK=""
        LAST_SEEN=""
        while true; do
            SIG_JSON=$(curl -sf -H "Metadata-Flavor: Google" "$METADATA_SIGNAL_URL" 2>/dev/null || true)
            if [[ -n "$SIG_JSON" ]]; then
                # Extract command and request_id, trimming whitespace
                CMD=$(echo "$SIG_JSON" | grep -o '"command"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"command"[[:space:]]*:[[:space:]]*"\\([^"]*\\)".*/\\1/' | tr -d '[:space:]')
                REQ_ID=$(echo "$SIG_JSON" | grep -o '"request_id"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"request_id"[[:space:]]*:[[:space:]]*"\\([^"]*\\)".*/\\1/' | tr -d '[:space:]')
                if [[ "$CMD" == "sync" && -n "$REQ_ID" && "$REQ_ID" != "$LAST_SEEN" ]]; then
                    echo "SYNCER: Processing sync request $REQ_ID (last_seen=$LAST_SEEN)"
                    LAST_SEEN="$REQ_ID"
                    # Set ACK FIRST to tell dev side we received the signal
                    # This allows dev side to start polling GCS while we upload
                    if command -v gcloud >/dev/null 2>&1; then
                        gcloud compute instances add-metadata "$INSTANCE_NAME" \
                            --zone="$INSTANCE_ZONE" \
                            --project="$PROJECT_ID" \
                            --metadata "goldfish_ack=$REQ_ID" \
                            --quiet 2>&1 || echo "Failed to set goldfish_ack for $REQ_ID"
                    fi
                    # THEN upload files - dev side can poll for these
                    sync_final_logs || true
                    echo "SYNCER: Completed sync request $REQ_ID"
                fi
            fi
            sleep "$METADATA_SYNC_INTERVAL"
        done
    ) &
    METADATA_SYNCER_PID=$!
    echo "Metadata syncer started (PID=$METADATA_SYNCER_PID, interval=${{METADATA_SYNC_INTERVAL}}s)"
}}
"""


def build_startup_script(
    *,
    bucket: str,
    bucket_prefix: str,
    run_path: str,
    image: str,
    entrypoint: str,
    env_map: Mapping[str, str],
    mounts: Sequence[tuple[str, str]] = (),
    bucket_mount: str = "/mnt/gcs",
    gcsfuse: bool = True,
    shm_size: str = DEFAULT_SHM_SIZE,
    disk_mounts: Sequence[tuple[str, str, str]] = (),
    pre_run_cmds: Sequence[str] = (),
    post_run_cmds: Sequence[str] = (),
    cmd: str = "",
    max_runtime_seconds: int | None = None,
    heartbeat_timeout_seconds: int | None = None,
    log_sync_interval: int | None = None,
) -> str:
    """Build complete startup script for GCE instance.

    Args:
        bucket: GCS bucket name (without gs:// prefix)
        bucket_prefix: Prefix within bucket
        run_path: Path for this run within bucket
        image: Docker image to run
        entrypoint: Container entrypoint
        env_map: Environment variables to pass to container
        mounts: Additional volume mounts (host_path, container_path)
        bucket_mount: Local mount point for gcsfuse (default "/mnt/gcs")
        gcsfuse: Enable gcsfuse mounting (default True)
        shm_size: Shared memory size (default from DEFAULT_SHM_SIZE)
        disk_mounts: List of (disk_id, mount_point, mode) tuples
        pre_run_cmds: Commands to run before Docker
        post_run_cmds: Commands to run after Docker
        cmd: Command/script to pass to entrypoint (e.g., "/entrypoint.sh")
        max_runtime_seconds: Maximum runtime before watchdog kills instance (None=no limit)
        heartbeat_timeout_seconds: Heartbeat timeout for supervisor (None=no supervisor)
        log_sync_interval: Seconds between log syncs to GCS (None=use gcsfuse, >0=use syncer)

    Returns:
        Complete startup script as string
    """
    # Build GCS paths
    bucket_path = "/".join(part for part in [bucket_prefix.strip("/"), run_path.strip("/")] if part)
    stage_uri = f"gs://{bucket}/{bucket_path}/logs/stage_times.log"

    # Determine log paths based on sync mode
    # When log_sync_interval is set, use local /tmp/ paths and periodic sync
    # This works around gcsfuse streaming writes not finalizing until close()
    use_log_syncer = log_sync_interval is not None and log_sync_interval > 0
    if use_log_syncer:
        stdout_log = "/tmp/stdout.log"
        stderr_log = "/tmp/stderr.log"
    else:
        # Legacy: write directly to gcsfuse mount (only visible after container exits)
        stdout_log = f"{bucket_mount}/{bucket_path}/logs/stdout.log"
        stderr_log = f"{bucket_mount}/{bucket_path}/logs/stderr.log"
    log_file = stdout_log  # Backward compat: log_file points to stdout

    # Export environment variables with proper shell escaping
    env_exports = []
    for k, v in env_map.items():
        # Use shlex.quote for proper shell escaping (prevents command injection)
        safe_v = shlex.quote(str(v))
        env_exports.append(f"export {k}={safe_v}")

    env_exports_block = "\n".join(env_exports)
    env_keys = list(env_map.keys())

    # Build script parts
    # IMPORTANT: Order matters! Log section must come before self_deletion (uses log_stage)
    # Self-deletion trap must be set up early to catch any failures
    parts: list[str] = [
        "#!/bin/bash",
        "set -euxo pipefail",
        "export DEBIAN_FRONTEND=noninteractive",
        stage_log_section(stage_uri),
        # Self-deletion trap - MUST be early to catch failures in apt-get, driver install, etc.
        self_deletion_section(),
        # Set exit code paths early so EXIT trap can write exit code on startup failures
        # EXIT_CODE_FILE is the gcsfuse path (only works if gcsfuse is mounted)
        # GCS_EXIT_CODE_PATH is the direct GCS path (always works if gcloud is available)
        f'EXIT_CODE_FILE="{bucket_mount}/{bucket_path}/logs/exit_code.txt"',
        f'GCS_EXIT_CODE_PATH="gs://{bucket}/{bucket_path}/logs/exit_code.txt"',
        # Log upload helper - retry and verify uploads before deletion
        upload_helper_section(),
        'log_stage "startup_begin"',
        # Reboot cleanup - handles stale state if instance rebooted after preemption
        reboot_cleanup_section(bucket_mount, gcsfuse=gcsfuse),
    ]

    # Add watchdog if max_runtime specified
    if max_runtime_seconds is not None and max_runtime_seconds > 0:
        parts.append(watchdog_section(max_runtime_seconds))

    # Add supervisor if heartbeat monitoring enabled
    gcs_log_path = f"gs://{bucket}/{bucket_path}/logs"
    if heartbeat_timeout_seconds is not None and heartbeat_timeout_seconds > 0:
        parts.append(supervisor_section(heartbeat_timeout_seconds, gcs_log_path))

    # Add log syncer for real-time log visibility
    if use_log_syncer and log_sync_interval is not None:
        parts.append(log_syncer_section(bucket, bucket_path, log_sync_interval))

    # Add metadata syncer for on-demand Overdrive sync
    parts.append(metadata_syncer_section())

    # Always set log/metrics paths for sync_final_logs() (used by metadata syncer and EXIT trap)
    # log_syncer_section may override these, but they're needed even if periodic sync is disabled
    gcs_svs_during_path = f"gs://{bucket}/{bucket_path}/outputs/.goldfish/svs_findings_during.json"
    gcs_termination_cause_path = f"gs://{bucket}/{bucket_path}/logs/termination_cause.txt"
    path_exports = f"""
# Paths for sync_final_logs() - always needed for on-demand Overdrive sync
LOCAL_STDOUT=/tmp/stdout.log
LOCAL_STDERR=/tmp/stderr.log
LOCAL_METRICS=/mnt/outputs/.goldfish/metrics.jsonl
LOCAL_SVS_DURING=/mnt/outputs/.goldfish/svs_findings_during.json
GCS_STDOUT_PATH="gs://{bucket}/{bucket_path}/logs/stdout.log"
GCS_STDERR_PATH="gs://{bucket}/{bucket_path}/logs/stderr.log"
GCS_METRICS_PATH="gs://{bucket}/{bucket_path}/logs/metrics.jsonl"
GCS_SVS_DURING_PATH="{gcs_svs_during_path}"
GCS_TERMINATION_CAUSE_PATH="{gcs_termination_cause_path}"
"""

    parts.extend(
        [
            "apt-get update -y",
            "apt-get install -y ca-certificates gnupg curl docker.io lsb-release",
            "systemctl enable --now docker || true",
            'log_stage "docker_ready"',
            env_exports_block,
            path_exports,
            "start_metadata_syncer",
        ]
    )

    # GPU driver installation
    parts.append(gpu_driver_section())

    # gcsfuse mounting
    if gcsfuse:
        parts.append('log_stage "gcsfuse_begin"')
        parts.append(gcsfuse_section(bucket, bucket_mount))
        parts.append('log_stage "gcsfuse_ready"')

    # Disk mounting
    for disk_id, mount_point, mode in disk_mounts:
        parts.append(disk_mount_section(disk_id, mount_point, mode))

    if disk_mounts:
        parts.append('log_stage "cache_ready"')

    # Docker login and image pull - extract registry domain from image
    # Image format: <registry>/<project>/<repo>/<name>:<tag>
    # e.g., us-docker.pkg.dev/my-project/goldfish/image:v1
    registry_domain = image.split("/")[0] if "/" in image else ""
    if registry_domain and "." in registry_domain:
        # Use robust Docker auth that works on GCE instances with service accounts
        # gcloud auth configure-docker may fail silently on some VM images
        # Fallback to access token auth which is more reliable
        parts.append(f"""
# Configure Docker to pull from Artifact Registry
if ! gcloud auth configure-docker {registry_domain} --quiet 2>/dev/null; then
    echo "gcloud auth configure-docker failed, trying access token auth..."
    gcloud auth print-access-token 2>/dev/null | docker login -u oauth2accesstoken --password-stdin https://{registry_domain} || echo "Docker login failed"
fi
""")
    parts.append('log_stage "docker_login"')
    # Don't silently ignore pull failures - log them for debugging
    parts.append(f"""
if ! docker pull {image}; then
    echo "ERROR: Failed to pull Docker image: {image}" | tee -a /tmp/stderr.log
    echo "Attempting to diagnose..." | tee -a /tmp/stderr.log
    set +e
    docker info 2>&1 | head -20 | tee -a /tmp/stderr.log
    echo "Docker config:" | tee -a /tmp/stderr.log
    cat ~/.docker/config.json 2>/dev/null | tee -a /tmp/stderr.log || echo "No Docker config found" | tee -a /tmp/stderr.log
    set -e
    log_stage "docker_pull_failed" || true
    exit 1
fi
""")
    parts.append('log_stage "docker_pull"')

    # Pre-run commands
    for pre_cmd in pre_run_cmds:
        parts.append(pre_cmd)

    # Docker run command
    parts.append(
        docker_run_section(
            image=image,
            env_keys=env_keys,
            mounts=list(mounts),
            entrypoint=entrypoint,
            cmd=cmd,
            shm_size=shm_size,
        )
    )

    # Execute Docker container
    parts.append(f'mkdir -p "$(dirname "{log_file}")"')

    # Start supervisor before docker if heartbeat monitoring enabled
    if heartbeat_timeout_seconds is not None and heartbeat_timeout_seconds > 0:
        parts.append("start_supervisor")

    # Define log file paths
    parts.append(f'STDOUT_LOG="{stdout_log}"')
    parts.append(f'STDERR_LOG="{stderr_log}"')
    parts.append(f'EXIT_CODE_FILE="{bucket_mount}/{bucket_path}/logs/exit_code.txt"')

    parts.append('log_stage "docker_run_begin"')
    # Capture stdout and stderr separately for better error visibility
    # Run Docker in background to avoid waiting for watchdog
    parts.append('"${DOCKER_CMD[@]}" > >(tee -a "$STDOUT_LOG") 2> >(tee -a "$STDERR_LOG") & DOCKER_PID=$!')

    # Start log syncer after Docker begins (needs DOCKER_PID)
    if use_log_syncer:
        parts.append("start_log_syncer")

    # Wait for Docker and capture its exit code (no || true - we need real exit code)
    parts.append("wait $DOCKER_PID")
    parts.append("EXIT_CODE=$?")
    # Give tee processes a moment to flush (they close when Docker exits)
    parts.append("sleep 1")
    parts.append('log_stage "docker_run_end"')

    # Post-run commands
    for post_cmd in post_run_cmds:
        parts.append(post_cmd)

    # Write exit code to file
    parts.append('echo "$EXIT_CODE" > "$EXIT_CODE_FILE"')

    # Set exit code in instance metadata as FALLBACK channel for daemon
    # This is fast (local metadata API) and doesn't depend on GCS being available.
    # The daemon can read this if GCS upload fails but instance is still visible.
    parts.append("""
# Set exit code in instance metadata (fallback channel for daemon)
# This is fast and reliable - daemon can read it if GCS upload fails
if [[ -n "$INSTANCE_NAME" && -n "$INSTANCE_ZONE" && -n "$PROJECT_ID" ]]; then
    gcloud compute instances add-metadata "$INSTANCE_NAME" \\
        --zone="$INSTANCE_ZONE" \\
        --project="$PROJECT_ID" \\
        --metadata "goldfish_exit_code=$EXIT_CODE" \\
        --quiet 2>/dev/null || echo "WARNING: Failed to set exit code metadata"
fi
""")

    # Upload logs with retry and verification (BLOCKING before exit)
    parts.append('echo "Uploading logs to GCS with verification..."')
    parts.append(
        f'upload_logs_with_retry "$STDOUT_LOG" gs://{bucket}/{bucket_path}/logs/stdout.log || echo "WARNING: stdout upload failed"'
    )
    parts.append(
        f'upload_logs_with_retry "$STDERR_LOG" gs://{bucket}/{bucket_path}/logs/stderr.log || echo "WARNING: stderr upload failed"'
    )
    # CRITICAL: exit_code.txt upload uses dedicated function with extended retries
    # and NO FALLBACK - this is mandatory for correct state machine operation.
    # If this fails after all retries, script continues (watchdog will eventually kill)
    # but at least we tried much harder than for regular logs.
    parts.append(f'upload_exit_code "$EXIT_CODE_FILE" gs://{bucket}/{bucket_path}/logs/exit_code.txt')
    parts.append('echo "Log upload attempts completed (instance will delete regardless for cost protection)"')

    # Exit with docker exit code - the EXIT trap will handle self-deletion
    # Logs are already uploaded and verified at this point
    parts.append('log_stage "cleanup_begin"')
    parts.append("exit $EXIT_CODE")

    return "\n".join(parts) + "\n"
