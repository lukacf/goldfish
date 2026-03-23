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

    # Write exit code to instance metadata (always — needed by daemon)
    if [[ -n "$INSTANCE_NAME" && -n "$INSTANCE_ZONE" && -n "$PROJECT_ID" ]]; then
        gcloud compute instances add-metadata "$INSTANCE_NAME" \
            --zone="$INSTANCE_ZONE" \
            --project="$PROJECT_ID" \
            --metadata "goldfish_exit_code=$trap_exit_code" \
            --quiet 2>/dev/null || true

        # On FAILURE only: upload infra log (apt-get/driver output — no user data).
        if [[ "$trap_exit_code" != "0" ]]; then
            local crash_file="${INFRA_LOG:-/tmp/goldfish_infra.log}"
            if [[ -s "$crash_file" && -n "${GCS_EXIT_CODE_PATH:-}" ]]; then
                local gcs_crash_log="${GCS_EXIT_CODE_PATH%exit_code.txt}startup_crash.log"
                timeout 30 gsutil cp "$crash_file" "$gcs_crash_log" 2>/dev/null || true
            fi
            local crash_context=""
            if [[ -s "$crash_file" ]]; then
                crash_context=$(tail -30 "$crash_file" 2>/dev/null | head -c 3000 || true)
            elif [[ -s "$LOCAL_STAGE_LOG" ]]; then
                crash_context=$(tail -20 "$LOCAL_STAGE_LOG" 2>/dev/null | head -c 2000 || true)
            fi
            if [[ -n "$crash_context" ]]; then
                gcloud compute instances add-metadata "$INSTANCE_NAME" \
                    --zone="$INSTANCE_ZONE" \
                    --project="$PROJECT_ID" \
                    --metadata-from-file "goldfish_crash_log=/dev/stdin" \
                    --quiet 2>/dev/null <<< "$crash_context" || true
            fi
        fi
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
# If the instance rebooted (kernel headers fallback), subtract elapsed time
# so the total instance lifetime stays within the configured budget.
(
    # GOLDFISH_FIRST_BOOT is set at script start (first or second boot).
    # Subtract time already spent to enforce the original budget.
    ELAPSED=$(($(date +%s) - ${{GOLDFISH_FIRST_BOOT:-$(date +%s)}}))
    WATCHDOG_BUDGET=$(({max_runtime_seconds} - ELAPSED))
    if [[ $WATCHDOG_BUDGET -lt 60 ]]; then WATCHDOG_BUDGET=60; fi
    echo "Watchdog: budget=${{WATCHDOG_BUDGET}}s (elapsed=${{ELAPSED}}s, cap={max_runtime_seconds}s)"
    sleep $WATCHDOG_BUDGET
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
    # Install kernel headers — exact match first, fall back to cloud meta-package.
    # GCE images sometimes ship a kernel newer than what's in the Debian repos
    # (e.g., 6.1.0-43-cloud-amd64 when repos only have up to 6.1.0-42).
    HEADERS_INSTALLED=0
    if apt-get install -y linux-headers-$(uname -r) 2>/dev/null; then
      HEADERS_INSTALLED=1
    elif apt-get install -y linux-image-cloud-amd64 linux-headers-cloud-amd64 2>&1 | tee -a ${{INFRA_LOG:-/dev/null}}; then
      HEADERS_INSTALLED=1
      # The cloud meta-packages install a kernel+headers that may differ from the
      # running kernel. DKMS needs headers matching the running kernel, so reboot
      # into the newly installed one. Only reboot once (check metadata flag).
      INSTALLED_KERNEL=$(dpkg -l 'linux-image-*-cloud-*' 2>/dev/null | awk '/^ii/ {{print $2}}' | sed 's/linux-image-//' | sort -V | tail -1)
      ALREADY_REBOOTED=$(curl -sf -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/goldfish_kernel_reboot 2>/dev/null || echo "")
      if [[ -n "$INSTALLED_KERNEL" && "$INSTALLED_KERNEL" != "$(uname -r)" && "$ALREADY_REBOOTED" != "1" ]]; then
        echo "Kernel mismatch: running=$(uname -r), installed=$INSTALLED_KERNEL — rebooting once"
        log_stage "kernel_reboot" || true
        # Mark that we've rebooted (prevents infinite loop).
        # Boot epoch was already set at script start for accurate watchdog budget.
        gcloud compute instances add-metadata "$INSTANCE_NAME" \
          --zone="$INSTANCE_ZONE" --project="$PROJECT_ID" \
          --metadata "goldfish_kernel_reboot=1" \
          --quiet 2>/dev/null || true
        # Disable self-delete trap during intentional reboot
        trap '' EXIT
        reboot
        exit 0
      fi
    else
      echo "WARNING: Could not install kernel headers (DKMS may still work)"
    fi
    apt-get install -y curl gnupg
    DISTRO=$(lsb_release -cs 2>/dev/null || echo "bookworm")
    case "$DISTRO" in
      bookworm) CUDA_REPO="https://developer.download.nvidia.com/compute/cuda/repos/debian12/x86_64/" ;;
      bullseye) CUDA_REPO="https://developer.download.nvidia.com/compute/cuda/repos/debian11/x86_64/" ;;
      noble) CUDA_REPO="https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2404/x86_64/" ;;
      jammy|focal) CUDA_REPO="https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/" ;;
      *) CUDA_REPO="https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/" ;;
    esac
    curl -fsSL "${{CUDA_REPO}}/3bf863cc.pub" | gpg --dearmor -o /usr/share/keyrings/cuda-archive-keyring.gpg
    echo "deb [signed-by=/usr/share/keyrings/cuda-archive-keyring.gpg] ${{CUDA_REPO}} /" > /etc/apt/sources.list.d/cuda.list
    apt-get update -y 2>&1 | tee -a ${{INFRA_LOG:-/dev/null}}
    apt-get install -y cuda-drivers nvidia-container-toolkit 2>&1 | tee -a ${{INFRA_LOG:-/dev/null}}
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
    gpu_count: int = 0,
) -> str:
    """Generate Docker run command with GPU support.

    Args:
        image: Docker image to run
        env_keys: Environment variable names to pass through
        mounts: List of (host_path, container_path) tuples
        entrypoint: Container entrypoint command
        cmd: Command/script to pass to entrypoint (e.g., "/entrypoint.sh")
        shm_size: Shared memory size (default from DEFAULT_SHM_SIZE)
        gpu_count: Number of GPUs requested by the profile (0 = CPU-only)

    Returns:
        Shell script fragment defining DOCKER_CMD array
    """
    env_flags = " ".join(f"-e {key}" for key in env_keys)
    mount_flags = " ".join(f"-v {host}:{target}" for host, target in mounts)
    cmd_part = f" {cmd}" if cmd else ""

    # Use profile-based GPU flag, not runtime detection.
    # NVIDIA drivers load asynchronously on GCE (~210s on a3-highgpu-8g).
    # Runtime `nvidia-smi` checks race with driver loading and fail silently.
    gpu_args = "--gpus all" if gpu_count > 0 else ""

    return f"""
DOCKER_GPU_ARGS="{gpu_args}"

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
GCS_STAGE_LOG_PATH="{gcs_uri}"
: > "$LOCAL_STAGE_LOG"
log_stage() {{
  echo "$(date --iso-8601=seconds) :: $1" | tee -a "$LOCAL_STAGE_LOG"
  gsutil cp "$LOCAL_STAGE_LOG" "$GCS_STAGE_LOG_PATH" >/dev/null 2>&1 || true
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
    # Capture current GCS paths into local vars for the subshell.
    # These may be reset between warm-pool jobs; the subshell needs a snapshot.
    local _GCS_STDOUT="$GCS_STDOUT_PATH"
    local _GCS_STDERR="$GCS_STDERR_PATH"
    local _GCS_METRICS="$GCS_METRICS_PATH"
    local _GCS_SVS_DURING="$GCS_SVS_DURING_PATH"
    (
        # Wait for Docker to start and create initial logs
        sleep 5

        # Sync logs periodically while Docker is running
        # IMPORTANT: Metrics and SVS are synced FIRST for faster dashboard updates
        while kill -0 $DOCKER_PID 2>/dev/null; do
            sleep $LOG_SYNC_INTERVAL

            # Sync metrics.jsonl FIRST if it exists (needed for dashboard)
            if [[ -f "$LOCAL_METRICS" ]]; then
                gcloud storage cp "$LOCAL_METRICS" "$_GCS_METRICS" --quiet 2>/dev/null || true
            fi

            # Sync during-run SVS findings SECOND (needed for dashboard)
            if [[ -f "$LOCAL_SVS_DURING" ]]; then
                gcloud storage cp "$LOCAL_SVS_DURING" "$_GCS_SVS_DURING" --quiet 2>/dev/null || true
            fi

            # Sync stdout/stderr LAST (can be large, less time-critical)
            gcloud storage cp "$LOCAL_STDOUT" "$_GCS_STDOUT" --quiet 2>/dev/null || true
            gcloud storage cp "$LOCAL_STDERR" "$_GCS_STDERR" --quiet 2>/dev/null || true
        done

        # Final sync after Docker exits to capture last logs
        sleep 2
        # Final sync of metrics and SVS findings FIRST
        if [[ -f "$LOCAL_METRICS" ]]; then
            gcloud storage cp "$LOCAL_METRICS" "$_GCS_METRICS" --quiet 2>/dev/null || true
        fi
        if [[ -f "$LOCAL_SVS_DURING" ]]; then
            gcloud storage cp "$LOCAL_SVS_DURING" "$_GCS_SVS_DURING" --quiet 2>/dev/null || true
        fi
        # Then logs
        gcloud storage cp "$LOCAL_STDOUT" "$_GCS_STDOUT" --quiet 2>/dev/null || true
        gcloud storage cp "$LOCAL_STDERR" "$_GCS_STDERR" --quiet 2>/dev/null || true
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


def idle_loop_section(
    idle_timeout_seconds: int,
    preserve_paths: list[str] | None = None,
) -> str:
    """Generate shell script fragment for the warm pool idle loop.

    After the first Docker container exits, instead of self-deleting, the VM
    enters an idle loop that polls for new job commands via instance metadata.

    Args:
        idle_timeout_seconds: Seconds to wait idle before self-deleting.
        preserve_paths: Glob patterns for paths to preserve between jobs
            (e.g., ["/tmp/triton*", "/mnt/cache/model*"]).

    Returns:
        Shell script fragment defining warm_pool_idle_loop() function.
    """
    # Build cleanup logic based on preserve_paths
    if preserve_paths:
        # Use find-based cleanup that excludes preserve_paths
        exclude_args = ""
        for path in preserve_paths:
            exclude_args += f' ! -path "{path}"'
        cleanup_block = f"""\
                # Clean ALL previous stage outputs to prevent leaking into next run.
                # gsutil rsync uploads everything in /mnt/outputs/, so stale signals
                # from the previous run would be attributed to the new run.
                find /mnt/outputs -mindepth 1 -maxdepth 1{exclude_args} -exec rm -rf {{}} \\; 2>/dev/null || true
                rm -rf /tmp/goldfish_* 2>/dev/null || true
                find /tmp -maxdepth 1 \\( -name "*.log" -o -name "*.json" \\) ! -name "job_spec.json"{exclude_args} 2>/dev/null | xargs rm -f 2>/dev/null || true"""
    else:
        cleanup_block = """\
                # Clean ALL previous stage outputs to prevent leaking into next run.
                # gsutil rsync uploads everything in /mnt/outputs/, so stale signals
                # from the previous run would be attributed to the new run.
                rm -rf /mnt/outputs/* /tmp/goldfish_* 2>/dev/null || true
                find /tmp -maxdepth 1 \\( -name "*.log" -o -name "*.json" \\) ! -name "job_spec.json" 2>/dev/null | xargs rm -f 2>/dev/null || true"""

    return f"""
# === WARM POOL IDLE LOOP ===
warm_pool_idle_loop() {{
    local IDLE_TIMEOUT={idle_timeout_seconds}
    local IDLE_START=$(date +%s)

    # Suspend self-delete EXIT trap during idle
    trap '' EXIT

    # SIGTERM handler for spot preemption during idle
    trap 'echo "PREEMPTED during idle"; exit 143' SIGTERM

    echo "=== ENTERING WARM POOL IDLE LOOP (timeout=${{IDLE_TIMEOUT}}s) ==="

    while true; do
        # Check idle timeout
        local ELAPSED=$(($(date +%s) - IDLE_START))
        if [[ $ELAPSED -gt $IDLE_TIMEOUT ]]; then
            echo "=== IDLE TIMEOUT REACHED (${{ELAPSED}}s > ${{IDLE_TIMEOUT}}s) - SELF-DELETING ==="
            # Re-enable self-delete trap
            trap 'GOLDFISH_TRAP_EXIT_CODE=$?; self_delete' EXIT
            exit 0
        fi

        # Poll metadata for new_job command (reuse Overdrive pattern)
        local SIG_JSON=$(curl -sf -H "Metadata-Flavor: Google" \\
            "http://metadata.google.internal/computeMetadata/v1/instance/attributes/goldfish" 2>/dev/null || true)

        if [[ -n "$SIG_JSON" ]]; then
            local CMD=$(echo "$SIG_JSON" | grep -o '"command"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"command"[[:space:]]*:[[:space:]]*"\\([^"]*\\)".*/\\1/' | tr -d '[:space:]')
            local REQ_ID=$(echo "$SIG_JSON" | grep -o '"request_id"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"request_id"[[:space:]]*:[[:space:]]*"\\([^"]*\\)".*/\\1/' | tr -d '[:space:]')
            local SPEC_PATH=$(echo "$SIG_JSON" | grep -o '"spec_gcs_path"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"spec_gcs_path"[[:space:]]*:[[:space:]]*"\\([^"]*\\)".*/\\1/' | tr -d '[:space:]')

            if [[ "$CMD" == "new_job" && -n "$REQ_ID" && "$REQ_ID" != "$LAST_JOB_REQ_ID" ]]; then
                echo "=== NEW JOB RECEIVED: $REQ_ID ==="
                LAST_JOB_REQ_ID="$REQ_ID"

                # Kill stale background processes with PID guards.
                # KEEP watchdog alive — it tracks total instance lifetime from boot
                # and should NOT be restarted per job. Kill only per-job processes.
                for pid_var in SUPERVISOR_PID LOG_SYNCER_PID METADATA_SYNCER_PID; do
                    local PID_VAL="${{!pid_var:-}}"
                    if [[ -n "$PID_VAL" && "$PID_VAL" != "0" ]]; then
                        kill "$PID_VAL" 2>/dev/null || true
                    fi
                done

                # Download job spec from GCS BEFORE ACKing.
                # ACK signals "ready to run" — must not fire until the spec is validated.
                if [[ -n "$SPEC_PATH" ]]; then
                    gsutil cp "$SPEC_PATH" /tmp/job_spec.json 2>/dev/null || {{
                        echo "ERROR: Failed to download job spec from $SPEC_PATH"
                        IDLE_START=$(date +%s)
                        continue
                    }}
                else
                    echo "ERROR: No spec_gcs_path in signal"
                    IDLE_START=$(date +%s)
                    continue
                fi

                # Parse job spec (uses python for JSON parsing — already installed)
                local NEW_IMAGE=$(python3 -c "import json; print(json.load(open('/tmp/job_spec.json'))['image'])" 2>/dev/null)
                local NEW_RUN_PATH=$(python3 -c "import json; print(json.load(open('/tmp/job_spec.json'))['run_path'])" 2>/dev/null)
                local NEW_ENTRYPOINT=$(python3 -c "import json; print(json.load(open('/tmp/job_spec.json')).get('docker_entrypoint', '/bin/bash'))" 2>/dev/null)
                local NEW_CMD=$(python3 -c "import json; print(json.load(open('/tmp/job_spec.json')).get('docker_cmd', '/entrypoint.sh'))" 2>/dev/null)
                local NEW_SHM_SIZE=$(python3 -c "import json; print(json.load(open('/tmp/job_spec.json')).get('shm_size', '16g'))" 2>/dev/null)
                local NEW_GPU_COUNT=$(python3 -c "import json; print(json.load(open('/tmp/job_spec.json')).get('gpu_count', 0))" 2>/dev/null)

                # Validate required fields before ACKing
                if [[ -z "$NEW_IMAGE" || -z "$NEW_RUN_PATH" ]]; then
                    echo "ERROR: Job spec missing required fields (image or run_path)"
                    IDLE_START=$(date +%s)
                    continue
                fi

                # ACK AFTER spec download + parse validation.
                # This ensures the claim only succeeds when the VM has a valid job to run.
                if command -v gcloud >/dev/null 2>&1; then
                    gcloud compute instances add-metadata "$INSTANCE_NAME" \\
                        --zone="$INSTANCE_ZONE" --project="$PROJECT_ID" \\
                        --metadata "goldfish_ack=$REQ_ID" --quiet 2>/dev/null || true
                fi

                # CRITICAL: Reset ALL GCS paths for new run BEFORE input staging.
                # Early failures (input staging, image pull) need correct paths to
                # write exit codes and logs for the current run, not the previous one.
                # Also reset log_stage and log_syncer destinations so they don't
                # upload to the first-boot run's paths.
                GCS_STDOUT_PATH="gs://$GCS_BUCKET/$NEW_RUN_PATH/logs/stdout.log"
                GCS_STDERR_PATH="gs://$GCS_BUCKET/$NEW_RUN_PATH/logs/stderr.log"
                GCS_EXIT_CODE_PATH="gs://$GCS_BUCKET/$NEW_RUN_PATH/logs/exit_code.txt"
                GCS_METRICS_PATH="gs://$GCS_BUCKET/$NEW_RUN_PATH/logs/metrics.jsonl"
                GCS_SVS_DURING_PATH="gs://$GCS_BUCKET/$NEW_RUN_PATH/outputs/.goldfish/svs_findings_during.json"
                GCS_TERMINATION_CAUSE_PATH="gs://$GCS_BUCKET/$NEW_RUN_PATH/logs/termination_cause.txt"
                GCS_STAGE_LOG_PATH="gs://$GCS_BUCKET/$NEW_RUN_PATH/logs/stage_times.log"
                GCS_LOG_PATH="gs://$GCS_BUCKET/$NEW_RUN_PATH/logs"
                EXIT_CODE_FILE="/mnt/gcs/$NEW_RUN_PATH/logs/exit_code.txt"
                LOCAL_STDOUT=/tmp/stdout.log
                LOCAL_STDERR=/tmp/stderr.log
                LOCAL_METRICS=/mnt/outputs/.goldfish/metrics.jsonl
                LOCAL_SVS_DURING=/mnt/outputs/.goldfish/svs_findings_during.json
                : > "$LOCAL_STDOUT"
                : > "$LOCAL_STDERR"

                # Write entrypoint script (same as first-boot: gce_launcher writes to /mnt/entrypoint.sh)
                python3 -c "
import json
spec = json.load(open('/tmp/job_spec.json'))
script = spec.get('entrypoint_script', 'echo No entrypoint')
with open('/mnt/entrypoint.sh', 'w') as f:
    f.write(script)
" 2>/dev/null
                chmod +x /mnt/entrypoint.sh

                # Stage inputs from GCS to /mnt/inputs/ (replicates gce_launcher input staging)
                rm -rf /mnt/inputs/* 2>/dev/null || true
                mkdir -p /mnt/inputs /mnt/outputs
                python3 -c "
import json, subprocess, os
spec = json.load(open('/tmp/job_spec.json'))
bucket = os.environ.get('GCS_BUCKET', '')
for name, uri in spec.get('inputs', {{}}).items():
    if not uri.startswith('gs://'):
        continue
    uri_parts = uri.replace('gs://', '').split('/', 1)
    if len(uri_parts) == 2:
        input_bucket, input_path = uri_parts
        gcsfuse_path = f'/mnt/gcs/{{input_path.rstrip(\"/\")}}'
        target = f'/mnt/inputs/{{name}}'
        if input_bucket == bucket and os.path.exists(gcsfuse_path):
            os.symlink(gcsfuse_path, target)
            print(f'Symlinked {{name}} -> {{gcsfuse_path}}')
        else:
            subprocess.run(['gsutil', '-m', 'cp', '-r', uri.rstrip('/'), target], check=True)
            print(f'Copied {{name}} from {{uri}}')
" 2>&1 || {{
                    echo "ERROR: Input staging failed — aborting job"
                    echo "1" | gsutil cp - "$GCS_EXIT_CODE_PATH" 2>/dev/null || true
                    upload_logs_with_retry "$LOCAL_STDOUT" "$GCS_STDOUT_PATH" || true
                    upload_logs_with_retry "$LOCAL_STDERR" "$GCS_STDERR_PATH" || true
                    IDLE_START=$(date +%s)
                    continue
                }}
                chown 1000:100 /mnt/inputs /mnt/outputs 2>/dev/null || true
                chown -h 1000:100 /mnt/inputs/* 2>/dev/null || true

{cleanup_block}

                # Pull image if different (Docker layer cache makes this fast for shared base)
                if [[ "$NEW_IMAGE" != "$CURRENT_IMAGE" ]]; then
                    echo "Pulling new image: $NEW_IMAGE (was: $CURRENT_IMAGE)"
                    docker pull "$NEW_IMAGE" || {{
                        echo "ERROR: Failed to pull $NEW_IMAGE"
                        echo "1" | gsutil cp - "$GCS_EXIT_CODE_PATH" 2>/dev/null || true
                        upload_logs_with_retry "$LOCAL_STDOUT" "$GCS_STDOUT_PATH" || true
                        upload_logs_with_retry "$LOCAL_STDERR" "$GCS_STDERR_PATH" || true
                        IDLE_START=$(date +%s)
                        continue
                    }}
                    CURRENT_IMAGE="$NEW_IMAGE"
                fi

                # Restore self-delete EXIT trap during job execution
                trap 'GOLDFISH_TRAP_EXIT_CODE=$?; echo "EXIT TRAP TRIGGERED (exit code: $GOLDFISH_TRAP_EXIT_CODE)"; self_delete' EXIT

                # Restart per-job background monitors.
                # Watchdog is NOT restarted — it stays alive across jobs, tracking total
                # VM lifetime from GOLDFISH_FIRST_BOOT (hard cap on instance existence).
                METADATA_SYNCER_STARTED=0
                start_metadata_syncer
                # Restart supervisor if heartbeat monitoring was enabled
                if type start_supervisor &>/dev/null 2>&1; then
                    start_supervisor
                fi

                # Build and run Docker command
                local DOCKER_GPU_ARGS=""
                if [[ "$NEW_GPU_COUNT" -gt 0 ]]; then
                    DOCKER_GPU_ARGS="--gpus all"
                fi

                # Build mounts from job spec
                local MOUNT_FLAGS=$(python3 -c "
import json
spec = json.load(open('/tmp/job_spec.json'))
for host, container in spec.get('mounts', []):
    print(f'-v {{host}}:{{container}}')
" 2>/dev/null | tr '\\n' ' ')

                log_stage "docker_run_begin"

                # Build the full docker run command via Python to preserve quoting.
                # NEW_CMD may contain embedded quotes (e.g., GPU CUDA wrapper:
                # -c 'mkdir -p /tmp/cuda-symlinks && ... && exec /entrypoint.sh')
                # that would be broken by shell word splitting if used unquoted.
                python3 -c "
import json, shlex
spec = json.load(open('/tmp/job_spec.json'))
parts = ['docker', 'run', '--rm']
gpu = spec.get('gpu_count', 0)
if int(gpu) > 0:
    parts.extend(['--gpus', 'all'])
parts.extend(['--ipc=host', '--ulimit', 'memlock=-1', '--ulimit', 'stack=67108864'])
parts.extend(['--shm-size=' + spec.get('shm_size', '16g')])
for k, v in spec.get('env', {{}}).items():
    parts.extend(['-e', f'{{k}}={{v}}'])
for host, container in spec.get('mounts', []):
    parts.extend(['-v', f'{{host}}:{{container}}'])
parts.extend(['--entrypoint', spec.get('docker_entrypoint', '/bin/bash')])
parts.append(spec['image'])
cmd = spec.get('docker_cmd', '')
if cmd:
    # docker_cmd is already shell-formatted (e.g., \"/entrypoint.sh\" or
    # \"-c 'mkdir ... && exec /entrypoint.sh'\"). Write as-is for eval.
    pass
# Write command for eval (preserves quoting in docker_cmd)
with open('/tmp/docker_cmd.sh', 'w') as f:
    f.write(' '.join(shlex.quote(p) for p in parts))
    if cmd:
        f.write(' ' + cmd)  # Append unquoted — already shell-formatted
    f.write(' > >(tee -a \"$LOCAL_STDOUT\") 2> >(tee -a \"$LOCAL_STDERR\") &')
" 2>/dev/null
                eval "$(cat /tmp/docker_cmd.sh)"
                DOCKER_PID=$!

                # Per-job timeout enforcement (separate from instance-level watchdog).
                # The watchdog guards total VM lifetime; this guards individual job runtime.
                local JOB_TIMEOUT=$(python3 -c "import json; print(json.load(open('/tmp/job_spec.json')).get('max_runtime_seconds') or 0)" 2>/dev/null)
                local JOB_TIMER_PID=""
                if [[ "$JOB_TIMEOUT" -gt 0 ]]; then
                    (
                        sleep "$JOB_TIMEOUT"
                        echo "=== PER-JOB TIMEOUT (${{JOB_TIMEOUT}}s) — KILLING DOCKER ==="
                        docker kill $(docker ps -q) 2>/dev/null || true
                    ) &
                    JOB_TIMER_PID=$!
                fi

                # Start log syncer (needs DOCKER_PID)
                start_log_syncer

                # Wait for Docker. Capture exit code without triggering set -e.
                # Plain `wait $PID` under set -e aborts on non-zero, skipping the
                # post-run upload path and triggering the self-delete trap.
                EXIT_CODE=0
                wait $DOCKER_PID || EXIT_CODE=$?

                # Clean up job timer if it's still running
                if [[ -n "$JOB_TIMER_PID" ]]; then
                    kill "$JOB_TIMER_PID" 2>/dev/null || true
                fi
                sleep 1
                log_stage "docker_run_end"

                # Upload stage outputs to GCS (same rsync pattern as first-boot post_run_cmds)
                # This is CRITICAL — without it, downstream stages would see missing outputs.
                local OUTPUTS_GCS_PATH="gs://$GCS_BUCKET/$NEW_RUN_PATH/outputs"
                echo "Uploading outputs to GCS..."
                for i in {{1..3}}; do
                    if timeout 600 gsutil -m rsync -r /mnt/outputs/ "$OUTPUTS_GCS_PATH/"; then
                        echo "Outputs uploaded successfully"
                        break
                    else
                        echo "Output upload attempt $i failed"
                        [[ $i -lt 3 ]] && sleep 5
                    fi
                done

                # Upload logs FIRST (before exit code).
                # The daemon treats exit_code as the completion signal, so it must be
                # written LAST — after outputs, logs, and trap suspension. Otherwise
                # finalization can release the VM to idle before it enters the poll loop,
                # causing the next claim's ACK to time out.
                upload_logs_with_retry "$LOCAL_STDOUT" "$GCS_STDOUT_PATH" || true
                upload_logs_with_retry "$LOCAL_STDERR" "$GCS_STDERR_PATH" || true

                # Suspend EXIT trap BEFORE writing exit code — the VM is about to go
                # idle, not self-delete. If we wrote exit code with trap active, a
                # transient failure could trigger self-delete after a successful job.
                trap '' EXIT

                # Reset idle timer BEFORE writing exit code — the VM is ready to accept
                # new jobs as soon as the exit code is visible to the daemon.
                IDLE_START=$(date +%s)

                # NOW write exit code — this is the completion signal for the daemon.
                # Everything above (outputs, logs, trap suspension) must happen first.
                echo "$EXIT_CODE" > "$EXIT_CODE_FILE" 2>/dev/null || true
                echo "$EXIT_CODE" | timeout 30 gsutil cp - "$GCS_EXIT_CODE_PATH" 2>/dev/null || true
                gcloud compute instances add-metadata "$INSTANCE_NAME" \\
                    --zone="$INSTANCE_ZONE" --project="$PROJECT_ID" \\
                    --metadata "goldfish_exit_code=$EXIT_CODE" --quiet 2>/dev/null || true
                upload_exit_code "$EXIT_CODE_FILE" "$GCS_EXIT_CODE_PATH" || true

                # Signal the daemon that this instance is ready for reuse.
                # The daemon's poll_warm_instances checks this metadata on draining
                # instances to trigger DRAIN_COMPLETE → idle_ready transition.
                # Without this, instances get stuck in draining forever.
                gcloud compute instances add-metadata "$INSTANCE_NAME" \\
                    --zone="$INSTANCE_ZONE" --project="$PROJECT_ID" \\
                    --metadata "goldfish_instance_state=idle_ready" --quiet 2>/dev/null || true

                echo "=== JOB $REQ_ID COMPLETE (exit=$EXIT_CODE) - RETURNING TO IDLE ==="
            fi
        fi

        sleep 1
    done
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
    gpu_count: int = 0,
    warm_pool_idle_timeout_seconds: int | None = None,
    warm_pool_preserve_paths: list[str] | None = None,
    warm_pool_first_job_timeout: int | None = None,
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
        gpu_count: Number of GPUs requested by the profile (0 = CPU-only)
        warm_pool_idle_timeout_seconds: If set, VM enters idle loop after first
            job instead of self-deleting. Value is idle timeout in seconds.
        warm_pool_preserve_paths: Glob patterns for paths to preserve between
            warm pool jobs (e.g., ["/tmp/triton*"]).

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
        # Record first-boot epoch for watchdog budget calculation across reboots.
        # Only set on first boot — second boot reads the existing value.
        'GOLDFISH_FIRST_BOOT=$(curl -sf -H "Metadata-Flavor: Google" '
        "http://metadata.google.internal/computeMetadata/v1/instance/attributes/goldfish_boot_epoch "
        '2>/dev/null || echo "")',
        'if [[ -z "$GOLDFISH_FIRST_BOOT" ]]; then ' "GOLDFISH_FIRST_BOOT=$(date +%s); fi",
        stage_log_section(stage_uri),
        # Capture infrastructure command output (pre-Docker) for crash diagnosis.
        # This file is safe to upload — it only contains apt-get, driver install, etc.
        # Docker container output goes to STDOUT_LOG/STDERR_LOG instead.
        "INFRA_LOG=/tmp/goldfish_infra.log",
        ": > $INFRA_LOG",
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
            "apt-get update -y 2>&1 | tee -a $INFRA_LOG",
            "apt-get install -y ca-certificates gnupg curl docker.io lsb-release 2>&1 | tee -a $INFRA_LOG",
            "systemctl enable --now docker || true",
            'log_stage "docker_ready"',
            "{ set +x; } 2>/dev/null  # Suppress xtrace for env exports (may contain secrets)",
            env_exports_block,
            "set -x",
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

    # Warm pool variables — must be set after Docker pull
    warm_pool_enabled = warm_pool_idle_timeout_seconds is not None and warm_pool_idle_timeout_seconds > 0
    if warm_pool_enabled:
        parts.append(f'export GCS_BUCKET="{bucket}"')
        parts.append(f'CURRENT_IMAGE="{image}"')
        parts.append('LAST_JOB_REQ_ID=""')

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
            gpu_count=gpu_count,
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

    # For warm pool VMs: per-stage timeout kills Docker (not the VM).
    # The instance-level watchdog uses watchdog_seconds; this timer enforces the
    # individual job's max_runtime so the first job doesn't run until the watchdog.
    if warm_pool_enabled and warm_pool_first_job_timeout and warm_pool_first_job_timeout > 0:
        parts.append(f"""
# Per-stage timeout for first warm pool job (kills Docker, not VM)
FIRST_JOB_TIMEOUT={warm_pool_first_job_timeout}
(
    sleep $FIRST_JOB_TIMEOUT
    echo "=== FIRST JOB TIMEOUT (${{FIRST_JOB_TIMEOUT}}s) — KILLING DOCKER ==="
    docker kill $(docker ps -q) 2>/dev/null || true
) &
FIRST_JOB_TIMER_PID=$!
""")

    # Start log syncer after Docker begins (needs DOCKER_PID)
    if use_log_syncer:
        parts.append("start_log_syncer")

    # Wait for Docker. Capture exit code without triggering set -e.
    # Plain `wait $PID` under set -e aborts on non-zero, skipping post-run uploads.
    parts.append("EXIT_CODE=0")
    parts.append("wait $DOCKER_PID || EXIT_CODE=$?")

    # Clean up first-job timer if warm pool
    if warm_pool_enabled and warm_pool_first_job_timeout and warm_pool_first_job_timeout > 0:
        parts.append('if [[ -n "${FIRST_JOB_TIMER_PID:-}" ]]; then kill "$FIRST_JOB_TIMER_PID" 2>/dev/null || true; fi')
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

    if warm_pool_enabled:
        # Include the idle loop function definition and call it instead of exiting
        parts.append(
            idle_loop_section(
                idle_timeout_seconds=warm_pool_idle_timeout_seconds,  # type: ignore[arg-type]
                preserve_paths=warm_pool_preserve_paths,
            )
        )
        # Check metadata flag: if registration failed (pool full race), skip idle mode
        parts.append("""
# Check if warm pool was disabled post-launch (registration failed due to race)
WARM_DISABLED=$(curl -sf -H "Metadata-Flavor: Google" \
    http://metadata.google.internal/computeMetadata/v1/instance/attributes/goldfish_warm_pool_disabled 2>/dev/null || echo "")
if [[ "$WARM_DISABLED" == "true" ]]; then
    echo "Warm pool disabled by metadata — self-deleting normally"
    exit $EXIT_CODE
fi
""")
        # Signal idle_ready BEFORE entering the loop. The daemon's poll_warm_instances
        # checks this metadata on draining instances to trigger DRAIN_COMPLETE → idle_ready.
        # Without this on first boot, the instance gets stuck in draining forever.
        parts.append("""
# Signal the daemon that first-boot job is done and VM is entering idle loop
gcloud compute instances add-metadata "$INSTANCE_NAME" \
    --zone="$INSTANCE_ZONE" --project="$PROJECT_ID" \
    --metadata "goldfish_instance_state=idle_ready" --quiet 2>/dev/null || true
""")
        parts.append("warm_pool_idle_loop")
    else:
        parts.append("exit $EXIT_CODE")

    return "\n".join(parts) + "\n"
