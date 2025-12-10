"""Startup script builder for GCE instances.

Ported from legacy infra/startup_builder.py.
Composable functions that build shell script fragments for:
- GPU driver installation
- gcsfuse mounting
- Disk mounting
- Docker execution with proper environment
"""

import shlex
from collections.abc import Mapping, Sequence
from pathlib import Path

# Configuration constants
GPU_DRIVER_MAX_ATTEMPTS = 160  # Maximum attempts to wait for GPU driver
GPU_DRIVER_RETRY_SLEEP_SEC = 15  # Seconds to sleep between GPU driver retries
GCSFUSE_MAX_ATTEMPTS = 5  # Maximum attempts to mount gcsfuse
GCSFUSE_RETRY_SLEEP_SEC = 2  # Seconds to sleep between gcsfuse retries
DEFAULT_SHM_SIZE = "16g"  # Default Docker shared memory size


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
for attempt in $(seq 1 {GCSFUSE_MAX_ATTEMPTS}); do
  if gcsfuse {bucket} {mount_point}; then
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
    shm_size: str = DEFAULT_SHM_SIZE,
) -> str:
    """Generate Docker run command with GPU detection.

    Args:
        image: Docker image to run
        env_keys: Environment variable names to pass through
        mounts: List of (host_path, container_path) tuples
        entrypoint: Container entrypoint command
        shm_size: Shared memory size (default from DEFAULT_SHM_SIZE)

    Returns:
        Shell script fragment defining DOCKER_CMD array
    """
    env_flags = " ".join(f"-e {key}" for key in env_keys)
    mount_flags = " ".join(f"-v {host}:{target}" for host, target in mounts)

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
  {image}
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

    Returns:
        Complete startup script as string
    """
    # Build GCS paths
    bucket_path = "/".join(part for part in [bucket_prefix.strip("/"), run_path.strip("/")] if part)
    stage_uri = f"gs://{bucket}/{bucket_path}/logs/stage_times.log"
    log_file = f"{bucket_mount}/{bucket_path}/logs/train.log"

    # Export environment variables with proper shell escaping
    env_exports = []
    for k, v in env_map.items():
        # Use shlex.quote for proper shell escaping (prevents command injection)
        safe_v = shlex.quote(str(v))
        env_exports.append(f"export {k}={safe_v}")

    env_exports_block = "\n".join(env_exports)
    env_keys = list(env_map.keys())

    # Build script parts
    parts: list[str] = [
        "#!/bin/bash",
        "set -euxo pipefail",
        "export DEBIAN_FRONTEND=noninteractive",
        stage_log_section(stage_uri),
        'log_stage "startup_begin"',
        "apt-get update -y",
        "apt-get install -y ca-certificates gnupg curl docker.io lsb-release",
        "systemctl enable --now docker || true",
        'log_stage "docker_ready"',
        env_exports_block,
    ]

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

    # Docker login and image pull
    parts.append("gcloud auth configure-docker us-docker.pkg.dev --quiet || true")
    parts.append('log_stage "docker_login"')
    parts.append(f"docker pull {image} || true")
    parts.append('log_stage "docker_pull"')

    # Pre-run commands
    for cmd in pre_run_cmds:
        parts.append(cmd)

    # Docker run command
    parts.append(
        docker_run_section(
            image=image,
            env_keys=env_keys,
            mounts=list(mounts),
            entrypoint=entrypoint,
            shm_size=shm_size,
        )
    )

    # Execute Docker container
    parts.append(f'mkdir -p "$(dirname "{log_file}")"')
    parts.append('log_stage "docker_run_begin"')
    parts.append(f'{{ "${{DOCKER_CMD[@]}}" | tee -a "{log_file}"; }}')
    parts.append("EXIT_CODE=${PIPESTATUS[0]}")
    parts.append('log_stage "docker_run_end"')

    # Post-run commands
    for cmd in post_run_cmds:
        parts.append(cmd)

    # Upload exit code and logs
    parts.append(f'echo "$EXIT_CODE" > {bucket_mount}/{bucket_path}/logs/exit_code.txt || true')
    parts.append(f'gsutil cp "{log_file}" gs://{bucket}/{bucket_path}/logs/train.log || true')
    parts.append(
        f"gsutil cp {bucket_mount}/{bucket_path}/logs/exit_code.txt gs://{bucket}/{bucket_path}/logs/exit_code.txt || true"
    )

    # Shutdown
    parts.append("shutdown -h now || true")
    parts.append("exit $EXIT_CODE")

    return "\n".join(parts) + "\n"
