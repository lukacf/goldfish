"""Composable startup-script builder for CE launches.

Functions return shell-script fragments (strings) so they can be unit-tested
without side effects. The assembled script handles docker setup, optional GPU
drivers, logging, and clean shutdown.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Sequence, Tuple, Mapping
import shlex


def gpu_driver_section() -> str:
    """Install NVIDIA drivers when a GPU is present (safe on CPU nodes)."""

    return """
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
    curl -fsSL "${CUDA_REPO}/3bf863cc.pub" | gpg --dearmor -o /usr/share/keyrings/cuda-archive-keyring.gpg
    echo "deb [signed-by=/usr/share/keyrings/cuda-archive-keyring.gpg] ${CUDA_REPO} /" > /etc/apt/sources.list.d/cuda.list
    apt-get update -y
    apt-get install -y cuda-drivers nvidia-container-toolkit
    nvidia-ctk runtime configure --runtime=docker --set-as-default || true
    systemctl restart docker || true
    log_stage "driver_wait"
    DRIVER_READY=0
    for attempt in $(seq 1 160); do
      if command -v nvidia-smi >/dev/null 2>&1; then
        if nvidia-smi >/tmp/nvidia-smi.log 2>&1; then
          DRIVER_READY=1
          break
        fi
      fi
      echo "nvidia-smi not ready (attempt ${attempt}); sleeping 15s"
      sleep 15
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
    mount_point = str(Path(mount_point))
    return f"""
curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt gcsfuse-$(lsb_release -c -s) main" | tee /etc/apt/sources.list.d/gcsfuse.list
apt-get update -y
timeout 120 apt-get install -y gcsfuse || true
mkdir -p {mount_point}
for attempt in $(seq 1 5); do
  if gcsfuse {bucket} {mount_point}; then
    if mountpoint -q {mount_point}; then
      break
    fi
  fi
  echo "gcsfuse mount retry $attempt failed; sleeping 2s" >&2
  fusermount -u {mount_point} 2>/dev/null || true
  sleep 2
done
if ! mountpoint -q {mount_point}; then
  echo "Failed to mount {bucket} at {mount_point}" >&2
  exit 1
fi
"""


def disk_mount_section(disk_id: str, mount_point: str, mode: str = "ro") -> str:
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
    shm_size: str = "16g",
) -> str:
    env_flags = " ".join(f"-e {key}" for key in env_keys)
    mount_flags = " ".join(f"-v {host}:{target}" for host, target in mounts)
    return f"""
DOCKER_GPU_ARGS=""
if command -v nvidia-smi >/dev/null 2>&1; then
  DOCKER_GPU_ARGS="--gpus all"
fi

DOCKER_CMD=(
  docker run --rm $DOCKER_GPU_ARGS \
  --ipc=host \
  --ulimit memlock=-1 --ulimit stack=67108864 \
  --shm-size={shm_size} \
  {env_flags} \
  {mount_flags} \
  --entrypoint {entrypoint} \
  {image}
)
"""


def stage_log_section(gcs_uri: str) -> str:
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
    mounts: Sequence[Tuple[str, str]] = (),
    bucket_mount: str = "/mnt/gcs",
    gcsfuse: bool = True,
    shm_size: str = "16g",
    disk_mounts: Sequence[Tuple[str, str, str]] = (),
    pre_run_cmds: Sequence[str] = (),
    post_run_cmds: Sequence[str] = (),
) -> str:
    bucket_path = "/".join(part for part in [bucket_prefix.strip("/"), run_path.strip("/")] if part)
    stage_uri = f"gs://{bucket}/{bucket_path}/logs/stage_times.log"
    log_file = f"{bucket_mount}/{bucket_path}/logs/train.log"

    # Export env values so docker sees them
    env_exports = []
    for k, v in env_map.items():
        safe_v = str(v).replace("'", "'\"'\"'")
        env_exports.append(f"export {k}='{safe_v}'")
    env_exports_block = "\n".join(env_exports)
    env_keys = list(env_map.keys())

    parts: List[str] = [
        "#!/bin/bash",
        "set -euxo pipefail",
        "export DEBIAN_FRONTEND=noninteractive",
        stage_log_section(stage_uri),
        "log_stage \"startup_begin\"",
        "apt-get update -y",
        "apt-get install -y ca-certificates gnupg curl docker.io lsb-release",
        "systemctl enable --now docker || true",
        "log_stage \"docker_ready\"",
        env_exports_block,
    ]

    parts.append(gpu_driver_section())

    if gcsfuse:
        parts.append("log_stage \"gcsfuse_begin\"")
        parts.append(gcsfuse_section(bucket, bucket_mount))
        parts.append("log_stage \"gcsfuse_ready\"")

    for disk_id, mount_point, mode in disk_mounts:
        parts.append(disk_mount_section(disk_id, mount_point, mode))
    if disk_mounts:
        parts.append("log_stage \"cache_ready\"")

    parts.append("gcloud auth configure-docker us-docker.pkg.dev --quiet || true")
    parts.append("log_stage \"docker_login\"")
    parts.append(f"docker pull {image} || true")
    parts.append("log_stage \"docker_pull\"")

    for cmd in pre_run_cmds:
        parts.append(cmd)

    parts.append(
        docker_run_section(
            image=image, env_keys=env_keys, mounts=list(mounts), entrypoint=entrypoint, shm_size=shm_size
        )
    )

    parts.append(f"mkdir -p \"$(dirname \"{log_file}\")\"")
    parts.append("log_stage \"docker_run_begin\"")
    parts.append(f'{{ "${{DOCKER_CMD[@]}}" | tee -a "{log_file}"; }}')
    parts.append("EXIT_CODE=${PIPESTATUS[0]}")
    parts.append("log_stage \"docker_run_end\"")

    for cmd in post_run_cmds:
        parts.append(cmd)

    parts.append(f"echo \"$EXIT_CODE\" > {bucket_mount}/{bucket_path}/logs/exit_code.txt || true")
    parts.append(f"gsutil cp \"{log_file}\" gs://{bucket}/{bucket_path}/logs/train.log || true")
    parts.append(f"gsutil cp {bucket_mount}/{bucket_path}/logs/exit_code.txt gs://{bucket}/{bucket_path}/logs/exit_code.txt || true")
    parts.append("shutdown -h now || true")
    parts.append("exit $EXIT_CODE")
    return "\n".join(parts) + "\n"
