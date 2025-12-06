#!/usr/bin/env python
"""Declarative job launcher (Phase 3 skeleton: local + dry-run, registry, env wiring)."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import time
import yaml
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from infra import job_loader as jl
from infra import image_rebuild
from infra import run_registry
from infra import storage_bridge
from infra.resource_launcher import ResourceLauncher, format_run_id
from infra.startup_builder import build_startup_script

DEFAULT_REGISTRY = Path("runs/registry.jsonl")
EXPERIMENTS_REGISTRY = Path("experiments/registry.yaml")  # Unified registry for experiments + runs
RUNS_DIR = Path("runs")  # Legacy fallback for non-experiment runs
EXPERIMENTS_DIR = Path("experiments")


def detect_run_context(job_path: Path) -> Dict[str, Any]:
    """Detect if job is being launched from a run directory.

    Returns dict with:
      - is_run: bool - True if in experiments/<exp>/runs/<run>/config.yaml
      - run_id: str | None - The run directory name
      - experiment_id: str | None - The experiment directory name
      - code_path: Path | None - Path to experiment code
      - run_dir: Path | None - The run directory
      - experiment_dir: Path | None - The experiment directory
    """
    resolved = job_path.resolve()
    parts = resolved.parts

    # Check if this is experiments/<exp>/runs/<run>/config.yaml
    if "experiments" in parts and "runs" in parts:
        exp_idx = parts.index("experiments")
        runs_idx = parts.index("runs")

        # Validate structure: experiments/<exp>/runs/<run>/...
        if runs_idx == exp_idx + 2 and runs_idx + 1 < len(parts):
            experiment_id = parts[exp_idx + 1]
            run_id = parts[runs_idx + 1]
            experiment_dir = Path(*parts[:exp_idx + 2])
            run_dir = Path(*parts[:runs_idx + 2])
            code_path = experiment_dir / "code"

            return {
                "is_run": True,
                "run_id": run_id,
                "experiment_id": experiment_id,
                "code_path": code_path if code_path.exists() else None,
                "run_dir": run_dir,
                "experiment_dir": experiment_dir,
            }

    return {
        "is_run": False,
        "run_id": None,
        "experiment_id": None,
        "code_path": None,
        "run_dir": None,
        "experiment_dir": None,
    }


def detect_experiment(job_path: Path, run_context: Dict[str, Any] | None = None) -> str | None:
    """Detect experiment name from job path or run context.

    For run directories, returns the experiment_id from meta.yaml.
    For legacy paths, extracts from project/mlm/experiments/<name>/...
    """
    # If we have run context with experiment, use that
    if run_context and run_context.get("experiment_id"):
        return run_context["experiment_id"]

    # Legacy detection from path
    parts = job_path.parts
    if "experiments" in parts:
        idx = parts.index("experiments")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return None


def update_run_status(run_id: str, status: str, outputs: Dict[str, str] | None = None) -> None:
    """Update run status in experiments/registry.yaml."""
    if not EXPERIMENTS_REGISTRY.exists():
        return

    registry = yaml.safe_load(EXPERIMENTS_REGISTRY.read_text()) or {}
    if run_id not in registry.get("runs", {}):
        return

    registry["runs"][run_id]["status"] = status
    registry["runs"][run_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
    if outputs:
        registry["runs"][run_id]["outputs"] = outputs

    EXPERIMENTS_REGISTRY.write_text(yaml.safe_dump(registry, sort_keys=False, default_flow_style=False))


def detect_stage(job_path: Path) -> str:
    """Get stage name from config filename (e.g., prep_smoke.yaml -> prep_smoke)."""
    return job_path.stem


def write_project_registry(
    run_id: str,
    experiment: str | None,
    stage: str,
    config_path: str,
    description: str,
    backend: str,
    status: str = "pending",
    inputs: Dict[str, str] | None = None,
    outputs: Dict[str, str] | None = None,
    instance: str | None = None,
    zone: str | None = None,
    code_version: Dict[str, str | None] | None = None,
    variant: Dict[str, Any] | None = None,
) -> None:
    """Write run record to project/registry.yaml for lineage tracking.

    code_version should contain:
      - git_sha: current git HEAD at launch
      - image_git_sha: git SHA label embedded in Docker image
      - image_digest: immutable Docker digest (sha256:...)

    variant documents algorithm changes:
      - name: short identifier (e.g., "mi-gating-v2")
      - description: what changed
      - parent: what this was based on
      - hypothesis: expected outcome
    """
    EXPERIMENTS_REGISTRY.parent.mkdir(parents=True, exist_ok=True)

    if EXPERIMENTS_REGISTRY.exists():
        registry = yaml.safe_load(EXPERIMENTS_REGISTRY.read_text()) or {}
    else:
        registry = {"schema_version": 1, "experiments": {}, "runs": {}}

    registry.setdefault("runs", {})
    registry["runs"][run_id] = {
        "experiment": experiment,
        "stage": stage,
        "config": config_path,
        "description": description,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
        "status": status,
        "backend": backend,
        "inputs": inputs or {},
        "outputs": outputs or {},
    }
    if instance:
        registry["runs"][run_id]["instance"] = instance
    if zone:
        registry["runs"][run_id]["zone"] = zone
    if code_version:
        registry["runs"][run_id]["code_version"] = {k: v for k, v in code_version.items() if v}
    if variant:
        registry["runs"][run_id]["variant"] = variant

    EXPERIMENTS_REGISTRY.write_text(yaml.safe_dump(registry, sort_keys=False, default_flow_style=False))


def update_project_registry_status(
    run_id: str,
    status: str,
    outputs: Dict[str, str] | None = None,
) -> None:
    """Update run status and outputs in project/registry.yaml."""
    if not EXPERIMENTS_REGISTRY.exists():
        return

    registry = yaml.safe_load(EXPERIMENTS_REGISTRY.read_text()) or {}
    if run_id not in registry.get("runs", {}):
        return

    registry["runs"][run_id]["status"] = status
    registry["runs"][run_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
    if outputs:
        registry["runs"][run_id]["outputs"] = outputs

    EXPERIMENTS_REGISTRY.write_text(yaml.safe_dump(registry, sort_keys=False, default_flow_style=False))


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def get_effective_image_uri(job_cfg: Dict[str, Any]) -> str:
    """Get the effective Docker image URI, preferring pinned_uri if available.

    Priority: pinned_uri > uri > raw string
    """
    image = job_cfg.get("image")
    if isinstance(image, str):
        return image
    if isinstance(image, dict):
        # Prefer pinned_uri (SHA-tagged) over base uri
        return image.get("pinned_uri") or image.get("uri", "")
    return ""


def snapshot_config(job_path: Path, run_dir: Path) -> None:
    ensure_dir(run_dir)
    dst = run_dir / "config_snapshot.yaml"
    shutil.copy(job_path, dst)


def resolve_inputs(job_cfg: Dict[str, Any], project_root: Path) -> List[Dict[str, Any]]:
    resolved = []
    for spec in job_cfg.get("inputs", {}).values():
        resolved.append(jl.resolve_input(spec, project_root))
    return resolved


def resolve_input_locations(job_cfg: Dict[str, Any], resolved_inputs: List[Dict[str, Any]]) -> Dict[str, str]:
    locations: Dict[str, str] = {}
    for name, spec in job_cfg.get("inputs", {}).items():
        required = spec.get("require", "gcs")
        upstream = resolved_inputs.pop(0)
        tier = "snapshot" if required == "hyperdisk" else "gcs"
        source_loc = storage_bridge.get_location(upstream, tier)
        if source_loc is None:
            raise RuntimeError(f"Missing location for input {name} ({required})")
        locations[name] = source_loc
    return locations


def run_local(job_cfg: Dict[str, Any], env_map: Dict[str, str], run_dir: Path) -> int:
    inputs = job_cfg.get("inputs", {})
    outputs = job_cfg.get("outputs", {})

    # Prepare mount points under run_dir
    input_mounts: List[Tuple[str, str]] = []
    for name in inputs:
        host_path = run_dir / "inputs" / name
        ensure_dir(host_path)
        input_mounts.append((str(host_path), f"/mnt/inputs/{name}"))

    output_mounts: List[Tuple[str, str]] = []
    for name in outputs:
        host_path = run_dir / "outputs" / name
        ensure_dir(host_path)
        output_mounts.append((str(host_path), f"/mnt/outputs/{name}"))

    mounts = input_mounts + output_mounts
    env_flags: List[str] = []
    for k, v in env_map.items():
        env_flags += ["-e", f"{k}={v}"]

    mount_flags: List[str] = []
    for host, container in mounts:
        mount_flags += ["-v", f"{host}:{container}"]

    image = get_effective_image_uri(job_cfg)
    entrypoint = job_cfg.get("entrypoint")

    cmd = [
        "docker",
        "run",
        "--rm",
        *env_flags,
        *mount_flags,
        image,
    ]
    if entrypoint:
        cmd.append(entrypoint)

    stdout_path = run_dir / "stdout.log"
    stderr_path = run_dir / "stderr.log"
    with stdout_path.open("w") as out, stderr_path.open("w") as err:
        result = subprocess.run(cmd, stdout=out, stderr=err)
    return result.returncode


def _poll_instance_terminated(instance: str, zone: str, timeout_sec: int = 900) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        status = (
            subprocess.run(
                ["gcloud", "compute", "instances", "describe", instance, f"--zone={zone}", "--format=value(status)"],
                capture_output=True,
                text=True,
            ).stdout.strip()
        )
        if status == "TERMINATED":
            return
        time.sleep(10)
    # On timeout, fetch serial console for debugging
    try:
        serr = subprocess.run(
            ["gcloud", "compute", "instances", "get-serial-port-output", instance, f"--zone={zone}", "--port=1"],
            capture_output=True,
            text=True,
            check=False,
        )
        serial_out = serr.stdout[-4000:] if serr.stdout else ""
    except Exception:
        serial_out = "<unavailable>"
    raise TimeoutError(
        f"Instance {instance} did not terminate within {timeout_sec}s. Serial log tail:\\n{serial_out}"
    )


def _try_fetch_exit_code(bucket: str, run_path: str) -> int | None:
    try:
        code_out = subprocess.run(
            ["gsutil", "cat", f"gs://{bucket}/{run_path}/logs/exit_code.txt"],
            capture_output=True,
            text=True,
            check=True,
        )
        return int(code_out.stdout.strip() or 0)
    except Exception:
        return None


def _snapshot_output_disks(output_disks: Dict[str, str], zone: str, run_id: str) -> Dict[str, str]:
    snap_map: Dict[str, str] = {}
    for out_name, disk_name in output_disks.items():
        snap_name = f"{out_name}-snap-{run_id}"
        subprocess.run(
            ["gcloud", "compute", "disks", "snapshot", disk_name, f"--snapshot-names={snap_name}", f"--zone={zone}", "--quiet"],
            check=True,
        )
        snap_map[out_name] = snap_name
    return snap_map


def _delete_disks(disks: Dict[str, str], zone: str) -> None:
    for disk in disks.values():
        subprocess.run(["gcloud", "compute", "disks", "delete", disk, f"--zone={zone}", "--quiet"], check=False)


def _delete_instance(instance: str, zone: str) -> None:
    """Delete a GCE instance (also auto-deletes boot disk by default)."""
    subprocess.run(["gcloud", "compute", "instances", "delete", instance, f"--zone={zone}", "--quiet"], check=False)


def _collect_manifests(outputs: Dict[str, Any], run_dir: Path) -> Dict[str, str]:
    envs: Dict[str, str] = {}
    for name in outputs:
        manifest_path = run_dir / "outputs" / name / "manifest.yaml"
        if manifest_path.exists():
            try:
                data = yaml.safe_load(manifest_path.read_text()) or {}
                env_block = data.get("env") or {}
                for k, v in env_block.items():
                    envs[k] = str(v)
            except Exception:
                continue
    return envs


def update_job_state(
    config_path: Path,
    *,
    run_id: str,
    status: str,
    started: datetime,
    completed: datetime,
    gcs_outputs: Dict[str, str],
    snapshot_outputs: Dict[str, str],
    manifest_env: Dict[str, str],
) -> None:
    cfg = yaml.safe_load(config_path.read_text())
    if not isinstance(cfg, dict):
        return
    state = cfg.setdefault("state", {})
    state["run_id"] = run_id
    state["status"] = status
    state["started"] = started.isoformat()
    state["completed"] = completed.isoformat()
    locations = state.setdefault("locations", {})
    for k, v in gcs_outputs.items():
        locations[f"{k}_gcs"] = v
        # Also set canonical 'gcs' key for the first/primary output for downstream resolution
        if "gcs" not in locations:
            locations["gcs"] = v
    for k, v in snapshot_outputs.items():
        locations[f"{k}_snapshot"] = v
        if "snapshot" not in locations:
            locations["snapshot"] = v
    if manifest_env:
        state["manifest"] = {"env": manifest_env}
    config_path.write_text(yaml.safe_dump(cfg, sort_keys=False))


def _ensure_bucket_prefix(path_tpl: str, run_id: str) -> str:
    path = path_tpl.format(name=run_id).strip("/")
    return path


def _strip_gs(uri: str) -> str:
    return uri.replace("gs://", "")


def _load_secrets() -> Dict[str, str]:
    secrets_path = Path("infra") / "secrets.yaml"
    if not secrets_path.exists():
        return {}
    cfg = yaml.safe_load(secrets_path.read_text()) or {}
    resolved: Dict[str, str] = {}
    for var, spec in cfg.items():
        if not isinstance(spec, dict):
            continue
        src = spec.get("source")
        if src != "secret_manager":
            continue
        name = spec.get("name")
        if not name:
            continue
        try:
            cmd = ["gcloud", "secrets", "versions", "access", "latest", f"--secret={name}"]
            project = spec.get("project")
            if project:
                cmd.append(f"--project={project}")
            out = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
            resolved[var] = out.stdout.strip()
        except Exception:
            continue
    return resolved


def run_gce(
    job_cfg: Dict[str, Any],
    env_map: Dict[str, str],
    run_dir: Path,
    run_id: str,
    args: argparse.Namespace,
    job_path: Path,
    registry: run_registry.RunRegistry,
    project_root: Path,
) -> tuple[int | None, str | None, str | None, dict]:
    gcp_cfg = yaml.safe_load((Path("infra") / "gcp.yaml").read_text())
    sinks_cfg = yaml.safe_load((Path("infra") / "sinks.yaml").read_text())

    bucket_uri = sinks_cfg.get("default_bucket")
    bucket = _strip_gs(bucket_uri)
    templates = sinks_cfg.get("path_templates") or {}
    run_path = _ensure_bucket_prefix(templates.get("runs", "runs/{name}/"), run_id)

    # Only use zone override if explicitly passed by user
    # Otherwise, let ResourceLauncher search all zones for the matched GPU resources
    zone = args.zone  # None if not specified

    # Resolve inputs again to get states (project_root passed as parameter)
    resolved_inputs = resolve_inputs(job_cfg, project_root)

    # Prepare disks and mounts
    disk_mounts: List[Tuple[str, str, str]] = []
    mounts: List[Tuple[str, str]] = []
    extra_disks: List[Dict[str, Any]] = []
    input_disks: Dict[str, str] = {}   # Track input disks for cleanup
    output_disks: Dict[str, str] = {}
    gcs_outputs: Dict[str, str] = {}

    pre_run_cmds: List[str] = []
    post_run_cmds: List[str] = []

    inputs_iter = iter(resolved_inputs)
    for name, spec in job_cfg.get("inputs", {}).items():
        cfg = next(inputs_iter)
        required = spec.get("require", "gcs")
        if required == "hyperdisk":
            snap = storage_bridge.get_location(cfg, "snapshot")
            if not snap:
                gcs_loc = storage_bridge.get_location(cfg, "gcs")
                if not gcs_loc:
                    raise RuntimeError(f"Input {name} missing gcs location for bridging")
                # GCE resource names only allow lowercase, numbers, and hyphens
                snap = f"{cfg.get('name','input')}-snap-{run_id[:8]}".replace("_", "-").lower()
                storage_bridge.create_snapshot(gcs_loc, snap, zone)
            # GCE resource names only allow lowercase, numbers, and hyphens
            disk_name = f"{name}-disk-{run_id[:8]}".replace("_", "-").lower()
            try:
                subprocess.run(
                    [
                        "gcloud",
                        "compute",
                        "disks",
                        "create",
                        disk_name,
                        f"--zone={zone}",
                        f"--source-snapshot={snap}",
                        "--type=hyperdisk-balanced",  # Required for c4/a3 machine types
                        "--size=600GB",
                        "--provisioned-iops=80000",
                        "--provisioned-throughput=2400",
                    ],
                    check=True,
                )
                # hyperdisk-balanced cannot be attached read-only, use rw
                extra_disks.append({"name": disk_name, "mode": "rw"})
                input_disks[name] = disk_name  # Track for cleanup
                disk_mounts.append((disk_name, f"/mnt/inputs/{name}", "rw"))
                mounts.append((f"/mnt/inputs/{name}", f"/mnt/inputs/{name}"))
            except Exception:
                _delete_disks({disk_name: disk_name}, zone)
                raise
        else:
            gcs_loc = storage_bridge.get_location(cfg, "gcs")
            if not gcs_loc:
                raise RuntimeError(f"Input {name} missing gcs location")
            local_path = f"/mnt/inputs/{name}"
            pre_run_cmds.append(f"mkdir -p {local_path}")
            pre_run_cmds.append(f"gsutil -m rsync -r {gcs_loc} {local_path}")
            mounts.append((local_path, local_path))

    for name, spec in job_cfg.get("outputs", {}).items():
        sink = spec.get("sink")
        if sink == "hyperdisk":
            disk_name = f"{name}-out-{run_id[:8]}"
            size_gb = spec.get("disk_size_gb", 600)
            try:
                subprocess.run(
                    [
                        "gcloud",
                        "compute",
                        "disks",
                        "create",
                        disk_name,
                        f"--zone={zone}",
                        f"--size={size_gb}GB",
                        "--type=pd-ssd",
                    ],
                    check=True,
                )
                extra_disks.append({"name": disk_name, "mode": "rw"})
                disk_mounts.append((disk_name, f"/mnt/outputs/{name}", "rw"))
                mounts.append((f"/mnt/outputs/{name}", f"/mnt/outputs/{name}"))
                output_disks[name] = disk_name
            except Exception:
                _delete_disks({disk_name: disk_name}, zone)
                raise
        elif sink == "gcs":
            local_path = f"/mnt/outputs/{name}"
            mounts.append((local_path, local_path))
            path_tpl = spec.get("path") or templates.get("runs", "runs/{name}/")
            gcs_out = f"{bucket_uri.rstrip('/')}/{path_tpl.format(name=run_id, run_id=run_id)}"
            post_run_cmds.append(f"gsutil -m rsync -r {local_path} {gcs_out}")
            gcs_outputs[name] = gcs_out
        else:
            raise RuntimeError(f"Unsupported sink {sink} for output {name}")

    env_keys = list(env_map.keys())

    startup_script = build_startup_script(
        bucket=bucket,
        bucket_prefix="",
        run_path=run_path,
        image=get_effective_image_uri(job_cfg),
        entrypoint=job_cfg.get("entrypoint", ""),
        env_map=env_map,
        mounts=mounts,
        disk_mounts=disk_mounts,
        pre_run_cmds=pre_run_cmds,
        post_run_cmds=post_run_cmds,
    )

    # Write env.json
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "env.json").write_text(json.dumps(env_map, indent=2))
    with open(run_dir / "startup.sh", "w") as f:
        f.write(startup_script)

    # Filter resources by required GPU type
    required_gpu = (job_cfg.get("compute", {}).get("gpu") or "none").lower()
    resources = [r for r in gcp_cfg.get("resources", []) if (r.get("gpu", {}).get("type", "none").lower() == required_gpu)]
    # prefer smallest data_disk size
    resources = sorted(
        resources,
        key=lambda r: r.get("data_disk", {}).get("size_gb", 999999),
    )
    if not resources:
        raise SystemExit(f"No resources match compute.gpu={required_gpu} in gcp.yaml")

    launcher_cfg = {
        "bucket": bucket_uri,
        "run": gcp_cfg.get("run", {}),
        "resources": resources,
    }
    # Only override zones if user explicitly passed --zone, otherwise search all zones
    launcher = ResourceLauncher(launcher_cfg, zones_override=[args.zone] if args.zone else None, force_gpu=required_gpu)
    run_path_logs = run_path
    result = None
    try:
        result = launcher.launch(
            run_id=run_id,
            startup_script=startup_script,
            run_path=run_path_logs,
            extra_disks=extra_disks,
        )
    except Exception:
        # Cleanup any disks we created before launch
        _delete_disks({d["name"]: d["name"] for d in extra_disks}, zone)
        raise

    # Collect code version info for GCE runs
    gce_image_uri = get_effective_image_uri(job_cfg)
    gce_code_version = {
        "git_sha": image_rebuild.get_current_sha(),
        "image_git_sha": image_rebuild.get_image_sha(gce_image_uri) if gce_image_uri else None,
        "image_digest": image_rebuild.get_image_digest(gce_image_uri) if gce_image_uri else None,
    }

    registry.log_started(
        run_id,
        str(job_path),
        args.backend,
        zone=result.selection.zone,
        instance_name=result.instance_name,
        machine=result.selection.resource,
        preemptible=result.selection.preemptible,
        inputs=list(job_cfg.get("inputs", {}).keys()),
        outputs=list(job_cfg.get("outputs", {}).keys()),
        image_uri=gce_image_uri,
        **{k: v for k, v in gce_code_version.items() if v},  # Include all code version fields
        log_uri=result.log_uri,
        stage_log_uri=result.stage_log_uri,
        artifact_uri=result.artifact_uri,
    )

    if args.wait is False:
        return None, result.instance_name, result.selection.zone, {
            "log_uri": result.log_uri,
            "stage_log_uri": result.stage_log_uri,
            "artifact_uri": result.artifact_uri,
            "instance_name": result.instance_name,
            "zone": result.selection.zone,
            "machine": result.selection.resource,
            "preemptible": result.selection.preemptible,
            "gcs_outputs": gcs_outputs,
            "snapshot_outputs": {},
        }

    _poll_instance_terminated(result.instance_name, result.selection.zone, timeout_sec=1200)

    # Read exit code if present
    exit_code = _try_fetch_exit_code(bucket, run_path)
    if exit_code is None:
        exit_code = 1

    # Snapshot hyperdisk outputs
    snap_map = {}
    if output_disks:
        snap_map = _snapshot_output_disks(output_disks, zone, run_id)
        _delete_disks(output_disks, zone)
        env_map.update({f"SNAPSHOT_{k.upper()}": v for k, v in snap_map.items()})
        (run_dir / "snapshots.json").write_text(json.dumps(snap_map, indent=2))

    # Cleanup: delete input disks and instance
    if input_disks:
        _delete_disks(input_disks, zone)
    _delete_instance(result.instance_name, result.selection.zone)

    return exit_code, result.instance_name, result.selection.zone, {
        "log_uri": result.log_uri,
        "stage_log_uri": result.stage_log_uri,
        "artifact_uri": result.artifact_uri,
        "instance_name": result.instance_name,
        "zone": result.selection.zone,
        "machine": result.selection.resource,
        "preemptible": result.selection.preemptible,
        "gcs_outputs": gcs_outputs,
        "snapshot_outputs": snap_map,
    }


def perform_rebuild_if_needed(job_cfg: Dict[str, Any], rebuild: bool | None = None) -> Dict[str, Any]:
    """Rebuild Docker image if needed, auto-tagging with git SHA for parallel runs.

    When rebuilt, the image is tagged with :<short_sha> and the job config is
    updated to use that specific tag. This enables running multiple code versions
    in parallel without interference.
    """
    image = job_cfg.get("image")
    variant = job_cfg.get("variant", {})
    variant_name = variant.get("name") if isinstance(variant, dict) else None

    if isinstance(image, dict):
        uri = image.get("uri")
        auto = image.get("auto_rebuild", True) if rebuild is None else rebuild
        if auto and uri:
            if image_rebuild.needs_rebuild(uri):
                new_sha, sha_uri = image_rebuild.rebuild_image(image, variant_name)
                image["pinned_sha"] = new_sha
                image["pinned_uri"] = sha_uri  # Use SHA-tagged image for this run
        if rebuild is False and "pinned_sha" in image:
            image["pin"] = image["pinned_sha"]
    return job_cfg


def _stage_outputs_to_gcs(outputs: Dict[str, Any], run_dir: Path, sinks_cfg: Dict[str, Any], run_id: str) -> Dict[str, str]:
    """Local post-run sync to GCS for gcs sinks. Returns mapping output->gcs_uri."""
    default_bucket = sinks_cfg.get("default_bucket")
    templates = sinks_cfg.get("path_templates") or {}
    gcs_map: Dict[str, str] = {}
    for name, spec in outputs.items():
        sink = spec.get("sink")
        if sink != "gcs":
            continue
        path_tpl = spec.get("path") or templates.get("runs", "runs/{name}/")
        gcs_uri = f"{default_bucket.rstrip('/')}/{path_tpl.format(name=run_id)}"
        local_path = run_dir / "outputs" / name
        local_path.mkdir(parents=True, exist_ok=True)
        subprocess.run(["gsutil", "-m", "rsync", "-r", str(local_path), gcs_uri], check=True)
        gcs_map[name] = gcs_uri
    return gcs_map


def execute_job(job_path: Path, args: argparse.Namespace, registry: run_registry.RunRegistry) -> None:
    job_cfg = jl.load_config(job_path)
    jl.validate_job(job_cfg)

    # Detect if we're launching from a run directory
    run_context = detect_run_context(job_path)

    # For run directories, project_root should be repo root, not run dir
    if run_context["is_run"]:
        project_root = Path.cwd()  # Repo root
    else:
        project_root = job_path.parent.parent  # assumes project/<name>/...
    resolved_inputs = resolve_inputs(job_cfg, project_root)

    # Use run_id from context if available, otherwise generate new one
    if run_context["is_run"]:
        run_id = run_context["run_id"]
        run_dir = run_context["run_dir"]
    else:
        run_id = format_run_id(args.run_id_prefix or job_cfg.get("name"))
        run_dir = RUNS_DIR / run_id

    env_map = jl.collect_env(job_cfg, resolved_inputs.copy(), run_id=run_id)
    env_map.update(_load_secrets())

    # resolve locations (for future use)
    if args.backend == "local":
        _ = resolve_input_locations(job_cfg, resolved_inputs.copy())

    # image rebuild check - for run contexts, use experiment's code directory
    rebuild_pref = None
    if args.rebuild:
        rebuild_pref = True
    if args.no_rebuild:
        rebuild_pref = False
    if args.image_sha and isinstance(job_cfg.get("image"), dict):
        job_cfg["image"]["pin"] = args.image_sha
        job_cfg["image"]["auto_rebuild"] = False
    if args.compute_gpu:
        job_cfg.setdefault("compute", {})["gpu"] = args.compute_gpu

    # For run contexts, set the build context to experiment's code directory
    if run_context["code_path"] and isinstance(job_cfg.get("image"), dict):
        job_cfg["image"]["context"] = str(run_context["code_path"].parent)  # experiment dir

    job_cfg = perform_rebuild_if_needed(job_cfg, rebuild=rebuild_pref)

    if not run_context["is_run"]:
        snapshot_config(job_path, run_dir)

    # Detect experiment and stage for project registry
    experiment = detect_experiment(job_path, run_context)
    stage = detect_stage(job_path)

    if args.dry_run:
        print("=== DRY RUN ===")
        print(json.dumps({"run_id": run_id, "experiment": experiment, "stage": stage, "env": env_map}, indent=2))
        return

    # Require description for non-dry-run
    if not args.description:
        raise SystemExit("--description / -d is required for non-dry-run. Example: -d 'Testing smoke pipeline'")

    # Collect code version info for reproducibility
    image_uri = get_effective_image_uri(job_cfg)
    code_version = {
        "git_sha": image_rebuild.get_current_sha(),
        "image_git_sha": image_rebuild.get_image_sha(image_uri) if image_uri else None,
        "image_digest": image_rebuild.get_image_digest(image_uri) if image_uri else None,
    }

    # Write to project registry at start
    write_project_registry(
        run_id=run_id,
        experiment=experiment,
        stage=stage,
        config_path=str(job_path),
        description=args.description,
        backend=args.backend,
        status="running",
        inputs={name: "" for name in job_cfg.get("inputs", {}).keys()},  # Lineage filled later
        code_version=code_version,
        variant=job_cfg.get("variant"),
    )

    start = datetime.now(timezone.utc)
    started = datetime.now(timezone.utc)

    # Save code version to run directory for easy access
    (run_dir / "code_version.json").write_text(json.dumps(code_version, indent=2))

    # Snapshot key algorithm files for diffing between runs
    snapshotted = image_rebuild.snapshot_code_files(run_dir)
    if snapshotted:
        (run_dir / "code_snapshot" / "manifest.txt").write_text("\n".join(snapshotted))

    # Save variant info if present in config
    variant = job_cfg.get("variant")
    if variant:
        (run_dir / "variant.yaml").write_text(yaml.safe_dump(variant, sort_keys=False))

    if args.backend == "local":
        # log started for local with minimal metadata
        registry.log_started(
            run_id,
            str(job_path),
            args.backend,
            zone=args.zone,
            inputs=list(job_cfg.get("inputs", {}).keys()),
            outputs=list(job_cfg.get("outputs", {}).keys()),
            image_uri=image_uri,
            **{k: v for k, v in code_version.items() if v},  # Include all code version fields
        )
        code = run_local(job_cfg, env_map, run_dir)
        instance_name = None
        zone_used = args.zone
        # Provide local log/artifact URIs (on filesystem)
        meta_run = {
            "log_uri": str(run_dir / "stdout.log"),
            "artifact_uri": str(run_dir),
            "stage_log_uri": None,
            "gcs_outputs": {},
            "snapshot_outputs": {},
        }
    else:
        code, instance_name, zone_used, meta_run = run_gce(job_cfg, env_map, run_dir, run_id, args, job_path, registry, project_root)
    duration = (datetime.now(timezone.utc) - started).total_seconds()
    meta_complete = {"instance_name": instance_name, "zone": zone_used}
    meta_complete.update(meta_run)
    if code == 0:
        registry.log_completed(run_id, [], duration, **{k: v for k, v in meta_complete.items() if v})
        # Update project registry with completed status
        update_project_registry_status(
            run_id,
            status="completed",
            outputs={"gcs": meta_run.get("gcs_outputs", {}), "snapshot": meta_run.get("snapshot_outputs", {})},
        )
    else:
        registry.log_failed(run_id, f"exit {code}", code, duration, job_path=str(job_path))
        update_project_registry_status(run_id, status="failed")
        raise SystemExit(code)

    # Handle outputs and manifests
    if args.backend == "local":
        sinks_cfg = yaml.safe_load((Path("infra") / "sinks.yaml").read_text())
        gcs_outputs = _stage_outputs_to_gcs(job_cfg.get("outputs", {}), run_dir, sinks_cfg, run_id)
        snapshot_outputs = {}
        meta_run["gcs_outputs"] = gcs_outputs
    else:
        gcs_outputs = meta_run.get("gcs_outputs", {})
        snapshot_outputs = meta_run.get("snapshot_outputs", {})
    manifest_env = _collect_manifests(job_cfg.get("outputs", {}), run_dir)
    update_job_state(job_path, run_id=run_id, status="completed" if code == 0 else "failed", started=started, completed=datetime.now(timezone.utc), gcs_outputs=gcs_outputs, snapshot_outputs=snapshot_outputs, manifest_env=manifest_env)


def list_runs(registry: run_registry.RunRegistry, status: str | None, job: str | None, limit: int) -> None:
    rows = registry.list_runs(limit=limit, status_filter=status, job_filter=job)
    for row in rows:
        print(json.dumps(row))


def show_run(registry: run_registry.RunRegistry, run_id: str) -> None:
    row = registry.get_run(run_id)
    if not row:
        raise SystemExit(f"Run not found: {run_id}")
    print(json.dumps(row, indent=2))


def abort_run(registry: run_registry.RunRegistry, run_id: str) -> None:
    # Best-effort: find instance name and zone from registry and delete it
    row = registry.get_run(run_id)
    instance = row.get("instance_name") if row else None
    zone = row.get("zone") if row else None
    if instance and zone:
        subprocess.run(["gcloud", "compute", "instances", "delete", instance, f"--zone={zone}", "--quiet"], check=False)
    registry.log_aborted(run_id, "SIGTERM", 0)
    print(f"Recorded abort for {run_id}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Declarative job launcher")
    p.add_argument("job", nargs="?", help="Path to job YAML")
    p.add_argument("--backend", default="local", choices=["local", "gce"])
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--rebuild", action="store_true", help="Force image rebuild")
    p.add_argument("--no-rebuild", action="store_true", help="Skip image rebuild check")
    p.add_argument("--force", action="store_true", help="Force run even if already completed (placeholder)")
    p.add_argument("--image-sha", help="Override image SHA/pin")
    p.add_argument("--compute.gpu", dest="compute_gpu", help="Override compute.gpu for this run")
    p.add_argument("--list", action="store_true")
    p.add_argument("--status")
    p.add_argument("--job-filter")
    p.add_argument("--show")
    p.add_argument("--abort")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--zone")  # optional override for GCE
    p.add_argument("--run-id-prefix")
    p.add_argument("-d", "--description", help="Run description (mandatory for non-dry-run)")
    p.add_argument("--wait", dest="wait", action="store_true", help="Wait for GCE job completion (blocking)")
    p.add_argument("--no-wait", dest="wait", action="store_false", help="Do not wait (default for GCE)")
    p.set_defaults(wait=None)
    return p.parse_args(argv)


def main() -> None:
    """Internal entry point - use create_run.py or start_run.py instead."""
    args = parse_args()
    registry = run_registry.RunRegistry(DEFAULT_REGISTRY)
    if args.list:
        list_runs(registry, args.status, args.job_filter, args.limit)
        return
    if args.show:
        show_run(registry, args.show)
        return
    if args.abort:
        abort_run(registry, args.abort)
        return
    if not args.job:
        raise SystemExit(
            "This is an internal module. Use the public interface:\n"
            "  - python create_run.py --experiment <exp> -d 'description'  # Create and launch\n"
            "  - python start_run.py --run <run_id>  # Launch existing run"
        )

    execute_job(Path(args.job), args, registry)


if __name__ == "__main__":
    main()
