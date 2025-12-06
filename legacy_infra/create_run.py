#!/usr/bin/env python
"""Create a new run from an experiment with config overrides.

By default, creates and launches the run. Use --no-launch to create without launching.

Usage:
    # Create and launch run with default config (GCE backend)
    python create_run.py --experiment tbpe-v1-20251128-103000 -d "Baseline run" --backend gce

    # Create run without launching (review config first)
    python create_run.py --experiment tbpe-v1-20251128-103000 -d "Baseline run" --no-launch

    # Override specific config values
    python create_run.py --experiment tbpe-v1-20251128-103000 \
        --set env.MI_THRESH=0.05 \
        --set env.MAX_MERGES=500 \
        -d "Higher MI threshold test" \
        --backend gce

    # Dry run - show what would happen
    python create_run.py --experiment tbpe-v1-20251128-103000 -d "Test" --dry-run
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import sys
import yaml
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

# Add repo root to path for infra imports
REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

EXPERIMENTS_DIR = REPO_ROOT / "experiments"
EXPERIMENTS_REGISTRY = EXPERIMENTS_DIR / "registry.yaml"


def deep_set(d: Dict[str, Any], key_path: str, value: Any) -> None:
    """Set a nested dict value using dot notation (e.g., 'env.MI_THRESH')."""
    keys = key_path.split(".")
    current = d
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    # Try to parse value as number or bool
    if value.lower() == "true":
        value = True
    elif value.lower() == "false":
        value = False
    else:
        try:
            value = int(value)
        except ValueError:
            try:
                value = float(value)
            except ValueError:
                pass  # Keep as string
    current[keys[-1]] = value


def merge_configs(base: Dict[str, Any], overrides: List[str]) -> Dict[str, Any]:
    """Merge base config with command-line overrides."""
    result = copy.deepcopy(base)
    for override in overrides:
        if "=" not in override:
            raise ValueError(f"Invalid override format: {override}. Use key.path=value")
        key_path, value = override.split("=", 1)
        deep_set(result, key_path, value)
    return result


def compute_config_hash(config: Dict[str, Any]) -> str:
    """Compute hash of config for change detection."""
    config_str = yaml.safe_dump(config, sort_keys=True)
    return hashlib.sha256(config_str.encode()).hexdigest()[:12]


def format_run_id(run_name: str | None = None) -> str:
    """Generate run ID with timestamp."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    if run_name:
        return f"{run_name}-{ts}"
    return f"run-{ts}"


def find_experiment(experiment_ref: str) -> Path:
    """Find experiment directory by name or path."""
    # Direct path
    if "/" in experiment_ref:
        path = Path(experiment_ref)
        if path.exists():
            return path
        raise ValueError(f"Experiment not found: {experiment_ref}")

    # Search in experiments/
    matches = list(EXPERIMENTS_DIR.glob(f"{experiment_ref}*"))
    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        raise ValueError(f"Ambiguous experiment reference '{experiment_ref}'. Matches: {[m.name for m in matches]}")
    else:
        raise ValueError(f"Experiment not found: {experiment_ref}")


def update_registry(
    run_id: str,
    experiment_id: str,
    description: str,
    config_hash: str,
    overrides: List[str],
) -> None:
    """Update experiments registry with new run."""
    if not EXPERIMENTS_REGISTRY.exists():
        raise ValueError("Registry not found. Create an experiment first.")

    registry = yaml.safe_load(EXPERIMENTS_REGISTRY.read_text()) or {}

    # Add run to runs section
    registry.setdefault("runs", {})
    registry["runs"][run_id] = {
        "experiment": experiment_id,
        "description": description,
        "config_hash": config_hash,
        "overrides": overrides,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "pending",
    }

    # Add run to experiment's run list
    if experiment_id in registry.get("experiments", {}):
        registry["experiments"][experiment_id].setdefault("runs", [])
        registry["experiments"][experiment_id]["runs"].append(run_id)

    EXPERIMENTS_REGISTRY.write_text(yaml.safe_dump(registry, sort_keys=False, default_flow_style=False))


def launch_run(config_path: Path, args: argparse.Namespace) -> None:
    """Launch the run using internal _launch module."""
    from infra._launch import execute_job, parse_args as launch_parse_args
    from infra import run_registry

    # Build launch args
    launch_argv = [
        str(config_path),
        f"--backend={args.backend}",
        f"-d={args.description}",
    ]
    if args.dry_run:
        launch_argv.append("--dry-run")
    if args.rebuild:
        launch_argv.append("--rebuild")
    if args.no_rebuild:
        launch_argv.append("--no-rebuild")
    if args.zone:
        launch_argv.append(f"--zone={args.zone}")
    if args.compute_gpu:
        launch_argv.append(f"--compute.gpu={args.compute_gpu}")

    launch_args = launch_parse_args(launch_argv)
    registry = run_registry.RunRegistry(REPO_ROOT / "runs" / "registry.jsonl")
    execute_job(config_path, launch_args, registry)


def main():
    parser = argparse.ArgumentParser(description="Create and launch a new run from an experiment")

    # Run creation args
    parser.add_argument("--experiment", "-e", required=True, help="Experiment to create run from")
    parser.add_argument("--name", "-n", help="Run name suffix (optional)")
    parser.add_argument("--set", "-s", action="append", dest="overrides", default=[], help="Config override (key.path=value)")
    parser.add_argument("-d", "--description", required=True, help="What this run is testing")

    # Launch control
    parser.add_argument("--no-launch", action="store_true", help="Create run without launching")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without executing")

    # Backend args (passed to launcher)
    parser.add_argument("--backend", default="gce", choices=["local", "gce"], help="Execution backend")
    parser.add_argument("--zone", help="GCE zone override")
    parser.add_argument("--rebuild", action="store_true", help="Force Docker image rebuild")
    parser.add_argument("--no-rebuild", action="store_true", help="Skip image rebuild check")
    parser.add_argument("--compute.gpu", dest="compute_gpu", help="Override compute.gpu")

    args = parser.parse_args()

    # Find experiment
    experiment_dir = find_experiment(args.experiment)
    experiment_id = experiment_dir.name

    # Load base config
    base_config_path = experiment_dir / "base_config.yaml"
    if base_config_path.exists():
        base_config = yaml.safe_load(base_config_path.read_text()) or {}
    else:
        base_config = {}
        print(f"Warning: No base_config.yaml in {experiment_dir}")

    # Merge with overrides
    config = merge_configs(base_config, args.overrides)
    config_hash = compute_config_hash(config)

    # Create run directory UNDER the experiment
    run_id = format_run_id(args.name)
    runs_dir = experiment_dir / "runs"
    runs_dir.mkdir(exist_ok=True)
    run_dir = runs_dir / run_id

    if run_dir.exists():
        raise SystemExit(f"Run directory already exists: {run_dir}")

    run_dir.mkdir(parents=True)
    (run_dir / "outputs").mkdir()
    (run_dir / "logs").mkdir()

    # Write config
    config_path = run_dir / "config.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False))

    # Write run metadata
    meta = {
        "experiment": experiment_id,
        "run_id": run_id,
        "description": args.description,
        "config_hash": config_hash,
        "overrides": args.overrides,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "pending",
    }
    (run_dir / "meta.yaml").write_text(yaml.safe_dump(meta, sort_keys=False))

    # Update registry
    update_registry(
        run_id=run_id,
        experiment_id=experiment_id,
        description=args.description,
        config_hash=config_hash,
        overrides=args.overrides,
    )

    print(f"Created run: {run_id}")
    print(f"  Location: {run_dir}")
    print(f"  Experiment: {experiment_id}")
    print(f"  Config hash: {config_hash}")
    if args.overrides:
        print(f"  Overrides: {args.overrides}")

    # Launch unless --no-launch
    if args.no_launch:
        print()
        print("Run created but not launched (--no-launch).")
        print(f"To launch later: python start_run.py --run {run_id}")
    else:
        print()
        print(f"Launching on {args.backend}...")
        launch_run(config_path, args)


if __name__ == "__main__":
    main()
