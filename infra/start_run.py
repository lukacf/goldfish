#!/usr/bin/env python
"""Start an existing run that was created with --no-launch.

Usage:
    # Start by run ID (can be partial match)
    python start_run.py --run my-run-20251203-123456

    # Start with different backend
    python start_run.py --run my-run --backend local

    # Dry run
    python start_run.py --run my-run --dry-run
"""
from __future__ import annotations

import argparse
import sys
import yaml
from pathlib import Path

# Add repo root to path for infra imports
REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

EXPERIMENTS_DIR = REPO_ROOT / "experiments"


def find_run(run_ref: str) -> tuple[Path, str, str]:
    """Find run directory by ID (supports partial match).

    Returns (config_path, run_id, experiment_id).
    """
    # Search all experiments for matching run
    matches = []

    for exp_dir in EXPERIMENTS_DIR.iterdir():
        if not exp_dir.is_dir():
            continue
        runs_dir = exp_dir / "runs"
        if not runs_dir.exists():
            continue

        for run_dir in runs_dir.iterdir():
            if not run_dir.is_dir():
                continue
            if run_ref in run_dir.name:
                config_path = run_dir / "config.yaml"
                if config_path.exists():
                    matches.append((config_path, run_dir.name, exp_dir.name))

    if len(matches) == 0:
        raise ValueError(f"Run not found: {run_ref}")
    elif len(matches) > 1:
        run_names = [m[1] for m in matches]
        raise ValueError(f"Ambiguous run reference '{run_ref}'. Matches: {run_names}")

    return matches[0]


def main():
    parser = argparse.ArgumentParser(description="Start an existing run")
    parser.add_argument("--run", "-r", required=True, help="Run ID (supports partial match)")
    parser.add_argument("--backend", default="gce", choices=["local", "gce"], help="Execution backend")
    parser.add_argument("--zone", help="GCE zone override")
    parser.add_argument("--rebuild", action="store_true", help="Force Docker image rebuild")
    parser.add_argument("--no-rebuild", action="store_true", help="Skip image rebuild check")
    parser.add_argument("--compute.gpu", dest="compute_gpu", help="Override compute.gpu")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen")

    args = parser.parse_args()

    # Find the run
    config_path, run_id, experiment_id = find_run(args.run)

    # Load run metadata for description
    meta_path = config_path.parent / "meta.yaml"
    if meta_path.exists():
        meta = yaml.safe_load(meta_path.read_text()) or {}
        description = meta.get("description", "Resumed run")
    else:
        description = "Resumed run"

    print(f"Found run: {run_id}")
    print(f"  Experiment: {experiment_id}")
    print(f"  Config: {config_path}")
    print()

    # Import and launch
    from scripts._launch import execute_job, parse_args as launch_parse_args
    from infra import run_registry

    # Build launch args
    launch_argv = [
        str(config_path),
        f"--backend={args.backend}",
        f"-d={description}",
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

    print(f"Launching on {args.backend}...")
    execute_job(config_path, launch_args, registry)


if __name__ == "__main__":
    main()
