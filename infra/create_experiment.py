#!/usr/bin/env python
"""Create a new experiment by forking from a parent experiment.

IMPORTANT: Code lives ONLY in experiments. There is no central marketlm/.

Usage:
    # Create experiment from parent (PREFERRED)
    python scripts/create_experiment.py --name "v13-mi-fix" --parent v12-working -d "MI clamping fix"

    # Create from template (first experiment only)
    python scripts/create_experiment.py --name "v1-baseline" --from-template -d "Initial baseline"
"""
from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import yaml
from datetime import datetime, timezone
from pathlib import Path

# All paths relative to repo root
REPO_ROOT = Path(__file__).parent.parent
EXPERIMENTS_DIR = REPO_ROOT / "experiments"
TEMPLATES_DIR = REPO_ROOT / "_templates"
REGISTRY_PATH = EXPERIMENTS_DIR / "registry.yaml"

# What to copy from parent experiment
CODE_DIRS = ["marketlm"]
CODE_FILES = ["requirements.txt"]
EXPERIMENT_DIRS = ["scripts", "entrypoints"]


def get_current_sha() -> str | None:
    """Get current git HEAD SHA."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, check=True, cwd=REPO_ROOT
        )
        return result.stdout.strip()
    except Exception:
        return None


def compute_code_hash(experiment_dir: Path) -> str:
    """Compute a hash of all code files for change detection."""
    hasher = hashlib.sha256()
    code_dir = experiment_dir / "code"
    if code_dir.exists():
        for subdir in CODE_DIRS:
            src = code_dir / subdir
            if src.exists():
                for f in sorted(src.rglob("*.py")):
                    hasher.update(f.read_bytes())
    for subdir in EXPERIMENT_DIRS:
        src = experiment_dir / subdir
        if src.exists():
            for f in sorted(src.rglob("*")):
                if f.is_file():
                    hasher.update(f.read_bytes())
    return hasher.hexdigest()[:12]


def find_experiment(ref: str) -> Path:
    """Find experiment by name or partial match."""
    # Direct path
    if "/" in ref:
        path = Path(ref)
        if path.exists():
            return path
        raise ValueError(f"Experiment not found: {ref}")

    # Search in experiments/
    matches = list(EXPERIMENTS_DIR.glob(f"{ref}*"))
    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        raise ValueError(f"Ambiguous reference '{ref}'. Matches: {[m.name for m in matches]}")
    else:
        raise ValueError(f"Experiment not found: {ref}")


def copy_from_parent(parent_dir: Path, code_dest: Path, experiment_dest: Path) -> None:
    """Copy code from parent experiment."""
    parent_code = parent_dir / "code"
    if not parent_code.exists():
        raise ValueError(f"Parent experiment {parent_dir} has no code/ directory")

    # Copy code/ contents
    for item in parent_code.iterdir():
        if item.is_dir():
            shutil.copytree(item, code_dest / item.name,
                          ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        else:
            shutil.copy2(item, code_dest / item.name)

    # Copy experiment-level directories (scripts/, entrypoints/)
    for subdir in EXPERIMENT_DIRS:
        src = parent_dir / subdir
        dest = experiment_dest / subdir
        if src.exists():
            shutil.copytree(src, dest, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        else:
            dest.mkdir(exist_ok=True)

    # Copy base_config if exists
    base_config = parent_dir / "base_config.yaml"
    if base_config.exists():
        shutil.copy2(base_config, experiment_dest / "base_config.yaml")


def copy_from_template(code_dest: Path, experiment_dest: Path) -> None:
    """Copy from _templates/ for first experiment."""
    template_marketlm = TEMPLATES_DIR / "base_marketlm"
    if not template_marketlm.exists():
        raise ValueError(f"Template not found: {template_marketlm}")

    # Copy marketlm
    shutil.copytree(template_marketlm, code_dest / "marketlm",
                   ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))

    # Copy requirements.txt from repo root if exists
    req_file = REPO_ROOT / "requirements.txt"
    if req_file.exists():
        shutil.copy2(req_file, code_dest / "requirements.txt")

    # Create empty experiment directories
    for subdir in EXPERIMENT_DIRS:
        (experiment_dest / subdir).mkdir(exist_ok=True)


def format_experiment_id(name: str) -> str:
    """Generate experiment ID with timestamp."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"{name}-{ts}"


def update_registry(
    experiment_id: str,
    description: str,
    parent: str | None,
    git_sha: str | None,
    code_hash: str,
) -> None:
    """Update experiments registry."""
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)

    if REGISTRY_PATH.exists():
        registry = yaml.safe_load(REGISTRY_PATH.read_text()) or {}
    else:
        registry = {"schema_version": 3, "experiments": {}}

    registry.setdefault("experiments", {})
    registry["experiments"][experiment_id] = {
        "description": description,
        "parent": parent,
        "git_sha": git_sha,
        "code_hash": code_hash,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    REGISTRY_PATH.write_text(yaml.safe_dump(registry, sort_keys=False, default_flow_style=False))


def main():
    parser = argparse.ArgumentParser(
        description="Create a new experiment",
        epilog="Code lives ONLY in experiments. There is no central marketlm/."
    )
    parser.add_argument("--name", "-n", required=True,
                       help="Experiment name (e.g., 'v13-mi-fix')")
    parser.add_argument("--parent", "-p",
                       help="Parent experiment to fork from (required unless --from-template)")
    parser.add_argument("--from-template", action="store_true",
                       help="Create from _templates/ (for first experiment only)")
    parser.add_argument("-d", "--description", required=True,
                       help="What this experiment is testing")
    args = parser.parse_args()

    # Validate: must specify either --parent or --from-template
    if not args.parent and not args.from_template:
        parser.error("Must specify either --parent <exp> or --from-template")

    experiment_id = format_experiment_id(args.name)
    experiment_dir = EXPERIMENTS_DIR / experiment_id
    code_dir = experiment_dir / "code"

    if experiment_dir.exists():
        raise SystemExit(f"Experiment directory already exists: {experiment_dir}")

    # Create directories
    experiment_dir.mkdir(parents=True)
    code_dir.mkdir()

    # Copy code
    parent_name = None
    git_sha = get_current_sha()

    if args.parent:
        parent_dir = find_experiment(args.parent)
        copy_from_parent(parent_dir, code_dir, experiment_dir)
        parent_name = parent_dir.name
    else:
        copy_from_template(code_dir, experiment_dir)
        parent_name = "_templates/base_marketlm"

    # Compute code hash
    code_hash = compute_code_hash(experiment_dir)

    # Write experiment metadata
    meta = {
        "description": args.description,
        "parent": parent_name,
        "git_sha": git_sha,
        "code_hash": code_hash,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (experiment_dir / "meta.yaml").write_text(yaml.safe_dump(meta, sort_keys=False))

    # Update registry
    update_registry(
        experiment_id=experiment_id,
        description=args.description,
        parent=parent_name,
        git_sha=git_sha,
        code_hash=code_hash,
    )

    print(f"Created experiment: {experiment_id}")
    print(f"  Location: {experiment_dir}")
    print(f"  Parent: {parent_name}")
    print(f"  Code hash: {code_hash}")
    print()
    print("Next steps:")
    print(f"  1. Edit code in: {code_dir}/marketlm/")
    print(f"  2. Edit scripts in: {experiment_dir}/scripts/")
    print(f"  3. Launch: python scripts/create_run.py --experiment {experiment_id} --backend gce -d 'desc'")


if __name__ == "__main__":
    main()
