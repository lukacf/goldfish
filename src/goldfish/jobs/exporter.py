"""Export workspace snapshot to experiment directory structure.

This module handles the critical step of converting a workspace snapshot
into the experiment directory format expected by the job infrastructure.
"""

import shutil
from datetime import UTC, datetime
from pathlib import Path

import yaml

from goldfish.errors import GoldfishError


def _safe_create_directory(path: Path) -> None:
    """Safely create a directory, preventing symlink attacks.

    This function prevents TOCTOU race conditions where an attacker
    creates a symlink at the target path between our existence check
    and directory creation.

    Args:
        path: Path to create as directory

    Raises:
        GoldfishError: If path exists, is a symlink, or cannot be created safely
    """
    # Check for symlinks first (symlinks can exist without target existing)
    if path.is_symlink():
        raise GoldfishError(f"Security error: path is a symlink, refusing to use: {path}")

    # Check if already exists
    if path.exists():
        raise GoldfishError(f"Directory already exists: {path}")

    # Create parent directories if needed
    path.parent.mkdir(parents=True, exist_ok=True)

    # Create the directory atomically
    try:
        path.mkdir(exist_ok=False)
    except FileExistsError as err:
        # Race condition: something was created between our check and mkdir
        raise GoldfishError(f"Directory already exists (race condition detected): {path}") from err

    # Verify what we created is actually a directory (not a symlink)
    if path.is_symlink():
        # This should be impossible, but check anyway
        path.rmdir()
        raise GoldfishError(f"Security error: created path became a symlink: {path}")

    if not path.is_dir():
        raise GoldfishError(f"Failed to create directory: {path}")


class SnapshotExporter:
    """Exports a workspace snapshot to experiment directory structure."""

    def __init__(self, experiments_dir: Path):
        """Initialize exporter.

        Args:
            experiments_dir: Directory where experiments are created
        """
        self.experiments_dir = experiments_dir
        self.experiments_dir.mkdir(parents=True, exist_ok=True)

    def export(
        self,
        workspace_path: Path,
        workspace_name: str,
        snapshot_id: str,
        script: str,
        reason: str,
        config_overrides: dict | None = None,
    ) -> Path:
        """Export workspace snapshot to experiment directory.

        Creates:
            experiments/goldfish-{workspace}-{timestamp}/
                code/         <- from workspace code/
                scripts/      <- from workspace scripts/
                entrypoints/  <- from workspace entrypoints/
                base_config.yaml  <- merged config
                meta.yaml     <- goldfish metadata

        Args:
            workspace_path: Path to the mounted workspace (e.g., workspaces/w1)
            workspace_name: Name of the workspace
            snapshot_id: Git snapshot tag (e.g., snap-abc123-20251204-120000)
            script: Script to run (e.g., "scripts/train.py")
            reason: Reason for running this job
            config_overrides: Optional config overrides for this run

        Returns:
            Path to the created experiment directory
        """
        # Generate experiment name
        timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        exp_name = f"goldfish-{workspace_name}-{timestamp}"
        exp_dir = self.experiments_dir / exp_name

        # Safely create experiment directory (prevents symlink attacks)
        _safe_create_directory(exp_dir)

        # Copy workspace contents
        self._copy_workspace(workspace_path, exp_dir)

        # Merge and write config
        self._write_config(workspace_path, exp_dir, config_overrides)

        # Write goldfish metadata
        self._write_meta(
            exp_dir,
            workspace_name=workspace_name,
            snapshot_id=snapshot_id,
            script=script,
            reason=reason,
        )

        return exp_dir

    def _copy_workspace(self, workspace_path: Path, exp_dir: Path) -> None:
        """Copy workspace contents to experiment directory.

        Security: symlinks are preserved as symlinks (not dereferenced).
        This prevents symlink attacks where an attacker places a symlink
        pointing to sensitive data outside the workspace.
        """
        # Directories to copy
        dirs_to_copy = ["code", "scripts", "entrypoints"]

        for dir_name in dirs_to_copy:
            src = workspace_path / dir_name
            dst = exp_dir / dir_name

            if src.exists() and src.is_dir():
                shutil.copytree(
                    src,
                    dst,
                    symlinks=True,  # Preserve symlinks, don't dereference
                    ignore=shutil.ignore_patterns(
                        "__pycache__",
                        "*.pyc",
                        ".git",
                        ".gitkeep",
                        "*.egg-info",
                    ),
                )
            else:
                # Create empty directory with placeholder
                dst.mkdir(parents=True, exist_ok=True)

    def _write_config(
        self,
        workspace_path: Path,
        exp_dir: Path,
        config_overrides: dict | None,
    ) -> None:
        """Write merged base_config.yaml to experiment directory."""
        # Start with workspace base_config if exists
        workspace_config_path = workspace_path / "base_config.yaml"
        base_config: dict = {}

        if workspace_config_path.exists():
            try:
                with open(workspace_config_path) as f:
                    base_config = yaml.safe_load(f) or {}
            except yaml.YAMLError as e:
                raise GoldfishError("Failed to parse base_config.yaml: invalid YAML syntax") from e

        # Apply overrides
        if config_overrides:
            base_config = self._deep_merge(base_config, config_overrides)

        # Write merged config
        exp_config_path = exp_dir / "base_config.yaml"
        with open(exp_config_path, "w") as f:
            yaml.dump(base_config, f, default_flow_style=False)

    def _write_meta(
        self,
        exp_dir: Path,
        workspace_name: str,
        snapshot_id: str,
        script: str,
        reason: str,
    ) -> None:
        """Write goldfish metadata to experiment directory."""
        meta = {
            "goldfish": {
                "workspace": workspace_name,
                "snapshot_id": snapshot_id,
                "script": script,
                "reason": reason,
                "exported_at": datetime.now(UTC).isoformat(),
            }
        }

        meta_path = exp_dir / "meta.yaml"
        with open(meta_path, "w") as f:
            yaml.dump(meta, f, default_flow_style=False)

    def _deep_merge(self, base: dict, override: dict) -> dict:
        """Deep merge two dictionaries."""
        result = base.copy()

        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value

        return result
