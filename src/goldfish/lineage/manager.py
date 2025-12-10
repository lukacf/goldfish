"""Lineage tracking for Goldfish workspaces and runs."""

from typing import Optional

from goldfish.db.database import Database
from goldfish.errors import GoldfishError
from goldfish.workspace.manager import WorkspaceManager


class LineageManager:
    """Track workspace lineage and version history."""

    def __init__(self, db: Database, workspace_manager: WorkspaceManager):
        self.db = db
        self.workspace_manager = workspace_manager

    def get_workspace_lineage(self, workspace: str) -> dict:
        """Get full lineage for workspace.

        Returns:
            {
                "name": str,
                "created": str,
                "parent": str | None,
                "parent_version": str | None,
                "description": str,
                "versions": [
                    {
                        "version": str,
                        "git_tag": str,
                        "git_sha": str,
                        "created_by": str,
                        "created_at": str,
                        "job_id": str | None,
                        "description": str
                    },
                    ...
                ],
                "branches": [
                    {
                        "workspace": str,
                        "branched_from": str,
                        "branched_at": str,
                        "description": str
                    },
                    ...
                ]
            }
        """
        # Get workspace lineage record
        lineage_row = self.db.get_workspace_lineage(workspace)

        if not lineage_row:
            raise GoldfishError(f"Workspace '{workspace}' not found")

        # Get versions
        versions = self.db.list_versions(workspace)

        # Get branches (child workspaces)
        branches = self.db.get_workspace_branches(workspace)

        return {
            "name": workspace,
            "created": lineage_row["created_at"],
            "parent": lineage_row["parent_workspace"],
            "parent_version": lineage_row["parent_version"],
            "description": lineage_row["description"],
            "versions": [
                {
                    "version": v["version"],
                    "git_tag": v["git_tag"],
                    "git_sha": v["git_sha"],
                    "created_by": v["created_by"],
                    "created_at": v["created_at"],
                    "job_id": v.get("job_id"),
                    "description": v.get("description"),
                }
                for v in versions
            ],
            "branches": [
                {
                    "workspace": b["workspace_name"],
                    "branched_from": b["parent_version"],
                    "branched_at": b["created_at"],
                    "description": b["description"],
                }
                for b in branches
            ],
        }

    def get_version_diff(
        self, workspace: str, from_version: str, to_version: str
    ) -> dict:
        """Compare two versions.

        Returns:
            {
                "from_version": str,
                "to_version": str,
                "commits": [{"sha": str, "message": str}, ...],
                "files": {file_path: changes_info}
            }
        """
        # Get version records
        from_row = self.db.get_version(workspace, from_version)
        to_row = self.db.get_version(workspace, to_version)

        if not from_row:
            raise GoldfishError(
                f"Version '{from_version}' not found in workspace '{workspace}'"
            )
        if not to_row:
            raise GoldfishError(
                f"Version '{to_version}' not found in workspace '{workspace}'"
            )

        # Get git diff
        git_diff = self.workspace_manager.git.diff_commits(
            from_row["git_sha"], to_row["git_sha"]
        )

        return {
            "from_version": from_version,
            "to_version": to_version,
            "commits": git_diff.get("commits", []),
            "files": git_diff.get("files", {}),
        }

    def get_run_provenance(self, stage_run_id: str) -> dict:
        """Get exact provenance of a stage run.

        Returns:
            {
                "stage_run_id": str,
                "workspace": str,
                "version": str,
                "git_sha": str,
                "stage": str,
                "config_override": dict,
                "inputs": [signal_info, ...],
                "outputs": [signal_info, ...]
            }
        """
        # Get stage run
        run = self.db.get_stage_run(stage_run_id)
        if not run:
            raise GoldfishError(f"Stage run '{stage_run_id}' not found")

        # Get version info
        version = self.db.get_version(run["workspace_name"], run["version"])

        # Get input signals (signals consumed by this run)
        inputs = self.db.list_signals(consumed_by=stage_run_id)

        # Get output signals (signals produced by this run)
        outputs = self.db.list_signals(stage_run_id=stage_run_id)

        # Parse config_override if it's a JSON string
        config_override = run.get("config_override") or {}
        if isinstance(config_override, str):
            import json
            config_override = json.loads(config_override) if config_override else {}

        return {
            "stage_run_id": stage_run_id,
            "workspace": run["workspace_name"],
            "version": run["version"],
            "git_sha": version["git_sha"],
            "stage": run["stage_name"],
            "config_override": config_override,
            "inputs": [
                {
                    "signal_name": inp["signal_name"],
                    "signal_type": inp["signal_type"],
                    "storage_location": inp["storage_location"],
                }
                for inp in inputs
            ],
            "outputs": [
                {
                    "signal_name": out["signal_name"],
                    "signal_type": out["signal_type"],
                    "storage_location": out["storage_location"],
                }
                for out in outputs
            ],
        }

    def branch_workspace(
        self,
        from_workspace: str,
        from_version: str,
        new_workspace: str,
        reason: str,
    ) -> None:
        """Create new workspace branched from specific version.

        Args:
            from_workspace: Source workspace
            from_version: Version to branch from (e.g., "v3")
            new_workspace: Name for new workspace
            reason: Why branching
        """
        # Verify version exists
        version = self.db.get_version(from_workspace, from_version)
        if not version:
            raise GoldfishError(
                f"Version '{from_version}' not found in workspace '{from_workspace}'"
            )

        # Create git branch
        self.workspace_manager.branch_workspace(
            from_workspace, from_version, new_workspace
        )

        # Record lineage
        self.db.create_workspace_lineage(
            workspace_name=new_workspace,
            parent_workspace=from_workspace,
            parent_version=from_version,
            description=reason,
        )
