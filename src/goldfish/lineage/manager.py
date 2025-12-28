"""Lineage tracking for Goldfish workspaces and runs."""

from goldfish.db.database import Database
from goldfish.errors import GoldfishError
from goldfish.workspace.manager import WorkspaceManager


class LineageManager:
    """Track workspace lineage and version history."""

    def __init__(self, db: Database, workspace_manager: WorkspaceManager):
        self.db = db
        self.workspace_manager = workspace_manager

    def get_workspace_lineage(
        self,
        workspace: str,
        version_limit: int | None = None,
        version_offset: int | None = None,
    ) -> dict:
        """Get lineage for workspace with paginated version history.

        Returns:
            {
                "name": str,
                "created": str,
                "parent": str | None,
                "parent_version": str | None,
                "description": str,
                "version_count": int,
                "versions": [
                    {
                        "version": str,
                        "git_tag": str,
                        "git_sha": str,
                        "created_by": str,
                        "created_at": str,
                        "job_id": str | None,
                        "description": str,
                        "message": str  # Alias for description
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

        # Get total version count
        all_versions = self.db.list_versions(workspace)
        version_count = len(all_versions)

        # Get limited versions (reversed order: newest first is better for limited views)
        # We fetch them ASC from DB then reverse or just change DB query to DESC.
        # Let's change DB query to DESC in our call.
        with self.db._conn() as conn:
            limit_clause = f"LIMIT {version_limit}" if version_limit is not None else ""
            offset_clause = f"OFFSET {version_offset}" if version_offset is not None else ""
            rows = conn.execute(
                f"""
                SELECT * FROM workspace_versions
                WHERE workspace_name = ?
                AND pruned_at IS NULL
                ORDER BY created_at DESC
                {limit_clause} {offset_clause}
                """,
                (workspace,),
            ).fetchall()
            versions = [dict(row) for row in rows]

        # Add 'message' alias for description to help agents
        for v in versions:
            v["message"] = v.get("description")

        # Get branches (child workspaces)
        branches_raw = self.db.get_workspace_branches(workspace)
        # Transform to documented format (workspace_name -> workspace)
        branches = [
            {
                "workspace": b["workspace_name"],
                "branched_from": b.get("parent_version"),
                "branched_at": b.get("created_at"),
                "description": b.get("description"),
            }
            for b in branches_raw
        ]

        return {
            "name": workspace,
            "created": lineage_row["created_at"],
            "parent": lineage_row["parent_workspace"],
            "parent_version": lineage_row["parent_version"],
            "description": lineage_row["description"],
            "version_count": version_count,
            "versions": versions,
            "branches": branches,
        }

    def get_version_diff(self, workspace: str, from_version: str, to_version: str) -> dict:
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
            raise GoldfishError(f"Version '{from_version}' not found in workspace '{workspace}'")
        if not to_row:
            raise GoldfishError(f"Version '{to_version}' not found in workspace '{workspace}'")

        # Get git diff
        git_diff = self.workspace_manager.git.diff_commits(from_row["git_sha"], to_row["git_sha"])

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
        if not version:
            raise GoldfishError(f"Version '{run['version']}' not found for workspace '{run['workspace_name']}'")

        # Get input signals (signals consumed by this run)
        # We support both the new explicit 'input' model and the legacy 'consumed_by' model
        explicit_inputs = self.db.list_signals(stage_run_id=stage_run_id, signal_type="input")
        legacy_inputs = self.db.list_signals(consumed_by=stage_run_id)

        # Merge and deduplicate (by signal name and source)
        inputs = []
        seen_inputs = set()
        for inp in explicit_inputs + legacy_inputs:
            key = (inp["signal_name"], inp.get("source_stage_run_id") or inp.get("stage_run_id"))
            if key not in seen_inputs:
                inputs.append(inp)
                seen_inputs.add(key)

        # Get output signals (signals produced by this run)
        # We fetch all signals for this run except 'input'
        all_signals = self.db.list_signals(stage_run_id=stage_run_id)
        outputs = [s for s in all_signals if s["signal_type"] != "input"]

        # Get downstream signals (runs produced using outputs from this run)
        downstream_signals = self.db.list_signals(source_stage_run_id=stage_run_id)
        # Deduplicate consumer runs
        downstream_run_ids = sorted({s["stage_run_id"] for s in downstream_signals if s.get("stage_run_id")})
        downstream_runs = []
        for d_id in downstream_run_ids:
            d_run = self.db.get_stage_run(d_id)
            if d_run:
                downstream_runs.append(
                    {
                        "stage_run_id": d_run["id"],
                        "stage": d_run["stage_name"],
                        "status": d_run["status"],
                        "started_at": d_run.get("started_at"),
                    }
                )

        # Parse config_override if it's a JSON string
        config_override = run.get("config_override") or run.get("config_json") or {}
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
                    "source_stage_run_id": inp.get("source_stage_run_id"),
                }
                for inp in inputs
            ],
            "outputs": [
                {
                    "signal_name": out["signal_name"],
                    "signal_type": out["signal_type"],
                    "storage_location": out["storage_location"],
                    "size_bytes": out.get("size_bytes"),
                }
                for out in outputs
            ],
            "downstream": downstream_runs,
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
            raise GoldfishError(f"Version '{from_version}' not found in workspace '{from_workspace}'")

        # Create git branch
        self.workspace_manager.branch_workspace(from_workspace, from_version, new_workspace)

        # Record lineage
        self.db.create_workspace_lineage(
            workspace_name=new_workspace,
            parent_workspace=from_workspace,
            parent_version=from_version,
            description=reason,
        )
