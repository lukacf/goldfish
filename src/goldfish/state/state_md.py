"""STATE.md generation and maintenance.

This is Claude's primary context recovery mechanism after compaction.
"""

import logging
import os
import re
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from goldfish.config import GoldfishConfig
from goldfish.models import DirtyState, JobStatus, SlotInfo, SlotState

logger = logging.getLogger(__name__)


def _sanitize_markdown(text: str, max_length: int = 200) -> str:
    """Sanitize text for safe markdown rendering.

    Prevents markdown injection attacks by:
    - Escaping markdown structural characters
    - Replacing newlines with spaces
    - Truncating to max_length

    Args:
        text: User-provided text to sanitize
        max_length: Maximum length before truncation

    Returns:
        Sanitized text safe for inline markdown rendering
    """
    if not text:
        return ""

    # Replace newlines with spaces to prevent multi-line injection
    text = re.sub(r"[\r\n]+", " ", text)

    # Escape markdown special characters that could create structure
    # - # could create headers
    # - [ ] could create links
    # - * _ could create emphasis that breaks structure
    text = text.replace("#", "\\#")
    text = text.replace("[", "\\[")
    text = text.replace("]", "\\]")

    # Truncate to max length
    if len(text) > max_length:
        text = text[: max_length - 3] + "..."

    return text


class StateManager:
    """Manages STATE.md generation and updates."""

    def __init__(self, state_path: Path, config: GoldfishConfig):
        self.state_path = state_path
        self.config = config
        self.max_recent = config.state_md.max_recent_actions

        self._recent_actions: deque[str] = deque(maxlen=self.max_recent)
        self._active_goal: str = "Not set"

        # Load existing state if present
        self._load_existing()

    def _load_existing(self) -> None:
        """Parse existing STATE.md to preserve goal and recent actions."""
        if not self.state_path.exists():
            return

        try:
            content = self.state_path.read_text()
        except (OSError, PermissionError):
            # Can't read existing state - start fresh
            return

        # Extract active goal
        if "## Active Goal" in content:
            lines = content.split("## Active Goal")[1].split("##")[0].strip().split("\n")
            if lines:
                self._active_goal = lines[0].strip()

        # Extract recent actions
        if "## Recent Actions" in content:
            section = content.split("## Recent Actions")[1].split("##")[0]
            for line in section.strip().split("\n"):
                line = line.strip()
                if line.startswith("- "):
                    self._recent_actions.append(line[2:])

    def set_goal(self, goal: str) -> None:
        """Update the active goal."""
        self._active_goal = goal
        self._write()

    def add_action(self, action: str) -> None:
        """Record an action with timestamp."""
        timestamp = datetime.now(UTC).strftime("%H:%M")
        self._recent_actions.append(f"[{timestamp}] {action}")

    def read(self) -> str:
        """Read current STATE.md content."""
        if self.state_path.exists():
            return self.state_path.read_text()
        return f"# {self.config.project_name}\n\nSTATE.md not initialized."

    def regenerate(
        self,
        slots: list[SlotInfo],
        jobs: list[dict[str, Any]],
        source_count: int = 0,
        recent_runs: list[dict[str, Any]] | None = None,
        experiment_contexts: dict[str, dict[str, Any]] | None = None,
    ) -> str:
        """Generate complete STATE.md content and write to file.

        Args:
            slots: List of workspace slot info
            jobs: List of background jobs
            source_count: Number of registered data sources
            recent_runs: Recent run history
            experiment_contexts: Dict mapping workspace names to experiment context
                (from ExperimentRecordManager.get_experiment_context)
        """
        lines = [f"# {self.config.project_name}", ""]

        # Active Goal
        lines.extend(["## Active Goal", self._active_goal, ""])

        # Workspaces
        lines.append("## Workspaces")
        for slot in slots:
            if slot.state == SlotState.EMPTY:
                lines.append(f"- {slot.slot}: [empty]")
            else:
                dirty_marker = "DIRTY" if slot.dirty == DirtyState.DIRTY else "CLEAN"
                context = slot.context or ""
                checkpoint = f" (last: {slot.last_checkpoint})" if slot.last_checkpoint else ""

                # Base workspace line
                workspace_line = f"- {slot.slot}: {slot.workspace} ({dirty_marker}){checkpoint}"
                if context:
                    workspace_line += f" - {context}"
                lines.append(workspace_line)

                # Add lineage information if available
                if slot.current_version:
                    version_info = f"  └─ Version: {slot.current_version}"
                    if slot.version_count:
                        version_info += f" (total: {slot.version_count})"
                    lines.append(version_info)

                if slot.parent_workspace:
                    parent_info = f"  └─ Branched from: {slot.parent_workspace}"
                    if slot.parent_version:
                        parent_info += f" @ {slot.parent_version}"
                    lines.append(parent_info)

                # Show recent versions (max 3)
                if slot.version_history:
                    recent_versions = slot.version_history[:3]
                    if recent_versions:
                        lines.append("  └─ Recent versions:")
                        for v in recent_versions:
                            desc = v.get("description", "")
                            desc_suffix = f" - {desc}" if desc else ""
                            lines.append(f"     • {v['version']}{desc_suffix}")

                # Show branches (max 3)
                if slot.branches:
                    shown_branches = slot.branches[:3]
                    if shown_branches:
                        branch_names = ", ".join(b["workspace"] for b in shown_branches)
                        extra = f" (+{len(slot.branches) - 3} more)" if len(slot.branches) > 3 else ""
                        lines.append(f"  └─ Branches: {branch_names}{extra}")

        lines.append("")

        # Configuration Invariants
        if self.config.invariants:
            lines.append("## Configuration Invariants (DO NOT CHANGE)")
            for inv in self.config.invariants:
                lines.append(f"- {inv}")
            lines.append("")

        # Data Sources summary
        if source_count > 0:
            lines.append("## Data Sources")
            lines.append(f"- {source_count} sources registered (use list_sources() to see)")
            lines.append("")

        # Experiment Summary (per workspace)
        if experiment_contexts:
            lines.append("## Experiment Summary")
            for ws_name, exp_ctx in experiment_contexts.items():
                lines.append(f"### {ws_name}")

                # Current best
                current_best = exp_ctx.get("current_best")
                if current_best:
                    tag = current_best.get("tag", "untagged")
                    metric = current_best.get("metric", "")
                    value = current_best.get("value")
                    if value is not None:
                        lines.append(f"- **Best**: @{tag} ({metric}: {value})")
                    else:
                        lines.append(f"- **Best**: @{tag}")
                else:
                    lines.append("- **Best**: None tagged")

                # Pending finalizations
                awaiting = exp_ctx.get("awaiting_finalization", [])
                if awaiting:
                    count = len(awaiting)
                    lines.append(f"- **Pending finalization**: {count} run(s)")
                else:
                    lines.append("- **Pending finalization**: None")

                # Recent trend (last 3)
                trend = exp_ctx.get("recent_trend", [])
                if trend:
                    values = [str(t.get("value", "?")) for t in trend[:3]]
                    lines.append(f"- **Recent trend**: [{', '.join(values)}]")

                # Regression alerts
                alerts = exp_ctx.get("regression_alerts", [])
                if alerts:
                    lines.append(f"- **Regression alerts**: {len(alerts)} detected")

                lines.append("")
            lines.append("")

        # Recent Runs with structured reasons
        if recent_runs:
            lines.append("## Recent Runs")
            for run in recent_runs:
                # Use state machine state (not legacy status)
                state = run.get("state", "unknown")
                active_states = (
                    "preparing",
                    "building",
                    "launching",
                    "running",
                    "post_run",
                    "awaiting_user_finalization",
                )
                state_emoji = "✓" if state == "completed" else "⏳" if state in active_states else "✗"
                run_line = f"- {state_emoji} {run.get('stage_name', 'unknown')} ({state})"
                if run.get("started_at"):
                    run_line += f" - {run['started_at'][:16]}"
                lines.append(run_line)

                # Show structured reason if present
                reason_json = run.get("reason_json")
                if reason_json:
                    import json

                    try:
                        reason_data = json.loads(reason_json) if isinstance(reason_json, str) else reason_json
                        if reason_data.get("description"):
                            # Sanitize to prevent markdown injection
                            desc = _sanitize_markdown(reason_data["description"])
                            lines.append(f"  └─ {desc}")
                        if reason_data.get("hypothesis"):
                            hyp = _sanitize_markdown(reason_data["hypothesis"])
                            lines.append(f"  └─ Hypothesis: {hyp}")
                    except (json.JSONDecodeError, KeyError, TypeError):
                        pass
            lines.append("")

        # Warm Pool (only shown when enabled)
        if self.config.gce and self.config.gce.warm_pool.enabled:
            lines.append("## Warm Pool")
            max_inst = self.config.gce.warm_pool.max_instances
            timeout = self.config.gce.warm_pool.idle_timeout_minutes
            profiles = self.config.gce.warm_pool.profiles or ["all"]
            lines.append(
                f"- Enabled: max {max_inst} instances, " f"idle timeout {timeout}m, " f"profiles: {', '.join(profiles)}"
            )
            lines.append("")

        # Recent Actions
        lines.append("## Recent Actions")
        if self._recent_actions:
            # Show in reverse chronological order
            for action in reversed(list(self._recent_actions)):
                lines.append(f"- {action}")
        else:
            lines.append("- No recent actions")
        lines.append("")

        # Background Jobs
        lines.append("## Background Jobs")
        active_jobs = [j for j in jobs if j.get("status") in (JobStatus.PENDING, JobStatus.RUNNING)]
        if active_jobs:
            for job in active_jobs:
                status = job.get("status", "unknown")
                script = job.get("script", "unknown")
                job_id = job.get("id", "unknown")
                lines.append(f"- {job_id}: {script} ({status})")
        else:
            lines.append("- No active jobs")

        content = "\n".join(lines)
        self._write_content(content)
        return content

    def _write_content(self, content: str) -> None:
        """Write content to STATE.md file.

        Uses atomic write pattern: write to temp file, then rename.
        This prevents partial writes on disk full / crash.
        """
        import tempfile

        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            # Log error but don't fail - STATE.md is not critical
            logger.warning(f"Failed to create directory for STATE.md at '{self.state_path.parent}': {e}")
            return

        try:
            # Write to temp file first (atomic write pattern)
            fd, tmp_path = tempfile.mkstemp(
                dir=self.state_path.parent,
                prefix=".state_md_",
                suffix=".tmp",
            )
            try:
                with os.fdopen(fd, "w") as f:
                    f.write(content)
                # Atomic rename
                Path(tmp_path).rename(self.state_path)
            except Exception:
                # Clean up temp file on error
                try:
                    Path(tmp_path).unlink()
                except Exception as cleanup_err:
                    logger.warning(f"Failed to clean up temp file '{tmp_path}' after write error: {cleanup_err}")
                raise
        except (OSError, PermissionError) as e:
            # Log error but don't fail - STATE.md is best-effort
            logger.warning(f"Failed to write STATE.md to '{self.state_path}': {e}")

    def _write(self) -> None:
        """Write current state (used for partial updates like set_goal).

        Reads existing STATE.md and updates the Active Goal section.
        If STATE.md doesn't exist, creates a minimal version.
        """
        if not self.state_path.exists():
            # Create minimal STATE.md with just the goal
            content = "\n".join(
                [
                    f"# {self.config.project_name}",
                    "",
                    "## Active Goal",
                    self._active_goal,
                    "",
                    "## Workspaces",
                    "- No workspaces mounted",
                    "",
                    "## Recent Actions",
                ]
            )
            for action in reversed(list(self._recent_actions)):
                content += f"\n- {action}"
            if not self._recent_actions:
                content += "\n- No recent actions"
            content += "\n\n## Background Jobs\n- No active jobs"
            self._write_content(content)
            return

        # Read existing content and update goal section
        try:
            content = self.state_path.read_text()
        except (OSError, PermissionError):
            return

        # Update the Active Goal section
        if "## Active Goal" in content:
            # Split into before, goal section, and after
            parts = content.split("## Active Goal", 1)
            before = parts[0]
            rest = parts[1]

            # Find end of goal section (next ## header)
            if "##" in rest[1:]:
                # Find the next ## after the first character
                next_section_idx = rest.index("##", 1)
                after = rest[next_section_idx:]
            else:
                after = ""

            # Reconstruct with new goal
            content = f"{before}## Active Goal\n{self._active_goal}\n\n{after}"
        else:
            # No goal section - prepend it
            content = f"## Active Goal\n{self._active_goal}\n\n{content}"

        self._write_content(content)

    @classmethod
    def create_initial(cls, state_path: Path, config: GoldfishConfig) -> "StateManager":
        """Create initial STATE.md for a new project."""
        manager = cls(state_path, config)

        lines = [
            f"# {config.project_name}",
            "",
            "## Active Goal",
            "Not set - update this with your current objective",
            "",
            "## Workspaces",
        ]

        for slot in config.slots:
            lines.append(f"- {slot}: [empty]")

        lines.extend(["", "## Configuration Invariants (DO NOT CHANGE)"])

        if config.invariants:
            for inv in config.invariants:
                lines.append(f"- {inv}")
        else:
            lines.append("- Add critical configuration here that must not change")

        lines.extend(
            [
                "",
                "## Recent Actions",
                f"- [{datetime.now(UTC).strftime('%H:%M')}] Project initialized",
                "",
                "## Background Jobs",
                "- No active jobs",
            ]
        )

        content = "\n".join(lines)
        manager._write_content(content)

        return manager
