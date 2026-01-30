"""Sync workspace phase for stage execution."""

from __future__ import annotations

from typing import Protocol

from goldfish.jobs.phases.context import StageRunContext


class _SyncDeps(Protocol):
    def _auto_version(self, workspace: str, stage_name: str, reason: str | None) -> tuple[str, str]: ...


def sync_workspace(deps: _SyncDeps, ctx: StageRunContext, *, reason: str | None) -> str:
    """Sync workspace changes and set ctx.version.

    Returns:
        git_sha for the synced commit.
    """
    _ = ctx.settings
    version, git_sha = deps._auto_version(ctx.workspace_name, ctx.stage_name, reason)
    ctx.version = version
    return git_sha
