"""Build image phase for stage execution."""

from __future__ import annotations

from typing import Protocol

from goldfish.jobs.phases.context import StageRunContext


class _BuildDeps(Protocol):
    def _build_docker_image(self, workspace: str, version: str, *, profile_name: str | None = None) -> str: ...


def build_image(deps: _BuildDeps, ctx: StageRunContext, *, profile_name: str | None) -> str:
    """Build the stage container image and return its tag."""
    _ = ctx.settings
    return deps._build_docker_image(ctx.workspace_name, ctx.version, profile_name=profile_name)
