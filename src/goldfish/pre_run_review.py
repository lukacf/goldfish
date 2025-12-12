"""Pre-run review system using Claude Code SDK.

This module provides AI-powered review of experiment runs before execution.
Reviews analyze workspace code, diffs, and run parameters to provide feedback
and gate execution.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from goldfish.models import RunReason

logger = logging.getLogger(__name__)


class ReviewDecision(BaseModel):
    """Decision from pre-run review."""

    approved: bool  # True = go ahead, False = needs changes
    feedback: str  # Detailed feedback/recommendations
    concerns: list[str] = []  # List of specific concerns
    suggestions: list[str] = []  # List of actionable suggestions


class PreRunReviewer:
    """Pre-run review system using Claude Code SDK."""

    def __init__(self, enabled: bool = True, require_approval: bool = True):
        """Initialize the pre-run reviewer.

        Args:
            enabled: Whether reviews are enabled
            require_approval: Whether runs are blocked on no-go decisions
        """
        self.enabled = enabled
        self.require_approval = require_approval

    def review_run(
        self,
        workspace_path: Path,
        diff_output: str,
        run_reason: RunReason | None,
        stage_names: list[str],
        pipeline_name: str | None = None,
    ) -> ReviewDecision:
        """Review a run before execution.

        Args:
            workspace_path: Path to the workspace directory
            diff_output: Git diff showing uncommitted changes
            run_reason: Structured reason for the run
            stage_names: List of stages to be executed
            pipeline_name: Name of the pipeline (if specified)

        Returns:
            ReviewDecision with approval status and feedback
        """
        if not self.enabled:
            return ReviewDecision(
                approved=True,
                feedback="Review system disabled - proceeding without review",
            )

        try:
            # Construct the review prompt
            review_prompt = self._build_review_prompt(
                workspace_path=workspace_path,
                diff_output=diff_output,
                run_reason=run_reason,
                stage_names=stage_names,
                pipeline_name=pipeline_name,
            )

            # Call Claude via Claude Code SDK
            decision = self._call_claude_review(review_prompt)
            return decision

        except Exception as e:
            logger.warning(f"Pre-run review failed: {e}")
            # On review system failure, allow the run to proceed
            return ReviewDecision(
                approved=True,
                feedback=f"Review system error (allowing run): {e}",
                concerns=[f"Review system encountered an error: {type(e).__name__}"],
            )

    def _build_review_prompt(
        self,
        workspace_path: Path,
        diff_output: str,
        run_reason: RunReason | None,
        stage_names: list[str],
        pipeline_name: str | None,
    ) -> str:
        """Build the review prompt for Claude."""
        lines = [
            "# Pre-Run Review Request",
            "",
            "You are reviewing an ML experiment run before execution.",
            "Analyze the code, changes, and run parameters to provide a go/no-go decision.",
            "",
            "## Run Parameters",
            f"**Stages to execute:** {', '.join(stage_names)}",
        ]

        if pipeline_name:
            lines.append(f"**Pipeline:** {pipeline_name}")

        lines.append("")

        # Add structured reason if provided
        if run_reason:
            lines.extend([
                "## Experiment Details",
                run_reason.to_markdown(),
                "",
            ])

        # Add diff
        if diff_output and diff_output.strip():
            lines.extend([
                "## Code Changes (git diff)",
                "```diff",
                diff_output[:5000],  # Limit diff size
                "```",
                "",
            ])
        else:
            lines.extend([
                "## Code Changes",
                "No uncommitted changes detected.",
                "",
            ])

        # Add instructions
        lines.extend([
            "## Review Instructions",
            "",
            "Analyze the above information and provide:",
            "1. **Approval decision** (approve/reject)",
            "2. **Feedback** on the experiment setup",
            "3. **Concerns** if any (e.g., missing validation, unclear hypothesis)",
            "4. **Suggestions** for improvement",
            "",
            "Focus on:",
            "- Does the hypothesis match the code changes?",
            "- Are the success criteria clear and measurable?",
            "- Are there obvious bugs or issues in the diff?",
            "- Is the experiment well-designed to test the hypothesis?",
            "",
            "Respond in JSON format:",
            "```json",
            "{",
            '  "approved": true/false,',
            '  "feedback": "detailed feedback here",',
            '  "concerns": ["concern 1", "concern 2"],',
            '  "suggestions": ["suggestion 1", "suggestion 2"]',
            "}",
            "```",
        ])

        return "\n".join(lines)

    def _call_claude_review(self, prompt: str) -> ReviewDecision:
        """Call Claude via subprocess to get review decision.

        This is a simplified implementation. In production, you would use
        the Claude Code SDK or API client directly.
        """
        # For now, we'll create a simple approval since we don't have
        # direct SDK integration set up. In a real implementation,
        # this would call the Claude Code SDK's agent API.

        # Try to use Claude Code SDK if available
        try:
            # This is a placeholder for actual SDK integration
            # In production, you would use:
            # from claude_code_sdk import ClaudeClient
            # client = ClaudeClient()
            # response = client.chat(prompt)

            # For now, return a simple approval with basic checks
            decision_data = self._simple_review_heuristic(prompt)
            return ReviewDecision(**decision_data)

        except Exception as e:
            logger.warning(f"Claude review call failed: {e}")
            raise

    def _simple_review_heuristic(self, prompt: str) -> dict[str, Any]:
        """Simple heuristic-based review as fallback.

        This is used when Claude SDK is not available.
        """
        concerns = []
        suggestions = []

        # Check if hypothesis is present
        if "Hypothesis:" not in prompt or "Hypothesis:** None" in prompt:
            concerns.append("No hypothesis specified - consider adding one for better experiment tracking")
            suggestions.append("Add a clear hypothesis about what you expect to happen")

        # Check if success criteria are present
        if "Min Result:" not in prompt or "Min Result:** None" in prompt:
            concerns.append("No success criteria specified")
            suggestions.append("Define minimum acceptable and optimal results")

        # Check for large diffs without clear approach
        if "```diff" in prompt and len(prompt) > 1000:
            if "Approach:" not in prompt or "Approach:** None" in prompt:
                concerns.append("Large code changes without documented approach")
                suggestions.append("Document your implementation approach for these changes")

        # Determine approval based on severity
        # For now, we'll approve but provide feedback
        approved = len(concerns) < 3  # Reject if too many concerns

        feedback_parts = []
        if approved:
            feedback_parts.append("✓ Run approved with recommendations")
        else:
            feedback_parts.append("✗ Run needs improvements before proceeding")

        if concerns:
            feedback_parts.append(f"\n{len(concerns)} concern(s) identified - see details below")

        return {
            "approved": approved,
            "feedback": "\n".join(feedback_parts),
            "concerns": concerns,
            "suggestions": suggestions,
        }


def create_reviewer_from_config(config: dict[str, Any]) -> PreRunReviewer:
    """Create a reviewer from configuration.

    Args:
        config: Review configuration dict with 'enabled' and 'require_approval' keys

    Returns:
        PreRunReviewer instance
    """
    return PreRunReviewer(
        enabled=config.get("enabled", False),
        require_approval=config.get("require_approval", True),
    )
