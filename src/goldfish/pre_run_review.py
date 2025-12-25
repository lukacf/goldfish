"""Pre-run review using Claude Agent SDK.

Reviews experiment code before execution to catch errors early.
This is like a "run request" review - similar to PR review but for ML runs.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path
from typing import TYPE_CHECKING

from goldfish.models import ReviewIssue, ReviewSeverity, RunReason, RunReview
from goldfish.svs.agent import (
    ClaudeCodeProvider,
    CodexCLIProvider,
    GeminiCLIProvider,
    NullProvider,
    ReviewRequest,
    ToolPolicy,
)

if TYPE_CHECKING:
    from goldfish.config import PreRunReviewConfig
    from goldfish.db.database import Database
    from goldfish.svs.config import SVSConfig

logger = logging.getLogger(__name__)

# Security limits
MAX_FILE_SIZE = 100_000  # 100KB per file
MAX_TOTAL_CONTEXT_SIZE = 500_000  # 500KB total context


def escape_for_prompt(text: str) -> str:
    """Escape special XML/HTML characters in user content for safe prompt inclusion.

    Prevents XML/HTML injection attacks by escaping special characters that could
    be interpreted as markup. This ensures user-provided content (file contents,
    descriptions, configs) cannot inject malicious tags into AI prompts.

    The escaping order is important:
    1. & must be escaped first, otherwise &lt; would become &amp;lt;
    2. Then < > " ' are escaped

    Args:
        text: User-provided text that may contain special characters

    Returns:
        Text with XML special characters escaped:
        - & -> &amp;
        - < -> &lt;
        - > -> &gt;
        - " -> &quot;
        - ' -> &#39;

    Examples:
        >>> escape_for_prompt("if x < 5:")
        'if x &lt; 5:'
        >>> escape_for_prompt('name = "value"')
        'name = &quot;value&quot;'
    """
    # Escape & first to avoid double-escaping other entities
    text = text.replace("&", "&amp;")
    # Then escape the other special characters
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    text = text.replace('"', "&quot;")
    text = text.replace("'", "&#39;")
    return text


# Review prompt template
REVIEW_PROMPT = """You are reviewing an ML experiment before execution. Your job is to catch errors that would waste compute time.

## Context

**Workspace:** {workspace}
**Stages to run:** {stages_to_run}

### Pipeline Definition (pipeline.yaml)
```yaml
{pipeline_yaml}
```

### Experiment Hypothesis
{run_reason}

### Code Changes Since Last Successful Run
```diff
{diff_text}
```

{stage_sections}

## Your Task

Review ONLY the stages being run: {stages_to_run}

Check each stage for:
1. **Syntax errors** - missing imports, typos, invalid Python
2. **Logic bugs** - wrong dimensions, off-by-one errors, incorrect APIs
3. **Config issues** - invalid hyperparameters (negative lr, etc.), mismatched shapes
4. **Pipeline issues** - missing inputs, type mismatches between stages
5. **Hypothesis coherence** - does the code actually test what's claimed?

## Output Format

Be brief but specific. Use these severity markers:
- ERROR: Blocking issues that WILL cause failure
- WARNING: Potential problems that MAY cause failure
- NOTE: Suggestions that won't cause failure

For each stage, format as:
```
## stage_name
ERROR: file.py:line - description
WARNING: file.py - description
NOTE: suggestion
```

If a stage looks good, just write:
```
## stage_name
No issues found.
```

Start your review now:
"""

STAGE_SECTION_TEMPLATE = """
### Stage: {stage_name}

#### Module: modules/{stage_name}.py
```python
{module_content}
```

#### Config: configs/{stage_name}.yaml (if exists)
```yaml
{config_content}
```
"""


class PreRunReviewer:
    """Reviews experiment runs before execution using unified agent abstraction."""

    def __init__(
        self,
        config: PreRunReviewConfig,
        svs_config: SVSConfig,
        workspace_path: Path,
        dev_repo_path: Path,
        db: Database | None = None,
    ):
        """Initialize the reviewer.

        Args:
            config: Pre-run review configuration
            svs_config: SVS configuration (for agent settings)
            workspace_path: Path to the user's workspace (slot)
            dev_repo_path: Path to the dev repository
            db: Database for looking up last successful run
        """
        self.config = config
        self.svs_config = svs_config
        self.workspace_path = workspace_path
        self.dev_repo_path = dev_repo_path
        self.db = db

    def _get_agent(self):
        """Get the configured SVS agent provider."""
        provider_name = self.svs_config.agent_provider
        if provider_name == "claude_code":
            return ClaudeCodeProvider()
        elif provider_name == "codex_cli":
            return CodexCLIProvider()
        elif provider_name == "gemini_cli":
            return GeminiCLIProvider()
        elif provider_name == "null":
            return NullProvider()
        return NullProvider()

    async def review(
        self,
        stages: list[str],
        reason: RunReason | None = None,
        diff_text: str = "",
    ) -> RunReview:
        """Review stages before execution.

        Args:
            stages: List of stage names to review
            reason: The RunReason explaining why this is being run
            diff_text: Git diff since last successful run

        Returns:
            RunReview with findings
        """
        start_time = time.time()

        # Gather context
        pipeline_yaml = self._read_pipeline_yaml()
        stage_sections = self._build_stage_sections(stages)
        run_reason_text = reason.to_markdown() if reason else "No reason provided"

        # Build prompt with escaped content
        prompt = REVIEW_PROMPT.format(
            workspace=escape_for_prompt(self.workspace_path.name),
            stages_to_run=escape_for_prompt(", ".join(stages)),
            pipeline_yaml=escape_for_prompt(pipeline_yaml),
            run_reason=escape_for_prompt(run_reason_text),
            diff_text=escape_for_prompt(diff_text or "No changes (first run or diff unavailable)"),
            stage_sections=stage_sections,  # Content within sections is already escaped by _build_stage_sections
        )

        # Enforce total context size limit
        if len(prompt) > MAX_TOTAL_CONTEXT_SIZE:
            logger.warning(f"Review context too large ({len(prompt)} bytes), truncating to {MAX_TOTAL_CONTEXT_SIZE}")
            # Truncate with indication that content was cut
            prompt = prompt[: MAX_TOTAL_CONTEXT_SIZE - 100] + "\n\n[... context truncated due to size limit ...]"

        # Call the agent provider
        try:
            agent = self._get_agent()

            # Pre-run tools: read-only
            tool_policy = ToolPolicy(
                permission_mode="auto",
                allow_tools=["Read", "Glob", "Grep"],
            )

            # Build agent request
            request = ReviewRequest(
                review_type="pre_run",
                context={
                    "cwd": str(self.workspace_path),
                    "model": self.svs_config.agent_model or self.config.model,
                    "max_turns": self.config.max_turns,
                    "timeout_seconds": self.config.timeout_seconds,
                    "tool_policy": tool_policy,
                    "prompt": prompt,
                },
            )

            # run() is sync in the provider, we run it in a thread if needed
            # but since we are in an async method, we'll wrap it
            loop = asyncio.get_running_loop()
            result = await asyncio.wait_for(
                loop.run_in_executor(None, agent.run, request),
                timeout=self.config.timeout_seconds,
            )

            review_text = result.raw_output
        except TimeoutError:
            logger.error(f"Pre-run review timed out after {self.config.timeout_seconds}s")
            return RunReview(
                approved=True,  # Don't block on timeout
                summary=f"Review timed out after {self.config.timeout_seconds}s",
                full_review="",
                reviewed_stages=stages,
            )
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            # Let these propagate - user cancellation should work
            raise
        except Exception as e:
            logger.error(f"Pre-run review failed: {e}", exc_info=True)
            return RunReview(
                approved=True,  # Don't block on review failures
                summary=f"Review failed: {e}",
                full_review="",
                reviewed_stages=stages,
            )

        # Parse the review
        issues = self._parse_review(review_text, stages)
        has_errors = any(i.severity == ReviewSeverity.ERROR for i in issues)

        elapsed_ms = int((time.time() - start_time) * 1000)

        # Build summary
        error_count = sum(1 for i in issues if i.severity == ReviewSeverity.ERROR)
        warning_count = sum(1 for i in issues if i.severity == ReviewSeverity.WARNING)

        if has_errors:
            summary = f"Review blocked: {error_count} error(s), {warning_count} warning(s)"
        elif warning_count > 0:
            summary = f"Review passed with {warning_count} warning(s)"
        else:
            summary = "Review passed: no issues found"

        return RunReview(
            approved=not has_errors,
            issues=issues,
            summary=summary,
            full_review=review_text,
            reviewed_stages=stages,
            review_time_ms=elapsed_ms,
        )

    def _read_pipeline_yaml(self) -> str:
        """Read pipeline.yaml from workspace."""
        pipeline_path = self.workspace_path / "pipeline.yaml"
        return self._read_file_safe(pipeline_path, "# No pipeline.yaml found")

    def _build_stage_sections(self, stages: list[str]) -> str:
        """Build detailed sections for each stage being reviewed."""
        sections = []
        for stage in stages:
            # Validate stage name to prevent path traversal
            if not self._is_safe_filename(stage):
                logger.warning(f"Skipping unsafe stage name: {stage}")
                continue

            module_path = self.workspace_path / "modules" / f"{stage}.py"
            config_path = self.workspace_path / "configs" / f"{stage}.yaml"

            module_content = self._read_file_safe(module_path, "# Module not found")
            config_content = self._read_file_safe(config_path, "# No config file")

            section = STAGE_SECTION_TEMPLATE.format(
                stage_name=escape_for_prompt(stage),
                module_content=escape_for_prompt(module_content),
                config_content=escape_for_prompt(config_content),
            )
            sections.append(section)

        return "\n".join(sections)

    def _is_safe_filename(self, name: str) -> bool:
        """Check if filename is safe (no path traversal)."""
        # Reject empty, dots only, or anything with path separators
        if not name or name in (".", ".."):
            return False
        if "/" in name or "\\" in name:
            return False
        # Reject hidden files
        if name.startswith("."):
            return False
        return True

    def _read_file_safe(self, file_path: Path, default: str) -> str:
        """Safely read a file with security and size checks.

        Args:
            file_path: Path to read
            default: Default value if file cannot be read

        Returns:
            File contents or default value
        """
        try:
            # Resolve to absolute path
            resolved = file_path.resolve()

            # SECURITY: Ensure path is within workspace
            try:
                resolved.relative_to(self.workspace_path.resolve())
            except ValueError:
                logger.warning(f"Path traversal attempt blocked: {file_path}")
                return default

            # SECURITY: Check for symlinks
            if file_path.is_symlink():
                logger.warning(f"Symlink blocked in review: {file_path}")
                return "# Symlink detected - not reading for security"

            if not resolved.exists():
                return default

            # SECURITY: Check file size before reading
            file_size = resolved.stat().st_size
            if file_size > MAX_FILE_SIZE:
                logger.warning(f"File too large for review: {file_path} ({file_size} bytes)")
                return f"# File too large ({file_size} bytes, max {MAX_FILE_SIZE})"

            # Read with explicit encoding
            return resolved.read_text(encoding="utf-8")

        except UnicodeDecodeError:
            logger.warning(f"File has invalid encoding: {file_path}")
            return "# File contains invalid UTF-8 encoding"
        except PermissionError:
            logger.warning(f"Permission denied reading: {file_path}")
            return "# Permission denied"
        except OSError as e:
            logger.warning(f"OS error reading {file_path}: {e}")
            return default

    def _parse_review(self, review_text: str, stages: list[str]) -> list[ReviewIssue]:
        """Parse Claude's review text into structured ReviewIssues.

        Expected format:
        ## stage_name
        ERROR: file.py:line - description
        WARNING: file.py - description
        NOTE: suggestion
        """
        issues: list[ReviewIssue] = []
        current_stage: str | None = None

        # Create a set of lowercase stage names for flexible matching
        stage_set = {s.lower() for s in stages}
        stage_map = {s.lower(): s for s in stages}  # Map lowercase to original

        for line in review_text.split("\n"):
            line = line.strip()
            if not line:
                continue

            # Check for stage header - more flexible matching
            # Handles: ## train, ### train, ## train (details), ## Stage: train
            if line.startswith("#"):
                # Strip all leading # and whitespace
                header_content = line.lstrip("#").strip()
                # Remove common prefixes like "Stage:" or "Stage "
                header_content = re.sub(r"^(?:stage\s*:?\s*)", "", header_content, flags=re.IGNORECASE)
                # Extract first word/identifier (the stage name)
                match = re.match(r"^(\S+)", header_content)
                if match:
                    potential_stage = match.group(1).lower()
                    if potential_stage in stage_set:
                        current_stage = stage_map[potential_stage]
                continue

            if current_stage is None:
                continue

            # Parse issue lines - handle various formats
            severity: ReviewSeverity | None = None
            content = ""

            # Check for severity markers (case-insensitive, with/without bold)
            lower_line = line.lower()
            if lower_line.startswith("error:") or lower_line.startswith("**error:**"):
                severity = ReviewSeverity.ERROR
                # Find the actual marker end position
                if line.lower().startswith("**error:**"):
                    content = line[10:].strip()
                else:
                    content = line[6:].strip()
            elif lower_line.startswith("warning:") or lower_line.startswith("**warning:**"):
                severity = ReviewSeverity.WARNING
                if line.lower().startswith("**warning:**"):
                    content = line[12:].strip()
                else:
                    content = line[8:].strip()
            elif lower_line.startswith("note:") or lower_line.startswith("**note:**"):
                severity = ReviewSeverity.NOTE
                if line.lower().startswith("**note:**"):
                    content = line[9:].strip()
                else:
                    content = line[5:].strip()
            # Also handle bullet point format: - ERROR: ...
            elif lower_line.startswith("- error:"):
                severity = ReviewSeverity.ERROR
                content = line[8:].strip()
            elif lower_line.startswith("- warning:"):
                severity = ReviewSeverity.WARNING
                content = line[10:].strip()
            elif lower_line.startswith("- note:"):
                severity = ReviewSeverity.NOTE
                content = line[7:].strip()

            if severity and content:
                issue = self._parse_issue_content(severity, current_stage, content)
                issues.append(issue)

        return issues

    def _parse_issue_content(self, severity: ReviewSeverity, stage: str, content: str) -> ReviewIssue:
        """Parse the content part of an issue line.

        Handles formats like:
        - file.py:10 - description
        - file.py - description
        - description only
        """
        # Extended file extensions pattern
        file_ext_pattern = r"(?:py|yaml|yml|json|txt|sh|md|toml|cfg|ini|csv)"

        # Try to extract file:line - description
        file_line_match = re.match(rf"([^\s:]+\.{file_ext_pattern}):(\d+)\s*[-:]\s*(.*)", content)
        if file_line_match:
            return ReviewIssue(
                severity=severity,
                stage=stage,
                file=file_line_match.group(1),
                line=int(file_line_match.group(2)),
                message=file_line_match.group(3) or content,
            )

        # Try to extract file - description (no line number)
        file_only_match = re.match(rf"([^\s:]+\.{file_ext_pattern})\s*[-:]\s*(.*)", content)
        if file_only_match:
            return ReviewIssue(
                severity=severity,
                stage=stage,
                file=file_only_match.group(1),
                message=file_only_match.group(2) or content,
            )

        # No file reference, just the message
        return ReviewIssue(
            severity=severity,
            stage=stage,
            message=content,
        )


async def review_before_run(
    config: PreRunReviewConfig,
    svs_config: SVSConfig,
    workspace_path: Path,
    dev_repo_path: Path,
    stages: list[str],
    reason: RunReason | None = None,
    diff_text: str = "",
    db: Database | None = None,
) -> RunReview:
    """Convenience function to perform pre-run review.

    Args:
        config: Pre-run review configuration
        svs_config: SVS configuration
        workspace_path: Path to the user's workspace (slot)
        dev_repo_path: Path to the dev repository
        stages: List of stage names to review
        reason: The RunReason explaining why this is being run
        diff_text: Git diff since last successful run
        db: Database for looking up last successful run

    Returns:
        RunReview with findings
    """
    reviewer = PreRunReviewer(
        config=config,
        svs_config=svs_config,
        workspace_path=workspace_path,
        dev_repo_path=dev_repo_path,
        db=db,
    )
    return await reviewer.review(stages=stages, reason=reason, diff_text=diff_text)
