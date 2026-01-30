"""Agent abstraction for SVS (Software Verification Service).

This module defines the core abstractions for agent-based code review:
- ToolPolicy: Controls agent tool permissions
- AgentRequest: Full-featured request for agent operations
- AgentResult: Comprehensive result from agent operations
- ReviewRequest: Simplified request for review operations (backward compat)
- ReviewResult: Simplified result for review operations (backward compat)
- AgentProvider: Protocol that all review agents must implement
- NullProvider: Test double for unit testing

The full AgentRequest/AgentResult types support swappable providers (Claude Code,
Codex CLI, Gemini CLI). The simpler ReviewRequest/ReviewResult types are kept
for backward compatibility with existing SVS code.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

from goldfish.errors import GoldfishError

logger = logging.getLogger("goldfish.svs.agent")
# =============================================================================
# Full Agent Abstraction (per SVS spec 2.0)
# =============================================================================


@dataclass
class ToolPolicy:
    """Controls agent tool permissions.

    Attributes:
        permission_mode: Claude CLI permission mode. Valid values:
            - "bypassPermissions": Auto-approve all tool calls (for automated reviews)
            - "default": Normal interactive mode
            - "acceptEdits": Auto-accept file edits
            - "dontAsk": Don't prompt for permissions
            - "plan": Planning mode only
            - "delegate": Delegate to sub-agents
        allow_tools: Explicit list of allowed tool names (None = all allowed)
        deny_tools: Explicit list of denied tool names (None = none denied)
        mcp_servers: List of MCP server names to enable
    """

    permission_mode: Literal["bypassPermissions", "default", "acceptEdits", "dontAsk", "plan", "delegate"] = (
        "bypassPermissions"
    )
    allow_tools: list[str] | None = None
    deny_tools: list[str] | None = None
    mcp_servers: list[str] | None = None


@dataclass
class AgentRequest:
    """Full-featured request for agent-based operations.

    This is the comprehensive request type that supports all provider features.
    Use ReviewRequest for simpler review-only operations.

    Attributes:
        mode: Execution mode - "batch" (non-interactive) or "interactive"
        prompt: The prompt/instructions for the agent
        context: Contextual information (workspace, stage, files, etc.)
        cwd: Working directory for the agent
        model: Optional model override (provider-specific)
        max_turns: Maximum conversation turns (for multi-turn agents)
        output_format: Expected output format - "text" or "json"
        tool_policy: Controls which tools the agent can use
        timeout_seconds: Maximum time for the operation
    """

    mode: Literal["batch", "interactive"] = "batch"
    prompt: str = ""
    context: dict[str, Any] = field(default_factory=dict)
    cwd: str = "."
    model: str | None = None
    max_turns: int | None = None
    output_format: Literal["text", "json"] = "text"
    tool_policy: ToolPolicy | None = None
    timeout_seconds: int | None = None


@dataclass
class AgentResult:
    """Comprehensive result from agent-based operations.

    This is the full result type that captures all provider output.
    Use ReviewResult for simpler review-only results.

    Attributes:
        decision: Review decision if applicable - "approved", "blocked", "warned", or None
        findings: List of issues found (ERROR/WARNING/NOTE messages)
        raw_output: Full raw output text from the agent
        structured_output: Parsed JSON output if output_format="json"
        tool_calls: List of tool calls made by the agent
        duration_ms: Time taken for the operation in milliseconds
        exit_code: Process exit code for CLI-based providers
    """

    decision: Literal["approved", "blocked", "warned"] | None = None
    findings: list[str] = field(default_factory=list)
    raw_output: str = ""
    structured_output: dict[str, Any] | None = None
    tool_calls: list[dict[str, Any]] | None = None
    duration_ms: int = 0
    exit_code: int = 0


# =============================================================================
# Simplified Review Types (backward compatibility)
# =============================================================================


@dataclass
class ReviewRequest:
    """Simplified request for agent-based code review.

    This is the simple request type for review operations.
    Use AgentRequest for full provider features.

    Attributes:
        review_type: Type of review - "pre_run", "during_run", or "post_run"
        context: Contextual information for the review (workspace, stage, etc.)
        stats: Optional statistics about the code being reviewed
    """

    review_type: str  # "pre_run" | "during_run" | "post_run"
    context: dict[str, Any] = field(default_factory=dict)
    stats: dict[str, Any] | None = None

    def to_agent_request(self, prompt: str = "", cwd: str = ".") -> AgentRequest:
        """Convert to full AgentRequest.

        Args:
            prompt: The review prompt
            cwd: Working directory

        Returns:
            AgentRequest with review context
        """
        return AgentRequest(
            mode="batch",
            prompt=prompt,
            context={
                "review_type": self.review_type,
                **self.context,
                **({"stats": self.stats} if self.stats else {}),
            },
            cwd=cwd,
        )


@dataclass
class ReviewResult:
    """Simplified result from agent-based code review.

    This is the simple result type for review operations.
    Use AgentResult for full provider features.

    Attributes:
        decision: Review decision - "approved", "blocked", or "warned"
        findings: List of issues found (ERROR/WARNING/NOTE messages)
        response_text: Full response text from the agent
        duration_ms: Time taken for review in milliseconds
    """

    decision: str  # "approved" | "blocked" | "warned"
    findings: list[str] = field(default_factory=list)
    response_text: str = ""
    duration_ms: int = 0

    @classmethod
    def from_agent_result(cls, result: AgentResult) -> ReviewResult:
        """Create ReviewResult from full AgentResult.

        Args:
            result: Full AgentResult

        Returns:
            Simplified ReviewResult
        """
        return cls(
            decision=result.decision or "approved",
            findings=result.findings,
            response_text=result.raw_output,
            duration_ms=result.duration_ms,
        )


# =============================================================================
# Provider Protocol
# =============================================================================


@runtime_checkable
class AgentProvider(Protocol):
    """Protocol for agent-based code review providers.

    All review agents must implement this protocol to be compatible with SVS.
    Providers can support either the simple ReviewRequest/ReviewResult interface
    or the full AgentRequest/AgentResult interface.
    """

    name: str

    def run(self, request: ReviewRequest) -> ReviewResult:
        """Execute code review based on the request.

        Args:
            request: ReviewRequest containing review type, context, and stats

        Returns:
            ReviewResult with decision, findings, response text, and duration
        """
        ...


class NullProvider:
    """Test double for agent-based code review.

    Returns configurable canned responses for testing purposes.
    Always returns near-zero duration (instant).

    Example:
        >>> provider = NullProvider()
        >>> provider.configure_response("blocked", findings=["ERROR: Missing import"])
        >>> request = ReviewRequest(review_type="pre_run", context={})
        >>> result = provider.run(request)
        >>> result.decision
        'blocked'
    """

    name: str = "null"

    def __init__(self, default_decision: str = "approved") -> None:
        """Initialize NullProvider with default decision.

        Args:
            default_decision: Default decision to return ("approved", "blocked", or "warned")
        """
        self._decision = default_decision
        self._findings: list[str] = []

    def configure_response(self, decision: str, findings: list[str] | None = None) -> None:
        """Configure the response that will be returned by run().

        Configuration persists across multiple run() calls until changed.

        Args:
            decision: Decision to return ("approved", "blocked", or "warned")
            findings: Optional list of finding messages. Defaults to empty list.
        """
        self._decision = decision
        self._findings = findings if findings is not None else []

    def run(self, request: ReviewRequest) -> ReviewResult:
        """Execute review and return configured response.

        Args:
            request: ReviewRequest (contents are ignored by NullProvider)

        Returns:
            ReviewResult with configured decision and findings
        """
        import json

        # Check if JSON output is expected (during_run always expects JSON)
        output_format = request.context.get("output_format", "text") if request.context else "text"
        expects_json = request.review_type == "during_run" or output_format == "json"

        if expects_json:
            # Build JSON response for during-run and other JSON-expecting reviews
            findings_list = (
                [{"check": "null_provider", "severity": "NOTE", "summary": f} for f in self._findings]
                if self._findings
                else [{"check": "null_provider", "severity": "NOTE", "summary": "NullProvider test observation"}]
            )
            json_response = {
                "findings": findings_list,
                "request_stop": False,
                "stop_reason": None,
            }
            response_text = f"```json\n{json.dumps(json_response, indent=2)}\n```"
        else:
            # Plain text for pre-run and post-run reviews
            findings_text = "\n".join(self._findings) if self._findings else ""
            response_text = f"NullProvider: {self._decision} for {request.review_type} review\n{findings_text}"

        return ReviewResult(
            decision=self._decision,
            findings=self._findings,
            response_text=response_text,
            duration_ms=0,  # Instant - always < 10ms
        )


# =============================================================================
# CLI Providers (Claude Code, Codex CLI, Gemini CLI)
# =============================================================================


def _build_tool_policy(context: dict[str, Any]) -> ToolPolicy | None:
    """Build ToolPolicy from context dict if provided."""
    tool_policy = context.get("tool_policy")
    if isinstance(tool_policy, ToolPolicy):
        return tool_policy
    if isinstance(tool_policy, dict):
        return ToolPolicy(
            permission_mode=tool_policy.get("permission_mode", "bypassPermissions"),
            allow_tools=tool_policy.get("allow_tools"),
            deny_tools=tool_policy.get("deny_tools"),
            mcp_servers=tool_policy.get("mcp_servers"),
        )
    return None


def _default_prompt(review_type: str, context: dict[str, Any], stats: dict[str, Any] | None) -> str:
    """Build a safe default prompt for providers when none is supplied."""
    # Extract tool policy if present in context
    tool_policy = context.get("tool_policy")
    tool_instructions = ""
    if tool_policy:
        if isinstance(tool_policy, ToolPolicy):
            policy_dict = {
                "permission_mode": tool_policy.permission_mode,
                "allow_tools": tool_policy.allow_tools,
                "deny_tools": tool_policy.deny_tools,
                "mcp_servers": tool_policy.mcp_servers,
            }
        else:
            policy_dict = tool_policy

        tool_instructions = f"\n## Tool Usage Policy\n{json.dumps(policy_dict, indent=2)}\n"

    # Test mode instructions
    test_mode_section = ""
    if context.get("test_mode"):
        test_mode_section = """
## TEST MODE ENABLED
This is a test run to verify the AI review system works. You MUST:
1. Always provide at least one finding, even if just a NOTE-level observation
2. Look for [SVS-TEST] markers - these indicate intentional test triggers
3. Be verbose - this is testing the review pipeline, not production monitoring
4. Comment on anything noteworthy in the outputs/stats
"""

    # Format run command section if run_context is available
    run_command_section = ""
    run_context = context.get("run_context", {})
    if run_context:
        parts = []
        if run_context.get("workspace"):
            parts.append(f"workspace={run_context['workspace']!r}")
        if run_context.get("stage_name"):
            parts.append(f"stage={run_context['stage_name']!r}")
        if run_context.get("pipeline_name"):
            parts.append(f"pipeline={run_context['pipeline_name']!r}")
        if run_context.get("config_override"):
            parts.append(f"config_override={json.dumps(run_context['config_override'])}")
        if run_context.get("inputs_override"):
            parts.append(f"inputs_override={json.dumps(run_context['inputs_override'])}")
        if parts:
            run_command_section = f"\n## Run Command\nrun({', '.join(parts)})\n"

        # Include run reason if available
        run_reason = run_context.get("run_reason", {})
        if run_reason:
            reason_lines = []
            if run_reason.get("description"):
                reason_lines.append(f"Description: {run_reason['description']}")
            if run_reason.get("hypothesis"):
                reason_lines.append(f"Hypothesis: {run_reason['hypothesis']}")
            if run_reason.get("goal"):
                reason_lines.append(f"Goal: {run_reason['goal']}")
            if reason_lines:
                run_command_section += "\n## Run Reason\n" + "\n".join(reason_lines) + "\n"

    # ML assessment instructions for post_run reviews
    ml_assessment_section = ""
    results_spec = run_context.get("results_spec") if run_context else None
    if review_type == "post_run" and results_spec:
        primary_metric = results_spec.get("primary_metric", "unknown")
        direction = results_spec.get("direction", "maximize")
        min_value = results_spec.get("min_value")
        goal_value = results_spec.get("goal_value")
        tolerance = results_spec.get("tolerance", 0)

        ml_assessment_section = f"""
## ML Assessment Required
This is a post-run review. You must assess the ML outcome based on these expected results:

**Expected Results (results_spec):**
- Primary Metric: {primary_metric}
- Direction: {direction}
- Minimum Acceptable Value: {min_value}
- Goal Value: {goal_value}
- Tolerance: {tolerance}

**Your Assessment:**
1. Find the final value of the primary metric ({primary_metric}) in the outputs/stats
2. Compare it to the expected values above
3. Determine the ML outcome:
   - **success**: Value {'≥' if direction == 'maximize' else '≤'} goal_value (within tolerance)
   - **partial**: Value between min_value and goal_value
   - **miss**: Value {'<' if direction == 'maximize' else '>'} min_value
   - **unknown**: Cannot determine (metric not found or error occurred)

**Required Output Format:**
At the end of your review, include this line (replace VALUE and OUTCOME):
ML_OUTCOME: {primary_metric}=VALUE, outcome=OUTCOME

Example: ML_OUTCOME: val_accuracy=0.85, outcome=success
"""

    # Filter out items that are displayed separately
    filtered_context = {k: v for k, v in context.items() if k not in ("tool_policy", "test_mode", "run_context")}

    payload = {
        "review_type": review_type,
        "context": filtered_context,
        "stats": stats or {},
    }
    return (
        "You are an AI reviewer. Return findings as lines prefixed with "
        "ERROR:, WARNING:, or NOTE:. If nothing is wrong, say 'OK'.\n"
        f"{test_mode_section}"
        f"{ml_assessment_section}"
        f"{run_command_section}"
        f"{tool_instructions}\n"
        "Review payload:\n" + json.dumps(payload, indent=2)
    )


def _coerce_agent_request(request: ReviewRequest | AgentRequest) -> AgentRequest:
    """Normalize ReviewRequest to AgentRequest, preserving context settings."""
    if isinstance(request, AgentRequest):
        return request

    context = dict(request.context)
    prompt = context.pop("prompt", "")
    cwd = context.pop("cwd", ".")
    output_format = context.pop("output_format", "text")
    model = context.pop("model", None)
    max_turns = context.pop("max_turns", None)
    timeout_seconds = context.pop("timeout_seconds", None)
    tool_policy = _build_tool_policy(context)

    if not prompt:
        prompt = _default_prompt(request.review_type, context, request.stats)

    return AgentRequest(
        mode="batch",
        prompt=prompt,
        context={
            "review_type": request.review_type,
            **context,
            **({"stats": request.stats} if request.stats else {}),
        },
        cwd=cwd,
        model=model,
        max_turns=max_turns,
        output_format=output_format,
        tool_policy=tool_policy,
        timeout_seconds=timeout_seconds,
    )


def _unwrap_cli_response(text: str) -> str:
    """Unwrap Claude CLI JSON response to get actual result text.

    Claude CLI with --print-only-result outputs:
    {"type":"result","subtype":"success","result":"actual response here"}

    This function extracts the "result" field if present.
    """
    try:
        wrapper = json.loads(text)
        if isinstance(wrapper, dict) and wrapper.get("type") == "result":
            result = wrapper.get("result")
            if isinstance(result, str):
                return result
    except json.JSONDecodeError:
        pass
    return text


def _is_cli_error_output(text: str) -> bool:
    """Return True if CLI output indicates a provider/API failure."""
    unwrapped = _unwrap_cli_response(text).strip()
    if not unwrapped:
        return False

    lower = unwrapped.lower()
    if lower.startswith("error:") or lower.startswith("api error"):
        return True

    # Common provider failure patterns (Anthropic/CLI outages, rate limits, timeouts)
    error_markers = (
        "overloaded",
        "rate limit",
        "rate-limit",
        "timeout",
        "timed out",
        "service unavailable",
        "gateway timeout",
        "internal server error",
        "http 429",
        "http 5",
        "status 429",
        "status 5",
        " 429 ",
        " 529 ",
        " 502 ",
        " 503 ",
    )

    return any(marker in lower for marker in error_markers)


def _parse_findings(text: str) -> tuple[str, list[str]]:
    """Parse decision + findings from agent output text.

    The AI may use ERROR: markers to discuss potential issues but then conclude
    they're not actually blocking. We check for explicit approval patterns at
    the end of the review to handle this case.
    """
    # First unwrap CLI JSON response if present
    text = _unwrap_cli_response(text)

    findings: list[str] = []
    decision = "approved"
    has_error_markers = False

    for line in text.splitlines():
        stripped = line.strip()
        # Strip markdown formatting and bullets for detection
        # Handles: **ERROR: ...**, **ERROR:** ..., ERROR: ..., - ERROR: ...
        check_text = stripped.lstrip("-").lstrip().lstrip("*").lstrip()
        upper = check_text.upper()
        if upper.startswith("ERROR"):
            findings.append(stripped)
            has_error_markers = True
            decision = "blocked"
        elif upper.startswith("WARNING") or upper.startswith("WARN"):
            findings.append(stripped)
            if decision != "blocked":
                decision = "warned"
        elif upper.startswith("NOTE"):
            findings.append(stripped)

    # Check for explicit approval patterns that override ERROR markers
    # The AI sometimes uses ERROR: to discuss potential issues but concludes they're fine
    if has_error_markers:
        lower_text = text.lower()
        approval_patterns = [
            "no blocking errors found",
            "no blocking issues found",
            "no blocking errors",
            "no blocking issues",
            "should run successfully",
            "no issues found",
            "looks good",
            "lgtm",
        ]
        for pattern in approval_patterns:
            if pattern in lower_text:
                logger.info("Found approval pattern '%s', overriding blocked decision", pattern)
                decision = "warned" if findings else "approved"
                break

    return decision, findings


def _ensure_binary(binary: str) -> None:
    """Ensure CLI binary exists."""
    if shutil.which(binary) is None:
        raise GoldfishError(
            f"Agent CLI binary not found: {binary}",
            details={"binary": binary},
        )


def _run_command(
    cmd: list[str],
    cwd: str | None,
    timeout_seconds: int | None,
    env: dict[str, str] | None = None,
) -> tuple[int, str, str, int]:
    """Run CLI command and capture output."""
    logger.debug("Running command: %s (timeout=%ss, cwd=%s)", cmd[0], timeout_seconds, cwd)

    start = time.time()
    try:
        proc = subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,  # Prevent stdin hang in non-interactive env
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            # CRITICAL: Start in new session to isolate signal handling from parent.
            # Without this, CLI tools (especially Node.js-based like Claude CLI) may
            # send signals to the process group that kill PID 1 in Docker containers.
            start_new_session=True,
        )
    except FileNotFoundError as e:
        logger.error("Agent CLI not found: %s", cmd[0])
        raise GoldfishError(
            f"Agent CLI not found: {cmd[0]}",
            details={"binary": cmd[0]},
        ) from e
    except subprocess.TimeoutExpired as e:
        logger.error("Agent CLI timed out after %ds", timeout_seconds)
        raise GoldfishError(
            f"Agent CLI timed out after {timeout_seconds}s",
            details={"command": " ".join(cmd)},
        ) from e

    duration_ms = int((time.time() - start) * 1000)
    logger.debug("Command completed: exit_code=%d, duration=%dms", proc.returncode, duration_ms)
    return proc.returncode, proc.stdout, proc.stderr, duration_ms


class CodexCLIProvider:
    """Provider that wraps Codex CLI (headless)."""

    name: str = "codex_cli"

    def __init__(self, binary: str | None = None) -> None:
        self.binary: str = binary or os.environ.get("GOLDFISH_CODEX_CLI_BIN") or "codex"

    def run(self, request: ReviewRequest) -> ReviewResult:
        agent_request = _coerce_agent_request(request)
        binary = self.binary
        if binary is None:
            raise GoldfishError("Codex CLI binary not configured")
        _ensure_binary(binary)

        context = agent_request.context
        sandbox = context.get("sandbox", "workspace-write")
        extra_args = context.get("cli_args", [])
        if not isinstance(extra_args, list):
            extra_args = []

        cmd = [
            binary,
            "exec",
            "--full-auto",
            "--sandbox",
            sandbox,
        ]
        cmd.extend([str(arg) for arg in extra_args])
        cmd.append(agent_request.prompt)

        env = os.environ.copy()
        extra_env = context.get("env")
        if isinstance(extra_env, dict):
            env.update({k: str(v) for k, v in extra_env.items()})

        exit_code, stdout, stderr, duration_ms = _run_command(
            cmd,
            cwd=agent_request.cwd,
            timeout_seconds=agent_request.timeout_seconds,
            env=env,
        )

        raw_output = stdout.strip() if stdout else stderr.strip()

        # Detect CLI failures and log warning (fail-open: still approve, but be loud about it)
        cli_failed = False
        if exit_code != 0:
            cli_failed = True
            logger.warning(
                f"Codex CLI exited with code {exit_code}. "
                f"AI review may not have run correctly. stderr: {stderr[:500] if stderr else 'none'}"
            )
        elif raw_output.lower().startswith("error:"):
            cli_failed = True
            logger.warning(f"Codex CLI returned error: {raw_output[:500]}. " "AI review may not have run correctly.")

        if cli_failed:
            return ReviewResult(
                decision="approved",
                findings=[f"WARNING: AI review failed to run (exit_code={exit_code}). Failing open."],
                response_text=raw_output,
                duration_ms=duration_ms,
            )

        decision, findings = _parse_findings(raw_output)

        return ReviewResult(
            decision=decision,
            findings=findings,
            response_text=raw_output,
            duration_ms=duration_ms,
        )


class GeminiCLIProvider:
    """Provider that wraps Gemini CLI."""

    name: str = "gemini_cli"

    def __init__(self, binary: str | None = None) -> None:
        self.binary: str = binary or os.environ.get("GOLDFISH_GEMINI_CLI_BIN") or "gemini"

    def run(self, request: ReviewRequest) -> ReviewResult:
        agent_request = _coerce_agent_request(request)
        binary = self.binary
        if binary is None:
            raise GoldfishError("Gemini CLI binary not configured")
        _ensure_binary(binary)

        context = agent_request.context
        extra_args = context.get("cli_args", [])
        if not isinstance(extra_args, list):
            extra_args = []

        prompt_arg = context.get("prompt_arg", "--prompt")
        cmd = [binary]
        cmd.extend([str(arg) for arg in extra_args])
        if prompt_arg:
            cmd.extend([prompt_arg, agent_request.prompt])
        else:
            cmd.append(agent_request.prompt)

        env = os.environ.copy()
        extra_env = context.get("env")
        if isinstance(extra_env, dict):
            env.update({k: str(v) for k, v in extra_env.items()})

        exit_code, stdout, stderr, duration_ms = _run_command(
            cmd,
            cwd=agent_request.cwd,
            timeout_seconds=agent_request.timeout_seconds,
            env=env,
        )

        raw_output = stdout.strip() if stdout else stderr.strip()

        # Detect CLI failures and log warning (fail-open: still approve, but be loud about it)
        cli_failed = False
        if exit_code != 0:
            cli_failed = True
            logger.warning(
                f"Gemini CLI exited with code {exit_code}. "
                f"AI review may not have run correctly. stderr: {stderr[:500] if stderr else 'none'}"
            )
        elif raw_output.lower().startswith("error:"):
            cli_failed = True
            logger.warning(f"Gemini CLI returned error: {raw_output[:500]}. " "AI review may not have run correctly.")

        if cli_failed:
            return ReviewResult(
                decision="approved",
                findings=[f"WARNING: AI review failed to run (exit_code={exit_code}). Failing open."],
                response_text=raw_output,
                duration_ms=duration_ms,
            )

        decision, findings = _parse_findings(raw_output)

        return ReviewResult(
            decision=decision,
            findings=findings,
            response_text=raw_output,
            duration_ms=duration_ms,
        )


def _binary_available(binary: str) -> bool:
    """Check if a CLI binary is available on PATH."""
    return shutil.which(binary) is not None


class AnthropicAPIProvider:
    """Provider that uses the Claude Agent SDK for agentic reviews with tool access.

    This provider uses the Claude Agent SDK (claude-agent-sdk package) which wraps
    the Claude Code CLI and provides programmatic access to Claude with tool use.
    This allows SVS reviews to read files from the workspace for better context.

    The SDK runs Claude in a controlled manner with specific tool permissions,
    avoiding conflicts with interactive Claude Code sessions.

    Requires:
        - claude-agent-sdk package installed: pip install claude-agent-sdk
        - ANTHROPIC_API_KEY environment variable set
    """

    name: str = "anthropic_api"

    def __init__(self, model: str | None = None) -> None:
        """Initialize the provider.

        Args:
            model: Model to use. Defaults to claude-sonnet-4-20250514.
        """
        self.model = model or os.environ.get("GOLDFISH_ANTHROPIC_MODEL") or "claude-sonnet-4-20250514"

    def _make_skip_response(self, reason: str) -> ReviewResult:
        """Create a skip response with proper JSON for during-run compatibility."""
        # Return valid JSON so during-run monitor can parse it
        json_response = {
            "findings": [{"check": "anthropic_api_skip", "severity": "NOTE", "summary": reason}],
            "request_stop": False,
            "stop_reason": None,
        }
        response_text = f"```json\n{json.dumps(json_response, indent=2)}\n```"
        return ReviewResult(
            decision="approved",
            findings=[f"WARNING: {reason}"],
            response_text=response_text,
            duration_ms=0,
        )

    def run(self, request: ReviewRequest) -> ReviewResult:
        """Execute review using Claude Agent SDK with tool access."""
        try:
            import anyio
            from claude_agent_sdk import AssistantMessage, ClaudeAgentOptions, TextBlock, query
        except ImportError:
            logger.error("claude-agent-sdk package not installed. Install with: pip install claude-agent-sdk")
            return self._make_skip_response("claude-agent-sdk package not installed. Review skipped.")

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            logger.error("ANTHROPIC_API_KEY not set")
            return self._make_skip_response("ANTHROPIC_API_KEY not set. Review skipped.")

        agent_request = _coerce_agent_request(request)
        start = time.time()

        try:
            # Use model from request context if provided, else default
            model = agent_request.model or self.model

            # Configure agent options with read-only file access
            # This allows the agent to read files in the workspace for context
            options = ClaudeAgentOptions(
                model=model,
                cwd=agent_request.cwd,
                # Allow read-only tools for file inspection
                allowed_tools=["Read", "Glob", "Grep"],
                # Don't allow edits - this is a review, not an editor
                permission_mode="bypassPermissions",
                # Limit turns to prevent runaway agent
                max_turns=agent_request.max_turns or 5,
            )

            # Run the agent query asynchronously
            async def run_agent() -> str:
                output_parts: list[str] = []
                async for message in query(prompt=agent_request.prompt, options=options):
                    # Only process AssistantMessage which has content blocks
                    if isinstance(message, AssistantMessage):
                        for block in message.content:
                            if isinstance(block, TextBlock):
                                output_parts.append(block.text)
                return "\n".join(output_parts)

            # Execute async function - anyio.run() creates a new event loop
            raw_output = anyio.run(run_agent)

            duration_ms = int((time.time() - start) * 1000)
            logger.info("Claude Agent SDK response: %d chars in %dms", len(raw_output), duration_ms)

            decision, findings = _parse_findings(raw_output)

            return ReviewResult(
                decision=decision,
                findings=findings,
                response_text=raw_output,
                duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            logger.exception("Claude Agent SDK call failed: %s", e)
            # Fail open - don't block on API errors, but return valid JSON for during-run
            json_response = {
                "findings": [
                    {"check": "anthropic_api_error", "severity": "WARNING", "summary": f"API call failed: {e}"}
                ],
                "request_stop": False,
                "stop_reason": None,
            }
            response_text = f"```json\n{json.dumps(json_response, indent=2)}\n```"
            return ReviewResult(
                decision="approved",
                findings=[f"WARNING: Claude Agent SDK call failed: {e}. Failing open."],
                response_text=response_text,
                duration_ms=duration_ms,
            )


def get_agent_provider(provider_name: str) -> AgentProvider:
    """Return an AgentProvider instance for the given name.

    The provider_name can be overridden by setting the GOLDFISH_SVS_AGENT_PROVIDER
    environment variable. This is useful for testing or when the config is cached.
    """
    # Check for environment variable override
    env_override = os.environ.get("GOLDFISH_SVS_AGENT_PROVIDER")
    if env_override:
        logger.debug("SVS agent provider override from env: %s", env_override)
        provider_name = env_override

    logger.debug("get_agent_provider() called with provider_name=%s", provider_name)

    if provider_name == "codex_cli":
        codex_provider = CodexCLIProvider()
        if not _binary_available(codex_provider.binary):
            logger.warning("Codex CLI not found; falling back to NullProvider")
            return NullProvider()
        return codex_provider
    if provider_name == "gemini_cli":
        gemini_provider = GeminiCLIProvider()
        if not _binary_available(gemini_provider.binary):
            logger.warning("Gemini CLI not found; falling back to NullProvider")
            return NullProvider()
        return gemini_provider
    if provider_name == "anthropic_api":
        # Check if claude-agent-sdk package is available
        try:
            from importlib.util import find_spec

            if find_spec("claude_agent_sdk") is None:
                raise ImportError("claude-agent-sdk not found")

            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if not api_key:
                logger.warning("ANTHROPIC_API_KEY not set; falling back to NullProvider")
                return NullProvider()
            logger.debug("Using AnthropicAPIProvider (Claude Agent SDK)")
            return AnthropicAPIProvider()
        except ImportError:
            logger.warning("claude-agent-sdk package not installed; falling back to NullProvider")
            return NullProvider()
    if provider_name == "null":
        logger.debug("Returning NullProvider (explicit null)")
        return NullProvider()
    logger.warning("Unknown provider %s, returning NullProvider", provider_name)
    return NullProvider()
