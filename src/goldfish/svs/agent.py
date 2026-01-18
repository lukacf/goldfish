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
    """Parse decision + findings from agent output text."""
    # First unwrap CLI JSON response if present
    text = _unwrap_cli_response(text)

    findings: list[str] = []
    decision = "approved"

    for line in text.splitlines():
        stripped = line.strip()
        upper = stripped.upper()
        if upper.startswith("ERROR"):
            findings.append(stripped)
            decision = "blocked"
        elif upper.startswith("WARNING") or upper.startswith("WARN"):
            findings.append(stripped)
            if decision != "blocked":
                decision = "warned"
        elif upper.startswith("NOTE"):
            findings.append(stripped)

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


class ClaudeCodeProvider:
    """Provider that wraps Claude Code CLI."""

    name: str = "claude_code"

    def __init__(self, binary: str | None = None) -> None:
        self.binary: str = binary or os.environ.get("GOLDFISH_CLAUDE_CLI_BIN") or "claude"

    def run(self, request: ReviewRequest) -> ReviewResult:
        agent_request = _coerce_agent_request(request)
        binary = self.binary
        if binary is None:
            raise GoldfishError("Claude CLI binary not configured")
        _ensure_binary(binary)

        def _build_cmd(model_override: str | None = None) -> list[str]:
            cmd = [binary, "-p", agent_request.prompt]

            if agent_request.output_format == "json":
                cmd += ["--output-format", "json"]

            model_to_use = model_override if model_override is not None else agent_request.model
            if model_to_use:
                cmd += ["--model", model_to_use]
            if agent_request.max_turns:
                cmd += ["--max-turns", str(agent_request.max_turns)]
            if agent_request.tool_policy is not None:
                # Use --dangerously-skip-permissions for automated reviews in Docker
                cmd += ["--dangerously-skip-permissions"]
                if agent_request.tool_policy.allow_tools:
                    for tool in agent_request.tool_policy.allow_tools:
                        cmd += ["--allowed-tools", tool]
                if agent_request.tool_policy.deny_tools:
                    for tool in agent_request.tool_policy.deny_tools:
                        cmd += ["--disallowed-tools", tool]
                if agent_request.tool_policy.mcp_servers:
                    for server in agent_request.tool_policy.mcp_servers:
                        cmd += ["--mcp-server", server]

            return cmd

        fallback_model = agent_request.context.get("fallback_model")

        env = os.environ.copy()
        # Enable IS_SANDBOX=1 when using --dangerously-skip-permissions with tool_policy.
        # This allows Claude CLI to run as root in Docker containers.
        # See: https://github.com/anthropics/claude-code/issues/3490
        if agent_request.tool_policy is not None:
            env["IS_SANDBOX"] = "1"
            # Set HOME to /tmp only if the current HOME is not writable.
            # In Docker containers, the default HOME (/app) is often read-only,
            # causing Claude CLI to hang while trying to create ~/.config/claude/.
            # On local machines, HOME is writable, so we leave it unchanged.
            current_home = os.environ.get("HOME", "/tmp")
            if not os.access(current_home, os.W_OK):
                env["HOME"] = "/tmp"
        extra_env = agent_request.context.get("env")
        if isinstance(extra_env, dict):
            env.update({k: str(v) for k, v in extra_env.items()})

        cmd = _build_cmd()
        exit_code, stdout, stderr, duration_ms = _run_command(
            cmd,
            cwd=agent_request.cwd,
            timeout_seconds=agent_request.timeout_seconds,
            env=env,
        )

        stdout_text = stdout.strip() if stdout else ""
        stderr_text = stderr.strip() if stderr else ""
        raw_output = stdout_text or stderr_text

        logger.debug(
            "Claude CLI response: stdout_len=%d, stderr_len=%d",
            len(stdout) if stdout else 0,
            len(stderr) if stderr else 0,
        )
        if not stdout_text and stderr:
            logger.debug("Claude CLI stderr: %s", stderr[:500])

        total_duration_ms = duration_ms

        # Detect CLI failures (fail-open: still approve, caller handles error tracking)
        # Empty output is also considered a failure (worth retrying with fallback model)
        cli_failed = False
        if exit_code != 0:
            cli_failed = True
            logger.warning(
                "Claude CLI exited with code %d. stderr: %s",
                exit_code,
                stderr[:500] if stderr else "none",
            )
        elif not raw_output:
            # Empty output - likely internal timeout or API issue
            cli_failed = True
            logger.warning(
                "Claude CLI returned empty output (exit_code=%d, duration=%dms)",
                exit_code,
                duration_ms,
            )
        elif _is_cli_error_output(raw_output):
            cli_failed = True
            logger.warning("Claude CLI returned error: %s", raw_output[:500])

        # Retry once with fallback model if configured and different from primary
        if cli_failed and fallback_model and str(fallback_model) != str(agent_request.model):
            logger.info("Retrying Claude CLI with fallback model: %s", fallback_model)
            retry_cmd = _build_cmd(model_override=str(fallback_model))
            exit_code, stdout, stderr, duration_ms = _run_command(
                retry_cmd,
                cwd=agent_request.cwd,
                timeout_seconds=agent_request.timeout_seconds,
                env=env,
            )
            total_duration_ms += duration_ms
            stdout_text = stdout.strip() if stdout else ""
            stderr_text = stderr.strip() if stderr else ""
            raw_output = stdout_text or stderr_text

            logger.debug(
                "Fallback result: exit_code=%d, stdout_len=%d, stderr_len=%d",
                exit_code,
                len(stdout) if stdout else 0,
                len(stderr) if stderr else 0,
            )

            cli_failed = False
            if exit_code != 0:
                cli_failed = True
                logger.warning(
                    "Claude CLI fallback exited with code %d. stderr: %s",
                    exit_code,
                    stderr[:500] if stderr else "none",
                )
            elif not raw_output:
                cli_failed = True
                logger.warning("Fallback also returned empty output. Both models failed.")
            elif _is_cli_error_output(raw_output):
                cli_failed = True
                logger.warning("Claude CLI fallback returned error: %s", raw_output[:500])

        if cli_failed:
            # Return approved with a clear finding that the review itself failed
            return ReviewResult(
                decision="approved",
                findings=[f"WARNING: AI review failed to run (exit_code={exit_code}). Failing open."],
                response_text=raw_output,
                duration_ms=total_duration_ms,
            )

        decision, findings = _parse_findings(raw_output)

        return ReviewResult(
            decision=decision,
            findings=findings,
            response_text=raw_output,
            duration_ms=total_duration_ms,
        )


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

    if provider_name == "claude_code":
        claude_provider = ClaudeCodeProvider()
        binary_path = shutil.which(claude_provider.binary)
        logger.debug("Claude CLI binary=%s, which result=%s", claude_provider.binary, binary_path)
        if not _binary_available(claude_provider.binary):
            logger.warning("Claude CLI not found; falling back to NullProvider")
            return NullProvider()
        return claude_provider
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
    if provider_name == "null":
        logger.debug("Returning NullProvider (explicit null)")
        return NullProvider()
    logger.warning("Unknown provider %s, returning NullProvider", provider_name)
    return NullProvider()
