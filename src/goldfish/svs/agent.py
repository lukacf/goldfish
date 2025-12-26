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
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

from goldfish.errors import GoldfishError

# =============================================================================
# Full Agent Abstraction (per SVS spec 2.0)
# =============================================================================


@dataclass
class ToolPolicy:
    """Controls agent tool permissions.

    Attributes:
        permission_mode: How tools are permitted - "plan", "ask", or "auto"
        allow_tools: Explicit list of allowed tool names (None = all allowed)
        deny_tools: Explicit list of denied tool names (None = none denied)
        mcp_servers: List of MCP server names to enable
    """

    permission_mode: Literal["plan", "ask", "auto"] = "auto"
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
        # Include findings in response_text so parsers can extract them
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
            permission_mode=tool_policy.get("permission_mode", "auto"),
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

    payload = {
        "review_type": review_type,
        "context": {k: v for k, v in context.items() if k != "tool_policy"},
        "stats": stats or {},
    }
    return (
        "You are an AI reviewer. Return findings as lines prefixed with "
        "ERROR:, WARNING:, or NOTE:. If nothing is wrong, say 'OK'.\n"
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


def _parse_findings(text: str) -> tuple[str, list[str]]:
    """Parse decision + findings from agent output text."""
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
    start = time.time()
    try:
        proc = subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except FileNotFoundError as e:
        raise GoldfishError(
            f"Agent CLI not found: {cmd[0]}",
            details={"binary": cmd[0]},
        ) from e
    except subprocess.TimeoutExpired as e:
        raise GoldfishError(
            f"Agent CLI timed out after {timeout_seconds}s",
            details={"command": " ".join(cmd)},
        ) from e

    duration_ms = int((time.time() - start) * 1000)
    return proc.returncode, proc.stdout, proc.stderr, duration_ms


class ClaudeCodeProvider:
    """Provider that wraps Claude Code CLI."""

    name: str = "claude_code"

    def __init__(self, binary: str | None = None) -> None:
        self.binary = binary or os.environ.get("GOLDFISH_CLAUDE_CLI_BIN", "claude")

    def run(self, request: ReviewRequest) -> ReviewResult:
        agent_request = _coerce_agent_request(request)
        binary = self.binary
        if binary is None:
            raise GoldfishError("Claude CLI binary not configured")
        _ensure_binary(binary)

        cmd = [binary, "-p", agent_request.prompt]

        if agent_request.output_format == "json":
            cmd += ["--output-format", "json"]
        if agent_request.model:
            cmd += ["--model", agent_request.model]
        if agent_request.max_turns:
            cmd += ["--max-turns", str(agent_request.max_turns)]
        if agent_request.tool_policy is not None:
            cmd += ["--permission-mode", agent_request.tool_policy.permission_mode]
            if agent_request.tool_policy.allow_tools:
                for tool in agent_request.tool_policy.allow_tools:
                    cmd += ["--allow-tool", tool]
            if agent_request.tool_policy.deny_tools:
                for tool in agent_request.tool_policy.deny_tools:
                    cmd += ["--deny-tool", tool]
            if agent_request.tool_policy.mcp_servers:
                for server in agent_request.tool_policy.mcp_servers:
                    cmd += ["--mcp-server", server]

        env = os.environ.copy()
        extra_env = agent_request.context.get("env")
        if isinstance(extra_env, dict):
            env.update({k: str(v) for k, v in extra_env.items()})

        exit_code, stdout, stderr, duration_ms = _run_command(
            cmd,
            cwd=agent_request.cwd,
            timeout_seconds=agent_request.timeout_seconds,
            env=env,
        )

        raw_output = stdout.strip() if stdout else stderr.strip()
        decision, findings = _parse_findings(raw_output)

        return ReviewResult(
            decision=decision,
            findings=findings,
            response_text=raw_output,
            duration_ms=duration_ms,
        )


class CodexCLIProvider:
    """Provider that wraps Codex CLI (headless)."""

    name: str = "codex_cli"

    def __init__(self, binary: str | None = None) -> None:
        self.binary = binary or os.environ.get("GOLDFISH_CODEX_CLI_BIN", "codex")

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
        self.binary = binary or os.environ.get("GOLDFISH_GEMINI_CLI_BIN", "gemini")

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
        decision, findings = _parse_findings(raw_output)

        return ReviewResult(
            decision=decision,
            findings=findings,
            response_text=raw_output,
            duration_ms=duration_ms,
        )


def get_agent_provider(provider_name: str) -> AgentProvider:
    """Return an AgentProvider instance for the given name."""
    if provider_name == "claude_code":
        return ClaudeCodeProvider()
    if provider_name == "codex_cli":
        return CodexCLIProvider()
    if provider_name == "gemini_cli":
        return GeminiCLIProvider()
    if provider_name == "null":
        return NullProvider()
    return NullProvider()
