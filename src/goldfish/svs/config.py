"""SVS Configuration - Semantic Validation System settings."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SVSConfig(BaseModel):
    """Configuration for Semantic Validation System.

    Controls all aspects of SVS behavior including:
    - Stats collection (mechanistic, container-side)
    - AI reviews (pre-run, post-run)
    - Agent settings (provider, timeouts, rate limits)
    - Self-learning failure patterns
    """

    model_config = ConfigDict(extra="forbid")

    # Master switch
    enabled: bool = True

    # Domain and policy settings
    domain: str = Field(default="default", min_length=1)
    default_policy: Literal["fail", "warn", "ignore"] = "warn"
    default_enforcement: Literal["blocking", "warning", "silent"] = "warning"

    # Post-run stats (mechanistic, container-side)
    stats_enabled: bool = True

    # AI reviews
    ai_pre_run_enabled: bool = True
    ai_post_run_enabled: bool = True

    # Agent settings
    agent_provider: Literal["claude_code", "codex_cli", "gemini_cli", "null"] = "claude_code"
    agent_model: str | None = None
    agent_timeout: int = Field(default=30, ge=0)
    agent_max_turns: int = Field(default=3, ge=1)
    rate_limit_per_hour: int = Field(default=60, ge=0)

    # Self-learning
    auto_learn_failures: bool = True

    @field_validator("domain")
    @classmethod
    def validate_domain(cls, v: str) -> str:
        """Validate domain is non-empty."""
        if not v or not v.strip():
            raise ValueError("domain cannot be empty")
        return v
