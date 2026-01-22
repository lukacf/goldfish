"""SVS Configuration - Semantic Validation System settings."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Domain profile presets
DOMAIN_PROFILES = {
    "nlp_tokenizer": {
        "check_policies": {
            "entropy": "fail",
            "vocab_utilization": "warn",
            "null_ratio": "fail",
        },
        "thresholds": {
            "entropy": {"min": 6.0},
            "vocab_utilization": {"min": 0.7},
            "null_ratio": {"max": 0.01},
        },
    },
    "image_embeddings": {
        "check_policies": {
            "entropy": "warn",
            "null_ratio": "fail",
        },
        "thresholds": {
            "entropy": {"min": 8.0},
            "null_ratio": {"max": 0.001},
        },
    },
    "tabular_features": {
        "check_policies": {
            "null_ratio": "fail",
            "top1_fraction": "warn",
        },
        "thresholds": {
            "null_ratio": {"max": 0.05},
            "top1_fraction": {"max": 0.5},
        },
    },
    "default": {
        "check_policies": {},
        "thresholds": {},
    },
}


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

    # Test mode - enables shorter intervals and verbose AI feedback
    test_mode: bool = False

    # Domain and policy settings
    domain: Literal["default", "nlp_tokenizer", "image_embeddings", "tabular_features"] = "default"
    default_policy: Literal["fail", "warn", "ignore"] = "warn"
    default_enforcement: Literal["blocking", "warning", "silent"] = "warning"

    # Post-run stats (mechanistic, container-side)
    stats_enabled: bool = True

    # AI reviews
    ai_pre_run_enabled: bool = True
    ai_post_run_enabled: bool = True
    ai_during_run_enabled: bool = True
    ai_during_run_interval_seconds: int = Field(default=300, ge=10)  # min 10s in test_mode, validated below
    ai_during_run_min_metrics: int = Field(default=1, ge=1)  # Trigger as soon as any metrics exist
    ai_during_run_min_log_lines: int = Field(default=1, ge=0)  # Trigger as soon as any logs exist
    ai_during_run_max_runs_per_hour: int = Field(default=12, ge=1)
    ai_during_run_auto_stop: bool = False
    ai_during_run_log_filters: list[str] = Field(
        default_factory=lambda: [r".*"]  # Match all lines by default - let AI decide what's relevant
    )
    ai_during_run_log_max_lines: int = Field(default=200, ge=10)
    ai_during_run_log_max_bytes: int = Field(default=16384, ge=1024)
    ai_during_run_log_file_max_bytes: int = Field(default=10_000_000, ge=100_000)
    ai_during_run_summary_max_chars: int = Field(default=1200, ge=100)

    # Agent settings
    agent_provider: Literal["claude_code", "codex_cli", "gemini_cli", "null"] = "claude_code"
    agent_model: str | None = None
    agent_fallback_model: str | None = None
    agent_timeout: int = Field(default=120, ge=0)
    agent_max_turns: int = Field(default=30, ge=1)
    rate_limit_per_hour: int = Field(default=60, ge=0)

    # Self-learning (opt-in: uses AI to extract failure patterns from failed runs)
    auto_learn_failures: bool = False

    @field_validator("domain")
    @classmethod
    def validate_domain(cls, v: str) -> str:
        """Validate domain is non-empty."""
        if not v or not v.strip():
            raise ValueError("domain cannot be empty")
        return v

    @field_validator("agent_provider", mode="before")
    @classmethod
    def coerce_null_agent_provider(cls, v: str | None) -> str:
        """Use default provider when YAML null is specified.

        In YAML, `agent_provider: null` is parsed as Python None.
        We treat this as "use default" (claude_code), NOT as NullProvider.
        To explicitly use NullProvider, write `agent_provider: "null"` (quoted).
        """
        if v is None:
            return "claude_code"
        return v

    @model_validator(mode="after")
    def validate_interval_for_test_mode(self) -> "SVSConfig":
        """Enforce minimum interval of 60s unless test_mode is enabled."""
        if not self.test_mode and self.ai_during_run_interval_seconds < 60:
            raise ValueError(
                f"ai_during_run_interval_seconds must be >= 60 (got {self.ai_during_run_interval_seconds}). "
                "Set test_mode=True to allow shorter intervals."
            )
        return self
