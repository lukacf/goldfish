"""SVS Configuration - Semantic Validation System settings."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

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
    ai_during_run_interval_seconds: int = Field(default=300, ge=60)
    ai_during_run_min_metrics: int = Field(default=200, ge=10)
    ai_during_run_min_log_lines: int = Field(default=20, ge=0)
    ai_during_run_max_runs_per_hour: int = Field(default=12, ge=1)
    ai_during_run_auto_stop: bool = False
    ai_during_run_log_filters: list[str] = Field(
        default_factory=lambda: [
            r"(ERROR|WARN|EXCEPTION|Traceback)",
            r"(CUDA|OOM|nan|inf|segfault|Killed|RuntimeError)",
            r"(loss=|ppl=|acc=|val_|train_|dir_)",
        ]
    )
    ai_during_run_log_max_lines: int = Field(default=200, ge=10)
    ai_during_run_log_max_bytes: int = Field(default=16384, ge=1024)
    ai_during_run_log_file_max_bytes: int = Field(default=10_000_000, ge=100_000)
    ai_during_run_summary_max_chars: int = Field(default=1200, ge=100)

    # Agent settings
    agent_provider: Literal["claude_code", "codex_cli", "gemini_cli", "null"] = "claude_code"
    agent_model: str | None = None
    agent_timeout: int = Field(default=120, ge=0)
    agent_max_turns: int = Field(default=3, ge=1)
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
